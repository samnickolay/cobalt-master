#!/usr/bin/env python

'''Super-Simple Scheduler for BG/L'''
__revision__ = '$Revision: 2156 $'

import logging
import sys
import time
import ConfigParser
import xmlrpclib
import math

import Cobalt.Logging, Cobalt.Util
from Cobalt.Data import Data, DataDict, ForeignData, ForeignDataDict, IncrID
from Cobalt.Components.base import Component, exposed, automatic, query, locking
from Cobalt.Components.cqm import QueueDict, Queue # dwang 
from Cobalt.Proxy import ComponentProxy
from Cobalt.Exceptions import ReservationError, DataCreationError, ComponentLookupError

import Cobalt.SchedulerPolicies
import traceback


logger = logging.getLogger("Cobalt.Components.scheduler")
config = ConfigParser.ConfigParser()
config.read(Cobalt.CONFIG_FILES)
if not config.has_section('bgsched'):
    logger.critical('''"bgsched" section missing from cobalt config file''')
    sys.exit(1)

SLOP_TIME = 180
DEFAULT_RESERVATION_POLICY = "default"

bgsched_id_gen = None
bgsched_cycle_id_gen = None

##
restart_jobs_global = [] 
##
resv_jobs = []
# 

def get_bgsched_config(option, default):
    try:
        value = config.get('bgsched', option)
    except ConfigParser.NoOptionError:
        value = default
    return value

def get_histm_config(option, default):
    try:
        value = config.get('histm', option)
    except ConfigParser.NoSectionError:
        value = default
    return value
running_job_walltime_prediction = get_histm_config("running_job_walltime_prediction", "False").lower()     #*AdjEst*
if running_job_walltime_prediction  == "true":
    running_job_walltime_prediction = True
else:
    running_job_walltime_prediction = False

#db writer initialization
dbwriter = Cobalt.Logging.dbwriter(logger)
use_db_logging = get_bgsched_config('use_db_logging', 'false')
if use_db_logging.lower() in ['true', '1', 'yes', 'on']:
    dbwriter.enabled = True
    overflow_filename = get_bgsched_config('overflow_file', None)
    max_queued = int(get_bgsched_config('max_queued_msgs', '-1'))
    if max_queued <= 0:
        max_queued = None
    if (overflow_filename == None) and (max_queued != None):
        logger.warning('No filename set for database logging messages, max_queued_msgs set to unlimited')
    if max_queued != None:
        dbwriter.overflow_filename = overflow_filename
        dbwriter.max_queued = max_queued



class Reservation (Data):
#pylint: disable=R0902
    """Cobalt scheduler reservation."""

    fields = Data.fields + [
        "tag", "name", "start", "duration", "cycle", "users", "partitions",
        "active", "queue", "res_id", "cycle_id", 'project', "block_passthrough"
    ]

    required = ["name", "start", "duration"]

    global bgsched_id_gen
    global bgsched_cycle_id_gen

    def __init__ (self, spec):
        Data.__init__(self, spec)
        self.tag = spec.get("tag", "reservation")
        self.cycle = spec.get("cycle")
        self.users = spec.get("users", "")
        self.createdQueue = False
        self.partitions = spec.get("partitions", "")
        self.name = spec['name']
        self.start = spec['start']
        self.queue = spec.get("queue", "R.%s" % self.name)
        self.duration = spec.get("duration")
        self.res_id = spec.get("res_id")
        self.cycle_id_gen = bgsched_cycle_id_gen
        if self.cycle:
            self.cycle_id = spec.get("cycle_id", self.cycle_id_gen.get())
        else:
            self.cycle_id = None

        self.running = False
        self.project = spec.get("project", None)
        self.block_passthrough = spec.get("block_passthrough", False)
        #self.deleting = False

    def _get_active(self):
        return self.is_active()

    active = property(_get_active)

    def update (self, spec):
        if spec.has_key("users"):
            qm = ComponentProxy("queue-manager")
            try:
                qm.set_queues([{'name':self.queue,}], {'users':spec['users']}, "bgsched")
            except ComponentLookupError:
                logger.error("unable to contact queue manager when updating reservation users")
                raise
        # try the above first -- if we can't contact the queue-manager,
        # don't update the users
        if spec.has_key('cycle') and not self.cycle:
            #just turned this into a cyclic reservation and need a cycle_id
            spec['cycle_id'] = self.cycle_id_gen.get()
        #get the user name of whoever issued the command
        user_name = None
        if spec.has_key('__cmd_user'):
            user_name = spec['__cmd_user']
            del spec['__cmd_user']

        #if we're defering, pull out the 'defer' entry and send a cobalt db
        #message.  There really isn't a corresponding field to update
        deferred = False
        if spec.has_key('defer'):
            logger.info("Res %s/%s: Deferring cyclic reservation: %s",
                        self.res_id, self.cycle_id, self.name) 
            dbwriter.log_to_db(user_name, "deferred", "reservation", self)
            del spec['defer']
            deferred = True

        Data.update(self, spec)

        if not deferred or not self.running:
            #we only want this if we aren't defering.  If we are, the cycle will
            #take care of the new data object creation.
            dbwriter.log_to_db(user_name, "modifying", "reservation", self)


    def overlaps(self, start, duration):
        '''check job overlap with reservations'''
        if start + duration < self.start:
            return False

        if self.cycle and duration >= self.cycle:
            return True

        my_stop = self.start + self.duration
        if self.start <= start < my_stop:
            # Job starts within reservation 
            return True
        elif self.start <= (start + duration) < my_stop:
            # Job ends within reservation 
            return True
        elif start < self.start and (start + duration) >= my_stop:
            # Job starts before and ends after reservation
            return True
        if not self.cycle:
            return False

        # 3 cases, front, back and complete coverage of a cycle
        cstart = (start - self.start) % self.cycle
        cend = (start + duration - self.start) % self.cycle
        if cstart < self.duration:
            return True
        if cend < self.duration:
            return True
        if cstart > cend:
            return True

        return False

    def job_within_reservation(self, job):
        '''Return true if the job fits within this reservation, otherwise,
        return false.

        '''
        if not self.is_active():
            return False

        if job.queue == self.queue:

            res_end  = self.start + self.duration
            cur_time = time.time()

            # if the job is non zero then just use the walltime as is else give the max reservation time possible
            _walltime = float(job.walltime)
            
            _walltime = _walltime if _walltime > 0 else (res_end - cur_time - 300)/60
            logger.info('Walltime: %s' % str(_walltime))

            job_end = cur_time + 60 * _walltime + SLOP_TIME
            if not self.cycle:
                if job_end < res_end:
                    job.walltime = _walltime
                    return True
                else:
                    return False
            else:
                if 60 * _walltime + SLOP_TIME > self.duration:
                    return False

                relative_start = (cur_time - self.start) % self.cycle
                relative_end = relative_start + 60 * _walltime + SLOP_TIME
                if relative_end < self.duration:
                    job.walltime = _walltime
                    return True
                else:
                    return False
        else:
            return False


    def is_active(self, stime=False):
        '''Determine if the reservation is active.  A reservation is active
        if we are between it's start time and its start time + duration.

        '''

        if not stime:
            stime = time.time()

        if stime < self.start:
            if self.running:
                self.running = False
                if self.cycle:
                    #handle a deferral of a cyclic reservation while active, shold not increment normally
                    #Time's already tweaked at this point.
                    logger.info("Res %s/%s: Active reservation %s deactivating: Deferred and cycling.",
                        self.res_id, self.cycle_id, self.name)
                    dbwriter.log_to_db(None, "deactivating", "reservation", self)
                    dbwriter.log_to_db(None, "instance_end","reservation", self)
                    self.res_id = bgsched_id_gen.get()
                    logger.info("Res %s/%s: Cycling reservation: %s", 
                             self.res_id, self.cycle_id, self.name) 
                    dbwriter.log_to_db(None, "cycling", "reservation", self)
                else:
                    logger.info("Res %s/%s: Active reservation %s deactivating: start time in future.",
                        self.res_id, self.cycle_id, self.name)
                    dbwriter.log_to_db(None, "deactivating", "reservation", self)
            return False

        if self.cycle:
            now = (stime - self.start) % self.cycle
        else:
            now = stime - self.start

        if now <= self.duration:
            if not self.running:
                self.running = True
                logger.info("Res %s/%s: Activating reservation: %s",
                        self.res_id, self.cycle_id, self.name) 
                dbwriter.log_to_db(None, "activating", "reservation", self)
            return True
        else:
            return False

    def is_over(self):
        '''Determine if a reservation is over and initiate cleanup.

        '''

        stime = time.time()
        # reservations with a cycle time are never "over"
        if self.cycle:
            #but it does need a new res_id, cycle_id remains constant.
            if((((stime - self.start) % self.cycle) > self.duration) 
               and self.running):
                #do this before incrementing id.
                logger.info("Res %s/%s: Deactivating reservation: %s: Reservation Cycling",
                    self.res_id, self.cycle_id, self.name) 
                dbwriter.log_to_db(None, "deactivating", "reservation", self)
                dbwriter.log_to_db(None, "instance_end", "reservation", self)
                self.set_start_to_next_cycle()
                self.running = False
                self.res_id = bgsched_id_gen.get()
                logger.info("Res %s/%s: Cycling reservation: %s", 
                             self.res_id, self.cycle_id, self.name) 
                dbwriter.log_to_db(None, "cycling", "reservation", self)
            return False

        if (self.start + self.duration) <= stime:
            if self.running == True:
                #The active reservation is no longer considered active
                #do this only once to prevent a potential double-deactivate 
                #depending on how/when this check is called.
                logger.info("Res %s/%s: Deactivating reservation: %s", 
                             self.res_id, self.cycle_id, self.name) 
                dbwriter.log_to_db(None, "deactivating", "reservation", self)
            self.running = False
            return True
        else:
            return False

    def set_start_to_next_cycle(self):
        '''Set the time of the reservation, when it would next go active,
        to the next start time based on it's cycle time.  The new time is the
        current reservation start time + the cycle time interval.

        '''


        if self.cycle:

            new_start = self.start
            now = time.time()
            periods = int(math.floor((now - self.start) / float(self.cycle)))

            #so here, we should always be coming out of a reservation.  The 
            #only time we wouldn't be is if for some reason the scheduler was 
            #disrupted.
            if now < self.start:
                new_start += self.cycle
            else: 
                #this is not going to start during a reservation, so we only
                #have to go periods + 1.
                new_start += (periods + 1) * self.cycle

            self.start = new_start


class ReservationDict (DataDict):
    '''DataDict-style dictionary for reservation data.
    Ensueres that there is a queue in the queue manager associated with a new
    reservation, by either associating with an extant queue, or creating a new
    one for this reservation's use.

    Will also kill a created queue on reservation deactivation and cleanup.

    '''
    item_cls = Reservation
    key = "name"

    global bgsched_id_gen

    def q_add (self, *args, **kwargs):
        '''Add a reservation to tracking.
        Side Efffects:
            -Add a queue to be tracked
            -If no cqm associated queue, create a reservation queue
            -set policies for new queue
            -emit numerous creation messages

        '''

        qm = ComponentProxy("queue-manager")
        try:
            queues = [spec['name'] for spec in qm.get_queues([{'name':"*"}])]
        except ComponentLookupError:
            logger.error("unable to contact queue manager when adding reservation")
            raise

        try:
            specs = args[0]
            for spec in specs:
                if "res_id" not in spec or spec['res_id'] == '*':
                    spec['res_id'] = bgsched_id_gen.get()
            reservations = Cobalt.Data.DataDict.q_add(self, *args, **kwargs)

        except KeyError, err:
            raise ReservationError("Error: a reservation named %s already exists" % err)

        for reservation in reservations:
            if reservation.queue not in queues:
                try:
                    qm.add_queues([{'tag': "queue", 'name':reservation.queue,
                        'policy':DEFAULT_RESERVATION_POLICY}], "bgsched")
                except Exception, err:
                    logger.error("unable to add reservation queue %s (%s)",
                                 reservation.queue, err)
                else:
                    reservation.createdQueue = True
                    logger.info("added reservation queue %s", reservation.queue)
            try:
                # we can't set the users list using add_queues, so we want to
                # call set_queues even if bgsched just created the queue
                qm.set_queues([{'name':reservation.queue}],
                              {'state':"running", 'users':reservation.users}, "bgsched")
            except Exception, err:
                logger.error("unable to update reservation queue %s (%s)",
                             reservation.queue, err)
            else:
                logger.info("updated reservation queue %s", reservation.queue)

        return reservations

    def q_del (self, *args, **kwargs):
        '''Delete a reservation from tracking.
        Side Effects: Removes a queue from tracking.
                      Logs that the reservation has terminated.
                      Emits a terminated database record
                      Attempts to mark the queue dead in the queue-manager.
                      Marks the reservation as dying

        '''
        reservations = Cobalt.Data.DataDict.q_del(self, *args, **kwargs)
        qm = ComponentProxy('queue-manager')
        queues = [spec['name'] for spec in qm.get_queues([{'name':"*"}])]
        spec = [{'name': reservation.queue} for reservation in reservations \
                if reservation.createdQueue and reservation.queue in queues \
                and not self.q_get([{'queue':reservation.queue}])]
        try:
            qm.set_queues(spec, {'state':"dead"}, "bgsched")
        except Exception, err:
            logger.error("problem disabling reservation queue (%s)" % err)

        for reservation in reservations:
            #reservation.deleting = True #Do not let the is_active check succeed.
            #This should be the last place we have handles to reservations,
            #after this they're heading to GC.
            if reservation.is_active():
                #if we are active, then drop a deactivating message.
                dbwriter.log_to_db(None, "deactivating", "reservation",
                        reservation)
                if reservation.cycle:
                    dbwriter.log_to_db(None, "instance_end","reservation", self)
            dbwriter.log_to_db(None, "terminated", "reservation", reservation)
        return reservations


class Job (ForeignData):
    """A field for the job metadata cache from cqm.  Used for finding a job
    location.

    """

    fields = ForeignData.fields + [
        "nodes", "location", "jobid", "state", "index", "walltime", "queue",
        "user", "submittime", "starttime", "project", 'is_runnable', 
        'is_active', 'has_resources', "score", 'attrs', 'walltime_p',
        'geometry',
        'restart_overhead' ]

    def __init__ (self, spec):
        ForeignData.__init__(self, spec)
        spec = spec.copy()
        self.partition = "none"
        self.nodes = spec.pop("nodes", None)
        self.location = spec.pop("location", None)
        self.jobid = spec.pop("jobid", None)
        self.state = spec.pop("state", None)
        self.index = spec.pop("index", None)
        self.walltime = spec.pop("walltime", None)
        self.walltime_p = spec.pop("walltime_p", None)   #*AdjEst*
        self.queue = spec.pop("queue", None)
        self.user = spec.pop("user", None)
        self.submittime = spec.pop("submittime", None)
        self.starttime = spec.pop("starttime", None)
        self.project = spec.pop("project", None)
        self.is_runnable = spec.pop("is_runnable", None)
        self.is_active = spec.pop("is_active", None)
        self.has_resources = spec.pop("has_resources", None)
        self.score = spec.pop("score", 0.0)
        self.attrs = spec.pop("attrs", {})
        self.geometry = spec.pop("geometry", None)

        logger.info("Job %s/%s: Found job" % (self.jobid, self.user))

class JobDict(ForeignDataDict):
    """Dictionary of job metadata from cqm for job location purposes.

    """
    item_cls = Job
    key = 'jobid'
    __oserror__ = Cobalt.Util.FailureMode("QM Connection (job)")
    __function__ = ComponentProxy("queue-manager").get_jobs
    __fields__ = ['nodes', 'location', 'jobid', 'state', 'index',
                  'walltime', 'queue', 'user', 'submittime', 'starttime',
                  'project', 'is_runnable', 'is_active', 'has_resources',
                  'score', 'attrs', 'walltime_p','geometry',
                  'restart_overhead'] #dwang

class Queue(ForeignData):
    """Cache of queue data for scheduling decisions and reservation 
    association.

    """
    fields = ForeignData.fields + [
        "name", "state", "policy", "priority"
    ]

    def __init__(self, spec):
        ForeignData.__init__(self, spec)
        spec = spec.copy()
        self.name = spec.pop("name", None)
        self.state = spec.pop("state", None)
        self.policy = spec.pop("policy", None)
        self.priority = spec.pop("priority", 0)

    def LoadPolicy(self):
        '''Instantiate queue policy modules upon demand'''
        if self.policy not in Cobalt.SchedulerPolicies.names:
            logger.error("Cannot load policy %s for queue %s" % \
                         (self.policy, self.name))
        else:
            pclass = Cobalt.SchedulerPolicies.names[self.policy]
            self.policy = pclass(self.name)


class QueueDict(ForeignDataDict):
    """Dictionary for the queue metadata cache.

    """
    item_cls = Queue
    key = 'name'
    __oserror__ = Cobalt.Util.FailureMode("QM Connection (queue)")
    __function__ = ComponentProxy("queue-manager").get_queues
    __fields__ = ['name', 'state', 'policy', 'priority']

class BGSched (Component):
    """The scheduler component interface and driver functions.

    """
    implementation = "bgsched"
    name = "scheduler"
    logger = logging.getLogger("Cobalt.Components.scheduler")


    _configfields = ['utility_file']
    _config = ConfigParser.ConfigParser()
    _config.read(Cobalt.CONFIG_FILES)
    if not _config._sections.has_key('bgsched'):
        logger.critical('''"bgsched" section missing from cobalt config file''')
        sys.exit(1)
    config = _config._sections['bgsched']
    mfields = [field for field in _configfields if not config.has_key(field)]
    if mfields:
        logger.critical("Missing option(s) in cobalt config file [bgsched] section: %s",
                (" ".join(mfields)))
        sys.exit(1)
    if config.get("default_reservation_policy"):
        global DEFAULT_RESERVATION_POLICY
        DEFAULT_RESERVATION_POLICY = config.get("default_reservation_policy")

    def __init__(self, *args, **kwargs):
        Component.__init__(self, *args, **kwargs)
        self.reservations = ReservationDict()
        self.queues = QueueDict()
        self.jobs = JobDict()
        self.started_jobs = {}
        # dwang:
        self.rtj_blocking = False
        # dwang:
        self.restart_jobs = [] # dwang
        self.restart_jobs_set = set() # dwang
        # dwang
        self.sync_state = Cobalt.Util.FailureMode("Foreign Data Sync")
        self.active = True

        self.get_current_time = time.time
        self.id_gen = IncrID()
        global bgsched_id_gen
        bgsched_id_gen = self.id_gen

        self.cycle_id_gen = IncrID()
        global bgsched_cycle_id_gen
        bgsched_cycle_id_gen = self.cycle_id_gen

    def __getstate__(self):
        state = {}
        state.update(Component.__getstate__(self))
        state.update({
                'sched_version':1,
                'reservations':self.reservations,
                'active':self.active,
                'next_res_id':self.id_gen.idnum+1, 
                'next_cycle_id':self.cycle_id_gen.idnum+1, 
                'msg_queue': dbwriter.msg_queue, 
                'overflow': dbwriter.overflow})
        return state

    def __setstate__(self, state):
        Component.__setstate__(self, state)

        self.reservations = state['reservations']
        if 'active' in state.keys():
            self.active = state['active']
        else:
            self.active = True

        self.id_gen = IncrID()
        self.id_gen.set(state['next_res_id'])
        global bgsched_id_gen
        bgsched_id_gen = self.id_gen

        self.cycle_id_gen = IncrID()
        self.cycle_id_gen.set(state['next_cycle_id'])
        global bgsched_cycle_id_gen
        bgsched_cycle_id_gen = self.cycle_id_gen

        self.queues = QueueDict()
        self.jobs = JobDict()
        self.started_jobs = {}
        self.sync_state = Cobalt.Util.FailureMode("Foreign Data Sync")

        self.get_current_time = time.time

        if state.has_key('msg_queue'):
            dbwriter.msg_queue = state['msg_queue']
        if state.has_key('overflow') and (dbwriter.max_queued != None):
            dbwriter.overflow = state['overflow']

    # order the jobs with biggest utility first
    def utilitycmp(self, job1, job2):
        return -cmp(job1.score, job2.score)

    def prioritycmp(self, job1, job2):
        """Compare 2 jobs first using queue priority and then first-in, first-out."""

        val = cmp(self.queues[job1.queue].priority, self.queues[job2.queue].priority)
        if val == 0:
            return self.fifocmp(job1, job2)
        else:
            # we want the higher priority first
            return -val

    def fifocmp (self, job1, job2):
        """Compare 2 jobs for first-in, first-out."""

        def fifo_value (job):
            if job.index is not None:
                return int(job.index)
            else:
                return job.jobid

        # Implement some simple variations on FIFO scheduling
        # within a particular queue, based on queue policy
        fifoval = cmp(fifo_value(job1), fifo_value(job2))
        if(job1.queue == job2.queue):
            qpolicy = self.queues[job1.queue].policy
            sizeval = cmp(int(job1.nodes), int(job2.nodes))
            wtimeval = cmp(int(job1.walltime), int(job2.walltime))
            if(qpolicy == 'largest-first' and sizeval):
                return -sizeval
            elif(qpolicy == 'smallest-first' and sizeval):
                return sizeval
            elif(qpolicy == 'longest-first' and wtimeval):
                return -wtimeval
            elif(qpolicy == 'shortest-first' and wtimeval):
                return wtimeval
            else:
                return fifoval
        else:
            return fifoval

        return cmp(fifo_value(job1), fifo_value(job2))

    def save_me(self):
        '''Automatic method for saving off the component statefile

        '''
        Component.save(self)
    save_me = automatic(save_me,
            float(get_bgsched_config('save_me_interval', 10)))

    #user_name in this context is the user setting/modifying the res.
    def add_reservations (self, specs, user_name):
        '''Exposed method for adding a reservation via setres.

        '''
        self.logger.info("%s adding reservation: %r" % (user_name, specs))
        added_reservations =  self.reservations.q_add(specs)
        for added_reservation in added_reservations:
            self.logger.info("Res %s/%s: %s adding reservation: %r" % 
                             (added_reservation.res_id,
                              added_reservation.cycle_id,
                              user_name, specs))
            dbwriter.log_to_db(user_name, "creating", "reservation", added_reservation)
        return added_reservations

    add_reservations = exposed(query(add_reservations))

    def del_reservations (self, specs, user_name):
        '''Exposed method for terminating a reservation from releaseres.

        '''
        self.logger.info("%s releasing reservation: %r" % (user_name, specs))
        del_reservations = self.reservations.q_del(specs)
        for del_reservation in del_reservations:
            self.logger.info("Res %s/%s/: %s releasing reservation: %r" % 
                             (del_reservation.res_id,
                              del_reservation.cycle_id,
                              user_name, specs))
            #database logging moved to the ReservationDict q_del method
            #the expected message is "terminated"
        return del_reservations

    del_reservations = exposed(query(del_reservations))

    def get_reservations (self, specs):
        '''Exposed method to get reservaton information.

        '''
        return self.reservations.q_get(specs)
    get_reservations = exposed(query(get_reservations))

    def set_reservations(self, specs, updates, user_name):
        '''Exposed method for resetting reservation information from setres.
        Must target an extant reservation.

        '''
        log_str = "%s modifying reservation: %r with updates %r" % (user_name, specs, updates)
        self.logger.info(log_str)
        # handle defers as a special case:  have to log these,
        # they are frequent enough we don't want a full a mod record
        def _set_reservations(res, newattr):
            res.update(newattr)
        updates['__cmd_user'] = user_name
        mod_reservations = self.reservations.q_get(specs, _set_reservations, updates)
        for mod_reservation in mod_reservations:
            self.logger.info("Res %s/%s: %s modifying reservation: %r" % 
                             (mod_reservation.res_id,
                              mod_reservation.cycle_id,
                              user_name, specs))
        return mod_reservations

    set_reservations = exposed(query(set_reservations))


    def release_reservations(self, specs, user_name):
        '''Exposed method used by releaseres for user-release of reserved 
        resrouces.

        '''
        self.logger.info("%s requested release of reservation: %r",
                user_name, specs)
        self.logger.info("%s releasing reservation: %r", user_name, specs)
        rel_res = self.get_reservations(specs)
        for res in rel_res:
            dbwriter.log_to_db(user_name, "released", "reservation", res) 
        del_reservations = self.reservations.q_del(specs)
        for del_reservation in del_reservations:
            self.logger.info("Res %s/%s/: %s releasing reservation: %r" % 
                             (del_reservation.res_id,
                              del_reservation.cycle_id,
                              user_name, specs))
        return del_reservations

    release_reservations = exposed(query(release_reservations))

    def check_reservations(self):
        '''Validation for reservation resources.  Complain if reservations
        overlap, since dueling reservation behavior is undefined.

        '''
        ret = ""
        reservations = self.reservations.values()
        for i in range(len(reservations)):
            for j in range(i+1, len(reservations)):
                # if at least one reservation is cyclic, we want *that* 
                # reservation to be the one getting its overlaps method called
                if reservations[i].cycle is not None:
                    res1 = reservations[i]
                    res2 = reservations[j]
                else:
                    res1 = reservations[j]
                    res2 = reservations[i]

                # we subtract a little bit because the overlaps method isn't 
                # really meant to do this it will report warnings when one 
                # reservation starts at the same time another ends
                if res1.overlaps(res2.start, res2.duration - 0.00001):
                    # now we need to check for overlap in space
                    results = ComponentProxy("system").get_partitions(
                        [ {'name': p, 'children': '*', 'parents': '*'} for p in res2.partitions.split(":") ]
                    )
                    for p in res1.partitions.split(":"):
                        for r in results:
                            if p==r['name'] or p in r['children'] or p in r['parents']:
                                ret += "Warning: reservation '%s' overlaps reservation '%s'\n" % (res1.name, res2.name)

        return ret
    check_reservations = exposed(check_reservations)

    def sync_data(self):
        started = self.get_current_time()
        for item in [self.jobs, self.queues]:
            try:
                item.Sync()
            except (ComponentLookupError, xmlrpclib.Fault):
                # the ForeignDataDicts already include FailureMode stuff
                pass
        # print "took %f seconds for sync_data" % (time.time() - started, )
    #sync_data = automatic(sync_data)

    def _run_reservation_jobs (self, reservations_cache):
        #FIXME: Get some logging in here so we know what job is being picked.
        # handle each reservation separately, as they shouldn't be competing for resources
        for cur_res in reservations_cache.itervalues():
            #print "trying to run res jobs in", cur_res.name, self.started_jobs
            queue = cur_res.queue
            #FIXME: this should probably check reservation active rather than just queue
            # running.
            if not (self.queues.has_key(queue) and self.queues[queue].state == 'running'):
                continue

            temp_jobs = self.jobs.q_get([{'is_runnable':True, 'queue':queue}])
            active_jobs = []
            for j in temp_jobs:
                if not self.started_jobs.has_key(j.jobid) and cur_res.job_within_reservation(j):
                    active_jobs.append(j)

            if not active_jobs:
                continue
            active_jobs.sort(self.utilitycmp)

            job_location_args = []
            #FIXME: make sure job_location_args is ordered.
            for job in active_jobs:
                job_location_args.append( 
                    { 'jobid': str(job.jobid), 
                      'nodes': job.nodes, 
                      'queue': job.queue, 
                      'required': cur_res.partitions.split(":"),
                      'utility_score': job.score,
                      'walltime': job.walltime,
                      'attrs': job.attrs,
                      'user': job.user,
                      'geometry':job.geometry
                    } )

            # there's no backfilling in reservations. Run whatever we get in
            # best_partition_dict.  There is no draining, backfill windows are
            # meaningless within reservations.
            try:
                self.logger.debug("calling from reservation")
                best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, [])
            except:
                self.logger.error("failed to connect to system component")
                best_partition_dict = {}

            for jobid in best_partition_dict:
                job = self.jobs[int(jobid)]
                self.logger.info("Starting job %d/%s in reservation %s",
                        job.jobid, job.user, cur_res.name)
                self._start_job(job, best_partition_dict[jobid], {str(job.jobid):cur_res.res_id})


    def _start_job(self, job, partition_list, resid=None):
        """Get the queue manager to start a job."""
        cqm = ComponentProxy("queue-manager")

        try:
            self.logger.info("trying to start job %d on partition %r" % (job.jobid, partition_list))
            cqm.run_jobs([{'tag':"job", 'jobid':job.jobid}], partition_list, None, resid, job.walltime)
        except ComponentLookupError:
            self.logger.error("failed to connect to queue manager")
            return
        self.started_jobs[job.jobid] = self.get_current_time()

    # dwang:
    def _start_rt_job(self, job, partition_list, resid=None):
        """Get the queue manager to start a job."""
        cqm = ComponentProxy("queue-manager")
        
        try:
            self.logger.info("trying to start job %d on partition %r" % (job.jobid, partition_list))
            cqm.run_rt_jobs([{'tag':"job", 'jobid':job.jobid}], partition_list, None, resid, job.walltime)
        except ComponentLookupError:
            self.logger.error("failed to connect to queue manager")
            return
        self.started_jobs[job.jobid] = self.get_current_time()
    # dwang
    
    # dwang:
    def _start_job_wOverhead(self, job, partition_list, rest_overhead, resid=None):
        """Get the queue manager to start a job."""
        cqm = ComponentProxy("queue-manager")
        
        try:
            self.logger.info("trying to start job %d on partition %r" % (job.jobid, partition_list))
	    # print "_____[v2] job.jobid: ", job.jobid
            cqm.run_jobs_wOverhead([{'tag':"job", 'jobid':job.jobid}], partition_list, rest_overhead, None, resid, job.walltime)
        except ComponentLookupError:
            self.logger.error("failed to connect to queue manager")
            return
        self.started_jobs[job.jobid] = self.get_current_time()
    # dwang
    
    # dwang:
    def _start_rt_job_wOverhead(self, job, partition_list, rtj_overhead, resid=None):
        """Get the queue manager to start a job."""
        cqm = ComponentProxy("queue-manager")
        
        try:
            self.logger.info("trying to start job %d on partition %r" % (job.jobid, partition_list))
            cqm.run_rt_jobs_wOverhead([{'tag':"job", 'jobid':job.jobid}], partition_list, rtj_overhead, None, resid, job.walltime)
        except ComponentLookupError:
            self.logger.error("failed to connect to queue manager")
            return
        self.started_jobs[job.jobid] = self.get_current_time()
    # dwang

    # dwang:
    def _kill_job(self, job, partition_name):
        """Get the queue manager to kill a job."""
        cqm = ComponentProxy("queue-manager")

        try:
            cqm.kill_job(job, partition_name) # w/o restart_overhead recording
        except ComponentLookupError:
            self.logger.error("failed to connect to queue manager")
            return 
    _kill_job = locking(exposed(_kill_job))
    # dwang
    

    # dwang:
    def _kill_job_wOverhead(self, job, partition_name, dsize_pnode, bw_temp_read):
        """Get the queue manager to kill a job."""
        cqm = ComponentProxy("queue-manager") 
        donetime, lefttime, partsize = cqm.kill_job_wOverhead(job, partition_name, dsize_pnode, bw_temp_read) # w restart_overhead recording
        return donetime, lefttime, partsize
    _kill_job_wOverhead = locking(exposed(_kill_job_wOverhead))
    # dwang


    # dwang:
    def _kill_job_wPcheckP(self, job, partition_name, checkp_t_internval, dsize_pnode, bw_temp_read, fp_backf):
        """Get the queue manager to kill a job."""
        cqm = ComponentProxy("queue-manager")
        donetime, lefttime, partsize = cqm.kill_job_wPcheckP( job, partition_name, checkp_t_internval, dsize_pnode, bw_temp_read, fp_backf )
        return donetime, lefttime, partsize
    _kill_job_wPcheckP = locking(exposed(_kill_job_wPcheckP))
    # dwang

    # dwang:
    def _kill_job_wPcheckP_app(self, job, partition_name, dsize_pnode, bw_temp_read):
        """Get the queue manager to kill a job."""
        cqm = ComponentProxy("queue-manager")
        donetime, lefttime, partsize = cqm.kill_job_wPcheckP_app(job, partition_name, dsize_pnode, bw_temp_read)
        return donetime, lefttime, partsize
    _kill_job_wPcheckP_app = locking(exposed(_kill_job_wPcheckP_app))
    # dwang

    # dwang:
    #def schedule_jobs (self, preempt):
    #def schedule_jobs (self, preempt, simu_name,simu_tid):

    ###
    # samnickolay
    def schedule_jobs_baseline(self, preempt, fp_backf):
        # dwang
        '''look at the queued jobs, and decide which ones to start
        This entire method completes prior to the job's timer starting
        in cqm.
        '''
        #
        # print "[sch] sch_j_none() ... "
        # print "     c_time: ", self.get_current_time()
        #
        if not self.active:
            return

        self.sync_data()

        # if we're missing information, don't bother trying to schedule jobs
        if not (self.queues.__oserror__.status and
                    self.jobs.__oserror__.status):
            self.sync_state.Fail()
            return
        self.sync_state.Pass()

        self.component_lock_acquire()
        try:
            # cleanup any reservations which have expired
            for res in self.reservations.values():
                if res.is_over():
                    self.logger.info("reservation %s has ended; removing" %
                                     (res.name))
                    self.logger.info("Res %s/%s: Ending reservation: %r" %
                                     (res.res_id, res.cycle_id, res.name))
                    # dbwriter.log_to_db(None, "ending", "reservation",
                    #        res)
                    del_reservations = self.reservations.q_del([
                        {'name': res.name}])

            # FIXME: this isn't a deepcopy.  it copies references to each reservation in the reservations dict.  is that really
            # sufficient?  --brt
            reservations_cache = self.reservations.copy()
        except:
            # just to make sure we don't keep the lock forever
            self.logger.error("error in schedule_jobs")
        self.component_lock_release()

        # clean up the started_jobs cached data
        # TODO: Make this tunable.
        # started_jobs are jobs that bgsched has prompted cqm to start
        # but may not have had job.run has been completed.
        now = self.get_current_time()
        for job_name in self.started_jobs.keys():
            if (now - self.started_jobs[job_name]) > 60:
                del self.started_jobs[job_name]

        active_queues = []
        spruce_queues = []
        res_queues = set()
        for item in reservations_cache.q_get([{'queue': '*'}]):
            if self.queues.has_key(item.queue):
                if self.queues[item.queue].state == 'running':
                    res_queues.add(item.queue)

        for queue in self.queues.itervalues():
            # print "[dw] queueName: ", queue.name
            if queue.name not in res_queues and queue.state == 'running':
                if queue.policy == "high_prio":
                    spruce_queues.append(queue)
                else:
                    active_queues.append(queue)

        # handle the reservation jobs that might be ready to go
        self._run_reservation_jobs(reservations_cache)

        # figure out stuff about queue equivalence classes
        res_info = {}
        pt_blocking_res = []
        for cur_res in reservations_cache.values():
            res_info[cur_res.name] = cur_res.partitions
            if cur_res.block_passthrough:
                pt_blocking_res.append(cur_res.name)

        # print "[dw] test_1 "
        try:
            ''' esjung: get a job from a system? -- no'''
            equiv = ComponentProxy("system").find_queue_equivalence_classes(
                res_info, [q.name for q in active_queues + spruce_queues],
                pt_blocking_res)
        except:
            self.logger.error("failed to connect to system component")
            return
        # print "[dw] test_2 "

        for eq_class in equiv:
            # recall that is_runnable is True for certain types of holds
            ''' esjung: get jobs with matching specs. '''
            temp_jobs = self.jobs.q_get([{'is_runnable': True, 'queue': queue.name} for queue in active_queues \
                                         if queue.name in eq_class['queues']])

            # print "[dw] len(temp_jobs): ", len(temp_jobs)
            active_jobs = []
            spruce_jobs = []
            for j in temp_jobs:
                # print "[dw] temp.queue: ", j.queue # dwang
                if not self.started_jobs.has_key(j.jobid):
                    ''' esjung: add to active job list '''
                    active_jobs.append(j)

            ''' esjung: follow the same way spruce jobs are implemented '''
            preempt = 0
            if preempt > 0:
                realtime_jobs = []
                for j in active_jobs:
                    if j.user == 'realtime':
                        realtime_jobs.append(j)

                if realtime_jobs:
                    active_jobs = realtime_jobs
                    # print "[dw] len(realtime_jobs): ", len(realtime_jobs)
            ''' esjung '''
            #
            for j in active_jobs:
                # print "[dw] j.queue: ", j.queue # dwang
                if j.queue == "high_prio":  # dwang
                    spruce_jobs.append(j)  # dwang
            #
            temp_jobs = self.jobs.q_get([{'is_runnable': True, 'queue': queue.name} for queue in spruce_queues \
                                         if queue.name in eq_class['queues']])

            for j in temp_jobs:
                if not self.started_jobs.has_key(j.jobid):
                    spruce_jobs.append(j)

            # print "[dw] len(spruce_job): ", len(spruce_jobs) # dwang
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            if spruce_jobs:
                # print "[dw] high_prio[] jobs ..."
                active_jobs = spruce_jobs

            # get the cutoff time for backfilling
            #
            # BRT: should we use 'has_resources' or 'is_active'?  has_resources returns to false once the resource epilogue
            # scripts have finished running while is_active only returns to false once the job (not just the running task) has
            # completely terminated.  the difference is likely to be slight unless the job epilogue scripts are heavy weight.
            temp_jobs = [job for job in self.jobs.q_get([{'has_resources': True}]) if job.queue in eq_class['queues']]
            end_times = []
            for job in temp_jobs:
                # take the max so that jobs which have gone overtime and are being killed
                # continue to cast a small backfilling shadow (we need this for the case
                # that the final job in a drained partition runs overtime -- which otherwise
                # allows things to be backfilled into the drained partition)

                ##*AdjEst*
                if running_job_walltime_prediction:
                    runtime_estimate = float(job.walltime_p)
                else:
                    runtime_estimate = float(job.walltime)

                end_time = max(float(job.starttime) + 60 * runtime_estimate, now + 5 * 60)
                end_times.append([job.location, end_time])

            for res_name in eq_class['reservations']:
                cur_res = reservations_cache[res_name]

                if not cur_res.cycle:
                    end_time = float(cur_res.start) + float(cur_res.duration)
                else:
                    done_after = float(cur_res.duration) - ((now - float(cur_res.start)) % float(cur_res.cycle))
                    if done_after < 0:
                        done_after += cur_res.cycle
                    end_time = now + done_after
                if cur_res.is_active():
                    for part_name in cur_res.partitions.split(":"):
                        end_times.append([[part_name], end_time])

            if not active_jobs:
                continue
            active_jobs.sort(self.utilitycmp)

            # now smoosh lots of data together to be passed to the allocator in the system component
            job_location_args = []
            for job in active_jobs:
                forbidden_locations = set()
                pt_blocking_locations = set()

                for res_name in eq_class['reservations']:
                    cur_res = reservations_cache[res_name]
                    if cur_res.overlaps(self.get_current_time(), 60 * float(job.walltime) + SLOP_TIME):
                        forbidden_locations.update(cur_res.partitions.split(':'))
                        if cur_res.block_passthrough:
                            pt_blocking_locations.update(cur_res.partitions.split(':'))

                job_location_args.append(
                    {'jobid': str(job.jobid),
                     'nodes': job.nodes,
                     'queue': job.queue,
                     'forbidden': list(forbidden_locations),
                     'pt_forbidden': list(pt_blocking_locations),
                     'utility_score': job.score,
                     'walltime': job.walltime,
                     'walltime_p': job.walltime_p,  # *AdjEst*
                     'attrs': job.attrs,
                     'user': job.user,
                     'geometry': job.geometry
                     })

            # dwang: checkpt/preempt ########################################## ##########################################
            ''' -> 1. get_preempt_list
                  -> 2. prempt_list_select
                  -> 3. job_checkpt
                  -> 4. job_kill
                  -> 5. partation_update
                -> 6.
            '''
            # best_partition_dict = ComponentProxy("system").find_job_location_wcheckp(job_location_args, end_times,fp_backf,0)
            # preempt_list = ComponentProxy("system").get_preempt_list(job_location_args, best_partition_dict, 0)

            # dwang: checkpt/preempt ########################################## ##########################################


            try:
                self.logger.debug("calling from main sched %s", eq_class)
                # dwang:
                # best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times)
                best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times, fp_backf)
                ''' 
                #
                best_partition_dict_wcheckp = ComponentProxy("system").find_job_location_wcheckp(job_location_args, end_times,fp_backf,0)
                #
                # dwang
                rtj_job = []
                rest_job = []
                for jobid in best_partition_dict_wcheckp:
                    job = self.jobs[int(jobid)]
                    if job.user == 'realtime':
                        rtj_job.append(job)
                        print "---> rtj_id: ", job.jobid
                    else:
                        rest_job.append(job)
                print "----> len_rtj: ", len(rtj_job)
                print "----> len_rest: ", len(rest_job)
                #

                if realtime_jobs and (best_partition_dict_wcheckp != best_partition_dict):
                    print "[dw_bqsim] best_partition_dict:         ", best_partition_dict
                    print "[dw_bqsim] best_partition_dict_wcheckpt:", best_partition_dict_wcheckp
                    #if best_partition_dict_wcheckp == best_partition_dict:
                        #print "[dw_bqsim] BEST_PARTITION EQUAL ---- "
                '''
                # dwang
                ''' esjung: if realtime jobs, do differently 
                    1) should modify find_job_location() function
                       find_job_location(job_location_args, end_times, preemption=true)
                '''
                # print "[dw_diag] testM_3 "

                # if preempt > 0 and job_location_args.user == 'realtime':
                # pass
            except:
                self.logger.error("failed to connect to system component")
                self.logger.debug("%s", traceback.format_exc())
                best_partition_dict = {}

            # dwang:
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            '''
            if realtime_jobs and (best_partition_dict_wcheckp != best_partition_dict):
                print "[dw_bgsched] ----> start job scheduling w/preempt ... "
                # --> get_preempt_list
                #
                #for jobid in best_partition_dict_wcheckp:
                #    pname = best_partition_dict_wcheckp[jobid]
                #    print "p_name: ", pname
                #
                preempt_list = ComponentProxy("system").get_preempt_list( best_partition_dict_wcheckp )
                print "             ----> preempt_size: ", len(preempt_list)

                for pjob in preempt_list:
                    print "---> PRE_job: ", pjob['jobid']
                    #
                    # ( --> checkpoint )
                    # ...

                    # --> kill job
                    jobid = pjob['jobid']
                    job = self.jobs[int(jobid)]
                    self._kill_job(job, pjob['pname'])
                    print "---> PRE_job_killed: ", pjob['jobid']
                    # ...

                    # ( --> _release_rt_partition() )
                    # ...

                    # ( --> _os_restart() )
                    # ...
                #
                best_partition_dict_ak = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf)
                print "[dw_bqsim] best_partition_dict _AK:     ", best_partition_dict
                print "[dw_bqsim] best_partition_dict_wcheckpt:", best_partition_dict_wcheckp
                #
                # --> _start_rt_job()
                for jobid in best_partition_dict_wcheckp:
                    job = self.jobs[int(jobid)]
                    self._start_job(job, best_partition_dict_wcheckp[jobid])

                    # ...

                # --> job restart()
                # ... 
            # dwang
            else:
                for jobid in best_partition_dict:
                    job = self.jobs[int(jobid)]
                    self._start_job(job, best_partition_dict[jobid])
            # --> _start_rt_job()
                for jobid in best_partition_dict_wcheckp:
                    job = self.jobs[int(jobid)]
                    self._start_job(job, best_partition_dict_wcheckp[jobid])
            '''
            # dwang
            for jobid in best_partition_dict:
                job = self.jobs[int(jobid)]
                self._start_job(job, best_partition_dict[jobid])

    schedule_jobs_baseline = locking(automatic(schedule_jobs_baseline,
                                      float(get_bgsched_config('schedule_jobs_interval', 10))))

    # samnickolay
    ###

    def schedule_jobs (self, preempt,fp_backf):
    # dwang
        '''look at the queued jobs, and decide which ones to start
        This entire method completes prior to the job's timer starting
        in cqm.
        '''
        #
        # print "[sch] sch_j_none() ... "
        # print "     c_time: ", self.get_current_time()
        #
        if not self.active:
            return

        self.sync_data()

        # if we're missing information, don't bother trying to schedule jobs
        if not (self.queues.__oserror__.status and 
                self.jobs.__oserror__.status):
            self.sync_state.Fail()
            return
        self.sync_state.Pass()

        self.component_lock_acquire()
        try:
            # cleanup any reservations which have expired
            for res in self.reservations.values():
                if res.is_over():
                    self.logger.info("reservation %s has ended; removing" % 
                            (res.name))
                    self.logger.info("Res %s/%s: Ending reservation: %r" % 
                             (res.res_id, res.cycle_id, res.name))
                    #dbwriter.log_to_db(None, "ending", "reservation", 
                    #        res) 
                    del_reservations = self.reservations.q_del([
                        {'name': res.name}])

            # FIXME: this isn't a deepcopy.  it copies references to each reservation in the reservations dict.  is that really
            # sufficient?  --brt
            reservations_cache = self.reservations.copy()
        except:
            # just to make sure we don't keep the lock forever
            self.logger.error("error in schedule_jobs")
        self.component_lock_release()

        # clean up the started_jobs cached data
        # TODO: Make this tunable.
        # started_jobs are jobs that bgsched has prompted cqm to start
        # but may not have had job.run has been completed.
        now = self.get_current_time()
        for job_name in self.started_jobs.keys():
            if (now - self.started_jobs[job_name]) > 60:
                del self.started_jobs[job_name]

        active_queues = []
        spruce_queues = []
        res_queues = set()
        for item in reservations_cache.q_get([{'queue':'*'}]):
            if self.queues.has_key(item.queue):
                if self.queues[item.queue].state == 'running':
                    res_queues.add(item.queue)

        for queue in self.queues.itervalues():
            # print "[dw] queueName: ", queue.name
            if queue.name not in res_queues and queue.state == 'running':
                if queue.policy == "high_prio":
                    spruce_queues.append(queue)
                else:
                    active_queues.append(queue)

        # handle the reservation jobs that might be ready to go
        self._run_reservation_jobs(reservations_cache)

        # figure out stuff about queue equivalence classes
        res_info = {}
        pt_blocking_res = []
        for cur_res in reservations_cache.values():
            res_info[cur_res.name] = cur_res.partitions
            if cur_res.block_passthrough:
                pt_blocking_res.append(cur_res.name)

        # print "[dw] test_1 "
        try:
            ''' esjung: get a job from a system? -- no'''
            equiv = ComponentProxy("system").find_queue_equivalence_classes(
                    res_info, [q.name for q in active_queues + spruce_queues],
                    pt_blocking_res)
        except:
            self.logger.error("failed to connect to system component")
            return
        # print "[dw] test_2 "

        for eq_class in equiv:
            # recall that is_runnable is True for certain types of holds
            ''' esjung: get jobs with matching specs. '''
            temp_jobs = self.jobs.q_get([{'is_runnable':True, 'queue':queue.name} for queue in active_queues \
                if queue.name in eq_class['queues']])

            # print "[dw] len(temp_jobs): ", len(temp_jobs)
            active_jobs = []
            spruce_jobs = []
            for j in temp_jobs:
                # print "[dw] temp.queue: ", j.queue # dwang
                if not self.started_jobs.has_key(j.jobid):
                    ''' esjung: add to active job list '''
                    active_jobs.append(j) 

            ''' esjung: follow the same way spruce jobs are implemented '''
            preempt = 5
            if preempt > 0:
                realtime_jobs = []
                for j in active_jobs:
                    if j.user == 'realtime':
                        realtime_jobs.append(j)
                
                if realtime_jobs:
                    active_jobs = realtime_jobs
                    # print "[dw] len(realtime_jobs): ", len(realtime_jobs)
            ''' esjung '''
            #
            for j in active_jobs:
                # print "[dw] j.queue: ", j.queue # dwang
                if j.queue == "high_prio": # dwang
                    spruce_jobs.append(j)  # dwang
            #
            temp_jobs = self.jobs.q_get([{'is_runnable':True, 'queue':queue.name} for queue in spruce_queues \
                if queue.name in eq_class['queues']])
            
            for j in temp_jobs:
                if not self.started_jobs.has_key(j.jobid):
                    spruce_jobs.append(j)

            # print "[dw] len(spruce_job): ", len(spruce_jobs) # dwang
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            if spruce_jobs:
                # print "[dw] high_prio[] jobs ..."
                active_jobs = spruce_jobs


            # get the cutoff time for backfilling
            #
            # BRT: should we use 'has_resources' or 'is_active'?  has_resources returns to false once the resource epilogue
            # scripts have finished running while is_active only returns to false once the job (not just the running task) has
            # completely terminated.  the difference is likely to be slight unless the job epilogue scripts are heavy weight.
            temp_jobs = [job for job in self.jobs.q_get([{'has_resources':True}]) if job.queue in eq_class['queues']]
            end_times = []
            for job in temp_jobs:
                # take the max so that jobs which have gone overtime and are being killed
                # continue to cast a small backfilling shadow (we need this for the case
                # that the final job in a drained partition runs overtime -- which otherwise
                # allows things to be backfilled into the drained partition)

                ##*AdjEst*
                if running_job_walltime_prediction:
                    runtime_estimate = float(job.walltime_p)
                else:
                    runtime_estimate = float(job.walltime)

                end_time = max(float(job.starttime) + 60 * runtime_estimate, now + 5*60)
                end_times.append([job.location, end_time])

            for res_name in eq_class['reservations']:
                cur_res = reservations_cache[res_name]

                if not cur_res.cycle:
                    end_time = float(cur_res.start) + float(cur_res.duration)
                else:
                    done_after = float(cur_res.duration) - ((now - float(cur_res.start)) % float(cur_res.cycle))
                    if done_after < 0:
                        done_after += cur_res.cycle
                    end_time = now + done_after
                if cur_res.is_active():
                    for part_name in cur_res.partitions.split(":"):
                        end_times.append([[part_name], end_time])

            if not active_jobs:
                continue
            active_jobs.sort(self.utilitycmp)
            

            # now smoosh lots of data together to be passed to the allocator in the system component
            job_location_args = []
            for job in active_jobs:
                forbidden_locations = set()
                pt_blocking_locations = set()

                for res_name in eq_class['reservations']:
                    cur_res = reservations_cache[res_name]
                    if cur_res.overlaps(self.get_current_time(), 60 * float(job.walltime) + SLOP_TIME):
                        forbidden_locations.update(cur_res.partitions.split(':'))
                        if cur_res.block_passthrough:
                            pt_blocking_locations.update(cur_res.partitions.split(':'))

                job_location_args.append( 
                    { 'jobid': str(job.jobid), 
                      'nodes': job.nodes, 
                      'queue': job.queue, 
                      'forbidden': list(forbidden_locations),
                      'pt_forbidden': list(pt_blocking_locations),
                      'utility_score': job.score,
                      'walltime': job.walltime,
                      'walltime_p': job.walltime_p, #*AdjEst*
                      'attrs': job.attrs,
                      'user': job.user,
                      'geometry':job.geometry
                    } )
            
            # dwang: checkpt/preempt ########################################## ##########################################
            ''' -> 1. get_preempt_list
                  -> 2. prempt_list_select
                  -> 3. job_checkpt
                  -> 4. job_kill
                  -> 5. partation_update
                -> 6.
            '''
            #best_partition_dict = ComponentProxy("system").find_job_location_wcheckp(job_location_args, end_times,fp_backf,0)
            #preempt_list = ComponentProxy("system").get_preempt_list(job_location_args, best_partition_dict, 0)
            
            # dwang: checkpt/preempt ########################################## ##########################################


            try:
                self.logger.debug("calling from main sched %s", eq_class)
                # dwang:
                #best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times)
                best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf)
                ''' 
                #
                best_partition_dict_wcheckp = ComponentProxy("system").find_job_location_wcheckp(job_location_args, end_times,fp_backf,0)
                #
                # dwang
                rtj_job = []
                rest_job = []
                for jobid in best_partition_dict_wcheckp:
                    job = self.jobs[int(jobid)]
                    if job.user == 'realtime':
                        rtj_job.append(job)
                        print "---> rtj_id: ", job.jobid
                    else:
                        rest_job.append(job)
                print "----> len_rtj: ", len(rtj_job)
                print "----> len_rest: ", len(rest_job)
                #
                
                if realtime_jobs and (best_partition_dict_wcheckp != best_partition_dict):
                    print "[dw_bqsim] best_partition_dict:         ", best_partition_dict
                    print "[dw_bqsim] best_partition_dict_wcheckpt:", best_partition_dict_wcheckp
                    #if best_partition_dict_wcheckp == best_partition_dict:
                        #print "[dw_bqsim] BEST_PARTITION EQUAL ---- "
                '''
                # dwang
                ''' esjung: if realtime jobs, do differently 
                    1) should modify find_job_location() function
                       find_job_location(job_location_args, end_times, preemption=true)
                '''
                # print "[dw_diag] testM_3 "
                
                # if preempt > 0 and job_location_args.user == 'realtime':
                # pass
            except:
                self.logger.error("failed to connect to system component")
                self.logger.debug("%s", traceback.format_exc())
                best_partition_dict = {}

            # dwang:
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            '''
            if realtime_jobs and (best_partition_dict_wcheckp != best_partition_dict):
                print "[dw_bgsched] ----> start job scheduling w/preempt ... "
                # --> get_preempt_list
                #
                #for jobid in best_partition_dict_wcheckp:
                #    pname = best_partition_dict_wcheckp[jobid]
                #    print "p_name: ", pname
                #
                preempt_list = ComponentProxy("system").get_preempt_list( best_partition_dict_wcheckp )
                print "             ----> preempt_size: ", len(preempt_list)

                for pjob in preempt_list:
                    print "---> PRE_job: ", pjob['jobid']
                    #
                    # ( --> checkpoint )
                    # ...
                    
                    # --> kill job
                    jobid = pjob['jobid']
                    job = self.jobs[int(jobid)]
                    self._kill_job(job, pjob['pname'])
                    print "---> PRE_job_killed: ", pjob['jobid']
                    # ...
                    
                    # ( --> _release_rt_partition() )
                    # ...
                    
                    # ( --> _os_restart() )
                    # ...
                #
                best_partition_dict_ak = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf)
                print "[dw_bqsim] best_partition_dict _AK:     ", best_partition_dict
                print "[dw_bqsim] best_partition_dict_wcheckpt:", best_partition_dict_wcheckp
                #
                # --> _start_rt_job()
                for jobid in best_partition_dict_wcheckp:
                    job = self.jobs[int(jobid)]
                    self._start_job(job, best_partition_dict_wcheckp[jobid])

                    # ...

                # --> job restart()
                # ... 
            # dwang
            else:
                for jobid in best_partition_dict:
                    job = self.jobs[int(jobid)]
                    self._start_job(job, best_partition_dict[jobid])
            # --> _start_rt_job()
                for jobid in best_partition_dict_wcheckp:
                    job = self.jobs[int(jobid)]
                    self._start_job(job, best_partition_dict_wcheckp[jobid])
            '''
            # dwang
            for jobid in best_partition_dict:
                job = self.jobs[int(jobid)]
                self._start_job(job, best_partition_dict[jobid]) 

    schedule_jobs = locking(automatic(schedule_jobs,
        float(get_bgsched_config('schedule_jobs_interval', 10))))
        
 

    def schedule_jobs_hpQ_resv (self, preempt,fp_backf):
    # dwang
        '''look at the queued jobs, and decide which ones to start
        This entire method completes prior to the job's timer starting
        in cqm.
        '''
        #
        # print "[sch] sch_j_none() ... "
        # print "     c_time: ", self.get_current_time()
        #
        if not self.active:
            return

        self.sync_data()

        # if we're missing information, don't bother trying to schedule jobs
        if not (self.queues.__oserror__.status and 
                self.jobs.__oserror__.status):
            self.sync_state.Fail()
            return
        self.sync_state.Pass()

        self.component_lock_acquire()
        try:
            # cleanup any reservations which have expired
            for res in self.reservations.values():
                if res.is_over():
                    self.logger.info("reservation %s has ended; removing" % 
                            (res.name))
                    self.logger.info("Res %s/%s: Ending reservation: %r" % 
                             (res.res_id, res.cycle_id, res.name))
                    #dbwriter.log_to_db(None, "ending", "reservation", 
                    #        res) 
                    del_reservations = self.reservations.q_del([
                        {'name': res.name}])

            # FIXME: this isn't a deepcopy.  it copies references to each reservation in the reservations dict.  is that really
            # sufficient?  --brt
            reservations_cache = self.reservations.copy()
        except:
            # just to make sure we don't keep the lock forever
            self.logger.error("error in schedule_jobs")
        self.component_lock_release()

        # clean up the started_jobs cached data
        # TODO: Make this tunable.
        # started_jobs are jobs that bgsched has prompted cqm to start
        # but may not have had job.run has been completed.
        now = self.get_current_time()
        for job_name in self.started_jobs.keys():
            if (now - self.started_jobs[job_name]) > 60:
                del self.started_jobs[job_name]

        active_queues = []
        spruce_queues = []
        res_queues = set()
        for item in reservations_cache.q_get([{'queue':'*'}]):
            if self.queues.has_key(item.queue):
                if self.queues[item.queue].state == 'running':
                    res_queues.add(item.queue)

        for queue in self.queues.itervalues():
            # print "[dw] queueName: ", queue.name
            if queue.name not in res_queues and queue.state == 'running':
                if queue.policy == "high_prio":
                    spruce_queues.append(queue)
                else:
                    active_queues.append(queue)

        # handle the reservation jobs that might be ready to go
        self._run_reservation_jobs(reservations_cache)

        # figure out stuff about queue equivalence classes
        res_info = {}
        pt_blocking_res = []
        for cur_res in reservations_cache.values():
            res_info[cur_res.name] = cur_res.partitions
            if cur_res.block_passthrough:
                pt_blocking_res.append(cur_res.name)

        # print "[dw] test_1 "
        try:
            ''' esjung: get a job from a system? -- no'''
            equiv = ComponentProxy("system").find_queue_equivalence_classes(
                    res_info, [q.name for q in active_queues + spruce_queues],
                    pt_blocking_res)
        except:
            self.logger.error("failed to connect to system component")
            return
        # print "[dw] test_2 "

        for eq_class in equiv:
            # recall that is_runnable is True for certain types of holds
            ''' esjung: get jobs with matching specs. '''
            temp_jobs = self.jobs.q_get([{'is_runnable':True, 'queue':queue.name} for queue in active_queues \
                if queue.name in eq_class['queues']])

            # print "[dw] len(temp_jobs): ", len(temp_jobs)
            active_jobs = []
            rest_jobs = [] 
            spruce_jobs = []
            for j in temp_jobs:
                # print "[dw] temp.queue: ", j.queue # dwang
                if not self.started_jobs.has_key(j.jobid):
                    ''' esjung: add to active job list '''
                    active_jobs.append(j) 

            ''' esjung: follow the same way spruce jobs are implemented '''
            preempt = 5
            if preempt > 0:
                realtime_jobs = []
                for j in active_jobs:
                    if j.user == 'realtime':
                        realtime_jobs.append(j)
                
                if realtime_jobs: 
                    rest_jobs = active_jobs 
                    active_jobs = realtime_jobs 
                    # print "[dw] len(realtime_jobs): ", len(realtime_jobs)
                    # print "[dw] len(rest_jobs): ", len(rest_jobs)
            ''' esjung '''
            #
            for j in active_jobs:
                # print "[dw] j.queue: ", j.queue # dwang
                if j.queue == "high_prio": # dwang
                    spruce_jobs.append(j)  # dwang
            #
            temp_jobs = self.jobs.q_get([{'is_runnable':True, 'queue':queue.name} for queue in spruce_queues \
                if queue.name in eq_class['queues']])
            
            for j in temp_jobs:
                if not self.started_jobs.has_key(j.jobid):
                    spruce_jobs.append(j)

            # print "[dw] len(spruce_job): ", len(spruce_jobs) # dwang
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            if spruce_jobs:
                # print "[dw] high_prio[] jobs ..."
                active_jobs = spruce_jobs 
                # print "[dw] len(active_jobs): ", len(active_jobs)
                # 


            # get the cutoff time for backfilling
            #
            # BRT: should we use 'has_resources' or 'is_active'?  has_resources returns to false once the resource epilogue
            # scripts have finished running while is_active only returns to false once the job (not just the running task) has
            # completely terminated.  the difference is likely to be slight unless the job epilogue scripts are heavy weight.
            temp_jobs = [job for job in self.jobs.q_get([{'has_resources':True}]) if job.queue in eq_class['queues']]
            end_times = []
            for job in temp_jobs:
                # take the max so that jobs which have gone overtime and are being killed
                # continue to cast a small backfilling shadow (we need this for the case
                # that the final job in a drained partition runs overtime -- which otherwise
                # allows things to be backfilled into the drained partition)

                ##*AdjEst*
                if running_job_walltime_prediction:
                    runtime_estimate = float(job.walltime_p)
                else:
                    runtime_estimate = float(job.walltime)

                end_time = max(float(job.starttime) + 60 * runtime_estimate, now + 5*60)
                end_times.append([job.location, end_time])

            for res_name in eq_class['reservations']:
                cur_res = reservations_cache[res_name]

                if not cur_res.cycle:
                    end_time = float(cur_res.start) + float(cur_res.duration)
                else:
                    done_after = float(cur_res.duration) - ((now - float(cur_res.start)) % float(cur_res.cycle))
                    if done_after < 0:
                        done_after += cur_res.cycle
                    end_time = now + done_after
                if cur_res.is_active():
                    for part_name in cur_res.partitions.split(":"):
                        end_times.append([[part_name], end_time])

            if not active_jobs:
                continue
            active_jobs.sort(self.utilitycmp)
            

            # now smoosh lots of data together to be passed to the allocator in the system component
            job_location_args = []
            for job in active_jobs:
                forbidden_locations = set()
                pt_blocking_locations = set()

                for res_name in eq_class['reservations']:
                    cur_res = reservations_cache[res_name]
                    if cur_res.overlaps(self.get_current_time(), 60 * float(job.walltime) + SLOP_TIME):
                        forbidden_locations.update(cur_res.partitions.split(':'))
                        if cur_res.block_passthrough:
                            pt_blocking_locations.update(cur_res.partitions.split(':'))

                job_location_args.append( 
                    { 'jobid': str(job.jobid), 
                      'nodes': job.nodes, 
                      'queue': job.queue, 
                      'forbidden': list(forbidden_locations),
                      'pt_forbidden': list(pt_blocking_locations),
                      'utility_score': job.score,
                      'walltime': job.walltime,
                      'walltime_p': job.walltime_p, #*AdjEst*
                      'attrs': job.attrs,
                      'user': job.user,
                      'geometry':job.geometry
                    } )
            ## 
            # dwang: highpQ_resv ########################################## ##########################################
	    ''' 
            global rtj_resv_part_dict 
            if realtime_jobs: 
                for job in active_jobs:
                    print "[hpQ_resv] rtj_jobid: ", job.get('jobid')
                    if job.get('jobid') in rtj_resv_part_dict.keys(): 
                        rtj_resv_part = rtj_resv_part_dict.get(job.get('jobid'))
                        print "[hpQ_resv] rtj_resv_part: ", rtj_resv_part 
	    ''' 
            # dwang: highpQ_resv ########################################## ##########################################
            try:
                self.logger.debug("calling from main sched %s", eq_class)
                # dwang:
                #best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times)
                best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times, fp_backf)
                # 
                # dwang
                # 
                # print "[dw_diag] testM_3 "
                
                # if preempt > 0 and job_location_args.user == 'realtime':
                # pass
            except:
                self.logger.error("failed to connect to system component")
                self.logger.debug("%s", traceback.format_exc())
                best_partition_dict = {}

            # dwang:
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            # 
            # dwang: 
            # - highpQ_resv
            if len(best_partition_dict)==0 and len(realtime_jobs)>0: 
            	# print "[hpQ_resv] RT-job pending ... ", len(realtime_jobs)
                # 
                global resv_jobs
                ## resv_jobs.append(realtime_jobs)
                resv_jobs = realtime_jobs
                # print "[hpQ_resv] RT-job reserving ... ", len(resv_jobs)
                #
                # print "[hpQ_resv] len(rest_jobs): ", len(rest_jobs)
                #
                # dwang: highpQ_resv ########################################## ##########################################
                active_jobs = rest_jobs 
                #####
                temp_jobs = [job for job in self.jobs.q_get([{'has_resources':True}]) if job.queue in eq_class['queues']]
                end_times = []
                for job in temp_jobs:
                    ##*AdjEst*
                    if running_job_walltime_prediction:
                        runtime_estimate = float(job.walltime_p)
                    else:
                        runtime_estimate = float(job.walltime)

                    end_time = max(float(job.starttime) + 60 * runtime_estimate, now + 5*60)
                    end_times.append([job.location, end_time])

                for res_name in eq_class['reservations']:
                    cur_res = reservations_cache[res_name]

                    if not cur_res.cycle:
                        end_time = float(cur_res.start) + float(cur_res.duration)
                    else:
                        done_after = float(cur_res.duration) - ((now - float(cur_res.start)) % float(cur_res.cycle))
                        if done_after < 0:
                            done_after += cur_res.cycle
                        end_time = now + done_after
                    if cur_res.is_active():
                        for part_name in cur_res.partitions.split(":"):
                            end_times.append([[part_name], end_time])

                if not active_jobs:
                    continue
                active_jobs.sort(self.utilitycmp)
                # dwang: highpQ_resv ########################################## ##########################################
                job_location_args_rtj = job_location_args 
                # now smoosh lots of data together to be passed to the allocator in the system component
                job_location_args = []
                for job in active_jobs:
                    forbidden_locations = set()
                    pt_blocking_locations = set()

                    for res_name in eq_class['reservations']:
                        cur_res = reservations_cache[res_name]
                        if cur_res.overlaps(self.get_current_time(), 60 * float(job.walltime) + SLOP_TIME):
                            forbidden_locations.update(cur_res.partitions.split(':'))
                            if cur_res.block_passthrough:
                                pt_blocking_locations.update(cur_res.partitions.split(':'))

                    job_location_args.append( 
                        { 'jobid': str(job.jobid), 
                        'nodes': job.nodes, 
                        'queue': job.queue, 
                        'forbidden': list(forbidden_locations),
                        'pt_forbidden': list(pt_blocking_locations),
                        'utility_score': job.score,
                        'walltime': job.walltime,
                        'walltime_p': job.walltime_p, #*AdjEst*
                        'attrs': job.attrs,
                        'user': job.user,
                        'geometry':job.geometry
                        } ) 
                # dwang: highpQ_resv ########################################## ########################################## 
                try:
                    self.logger.debug("calling from main sched %s", eq_class)
                    # dwang: 
                    best_partition_dict2 = ComponentProxy("system").find_job_location_hpq_resv(job_location_args, end_times, job_location_args_rtj, fp_backf)
                    #
                    # print "[sj_hpQ_resv] len(best_partition_dict2): ", len(best_partition_dict2)
                    # print "[sj_hpQ_resv] best_partition_dict2: ", best_partition_dict2
                    #
                except:
                    self.logger.error("failed to connect to system component")
                    self.logger.debug("%s", traceback.format_exc())
                    best_partition_dict2 = {} 
                # dwang: highpQ_resv ########################################## ##########################################
                # 
            # dwang 
            for jobid in best_partition_dict:
                job = self.jobs[int(jobid)]
                self._start_job(job, best_partition_dict[jobid]) 
            # 
    schedule_jobs_hpQ_resv = locking(automatic(schedule_jobs_hpQ_resv,
        float(get_bgsched_config('schedule_jobs_interval', 10))))


        
    # dwang: 
    def schedule_jobs_wcheckp_v0 (self, preempt,fp_backf):
    # dwang
        '''look at the queued jobs, and decide which ones to start
        This entire method completes prior to the job's timer starting
        in cqm.
        '''
        #
        # print "[sch] sch_j_v0() ... "
        # print "     c_time: ", self.get_current_time()
        # print "     c_RTJ_blocking: ", self.rtj_blocking
        # print "     c_util: ", ComponentProxy("system").get_utilization_rate(0)
        # print "     c_util_p: ", ComponentProxy("system").get_utilization_rate(10)
        fp_backf.write('--> c_time %s:\n' %self.get_current_time() )
        fp_backf.write('--> c_util %f:\n' %ComponentProxy("system").get_utilization_rate(0) )
        #
        if not self.active:
            return

        self.sync_data()

        # if we're missing information, don't bother trying to schedule jobs
        if not (self.queues.__oserror__.status and 
                self.jobs.__oserror__.status):
            self.sync_state.Fail()
            return
        self.sync_state.Pass()

        self.component_lock_acquire()
        try:
            # cleanup any reservations which have expired
            for res in self.reservations.values():
                if res.is_over():
                    self.logger.info("reservation %s has ended; removing" % 
                            (res.name))
                    self.logger.info("Res %s/%s: Ending reservation: %r" % 
                             (res.res_id, res.cycle_id, res.name))
                    #dbwriter.log_to_db(None, "ending", "reservation", 
                    #        res) 
                    del_reservations = self.reservations.q_del([
                        {'name': res.name}])

            # FIXME: this isn't a deepcopy.  it copies references to each reservation in the reservations dict.  is that really
            # sufficient?  --brt
            reservations_cache = self.reservations.copy()
        except:
            # just to make sure we don't keep the lock forever
            self.logger.error("error in schedule_jobs")
        self.component_lock_release()

        # clean up the started_jobs cached data
        # TODO: Make this tunable.
        # started_jobs are jobs that bgsched has prompted cqm to start
        # but may not have had job.run has been completed.
        now = self.get_current_time()
        for job_name in self.started_jobs.keys():
            if (now - self.started_jobs[job_name]) > 60:
                del self.started_jobs[job_name]

        active_queues = []
        spruce_queues = []
        res_queues = set()
        for item in reservations_cache.q_get([{'queue':'*'}]):
            if self.queues.has_key(item.queue):
                if self.queues[item.queue].state == 'running':
                    res_queues.add(item.queue)

        for queue in self.queues.itervalues():
            # print "[dw] queueName: ", queue.name
            if queue.name not in res_queues and queue.state == 'running':
                if queue.policy == "high_prio":
                    spruce_queues.append(queue)
                else:
                    active_queues.append(queue)

        # handle the reservation jobs that might be ready to go
        self._run_reservation_jobs(reservations_cache)

        # figure out stuff about queue equivalence classes
        res_info = {}
        pt_blocking_res = []
        for cur_res in reservations_cache.values():
            res_info[cur_res.name] = cur_res.partitions
            if cur_res.block_passthrough:
                pt_blocking_res.append(cur_res.name)

        # print "[dw] test_1 "
        try:
            ''' esjung: get a job from a system? -- no'''
            equiv = ComponentProxy("system").find_queue_equivalence_classes(
                    res_info, [q.name for q in active_queues + spruce_queues],
                    pt_blocking_res)
        except:
            self.logger.error("failed to connect to system component")
            return
        # print "[dw] test_2 "

        for eq_class in equiv:
            # recall that is_runnable is True for certain types of holds
            ''' esjung: get jobs with matching specs. '''
            temp_jobs = self.jobs.q_get([{'is_runnable':True, 'queue':queue.name} for queue in active_queues \
                if queue.name in eq_class['queues']])

            # print "[dw] len(temp_jobs): ", len(temp_jobs)
            active_jobs = []
            spruce_jobs = []
            for j in temp_jobs:
                # print "[dw] temp.queue: ", j.queue # dwang
                if not self.started_jobs.has_key(j.jobid):
                    ''' esjung: add to active job list '''
                    active_jobs.append(j) 

            ''' esjung: follow the same way spruce jobs are implemented '''
            preempt = 5
            if preempt > 0:
                realtime_jobs = []
                for j in active_jobs:
                    if j.user == 'realtime':
                        realtime_jobs.append(j)
                
                if realtime_jobs:
                    active_jobs = realtime_jobs
                    # print "[dw] len(realtime_jobs): ", len(realtime_jobs)
            ''' esjung '''
            #
            for j in active_jobs:
                # print "[dw] j.queue: ", j.queue # dwang
                if j.queue == "high_prio": # dwang
                    spruce_jobs.append(j)  # dwang
            #
            temp_jobs = self.jobs.q_get([{'is_runnable':True, 'queue':queue.name} for queue in spruce_queues \
                if queue.name in eq_class['queues']])
            
            for j in temp_jobs:
                if not self.started_jobs.has_key(j.jobid):
                    spruce_jobs.append(j)

            # print "[dw] len(spruce_job): ", len(spruce_jobs) # dwang
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            if spruce_jobs:
                # print "[dw] high_prio[] jobs ..."
                active_jobs = spruce_jobs 
                # 

            # get the cutoff time for backfilling
            #
            # BRT: should we use 'has_resources' or 'is_active'?  has_resources returns to false once the resource epilogue
            # scripts have finished running while is_active only returns to false once the job (not just the running task) has
            # completely terminated.  the difference is likely to be slight unless the job epilogue scripts are heavy weight.
            temp_jobs = [job for job in self.jobs.q_get([{'has_resources':True}]) if job.queue in eq_class['queues']]
            end_times = []
            for job in temp_jobs:
                # take the max so that jobs which have gone overtime and are being killed
                # continue to cast a small backfilling shadow (we need this for the case
                # that the final job in a drained partition runs overtime -- which otherwise
                # allows things to be backfilled into the drained partition)

                ##*AdjEst*
                if running_job_walltime_prediction:
                    runtime_estimate = float(job.walltime_p)
                else:
                    runtime_estimate = float(job.walltime)

                end_time = max(float(job.starttime) + 60 * runtime_estimate, now + 5*60)
                end_times.append([job.location, end_time])

            for res_name in eq_class['reservations']:
                cur_res = reservations_cache[res_name]

                if not cur_res.cycle:
                    end_time = float(cur_res.start) + float(cur_res.duration)
                else:
                    done_after = float(cur_res.duration) - ((now - float(cur_res.start)) % float(cur_res.cycle))
                    if done_after < 0:
                        done_after += cur_res.cycle
                    end_time = now + done_after
                if cur_res.is_active():
                    for part_name in cur_res.partitions.split(":"):
                        end_times.append([[part_name], end_time])
            #
            # print "END_Times: ", end_times
            #
            
            if not active_jobs:
                continue
            active_jobs.sort(self.utilitycmp)
            

            # now smoosh lots of data together to be passed to the allocator in the system component
            job_location_args = []
            for job in active_jobs:
                forbidden_locations = set()
                pt_blocking_locations = set()

                for res_name in eq_class['reservations']:
                    cur_res = reservations_cache[res_name]
                    if cur_res.overlaps(self.get_current_time(), 60 * float(job.walltime) + SLOP_TIME):
                        forbidden_locations.update(cur_res.partitions.split(':'))
                        if cur_res.block_passthrough:
                            pt_blocking_locations.update(cur_res.partitions.split(':'))

                job_location_args.append( 
                    { 'jobid': str(job.jobid), 
                      'nodes': job.nodes, 
                      'queue': job.queue, 
                      'forbidden': list(forbidden_locations),
                      'pt_forbidden': list(pt_blocking_locations),
                      'utility_score': job.score,
                      'walltime': job.walltime,
                      'walltime_p': job.walltime_p, #*AdjEst*
                      'attrs': job.attrs,
                      'user': job.user,
                      'geometry':job.geometry
                    } )
            
            # dwang: checkpt/preempt ########################################## ##########################################
            ''' -> 1. get_preempt_list
                  -> 2. prempt_list_select
                  -> 3. job_checkpt
                  -> 4. job_kill
                  -> 5. partation_update
                -> 6.
            '''
            #best_partition_dict = ComponentProxy("system").find_job_location_wcheckp(job_location_args, end_times,fp_backf,0)
            #preempt_list = ComponentProxy("system").get_preempt_list(job_location_args, best_partition_dict, 0)
            
            # dwang: checkpt/preempt ########################################## ##########################################


            try:
                self.logger.debug("calling from main sched %s", eq_class)
                # dwang:
                #best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times)
                #best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf)
                
                rtj_job = []
                rest_job = []
                if realtime_jobs:
                    best_partition_dict_wcheckp = ComponentProxy("system").find_job_location_wcheckp(job_location_args, end_times,fp_backf,0)
                    #best_partition_dict_wcheckp = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf,0)
                    # 
                    # dwang
                    for jobid in best_partition_dict_wcheckp:
                        job = self.jobs[int(jobid)]
                        if job.user == 'realtime':
                            rtj_job.append(job)
                            # print "---> rtj_id: ", job.jobid
                            # print "     c_time: ", self.get_current_time()
                        else:
                            rest_job.append(job)
                else:
                    best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf)
                    for jobid in best_partition_dict:
                        job = self.jobs[int(jobid)]
                        if job.user == 'realtime':
                            rtj_job.append(job)
                            # print "---> rtj_id: ", job.jobid
                        else:
                            rest_job.append(job)
                # print "----> len_rtj: ", len(rtj_job)
                # print "----> len_rest: ", len(rest_job)
                #
                
                #if realtime_jobs: # and (best_partition_dict_wcheckp != best_partition_dict):
                    #print "[dw_bqsim] best_partition_dict:         ", best_partition_dict
                    #print "[dw_bqsim] best_partition_dict_wcheckpt:", best_partition_dict_wcheckp
                    #if best_partition_dict_wcheckp == best_partition_dict:
                        #print "[dw_bqsim] BEST_PARTITION EQUAL ---- "
                
                # dwang
                ''' esjung: if realtime jobs, do differently 
                    1) should modify find_job_location() function
                       find_job_location(job_location_args, end_times, preemption=true)
                '''
                # print "[dw_diag] testM_3 "
                
                # if preempt > 0 and job_location_args.user == 'realtime':
                # pass
            except:
                self.logger.error("failed to connect to system component")
                self.logger.debug("%s", traceback.format_exc())
                best_partition_dict = {}

            # dwang:
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            
            if realtime_jobs: # and (best_partition_dict_wcheckp != best_partition_dict):
                if not best_partition_dict_wcheckp:
                    self.rtj_blocking = True
                    # print "[dw_bgsched] ----> RTJ_blocking  -------------------------------------------- START -------- "
                else:
                    #
                    if self.rtj_blocking:
                        self.rtj_blocking = False
                        # print "[dw_bgsched] ----> RTJ_blocking  ------------------------------------------ STOP ---------- "
                    #
                    # print "[dw_bgsched] ----> check job scheduling w/preempt ... "
                    for jobid in best_partition_dict_wcheckp:
                        job = self.jobs[int(jobid)]
                        fp_backf.write('[rtj_jid] %s:\n' %jobid )

                    preempt_list = ComponentProxy("system").get_preempt_list( best_partition_dict_wcheckp )
                    # print "             ----> preempt_size: ", len(preempt_list)

                    for pjob in preempt_list:
                        # print "---> PRE_job: ", pjob['jobid']
                        fp_backf.write('[rtj_pre] %s:\n' %pjob['jobid'] )
                        #
                        fp_backf.write('[pre] %s:\n' %pjob['jobid'] )
                        # ( --> checkpoint )
                        # ...
                    
                        #
                        util_before = ComponentProxy("system").get_utilization_rate(0)
                        util_before_p = ComponentProxy("system").get_utilization_rate(10)
                        # --> kill job
                        jobid = pjob['jobid']
                        job = self.jobs[int(jobid)]
                        self._kill_job(job, pjob['pname'])
                        # print "---> PRE_job_killed: ", pjob['jobid']
                        #
                        util_after = ComponentProxy("system").get_utilization_rate(0)
                        util_after_p = ComponentProxy("system").get_utilization_rate(10)
                        # print "---> PRE_util_before: ", util_before
                        # print "---> PRE_util_after: ", util_after
                        # print "---> PRE_util_before_p: ", util_before_p
                        # print "---> PRE_util_after_p: ", util_after_p
                            # ...
                    
                        # ( --> _release_rt_partition() )
                        # ...
                        
                        # ( --> _os_restart() )
                        # ...
                    #
                    #

            # dwang
            # --> _start_rt_job()
            # print "[dw_bgsched] ----> len(realtime_jobs): ", len(realtime_jobs)
            ##if realtime_jobs and len(preempt_list) == 0:
            if realtime_jobs:
                # print "[dw_bgsched] ----> start job scheduling w/preempt ... "
                for jobid in best_partition_dict_wcheckp:
                    job = self.jobs[int(jobid)]
                    #
                    #fp_backf.write('[rtj_nonpre] %s:\n' %jobid )
                    # print "     c_time: ", self.get_current_time()
                    #
                    #self._start_job(job, best_partition_dict_wcheckp[jobid])
                    self._start_rt_job(job, best_partition_dict_wcheckp[jobid])
                    #
                    util_job = ComponentProxy("system").get_utilization_rate(0)
                    util_job_p = ComponentProxy("system").get_utilization_rate(10)
                    # print "---> RTJ_util: ", util_job
                    # print "---> RTJ_util_p: ", util_job_p
                    fp_backf.write('RTJ: %s: %f \n' %( self.get_current_time(), util_job ))
            ##else:
            elif len(realtime_jobs)==0 and self.rtj_blocking==False:
                for jobid in best_partition_dict:
                    job = self.jobs[int(jobid)]
                    self._start_job(job, best_partition_dict[jobid])
                    #
                    util_job = ComponentProxy("system").get_utilization_rate(0)
                    util_job_p = ComponentProxy("system").get_utilization_rate(10)
                    # print "---> REST_util: ", util_job
                    # print "---> REST_util_p: ", util_job_p
                    fp_backf.write('REST: %s: %f \n' %( self.get_current_time(), util_job ))

    schedule_jobs_wcheckp_v0 = locking(automatic(schedule_jobs_wcheckp_v0,
        float(get_bgsched_config('schedule_jobs_interval', 10))))         
        


    # dwang: 
    def schedule_jobs_wcheckp_v1 (self, preempt,fp_backf,fp_pre_bj, checkp_heur_opt):
    # dwang
        '''look at the queued jobs, and decide which ones to start
        This entire method completes prior to the job's timer starting
        in cqm.
        '''
        #
        # print "[sch] sch_j_v1() ... "
        # print "     c_time: ", self.get_current_time()
        # print "     c_RTJ_blocking: ", self.rtj_blocking
        # print "     c_util: ", ComponentProxy("system").get_utilization_rate(0)
        # print "     c_util_p: ", ComponentProxy("system").get_utilization_rate(5)
        fp_backf.write('--> c_time %s:\n' %self.get_current_time() )
        fp_backf.write('--> c_util %f:\n' %ComponentProxy("system").get_utilization_rate(0) )
        #
        if not self.active:
            return

        self.sync_data() 

        # if we're missing information, don't bother trying to schedule jobs
        if not (self.queues.__oserror__.status and 
                self.jobs.__oserror__.status):
            self.sync_state.Fail()
            return
        self.sync_state.Pass()

        self.component_lock_acquire()
        try:
            # cleanup any reservations which have expired
            for res in self.reservations.values():
                if res.is_over():
                    self.logger.info("reservation %s has ended; removing" % 
                            (res.name))
                    self.logger.info("Res %s/%s: Ending reservation: %r" % 
                             (res.res_id, res.cycle_id, res.name))
                    #dbwriter.log_to_db(None, "ending", "reservation", 
                    #        res) 
                    del_reservations = self.reservations.q_del([
                        {'name': res.name}])

            # FIXME: this isn't a deepcopy.  it copies references to each reservation in the reservations dict.  is that really
            # sufficient?  --brt
            reservations_cache = self.reservations.copy()
        except:
            # just to make sure we don't keep the lock forever
            self.logger.error("error in schedule_jobs")
        self.component_lock_release()

        # clean up the started_jobs cached data
        # TODO: Make this tunable.
        # started_jobs are jobs that bgsched has prompted cqm to start
        # but may not have had job.run has been completed.
        now = self.get_current_time()
        for job_name in self.started_jobs.keys():
            if (now - self.started_jobs[job_name]) > 60:
                del self.started_jobs[job_name]

        active_queues = []
        spruce_queues = []
        res_queues = set()
        for item in reservations_cache.q_get([{'queue':'*'}]):
            if self.queues.has_key(item.queue):
                if self.queues[item.queue].state == 'running':
                    res_queues.add(item.queue)

        for queue in self.queues.itervalues():
            # print "[dw] queueName: ", queue.name
            if queue.name not in res_queues and queue.state == 'running':
                if queue.policy == "high_prio":
                    spruce_queues.append(queue)
                else:
                    active_queues.append(queue)

        # handle the reservation jobs that might be ready to go
        self._run_reservation_jobs(reservations_cache)

        # figure out stuff about queue equivalence classes
        res_info = {}
        pt_blocking_res = []
        for cur_res in reservations_cache.values():
            res_info[cur_res.name] = cur_res.partitions
            if cur_res.block_passthrough:
                pt_blocking_res.append(cur_res.name)

        # print "[dw] test_1 "
        try:
            ''' esjung: get a job from a system? -- no'''
            equiv = ComponentProxy("system").find_queue_equivalence_classes(
                    res_info, [q.name for q in active_queues + spruce_queues],
                    pt_blocking_res)
        except:
            self.logger.error("failed to connect to system component")
            return
        # print "[dw] test_2 "

        for eq_class in equiv:
            # recall that is_runnable is True for certain types of holds
            ''' esjung: get jobs with matching specs. '''
            temp_jobs = self.jobs.q_get([{'is_runnable':True, 'queue':queue.name} for queue in active_queues \
                if queue.name in eq_class['queues']])
            
            #
            temp_restart_jobs = self.jobs.q_get([{'is_restarted':True, 'queue':queue.name} for queue in active_queues \
                                         if queue.name in eq_class['queues']])
            # print "[dw] len(temp_jobs): ", len(temp_jobs)
            active_jobs = []
            spruce_jobs = []
            for j in temp_jobs:
                # print "[dw] temp.queue: ", j.queue # dwang
                if not self.started_jobs.has_key(j.jobid):
                    ''' esjung: add to active job list '''
                    active_jobs.append(j)

            ''' esjung: follow the same way spruce jobs are implemented '''
            preempt = 5
            if preempt > 0:
                realtime_jobs = []
                for j in active_jobs:
                    if j.user == 'realtime':
                        realtime_jobs.append(j)
            ''' esjung '''
            
            ''' dwang: restart_jobs '''
            # print "[dw] len(realtime_jobs): ", len(realtime_jobs)
            cqm = ComponentProxy("queue-manager")
            restart_count_25 = cqm.get_restart_size()
            # print "[dw] restart_count_221-4: ", restart_count_25
            # print "[dw] len(restart_jobs_221-4): ", len(temp_restart_jobs)
            global restart_jobs_global
            restart_jobs = [] #restart_jobs_global
            # print "[dw] len(global_restart_jobs_221-4): ", len(restart_jobs_global)
            #
            if realtime_jobs:
                active_jobs = realtime_jobs
            '''
            elif restart_jobs:
                active_jobs = restart_jobs
                global restart_jobs_global
                restart_jobs_global = []
                print "[] -->> queue_restart_jobs() --------------->> "
            ''' 
            #
            '''
            for j in active_jobs:
                # print "[dw] j.queue: ", j.queue # dwang
                if j.queue == "high_prio": # dwang
                    spruce_jobs.append(j)  # dwang
            '''
            #
            temp_jobs = self.jobs.q_get([{'is_runnable':True, 'queue':queue.name} for queue in spruce_queues \
                if queue.name in eq_class['queues']])
            
            for j in temp_jobs:
                if not self.started_jobs.has_key(j.jobid):
                    spruce_jobs.append(j)

            # print "[dw] len(spruce_job): ", len(spruce_jobs) # dwang
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            if spruce_jobs:
                # print "[dw] high_prio[] jobs ..."
                active_jobs = spruce_jobs


            # get the cutoff time for backfilling
            #
            # BRT: should we use 'has_resources' or 'is_active'?  has_resources returns to false once the resource epilogue
            # scripts have finished running while is_active only returns to false once the job (not just the running task) has
            # completely terminated.  the difference is likely to be slight unless the job epilogue scripts are heavy weight.
            temp_jobs = [job for job in self.jobs.q_get([{'has_resources':True}]) if job.queue in eq_class['queues']]
            end_times = []
            for job in temp_jobs:
                # take the max so that jobs which have gone overtime and are being killed
                # continue to cast a small backfilling shadow (we need this for the case
                # that the final job in a drained partition runs overtime -- which otherwise
                # allows things to be backfilled into the drained partition)

                ##*AdjEst*
                if running_job_walltime_prediction:
                    runtime_estimate = float(job.walltime_p)
                else:
                    runtime_estimate = float(job.walltime)

                end_time = max(float(job.starttime) + 60 * runtime_estimate, now + 5*60)
                end_times.append([job.location, end_time])

            for res_name in eq_class['reservations']:
                cur_res = reservations_cache[res_name]

                if not cur_res.cycle:
                    end_time = float(cur_res.start) + float(cur_res.duration)
                else:
                    done_after = float(cur_res.duration) - ((now - float(cur_res.start)) % float(cur_res.cycle))
                    if done_after < 0:
                        done_after += cur_res.cycle
                    end_time = now + done_after
                if cur_res.is_active():
                    for part_name in cur_res.partitions.split(":"):
                        end_times.append([[part_name], end_time])
            #
            # print "END_Times: ", end_times
            #
            
            if not active_jobs:
                continue
            active_jobs.sort(self.utilitycmp)
            

            # now smoosh lots of data together to be passed to the allocator in the system component
            job_location_args = []
            for job in active_jobs:
                forbidden_locations = set()
                pt_blocking_locations = set()

                for res_name in eq_class['reservations']:
                    cur_res = reservations_cache[res_name]
                    if cur_res.overlaps(self.get_current_time(), 60 * float(job.walltime) + SLOP_TIME):
                        forbidden_locations.update(cur_res.partitions.split(':'))
                        if cur_res.block_passthrough:
                            pt_blocking_locations.update(cur_res.partitions.split(':'))
                
                if job.queue:
                    temp_queue = job.queue
                else:
                    temp_queue = default
                ##
                job_location_args.append( 
                    { 'jobid': str(job.jobid), 
                      'nodes': job.nodes,
                      'queue': temp_queue, #job.queue, # dwang
                      'forbidden': list(forbidden_locations),
                      'pt_forbidden': list(pt_blocking_locations),
                      'utility_score': job.score,
                      'walltime': job.walltime,
                      'walltime_p': job.walltime_p, #*AdjEst*
                      'attrs': job.attrs,
                      'user': job.user,
                      'geometry':job.geometry
                    } )
            
            # dwang: checkpt/preempt ########################################## ##########################################
            ''' -> 1. get_preempt_list
                  -> 2. prempt_list_select
                  -> 3. job_checkpt
                  -> 4. job_kill
                  -> 5. partation_update
                -> 6.
            '''
            #best_partition_dict = ComponentProxy("system").find_job_location_wcheckp(job_location_args, end_times,fp_backf,0)
            #preempt_list = ComponentProxy("system").get_preempt_list(job_location_args, best_partition_dict, 0)
            
            # dwang: checkpt/preempt ########################################## ##########################################


            try:
                self.logger.debug("calling from main sched %s", eq_class)
                # dwang:
                #best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times)
                #best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf)
                
                rtj_job = []
                rest_job = []
                if realtime_jobs: 
                    #best_partition_dict_wcheckp = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf,0) 
                    # 
                    # __dw_1222: 
                    best_partition_dict_wcheckp = ComponentProxy("system").find_job_location_wcheckp( job_location_args, end_times, fp_backf, 0 )
                    ## best_partition_dict_wcheckp = ComponentProxy("system").find_job_location_wcheckpH( job_location_args, end_times, fp_backf, 0, checkp_heur_opt )
                    # __dw_1222 
                    # 
                    # dwang
                    for jobid in best_partition_dict_wcheckp:
                        job = self.jobs[int(jobid)]
                        if job.user == 'realtime':
                            rtj_job.append(job)
                            # print "---> rtj_id: ", job.jobid
                            # print "     c_time: ", self.get_current_time()
                        else:
                            rest_job.append(job)
                else:
                    best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf)
                    for jobid in best_partition_dict:
                        job = self.jobs[int(jobid)]
                        if job.user == 'realtime':
                            rtj_job.append(job)
                            # print "---> rtj_id: ", job.jobid
                        else:
                            rest_job.append(job)
                # print "----> len_rtj: ", len(rtj_job)
                # print "----> len_rest: ", len(rest_job)
                #
                
                #if realtime_jobs: # and (best_partition_dict_wcheckp != best_partition_dict):
                    #print "[dw_bqsim] best_partition_dict:         ", best_partition_dict
                    #print "[dw_bqsim] best_partition_dict_wcheckpt:", best_partition_dict_wcheckp
                    #if best_partition_dict_wcheckp == best_partition_dict:
                        #print "[dw_bqsim] BEST_PARTITION EQUAL ---- "
                
                # dwang
                ''' esjung: if realtime jobs, do differently 
                    1) should modify find_job_location() function
                       find_job_location(job_location_args, end_times, preemption=true)
                '''
                # print "[dw_diag] testM_3 "
                
                # if preempt > 0 and job_location_args.user == 'realtime':
                # pass
            except:
                self.logger.error("failed to connect to system component")
                self.logger.debug("%s", traceback.format_exc())
                best_partition_dict = {}

            # dwang:
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            preempt_list_full = [] 
            if realtime_jobs: # and (best_partition_dict_wcheckp != best_partition_dict):
                if not best_partition_dict_wcheckp:
                    self.rtj_blocking = True
                    # print "[dw_bgsched] ----> RTJ_blocking  -------------------------------------------- START -------- "
                else:
                    #
                    if self.rtj_blocking:
                        self.rtj_blocking = False
                        # print "[dw_bgsched] ----> RTJ_blocking  ------------------------------------------ STOP ---------- "
                    #
                    # print "[dw_bgsched] ----> check job scheduling w/preempt ... "
                    for jobid in best_partition_dict_wcheckp:
                        job = self.jobs[int(jobid)]
                        fp_backf.write('[rtj_jid] %s:\n' %jobid )

                    preempt_list = ComponentProxy("system").get_preempt_list( best_partition_dict_wcheckp )
                    # print "             ----> preempt_size: ", len(preempt_list)
                    preempt_list_full = preempt_list

                    for pjob in preempt_list:
                        # print "---> PRE_job: ", pjob['jobid']
                        fp_backf.write('[rtj_pre] %s:\n' %pjob['jobid'] )
                        #
                        fp_backf.write('[pre] %s:\n' %pjob['jobid'] )
                        # ( --> checkpoint )
                        # ...
                    
                        #
                        util_before = ComponentProxy("system").get_utilization_rate(0)
                        util_before_p = ComponentProxy("system").get_utilization_rate(5)
                        # --> kill job
                        jobid = pjob['jobid'] 
			print "[dwRE] self.jobs jobid: ", jobid
                        # print "[dwRE] self.jobs jobid_TYPE: ", type(jobid)
                        # print "[dwRE] self.jobs LENs: ", len(self.jobs)
			#
                        #job = self.jobs[int(jobid)]
                        job = self.jobs[jobid]
                        self._kill_job(job, pjob['pname'])
                        # print "---> PRE_job_killed: ", pjob['jobid']
                        #
                        # print "[RE] jid_org: ", job.jobid
                        #
                        fp_pre_bj.write('%d: %d, %.3f,%.3f, %.3f \n' %(jobid, 0, 0, 0, 0 ) )
                        #
                        ## RESTART -->>
                        cqm.restart_job_add_queue(jobid)
                        # print "[] -->> ADD_queue_restart_jobs() --------------->> "
                        '''
                        global restart_jobs_global
                        restart_jobs_global.append(job)
                        ##
                        cqm.restart_job_add_queue(job.jobid)
                        '''
                        ##
                        self.sync_data() 
                        ##
                        util_after = ComponentProxy("system").get_utilization_rate(0)
                        util_after_p = ComponentProxy("system").get_utilization_rate(5)
                        # print "---> PRE_util_before: ", util_before
                        # print "---> PRE_util_after: ", util_after
                        # print "---> PRE_util_before_p: ", util_before_p
                        # print "---> PRE_util_after_p: ", util_after_p
                    # ... 
                    # ( --> _release_rt_partition() )
                        # ...
                        
                        # ( --> _os_restart() )
                        # ...
                    #
                    #
            # dwang
            # --> _start_rt_job()
            # print "[dw_bgsched] ----> len(realtime_jobs): ", len(realtime_jobs)
            ##if realtime_jobs and len(preempt_list_full) == 0:
            if realtime_jobs:
                # print "[dw_bgsched] ----> start job scheduling w/preempt ... "
                for jobid in best_partition_dict_wcheckp:
                    job = self.jobs[int(jobid)]
                    #
                    #fp_backf.write('[rtj_nonpre] %s:\n' %jobid )
                    # print "     c_time: ", self.get_current_time()
                    #
                    ##self._start_job(job, best_partition_dict_wcheckp[jobid])
                    self._start_rt_job(job, best_partition_dict_wcheckp[jobid])
                    #
                    util_job = ComponentProxy("system").get_utilization_rate(0)
                    util_job_p = ComponentProxy("system").get_utilization_rate(5)
                    # print "---> RTJ_util: ", util_job
                    # print "---> RTJ_util_p: ", util_job_p
                    fp_backf.write('RTJ: %s: %f \n' %( self.get_current_time(), util_job ))
            else:
            ##elif not realtime_jobs:
                #elif len(realtime_jobs)==0 and self.rtj_blocking==False:
                for jobid in best_partition_dict:
                    job = self.jobs[int(jobid)]
                    self._start_job(job, best_partition_dict[jobid])
                    #
                    util_job = ComponentProxy("system").get_utilization_rate(0)
                    util_job_p = ComponentProxy("system").get_utilization_rate(5)
                    # print "---> REST_util: ", util_job
                    # print "---> REST_util_p: ", util_job_p
                    fp_backf.write('REST: %s: %f \n' %( self.get_current_time(), util_job ))

            '''
            # dwang:
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            if realtime_jobs: # and (best_partition_dict_wcheckp != best_partition_dict):
                if preempt_list_full: 
                    for pjob in preempt_list_full:
                        jobid = pjob['jobid'] 
                        #job = self.jobs[int(jobid)]
                        #
                        #global restart_jobs_global
                        #restart_jobs_global.append(job)
                        #
                        cqm.restart_job_add_queue(jobid)
                        print "[] -->> ADD_queue_restart_jobs() --------------->> "
                        ## 
                    
                        # ( --> _release_rt_partition() )
                        # ...
                        
                        # ( --> _os_restart() )
                        # ...
            '''
    schedule_jobs_wcheckp_v1 = locking(automatic(schedule_jobs_wcheckp_v1,
        float(get_bgsched_config('schedule_jobs_interval', 10))))     



    # dwang: 
    def schedule_jobs_wcheckp_v2 (self, preempt,fp_backf,fp_pre_bj, dsize_pnode, bw_temp_write, bw_temp_read, checkp_heur_opt):
    # dwang
        '''look at the queued jobs, and decide which ones to start
        This entire method completes prior to the job's timer starting
        in cqm.
       '''
        #
        # print "[sch] sch_j_v2() ... "
        # print "     c_time: ", self.get_current_time()
        # print "     c_RTJ_blocking: ", self.rtj_blocking
        # print "     c_util: ", ComponentProxy("system").get_utilization_rate(0)
        # print "     c_util_p: ", ComponentProxy("system").get_utilization_rate(5)
        fp_backf.write('--> c_time %s:\n' %self.get_current_time() )
        fp_backf.write('--> c_util %f:\n' %ComponentProxy("system").get_utilization_rate(0) )
        #
        if not self.active:
            return

        self.sync_data() 

        # if we're missing information, don't bother trying to schedule jobs
        if not (self.queues.__oserror__.status and 
                self.jobs.__oserror__.status):
            self.sync_state.Fail()
            return
        self.sync_state.Pass()

        self.component_lock_acquire()
        try:
            # cleanup any reservations which have expired
            for res in self.reservations.values():
                if res.is_over():
                    self.logger.info("reservation %s has ended; removing" % 
                            (res.name))
                    self.logger.info("Res %s/%s: Ending reservation: %r" % 
                             (res.res_id, res.cycle_id, res.name))
                    #dbwriter.log_to_db(None, "ending", "reservation", 
                    #        res) 
                    del_reservations = self.reservations.q_del([
                        {'name': res.name}])

            # FIXME: this isn't a deepcopy.  it copies references to each reservation in the reservations dict.  is that really
            # sufficient?  --brt
            reservations_cache = self.reservations.copy()
        except:
            # just to make sure we don't keep the lock forever
            self.logger.error("error in schedule_jobs")
        self.component_lock_release()

        # clean up the started_jobs cached data
        # TODO: Make this tunable.
        # started_jobs are jobs that bgsched has prompted cqm to start
        # but may not have had job.run has been completed.
        now = self.get_current_time()
        for job_name in self.started_jobs.keys():
            if (now - self.started_jobs[job_name]) > 60:
                del self.started_jobs[job_name]

        active_queues = []
        spruce_queues = []
        res_queues = set()
        for item in reservations_cache.q_get([{'queue':'*'}]):
            if self.queues.has_key(item.queue):
                if self.queues[item.queue].state == 'running':
                    res_queues.add(item.queue)

        for queue in self.queues.itervalues():
            # print "[dw] queueName: ", queue.name
            if queue.name not in res_queues and queue.state == 'running':
                if queue.policy == "high_prio":
                    spruce_queues.append(queue)
                else:
                    active_queues.append(queue)

        # handle the reservation jobs that might be ready to go
        self._run_reservation_jobs(reservations_cache)

        # figure out stuff about queue equivalence classes
        res_info = {}
        pt_blocking_res = []
        for cur_res in reservations_cache.values():
            res_info[cur_res.name] = cur_res.partitions
            if cur_res.block_passthrough:
                pt_blocking_res.append(cur_res.name)

        # print "[dw] test_1 "
        try:
            ''' esjung: get a job from a system? -- no'''
            equiv = ComponentProxy("system").find_queue_equivalence_classes(
                    res_info, [q.name for q in active_queues + spruce_queues],
                    pt_blocking_res)
        except:
            self.logger.error("failed to connect to system component")
            return
        # print "[dw] test_2 "

        for eq_class in equiv:
            # recall that is_runnable is True for certain types of holds
            ''' esjung: get jobs with matching specs. '''
            temp_jobs = self.jobs.q_get([{'is_runnable':True, 'queue':queue.name} for queue in active_queues \
                if queue.name in eq_class['queues']])
            
            #
            temp_restart_jobs = self.jobs.q_get([{'is_restarted':True, 'queue':queue.name} for queue in active_queues \
                                         if queue.name in eq_class['queues']])
            # print "[dw] len(temp_jobs): ", len(temp_jobs)
            active_jobs = []
            spruce_jobs = []
            for j in temp_jobs:
                # print "[dw] temp.queue: ", j.queue # dwang
                if not self.started_jobs.has_key(j.jobid):
                    ''' esjung: add to active job list '''
                    active_jobs.append(j)

            ''' esjung: follow the same way spruce jobs are implemented '''
            preempt = 5
            if preempt > 0:
                realtime_jobs = []
                for j in active_jobs:
                    if j.user == 'realtime':
                        realtime_jobs.append(j)
            ''' esjung '''
            
            ''' dwang: restart_jobs '''
            # print "[dw] len(realtime_jobs): ", len(realtime_jobs)
            cqm = ComponentProxy("queue-manager")
            restart_count_25 = cqm.get_restart_size()
            # print "[dw] restart_count_221-4: ", restart_count_25
            # print "[dw] len(restart_jobs_221-4): ", len(temp_restart_jobs)
            global restart_jobs_global
            restart_jobs = [] #restart_jobs_global
            # print "[dw] len(global_restart_jobs_221-4): ", len(restart_jobs_global)
            #
            if realtime_jobs:
                active_jobs = realtime_jobs
            '''
            elif restart_jobs:
                active_jobs = restart_jobs
                global restart_jobs_global
                restart_jobs_global = []
                print "[] -->> queue_restart_jobs() --------------->> "
            ''' 
            #
            '''
            for j in active_jobs:
                # print "[dw] j.queue: ", j.queue # dwang
                if j.queue == "high_prio": # dwang
                    spruce_jobs.append(j)  # dwang
            '''
            #
            temp_jobs = self.jobs.q_get([{'is_runnable':True, 'queue':queue.name} for queue in spruce_queues \
                if queue.name in eq_class['queues']])
            
            for j in temp_jobs:
                if not self.started_jobs.has_key(j.jobid):
                    spruce_jobs.append(j)

            # print "[dw] len(spruce_job): ", len(spruce_jobs) # dwang
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            if spruce_jobs:
                # print "[dw] high_prio[] jobs ..."
                active_jobs = spruce_jobs


            # get the cutoff time for backfilling
            #
            # BRT: should we use 'has_resources' or 'is_active'?  has_resources returns to false once the resource epilogue
            # scripts have finished running while is_active only returns to false once the job (not just the running task) has
            # completely terminated.  the difference is likely to be slight unless the job epilogue scripts are heavy weight.
            temp_jobs = [job for job in self.jobs.q_get([{'has_resources':True}]) if job.queue in eq_class['queues']]
            end_times = []
            for job in temp_jobs:
                # take the max so that jobs which have gone overtime and are being killed
                # continue to cast a small backfilling shadow (we need this for the case
                # that the final job in a drained partition runs overtime -- which otherwise
                # allows things to be backfilled into the drained partition)

                ##*AdjEst*
                if running_job_walltime_prediction:
                    runtime_estimate = float(job.walltime_p)
                else:
                    runtime_estimate = float(job.walltime)

                end_time = max(float(job.starttime) + 60 * runtime_estimate, now + 5*60)
                end_times.append([job.location, end_time])

            for res_name in eq_class['reservations']:
                cur_res = reservations_cache[res_name]

                if not cur_res.cycle:
                    end_time = float(cur_res.start) + float(cur_res.duration)
                else:
                    done_after = float(cur_res.duration) - ((now - float(cur_res.start)) % float(cur_res.cycle))
                    if done_after < 0:
                        done_after += cur_res.cycle
                    end_time = now + done_after
                if cur_res.is_active():
                    for part_name in cur_res.partitions.split(":"):
                        end_times.append([[part_name], end_time])
            #
            #print "END_Times: ", end_times
            #
            
            if not active_jobs:
                continue
            active_jobs.sort(self.utilitycmp)
            

            # now smoosh lots of data together to be passed to the allocator in the system component
            job_location_args = []
            for job in active_jobs:
                forbidden_locations = set()
                pt_blocking_locations = set()

                for res_name in eq_class['reservations']:
                    cur_res = reservations_cache[res_name]
                    if cur_res.overlaps(self.get_current_time(), 60 * float(job.walltime) + SLOP_TIME):
                        forbidden_locations.update(cur_res.partitions.split(':'))
                        if cur_res.block_passthrough:
                            pt_blocking_locations.update(cur_res.partitions.split(':'))
                
                if job.queue:
                    temp_queue = job.queue
                else:
                    temp_queue = default
                ##
                job_location_args.append( 
                    { 'jobid': str(job.jobid), 
                      'nodes': job.nodes,
                      'queue': temp_queue, #job.queue, # dwang
                      'forbidden': list(forbidden_locations),
                      'pt_forbidden': list(pt_blocking_locations),
                      'utility_score': job.score,
                      'walltime': job.walltime,
                      'walltime_p': job.walltime_p, #*AdjEst*
                      'attrs': job.attrs,
                      'user': job.user,
                      'geometry':job.geometry
                    } )
            
            # dwang: checkpt/preempt ########################################## ##########################################
            ''' -> 1. get_preempt_list
                  -> 2. prempt_list_select
                  -> 3. job_checkpt
                  -> 4. job_kill
                  -> 5. partation_update
                -> 6.
            '''
            #best_partition_dict = ComponentProxy("system").find_job_location_wcheckp(job_location_args, end_times,fp_backf,0)
            #preempt_list = ComponentProxy("system").get_preempt_list(job_location_args, best_partition_dict, 0)
            
            # dwang: checkpt/preempt ########################################## ##########################################


            try:
                self.logger.debug("calling from main sched %s", eq_class)
                # dwang:
                #best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times)
                #best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf)
                
                rtj_job = []
                rest_job = []
                if realtime_jobs:
                    best_partition_dict_wcheckp = ComponentProxy("system").find_job_location_wcheckp(job_location_args, end_times,fp_backf,0)
                    #best_partition_dict_wcheckp = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf,0)
                    # 
                    # dwang
                    for jobid in best_partition_dict_wcheckp:
                        job = self.jobs[int(jobid)]
                        if job.user == 'realtime':
                            rtj_job.append(job)
                            # print "---> rtj_id: ", job.jobid
                            # print "     c_time: ", self.get_current_time()
                        else:
                            rest_job.append(job)
                else:
                    best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf)
                    for jobid in best_partition_dict:
                        job = self.jobs[int(jobid)]
                        if job.user == 'realtime':
                            rtj_job.append(job)
                            # print "---> rtj_id: ", job.jobid
                        else:
                            rest_job.append(job)
                # print "----> len_rtj: ", len(rtj_job)
                # print "----> len_rest: ", len(rest_job)
                #
                
                #if realtime_jobs: # and (best_partition_dict_wcheckp != best_partition_dict):
                    #print "[dw_bqsim] best_partition_dict:         ", best_partition_dict
                    #print "[dw_bqsim] best_partition_dict_wcheckpt:", best_partition_dict_wcheckp
                    #if best_partition_dict_wcheckp == best_partition_dict:
                        #print "[dw_bqsim] BEST_PARTITION EQUAL ---- "
                
                # dwang
                ''' esjung: if realtime jobs, do differently 
                    1) should modify find_job_location() function
                       find_job_location(job_location_args, end_times, preemption=true)
                '''
                # print "[dw_diag] testM_3 "
                
                # if preempt > 0 and job_location_args.user == 'realtime':
                # pass
            except:
                self.logger.error("failed to connect to system component")
                self.logger.debug("%s", traceback.format_exc())
                best_partition_dict = {}

            # dwang:
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            preempt_list_full = [] 
            if realtime_jobs: # and (best_partition_dict_wcheckp != best_partition_dict):
                if not best_partition_dict_wcheckp:
                    self.rtj_blocking = True
                    # print "[dw_bgsched] ----> RTJ_blocking  -------------------------------------------- START -------- "
                else:
                    #
                    if self.rtj_blocking:
                        self.rtj_blocking = False
                        # print "[dw_bgsched] ----> RTJ_blocking  ------------------------------------------ STOP ---------- "
                    #
                    # print "[dw_bgsched] ----> check job scheduling w/preempt ... "
                    for jobid in best_partition_dict_wcheckp:
                        job = self.jobs[int(jobid)]
                        fp_backf.write('[rtj_jid] %s:\n' %jobid )

                    preempt_list = ComponentProxy("system").get_preempt_list( best_partition_dict_wcheckp )
                    # print "             ----> preempt_size: ", len(preempt_list)
                    preempt_list_full = preempt_list
                    
                    ##
                    #dsize_restart_pnode = 4.0 / 1024
                    #bw_temp = 1536 * 0.8
                    dsize_restart_pnode = dsize_pnode/1024.0
                    ### bw_temp = bw_temp_write
                    ##
                    rtj_checkp_overhead = 0.0
                    for pjob in preempt_list:
                        # print "---> PRE_job: ", pjob['jobid']
                        fp_backf.write('[rtj_pre] %s:\n' %pjob['jobid'] )
                        #
                        fp_backf.write('[pre] %s:\n' %pjob['jobid'] )
                        # ( --> checkpoint )
                        # ...
                    
                        #
                        util_before = ComponentProxy("system").get_utilization_rate(0)
                        util_before_p = ComponentProxy("system").get_utilization_rate(5)
                        # --> kill job
                        jobid = pjob['jobid']
                        #job = self.jobs[int(jobid)]
                        job = self.jobs[jobid]
                        ### self._kill_job(job, pjob['pname']) # w/o overhead_recording
                        donetime, lefttime, partsize = self._kill_job_wOverhead(job, pjob['pname'], dsize_pnode, bw_temp_read) # w overhead_recording
                        # print "---> PRE_job_killed: ", pjob['jobid']
                        # print "[RE] jid_org: ", job.jobid
                        #
                        bw_temp = min( partsize/128 * 4, bw_temp_write )
                        # print " [RE] partsize: ", partsize
                        # print " [RE] bw_temp: ", bw_temp
                        ## rtj_checkp_overhead += partsize * dsize_restart_pnode / bw_temp
                        if partsize * dsize_restart_pnode / bw_temp >= rtj_checkp_overhead:
                            rtj_checkp_overhead = partsize * dsize_restart_pnode / bw_temp
                            # print "  [rtj_ckp_oh] rtj_overhead_temp: ", partsize * dsize_restart_pnode / bw_temp
                            # print "  [rtj_ckp_oh] rtj_overhead_TOTAL: ", rtj_checkp_overhead
                        #
                            fp_pre_bj.write('%d: %d, %.3f,%.3f, %.3f \n' %(jobid, partsize, partsize * dsize_restart_pnode / bw_temp, 0, rtj_checkp_overhead ) )
                        #
                        ## RESTART -->>
                        cqm.restart_job_add_queue(jobid)
                        # print "[] -->> ADD_queue_restart_jobs() --------------->> "
                        '''
                        global restart_jobs_global
                        restart_jobs_global.append(job)
                        ##
                        cqm.restart_job_add_queue(job.jobid)
                        '''
                        ##
                        self.sync_data() 
                        ##
                        util_after = ComponentProxy("system").get_utilization_rate(0)
                        util_after_p = ComponentProxy("system").get_utilization_rate(5)
                        # print "---> PRE_util_before: ", util_before
                        # print "---> PRE_util_after: ", util_after
                        # print "---> PRE_util_before_p: ", util_before_p
                        # print "---> PRE_util_after_p: ", util_after_p
                    # ...
                    # --> rtj_checkp_overhead_TOTAL
                    # print "[CHECKP] rtj_overhead_TOTAL: ", rtj_checkp_overhead
                    #
                    #
            # dwang
            # --> _start_rt_job()
            # print "[dw_bgsched] ----> len(realtime_jobs): ", len(realtime_jobs)
            ##if realtime_jobs and len(preempt_list_full) == 0:
            if realtime_jobs:
                # print "[dw_bgsched] ----> start job scheduling w/preempt ... "
                for jobid in best_partition_dict_wcheckp:
                    job = self.jobs[int(jobid)]
                    #
                    #fp_backf.write('[rtj_nonpre] %s:\n' %jobid )
                    # print "     rtj_c_time: ", self.get_current_time()
                    #
                    # self._start_rt_job(job, best_partition_dict_wcheckp[jobid])
                    self._start_rt_job_wOverhead(job, best_partition_dict_wcheckp[jobid], rtj_checkp_overhead)
                    ### self._start_rt_job_wOverhead(job, best_partition_dict_wcheckp[jobid], 0)
                    #
                    util_job = ComponentProxy("system").get_utilization_rate(0)
                    util_job_p = ComponentProxy("system").get_utilization_rate(5)
                    # print "---> RTJ_util: ", util_job
                    # print "---> RTJ_util_p: ", util_job_p
                    fp_backf.write('RTJ: %s: %f \n' %( self.get_current_time(), util_job ))
            else:
            ##elif not realtime_jobs:
                #elif len(realtime_jobs)==0 and self.rtj_blocking==False:
                for jobid in best_partition_dict:
                    job = self.jobs[int(jobid)]
                    #
                    # print "     rest_c_time: ", self.get_current_time()
                    #
                    # print "[RE_OVER-115] job.jid: ", jobid
                    rest_overhead = cqm.get_restart_overhead(jobid)
                    # print "[RE_OVER-115] job.restart_over: ", rest_overhead
                    fp_backf.write('RE_OVER-115: %s: %f \n' %( jobid, rest_overhead ))
                    #
                    # self._start_job(job, best_partition_dict[jobid])
                    self._start_job_wOverhead(job, best_partition_dict[jobid], rest_overhead)
                    # self._start_job_wOverhead(job, best_partition_dict[jobid], 0)
                    ##
                    util_job = ComponentProxy("system").get_utilization_rate(0)
                    util_job_p = ComponentProxy("system").get_utilization_rate(5)
                    # print "---> REST_util: ", util_job
                    # print "---> REST_util_p: ", util_job_p
                    fp_backf.write('REST: %s: %f \n' %( self.get_current_time(), util_job ))

            '''
            # dwang:
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            if realtime_jobs: # and (best_partition_dict_wcheckp != best_partition_dict):
                if preempt_list_full: 
                    for pjob in preempt_list_full:
                        jobid = pjob['jobid'] 
                        #job = self.jobs[int(jobid)]
                        #
                        #global restart_jobs_global
                        #restart_jobs_global.append(job)
                        #
                        cqm.restart_job_add_queue(jobid)
                        print "[] -->> ADD_queue_restart_jobs() --------------->> "
                        ## 
                    
                        # ( --> _release_rt_partition() )
                        # ...
                        
                        # ( --> _os_restart() )
                        # ...
            '''
    schedule_jobs_wcheckp_v2 = locking(automatic(schedule_jobs_wcheckp_v2,
        float(get_bgsched_config('schedule_jobs_interval', 10))))

    ##
    # samnickolay:
    def schedule_jobs_wcheckp_v2_sam_v1(self, preempt, fp_backf, fp_pre_bj, dsize_pnode, bw_temp_write, bw_temp_read,
                                 checkp_heur_opt, job_length_type):
        # dwang
        '''look at the queued jobs, and decide which ones to start
        This entire method completes prior to the job's timer starting
        in cqm.
       '''
        #
        # print "[sch] sch_j_v2() ... "
        # print "     c_time: ", self.get_current_time()
        # print "     c_RTJ_blocking: ", self.rtj_blocking
        # print "     c_util: ", ComponentProxy("system").get_utilization_rate(0)
        # print "     c_util_p: ", ComponentProxy("system").get_utilization_rate(5)
        fp_backf.write('--> c_time %s:\n' % self.get_current_time())
        fp_backf.write('--> c_util %f:\n' % ComponentProxy("system").get_utilization_rate(0))
        #
        if not self.active:
            return

        self.sync_data()

        # if we're missing information, don't bother trying to schedule jobs
        if not (self.queues.__oserror__.status and
                    self.jobs.__oserror__.status):
            self.sync_state.Fail()
            return
        self.sync_state.Pass()

        self.component_lock_acquire()
        try:
            # cleanup any reservations which have expired
            for res in self.reservations.values():
                if res.is_over():
                    self.logger.info("reservation %s has ended; removing" %
                                     (res.name))
                    self.logger.info("Res %s/%s: Ending reservation: %r" %
                                     (res.res_id, res.cycle_id, res.name))
                    # dbwriter.log_to_db(None, "ending", "reservation",
                    #        res)
                    del_reservations = self.reservations.q_del([
                        {'name': res.name}])

            # FIXME: this isn't a deepcopy.  it copies references to each reservation in the reservations dict.  is that really
            # sufficient?  --brt
            reservations_cache = self.reservations.copy()
        except:
            # just to make sure we don't keep the lock forever
            self.logger.error("error in schedule_jobs")
        self.component_lock_release()

        # clean up the started_jobs cached data
        # TODO: Make this tunable.
        # started_jobs are jobs that bgsched has prompted cqm to start
        # but may not have had job.run has been completed.
        now = self.get_current_time()
        for job_name in self.started_jobs.keys():
            if (now - self.started_jobs[job_name]) > 60:
                del self.started_jobs[job_name]

        active_queues = []
        spruce_queues = []
        res_queues = set()
        for item in reservations_cache.q_get([{'queue': '*'}]):
            if self.queues.has_key(item.queue):
                if self.queues[item.queue].state == 'running':
                    res_queues.add(item.queue)

        for queue in self.queues.itervalues():
            # print "[dw] queueName: ", queue.name
            if queue.name not in res_queues and queue.state == 'running':
                if queue.policy == "high_prio":
                    spruce_queues.append(queue)
                else:
                    active_queues.append(queue)

        # handle the reservation jobs that might be ready to go
        self._run_reservation_jobs(reservations_cache)

        # figure out stuff about queue equivalence classes
        res_info = {}
        pt_blocking_res = []
        for cur_res in reservations_cache.values():
            res_info[cur_res.name] = cur_res.partitions
            if cur_res.block_passthrough:
                pt_blocking_res.append(cur_res.name)

        # print "[dw] test_1 "
        try:
            ''' esjung: get a job from a system? -- no'''
            equiv = ComponentProxy("system").find_queue_equivalence_classes(
                res_info, [q.name for q in active_queues + spruce_queues],
                pt_blocking_res)
        except:
            self.logger.error("failed to connect to system component")
            return
        # print "[dw] test_2 "

        for eq_class in equiv:
            # recall that is_runnable is True for certain types of holds
            ''' esjung: get jobs with matching specs. '''

            temp_jobs = self.jobs.q_get([{'is_runnable': True, 'queue': queue.name} for queue in active_queues \
                                         if queue.name in eq_class['queues']])

            #
            temp_restart_jobs = self.jobs.q_get([{'is_restarted': True, 'queue': queue.name} for queue in active_queues \
                                                 if queue.name in eq_class['queues']])
            # print "[dw] len(temp_jobs): ", len(temp_jobs)
            active_jobs = []
            spruce_jobs = []
            for j in temp_jobs:
                # print "[dw] temp.queue: ", j.queue # dwang
                if not self.started_jobs.has_key(j.jobid):
                    ''' esjung: add to active job list '''
                    active_jobs.append(j)

            # ''' esjung: follow the same way spruce jobs are implemented '''
            # preempt = 5
            # if preempt > 0:
            #     realtime_jobs = []
            #     for j in active_jobs:
            #         if j.user == 'realtime':
            #             realtime_jobs.append(j)
            # ''' esjung '''

            ###
            # samnickolay
            realtime_jobs = []
            slowdown_threshold = 1.1

            # check if there are any realtime jobs
            for j in active_jobs:
                if j.user == 'realtime':

                    if running_job_walltime_prediction:
                        runtime_estimate = float(j.walltime_p)  # *Adj_Est*
                    else:
                        runtime_estimate = float(j.walltime)

                    if job_length_type == 'walltime':
                        # check for realtime jobs that have reached slowdown threshold and so need to preempt batch job
                        temp_slowdown = (now - j.submittime + runtime_estimate * 60) / (runtime_estimate * 60)
                    elif job_length_type == 'actual':
                        # THIS IS A TEST - TO COMPUTE SLOWDOWN USING ACTUAL RUNTIME, REMOVE THIS WHEN DONE TESTING
                        from bqsim import orig_run_times
                        runtime_org = orig_run_times[str(j.get('jobid'))]
                        temp_slowdown = (now - j.submittime + runtime_org) / runtime_org
                    elif job_length_type == 'predicted':
                        # THIS IS A TEST - TO COMPUTE SLOWDOWN USING PREDICTED RUNTIME, REMOVE THIS WHEN DONE TESTING
                        from bqsim import predicted_run_times
                        predicted_runtime = predicted_run_times[str(j.get('jobid'))]
                        temp_slowdown = (now - j.get('submittime') + predicted_runtime) / predicted_runtime
                    else:
                        # print('Invalid argument for job_length_type: ', job_length_type)
                        exit(-1)

                    wait_time = now - j.submittime
                    wait_time_threshold = 10 * 60.0

                    # if temp_slowdown >= slowdown_threshold:
                    #     pass
                    # if wait_time >= wait_time_threshold:
                    #     pass

                    if temp_slowdown >= slowdown_threshold:
                        realtime_jobs.append(j)

                    # realtime_jobs.append(j)


                        # if temp_slowdown >= slowdown_threshold and wait_time >= wait_time_threshold:
                        #     realtime_jobs.append(j)

            # if there are realtime jobs that have reached threshold then schedule them with preemption
            if realtime_jobs:
                active_jobs = realtime_jobs
            # otherwise schedule the batch and realtime jobs below the slowdown threshold without preemption

            # samnickolay
            ###


            ''' dwang: restart_jobs '''
            # print "[dw] len(realtime_jobs): ", len(realtime_jobs)
            cqm = ComponentProxy("queue-manager")
            restart_count_25 = cqm.get_restart_size()
            # print "[dw] restart_count_221-4: ", restart_count_25
            # print "[dw] len(restart_jobs_221-4): ", len(temp_restart_jobs)
            global restart_jobs_global
            restart_jobs = []  # restart_jobs_global
            # print "[dw] len(global_restart_jobs_221-4): ", len(restart_jobs_global)
            #
            if realtime_jobs:
                active_jobs = realtime_jobs
            '''
            elif restart_jobs:
                active_jobs = restart_jobs
                global restart_jobs_global
                restart_jobs_global = []
                print "[] -->> queue_restart_jobs() --------------->> "
            '''
            #
            '''
            for j in active_jobs:
                # print "[dw] j.queue: ", j.queue # dwang
                if j.queue == "high_prio": # dwang
                    spruce_jobs.append(j)  # dwang
            '''
            #
            temp_jobs = self.jobs.q_get([{'is_runnable': True, 'queue': queue.name} for queue in spruce_queues \
                                         if queue.name in eq_class['queues']])

            for j in temp_jobs:
                if not self.started_jobs.has_key(j.jobid):
                    spruce_jobs.append(j)

            # print "[dw] len(spruce_job): ", len(spruce_jobs) # dwang
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            if spruce_jobs:
                # print "[dw] high_prio[] jobs ..."
                active_jobs = spruce_jobs

            # get the cutoff time for backfilling
            #
            # BRT: should we use 'has_resources' or 'is_active'?  has_resources returns to false once the resource epilogue
            # scripts have finished running while is_active only returns to false once the job (not just the running task) has
            # completely terminated.  the difference is likely to be slight unless the job epilogue scripts are heavy weight.
            temp_jobs = [job for job in self.jobs.q_get([{'has_resources': True}]) if job.queue in eq_class['queues']]
            end_times = []
            for job in temp_jobs:
                # take the max so that jobs which have gone overtime and are being killed
                # continue to cast a small backfilling shadow (we need this for the case
                # that the final job in a drained partition runs overtime -- which otherwise
                # allows things to be backfilled into the drained partition)

                ##*AdjEst*
                if running_job_walltime_prediction:
                    runtime_estimate = float(job.walltime_p)
                else:
                    runtime_estimate = float(job.walltime)

                end_time = max(float(job.starttime) + 60 * runtime_estimate, now + 5 * 60)
                end_times.append([job.location, end_time])

            for res_name in eq_class['reservations']:
                cur_res = reservations_cache[res_name]

                if not cur_res.cycle:
                    end_time = float(cur_res.start) + float(cur_res.duration)
                else:
                    done_after = float(cur_res.duration) - ((now - float(cur_res.start)) % float(cur_res.cycle))
                    if done_after < 0:
                        done_after += cur_res.cycle
                    end_time = now + done_after
                if cur_res.is_active():
                    for part_name in cur_res.partitions.split(":"):
                        end_times.append([[part_name], end_time])
            #
            # print "END_Times: ", end_times
            #

            if not active_jobs:
                continue

            # slowdown_dict = {}
            #
            # for j in active_jobs:
            #     temp_slowdown = (now - j.submittime + float(j.walltime) * 60) / (float(j.walltime) * 60)
            #     slowdown_dict[j.get('jobid')] = temp_slowdown
            #
            # if len(active_jobs) > 1:
            #     pass

            active_jobs.sort(self.utilitycmp)

            # now smoosh lots of data together to be passed to the allocator in the system component
            job_location_args = []
            for job in active_jobs:
                forbidden_locations = set()
                pt_blocking_locations = set()

                for res_name in eq_class['reservations']:
                    cur_res = reservations_cache[res_name]
                    if cur_res.overlaps(self.get_current_time(), 60 * float(job.walltime) + SLOP_TIME):
                        forbidden_locations.update(cur_res.partitions.split(':'))
                        if cur_res.block_passthrough:
                            pt_blocking_locations.update(cur_res.partitions.split(':'))

                if job.queue:
                    temp_queue = job.queue
                else:
                    temp_queue = default
                ##
                job_location_args.append(
                    {'jobid': str(job.jobid),
                     'nodes': job.nodes,
                     'queue': temp_queue,  # job.queue, # dwang
                     'forbidden': list(forbidden_locations),
                     'pt_forbidden': list(pt_blocking_locations),
                     'utility_score': job.score,
                     'walltime': job.walltime,
                     'walltime_p': job.walltime_p,  # *AdjEst*
                     'attrs': job.attrs,
                     'user': job.user,
                     'geometry': job.geometry
                     })

            # dwang: checkpt/preempt ########################################## ##########################################
            ''' -> 1. get_preempt_list
                  -> 2. prempt_list_select
                  -> 3. job_checkpt
                  -> 4. job_kill
                  -> 5. partation_update
                -> 6.
            '''
            # best_partition_dict = ComponentProxy("system").find_job_location_wcheckp(job_location_args, end_times,fp_backf,0)
            # preempt_list = ComponentProxy("system").get_preempt_list(job_location_args, best_partition_dict, 0)

            # dwang: checkpt/preempt ########################################## ##########################################

            # self.logger.debug("calling from main sched %s", eq_class)

            try:
                self.logger.debug("calling from main sched %s", eq_class)
                # dwang:
                # best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times)
                # best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf)

                rtj_job = []
                rest_job = []
                if realtime_jobs:

                    best_partition_dict_wcheckp = ComponentProxy("system").find_job_location_wcheckp_sam_v1(
                        job_location_args, end_times, fp_backf, 0, job_length_type, None, None)


                    # best_partition_dict_wcheckp = ComponentProxy("system").find_job_location_wcheckp(job_location_args,
                    #                                                                                  end_times,
                    #                                                                                  fp_backf, 0)
                    # best_partition_dict_wcheckp = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf,0)
                    #
                    # dwang
                    for jobid in best_partition_dict_wcheckp:
                        job = self.jobs[int(jobid)]
                        if job.user == 'realtime':
                            rtj_job.append(job)
                            # print "---> rtj_id: ", job.jobid
                            # print "     c_time: ", self.get_current_time()
                        else:
                            rest_job.append(job)
                else:
                    best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times,
                                                                                     fp_backf)
                    for jobid in best_partition_dict:
                        job = self.jobs[int(jobid)]
                        if job.user == 'realtime':
                            rtj_job.append(job)
                            # print "---> rtj_id: ", job.jobid
                        else:
                            rest_job.append(job)
                # print "----> len_rtj: ", len(rtj_job)
                # print "----> len_rest: ", len(rest_job)
                #

                # if realtime_jobs: # and (best_partition_dict_wcheckp != best_partition_dict):
                # print "[dw_bqsim] best_partition_dict:         ", best_partition_dict
                # print "[dw_bqsim] best_partition_dict_wcheckpt:", best_partition_dict_wcheckp
                # if best_partition_dict_wcheckp == best_partition_dict:
                # print "[dw_bqsim] BEST_PARTITION EQUAL ---- "

                # dwang
                ''' esjung: if realtime jobs, do differently 
                    1) should modify find_job_location() function
                       find_job_location(job_location_args, end_times, preemption=true)
                '''
                # print "[dw_diag] testM_3 "

                # if preempt > 0 and job_location_args.user == 'realtime':
                # pass
            except:
                self.logger.error("failed to connect to system component")
                self.logger.debug("%s", traceback.format_exc())
                best_partition_dict = {}

            # dwang:
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            preempt_list_full = []
            if realtime_jobs:  # and (best_partition_dict_wcheckp != best_partition_dict):
                if not best_partition_dict_wcheckp:
                    self.rtj_blocking = True
                    # print "[dw_bgsched] ----> RTJ_blocking  -------------------------------------------- START -------- "
                else:
                    #
                    if self.rtj_blocking:
                        self.rtj_blocking = False
                        # print "[dw_bgsched] ----> RTJ_blocking  ------------------------------------------ STOP ---------- "
                    #
                    # print "[dw_bgsched] ----> check job scheduling w/preempt ... "
                    for jobid in best_partition_dict_wcheckp:
                        job = self.jobs[int(jobid)]
                        fp_backf.write('[rtj_jid] %s:\n' % jobid)

                    preempt_list = ComponentProxy("system").get_preempt_list(best_partition_dict_wcheckp)
                    # print "             ----> preempt_size: ", len(preempt_list)
                    preempt_list_full = preempt_list

                    ##
                    # dsize_restart_pnode = 4.0 / 1024
                    # bw_temp = 1536 * 0.8
                    dsize_restart_pnode = dsize_pnode / 1024.0
                    ### bw_temp = bw_temp_write
                    ##
                    rtj_checkp_overhead = 0.0
                    for pjob in preempt_list:
                        # print "---> PRE_job: ", pjob['jobid']
                        fp_backf.write('[rtj_pre] %s:\n' % pjob['jobid'])
                        #
                        fp_backf.write('[pre] %s:\n' % pjob['jobid'])
                        # ( --> checkpoint )
                        # ...

                        #
                        util_before = ComponentProxy("system").get_utilization_rate(0)
                        util_before_p = ComponentProxy("system").get_utilization_rate(5)
                        # --> kill job
                        jobid = pjob['jobid']
                        # job = self.jobs[int(jobid)]
                        job = self.jobs[jobid]
                        ### self._kill_job(job, pjob['pname']) # w/o overhead_recording
                        donetime, lefttime, partsize = self._kill_job_wOverhead(job, pjob['pname'], dsize_pnode,
                                                                                bw_temp_read)  # w overhead_recording
                        # print "---> PRE_job_killed: ", pjob['jobid']
                        # print "[RE] jid_org: ", job.jobid
                        #
                        bw_temp = min(partsize / 128 * 4, bw_temp_write)
                        # print " [RE] partsize: ", partsize
                        # print " [RE] bw_temp: ", bw_temp
                        ## rtj_checkp_overhead += partsize * dsize_restart_pnode / bw_temp
                        if partsize * dsize_restart_pnode / bw_temp >= rtj_checkp_overhead:
                            rtj_checkp_overhead = partsize * dsize_restart_pnode / bw_temp
                            # print "  [rtj_ckp_oh] rtj_overhead_temp: ", partsize * dsize_restart_pnode / bw_temp
                            # print "  [rtj_ckp_oh] rtj_overhead_TOTAL: ", rtj_checkp_overhead
                            #
                            fp_pre_bj.write('%d: %d, %.3f,%.3f, %.3f \n' % (
                            jobid, partsize, partsize * dsize_restart_pnode / bw_temp, 0, rtj_checkp_overhead))
                        #
                        ## RESTART -->>
                        checkp_overhead = partsize * dsize_restart_pnode / bw_temp
                        cqm.restart_job_add_queue_wcheckp(jobid, checkp_overhead)
                        # print "[] -->> ADD_queue_restart_jobs() --------------->> "
                        '''
                        global restart_jobs_global
                        restart_jobs_global.append(job)
                        ##
                        cqm.restart_job_add_queue(job.jobid)
                        '''
                        ##
                        self.sync_data()
                        ##
                        util_after = ComponentProxy("system").get_utilization_rate(0)
                        util_after_p = ComponentProxy("system").get_utilization_rate(5)
                        # print "---> PRE_util_before: ", util_before
                        # print "---> PRE_util_after: ", util_after
                        # print "---> PRE_util_before_p: ", util_before_p
                        # print "---> PRE_util_after_p: ", util_after_p
                    # ...
                    # --> rtj_checkp_overhead_TOTAL
                    # print "[CHECKP] rtj_overhead_TOTAL: ", rtj_checkp_overhead
                    #
                    #
            # dwang
            # --> _start_rt_job()
            # print "[dw_bgsched] ----> len(realtime_jobs): ", len(realtime_jobs)
            ##if realtime_jobs and len(preempt_list_full) == 0:
            if realtime_jobs:
                # print "[dw_bgsched] ----> start job scheduling w/preempt ... "
                for jobid in best_partition_dict_wcheckp:
                    job = self.jobs[int(jobid)]
                    #
                    # fp_backf.write('[rtj_nonpre] %s:\n' %jobid )
                    # print "     rtj_c_time: ", self.get_current_time()
                    #
                    # self._start_rt_job(job, best_partition_dict_wcheckp[jobid])
                    self._start_rt_job_wOverhead(job, best_partition_dict_wcheckp[jobid], rtj_checkp_overhead)
                    ### self._start_rt_job_wOverhead(job, best_partition_dict_wcheckp[jobid], 0)
                    #
                    util_job = ComponentProxy("system").get_utilization_rate(0)
                    util_job_p = ComponentProxy("system").get_utilization_rate(5)
                    # print "---> RTJ_util: ", util_job
                    # print "---> RTJ_util_p: ", util_job_p
                    fp_backf.write('RTJ: %s: %f \n' % (self.get_current_time(), util_job))
            else:
                ##elif not realtime_jobs:
                # elif len(realtime_jobs)==0 and self.rtj_blocking==False:
                for jobid in best_partition_dict:
                    job = self.jobs[int(jobid)]

                    # samnickolay
                    if job.user == 'realtime':
                        # print "     rtj_c_time: ", self.get_current_time()
                        #
                        # self._start_rt_job(job, best_partition_dict_wcheckp[jobid])
                        rtj_checkp_overhead = 0.0
                        self._start_rt_job_wOverhead(job, best_partition_dict[jobid], rtj_checkp_overhead)
                        ### self._start_rt_job_wOverhead(job, best_partition_dict_wcheckp[jobid], 0)
                        #
                        util_job = ComponentProxy("system").get_utilization_rate(0)
                        util_job_p = ComponentProxy("system").get_utilization_rate(5)
                        # print "---> RTJ_util: ", util_job
                        # print "---> RTJ_util_p: ", util_job_p
                        fp_backf.write('RTJ: %s: %f \n' % (self.get_current_time(), util_job))
                    else:
                        #
                        # print "     rest_c_time: ", self.get_current_time()
                        #
                        # print "[RE_OVER-115] job.jid: ", jobid
                        rest_overhead = cqm.get_restart_overhead(jobid)
                        # print "[RE_OVER-115] job.restart_over: ", rest_overhead
                        fp_backf.write('RE_OVER-115: %s: %f \n' % (jobid, rest_overhead))
                        #
                        # self._start_job(job, best_partition_dict[jobid])
                        self._start_job_wOverhead(job, best_partition_dict[jobid], rest_overhead)
                        # self._start_job_wOverhead(job, best_partition_dict[jobid], 0)
                        ##
                        util_job = ComponentProxy("system").get_utilization_rate(0)
                        util_job_p = ComponentProxy("system").get_utilization_rate(5)
                        # print "---> REST_util: ", util_job
                        # print "---> REST_util_p: ", util_job_p
                        fp_backf.write('REST: %s: %f \n' % (self.get_current_time(), util_job))

            '''
            # dwang:
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            if realtime_jobs: # and (best_partition_dict_wcheckp != best_partition_dict):
                if preempt_list_full: 
                    for pjob in preempt_list_full:
                        jobid = pjob['jobid'] 
                        #job = self.jobs[int(jobid)]
                        #
                        #global restart_jobs_global
                        #restart_jobs_global.append(job)
                        #
                        cqm.restart_job_add_queue(jobid)
                        print "[] -->> ADD_queue_restart_jobs() --------------->> "
                        ## 

                        # ( --> _release_rt_partition() )
                        # ...

                        # ( --> _os_restart() )
                        # ...
            '''

    schedule_jobs_wcheckp_v2_sam_v1 = locking(automatic(schedule_jobs_wcheckp_v2_sam_v1,
                                                 float(get_bgsched_config('schedule_jobs_interval', 10))))

    # dwang:
    def schedule_jobs_wcheckp_v2p (self, preempt,fp_backf,fp_pre_bj, dsize_pnode, bw_temp_write, bw_temp_read, checkp_t_internval, checkp_heur_opt):
    # dwang
        '''look at the queued jobs, and decide which ones to start
        This entire method completes prior to the job's timer starting
        in cqm.
       '''
        #
        # print "[sch] sch_j_v2() ... "
        # print "     c_time: ", self.get_current_time()
        # print "     c_RTJ_blocking: ", self.rtj_blocking
        # print "     c_util: ", ComponentProxy("system").get_utilization_rate(0)
        # print "     c_util_p: ", ComponentProxy("system").get_utilization_rate(5)
        fp_backf.write('--> c_time %s:\n' %self.get_current_time() )
        fp_backf.write('--> c_util %f:\n' %ComponentProxy("system").get_utilization_rate(0) )
        #
        if not self.active:
            return

        self.sync_data() 

        # if we're missing information, don't bother trying to schedule jobs
        if not (self.queues.__oserror__.status and 
                self.jobs.__oserror__.status):
            self.sync_state.Fail()
            return
        self.sync_state.Pass()

        self.component_lock_acquire()
        try:
            # cleanup any reservations which have expired
            for res in self.reservations.values():
                if res.is_over():
                    self.logger.info("reservation %s has ended; removing" % 
                            (res.name))
                    self.logger.info("Res %s/%s: Ending reservation: %r" % 
                             (res.res_id, res.cycle_id, res.name))
                    #dbwriter.log_to_db(None, "ending", "reservation", 
                    #        res) 
                    del_reservations = self.reservations.q_del([
                        {'name': res.name}])

            # FIXME: this isn't a deepcopy.  it copies references to each reservation in the reservations dict.  is that really
            # sufficient?  --brt
            reservations_cache = self.reservations.copy()
        except:
            # just to make sure we don't keep the lock forever
            self.logger.error("error in schedule_jobs")
        self.component_lock_release()

        # clean up the started_jobs cached data
        # TODO: Make this tunable.
        # started_jobs are jobs that bgsched has prompted cqm to start
        # but may not have had job.run has been completed.
        now = self.get_current_time()
        for job_name in self.started_jobs.keys():
            if (now - self.started_jobs[job_name]) > 60:
                del self.started_jobs[job_name]

        active_queues = []
        spruce_queues = []
        res_queues = set()
        for item in reservations_cache.q_get([{'queue':'*'}]):
            if self.queues.has_key(item.queue):
                if self.queues[item.queue].state == 'running':
                    res_queues.add(item.queue)

        for queue in self.queues.itervalues():
            # print "[dw] queueName: ", queue.name
            if queue.name not in res_queues and queue.state == 'running':
                if queue.policy == "high_prio":
                    spruce_queues.append(queue)
                else:
                    active_queues.append(queue)

        # handle the reservation jobs that might be ready to go
        self._run_reservation_jobs(reservations_cache)

        # figure out stuff about queue equivalence classes
        res_info = {}
        pt_blocking_res = []
        for cur_res in reservations_cache.values():
            res_info[cur_res.name] = cur_res.partitions
            if cur_res.block_passthrough:
                pt_blocking_res.append(cur_res.name)

        # print "[dw] test_1 "
        try:
            ''' esjung: get a job from a system? -- no'''
            equiv = ComponentProxy("system").find_queue_equivalence_classes(
                    res_info, [q.name for q in active_queues + spruce_queues],
                    pt_blocking_res)
        except:
            self.logger.error("failed to connect to system component")
            return
        # print "[dw] test_2 "

        for eq_class in equiv:
            # recall that is_runnable is True for certain types of holds
            ''' esjung: get jobs with matching specs. '''
            temp_jobs = self.jobs.q_get([{'is_runnable':True, 'queue':queue.name} for queue in active_queues \
                if queue.name in eq_class['queues']])
            
            #
            temp_restart_jobs = self.jobs.q_get([{'is_restarted':True, 'queue':queue.name} for queue in active_queues \
                                         if queue.name in eq_class['queues']])
            # print "[dw] len(temp_jobs): ", len(temp_jobs)
            active_jobs = []
            spruce_jobs = []
            for j in temp_jobs:
                # print "[dw] temp.queue: ", j.queue # dwang
                if not self.started_jobs.has_key(j.jobid):
                    ''' esjung: add to active job list '''
                    active_jobs.append(j)

            ''' esjung: follow the same way spruce jobs are implemented '''
            preempt = 5
            if preempt > 0:
                realtime_jobs = []
                for j in active_jobs:
                    if j.user == 'realtime':
                        realtime_jobs.append(j)
            ''' esjung '''
            
            ''' dwang: restart_jobs '''
            # print "[dw] len(realtime_jobs): ", len(realtime_jobs)
            cqm = ComponentProxy("queue-manager")
            restart_count_25 = cqm.get_restart_size()
            # print "[dw] restart_count_221-4: ", restart_count_25
            # print "[dw] len(restart_jobs_221-4): ", len(temp_restart_jobs)
            global restart_jobs_global
            restart_jobs = [] #restart_jobs_global
            # print "[dw] len(global_restart_jobs_221-4): ", len(restart_jobs_global)
            #
            if realtime_jobs:
                active_jobs = realtime_jobs
            '''
            elif restart_jobs:
                active_jobs = restart_jobs
                global restart_jobs_global
                restart_jobs_global = []
                print "[] -->> queue_restart_jobs() --------------->> "
            ''' 
            #
            '''
            for j in active_jobs:
                # print "[dw] j.queue: ", j.queue # dwang
                if j.queue == "high_prio": # dwang
                    spruce_jobs.append(j)  # dwang
            '''
            #
            temp_jobs = self.jobs.q_get([{'is_runnable':True, 'queue':queue.name} for queue in spruce_queues \
                if queue.name in eq_class['queues']])
            
            for j in temp_jobs:
                if not self.started_jobs.has_key(j.jobid):
                    spruce_jobs.append(j)

            # print "[dw] len(spruce_job): ", len(spruce_jobs) # dwang
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            if spruce_jobs:
                # print "[dw] high_prio[] jobs ..."
                active_jobs = spruce_jobs


            # get the cutoff time for backfilling
            #
            # BRT: should we use 'has_resources' or 'is_active'?  has_resources returns to false once the resource epilogue
            # scripts have finished running while is_active only returns to false once the job (not just the running task) has
            # completely terminated.  the difference is likely to be slight unless the job epilogue scripts are heavy weight.
            temp_jobs = [job for job in self.jobs.q_get([{'has_resources':True}]) if job.queue in eq_class['queues']]
            end_times = []
            for job in temp_jobs:
                # take the max so that jobs which have gone overtime and are being killed
                # continue to cast a small backfilling shadow (we need this for the case
                # that the final job in a drained partition runs overtime -- which otherwise
                # allows things to be backfilled into the drained partition)

                ##*AdjEst*
                if running_job_walltime_prediction:
                    runtime_estimate = float(job.walltime_p)
                else:
                    runtime_estimate = float(job.walltime)

                end_time = max(float(job.starttime) + 60 * runtime_estimate, now + 5*60)
                end_times.append([job.location, end_time])

            for res_name in eq_class['reservations']:
                cur_res = reservations_cache[res_name]

                if not cur_res.cycle:
                    end_time = float(cur_res.start) + float(cur_res.duration)
                else:
                    done_after = float(cur_res.duration) - ((now - float(cur_res.start)) % float(cur_res.cycle))
                    if done_after < 0:
                        done_after += cur_res.cycle
                    end_time = now + done_after
                if cur_res.is_active():
                    for part_name in cur_res.partitions.split(":"):
                        end_times.append([[part_name], end_time])
            #
            #print "END_Times: ", end_times
            #
            
            if not active_jobs:
                continue
            active_jobs.sort(self.utilitycmp)
            

            # now smoosh lots of data together to be passed to the allocator in the system component
            job_location_args = []
            for job in active_jobs:
                forbidden_locations = set()
                pt_blocking_locations = set()

                for res_name in eq_class['reservations']:
                    cur_res = reservations_cache[res_name]
                    if cur_res.overlaps(self.get_current_time(), 60 * float(job.walltime) + SLOP_TIME):
                        forbidden_locations.update(cur_res.partitions.split(':'))
                        if cur_res.block_passthrough:
                            pt_blocking_locations.update(cur_res.partitions.split(':'))
                
                if job.queue:
                    temp_queue = job.queue
                else:
                    temp_queue = default
                ##
                job_location_args.append( 
                    { 'jobid': str(job.jobid), 
                      'nodes': job.nodes,
                      'queue': temp_queue, #job.queue, # dwang
                      'forbidden': list(forbidden_locations),
                      'pt_forbidden': list(pt_blocking_locations),
                      'utility_score': job.score,
                      'walltime': job.walltime,
                      'walltime_p': job.walltime_p, #*AdjEst*
                      'attrs': job.attrs,
                      'user': job.user,
                      'geometry':job.geometry
                    } )
            
            # dwang: checkpt/preempt ########################################## ##########################################
            ''' -> 1. get_preempt_list
                  -> 2. prempt_list_select
                  -> 3. job_checkpt
                  -> 4. job_kill
                  -> 5. partation_update
                -> 6.
            '''
            #best_partition_dict = ComponentProxy("system").find_job_location_wcheckp(job_location_args, end_times,fp_backf,0)
            #preempt_list = ComponentProxy("system").get_preempt_list(job_location_args, best_partition_dict, 0)
            
            # dwang: checkpt/preempt ########################################## ##########################################


            try:
                self.logger.debug("calling from main sched %s", eq_class)
                # dwang:
                #best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times)
                #best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf)
                
                rtj_job = []
                rest_job = []
                if realtime_jobs:
                    best_partition_dict_wcheckp = ComponentProxy("system").find_job_location_wcheckp(job_location_args, end_times,fp_backf,0)
                    #best_partition_dict_wcheckp = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf,0)
                    # 
                    # dwang
                    for jobid in best_partition_dict_wcheckp:
                        job = self.jobs[int(jobid)]
                        if job.user == 'realtime':
                            rtj_job.append(job)
                            # print "---> rtj_id: ", job.jobid
                            # print "     c_time: ", self.get_current_time()
                        else:
                            rest_job.append(job)
                else:
                    best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf)
                    for jobid in best_partition_dict:
                        job = self.jobs[int(jobid)]
                        if job.user == 'realtime':
                            rtj_job.append(job)
                            # print "---> rtj_id: ", job.jobid
                        else:
                            rest_job.append(job)
                # print "----> len_rtj: ", len(rtj_job)
                # print "----> len_rest: ", len(rest_job)
                #
                
                #if realtime_jobs: # and (best_partition_dict_wcheckp != best_partition_dict):
                    #print "[dw_bqsim] best_partition_dict:         ", best_partition_dict
                    #print "[dw_bqsim] best_partition_dict_wcheckpt:", best_partition_dict_wcheckp
                    #if best_partition_dict_wcheckp == best_partition_dict:
                        #print "[dw_bqsim] BEST_PARTITION EQUAL ---- "
                
                # dwang
                ''' esjung: if realtime jobs, do differently 
                    1) should modify find_job_location() function
                       find_job_location(job_location_args, end_times, preemption=true)
                '''
                # print "[dw_diag] testM_3 "
                
                # if preempt > 0 and job_location_args.user == 'realtime':
                # pass
            except:
                self.logger.error("failed to connect to system component")
                self.logger.debug("%s", traceback.format_exc())
                best_partition_dict = {}

            # dwang:
            #checkp_t_internval = 5*60
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            preempt_list_full = [] 
            if realtime_jobs: # and (best_partition_dict_wcheckp != best_partition_dict):
                if not best_partition_dict_wcheckp:
                    self.rtj_blocking = True
                    # print "[dw_bgsched] ----> RTJ_blocking  -------------------------------------------- START -------- "
                else:
                    #
                    if self.rtj_blocking:
                        self.rtj_blocking = False
                        # print "[dw_bgsched] ----> RTJ_blocking  ------------------------------------------ STOP ---------- "
                    #
                    # print "[dw_bgsched] ----> check job scheduling w/preempt ... "
                    for jobid in best_partition_dict_wcheckp:
                        job = self.jobs[int(jobid)]
                        fp_backf.write('[rtj_jid] %s:\n' %jobid )

                    preempt_list = ComponentProxy("system").get_preempt_list( best_partition_dict_wcheckp )
                    # print "             ----> preempt_size: ", len(preempt_list)
                    preempt_list_full = preempt_list
                    
                    ##
                    dsize_restart_pnode = 4.0 / 1024
                    bw_temp = 1536 * 0.8
                    #restart_overhead_temp = kill_partsize * dsize_restart_pnode / bw_temp
                    rtj_checkp_overhead = 0.0
                    for pjob in preempt_list:
                        # print "---> PRE_job: ", pjob['jobid']
                        fp_backf.write('[rtj_pre] %s:\n' %pjob['jobid'] )
                        #
                        fp_backf.write('[pre] %s:\n' %pjob['jobid'] )
                        # ( --> checkpoint )
                        # ...
                    
                        #
                        util_before = ComponentProxy("system").get_utilization_rate(0)
                        util_before_p = ComponentProxy("system").get_utilization_rate(5)
                        # --> kill job
                        jobid = pjob['jobid']
                        #job = self.jobs[int(jobid)]
                        job = self.jobs[jobid]
                        ### self._kill_job(job, pjob['pname']) # w/o overhead_recording
                        donetime, lefttime, partsize = self._kill_job_wOverhead(job, pjob['pname'], dsize_pnode, bw_temp_read) # w_overhead_recording
                        ### donetime, lefttime, partsize = self._kill_job_wPcheckP( job, pjob['pname'], checkp_t_internval, dsize_pnode, bw_temp_read, fp_backf )
                        # print "---> PRE_job_killed: ", pjob['jobid']
                        # print "[RE] jid_org: ", job.jobid
                        #
                        rtj_checkp_overhead += 0 #partsize * dsize_restart_pnode / bw_temp
                        #
                        ## RESTART -->>
                        cqm.restart_job_add_queue(jobid)
                        # print "[] -->> ADD_queue_restart_jobs() --------------->> "
                        '''
                        global restart_jobs_global
                        restart_jobs_global.append(job)
                        ##
                        cqm.restart_job_add_queue(job.jobid)
                        '''
                        ##
                        self.sync_data() 
                        ##
                        util_after = ComponentProxy("system").get_utilization_rate(0)
                        util_after_p = ComponentProxy("system").get_utilization_rate(5)
                        # print "---> PRE_util_before: ", util_before
                        # print "---> PRE_util_after: ", util_after
                        # print "---> PRE_util_before_p: ", util_before_p
                        # print "---> PRE_util_after_p: ", util_after_p
                    # ...
                    # --> rtj_checkp_overhead_TOTAL
                    # print "[CHECKP] rtj_overhead_TOTAL: ", rtj_checkp_overhead
                    #
                    #
            # dwang
            # --> _start_rt_job()
            # print "[dw_bgsched] ----> len(realtime_jobs): ", len(realtime_jobs)
            ##if realtime_jobs and len(preempt_list_full) == 0:
            if realtime_jobs:
                # print "[dw_bgsched] ----> start job scheduling w/preempt ... "
                for jobid in best_partition_dict_wcheckp:
                    job = self.jobs[int(jobid)]
                    #
                    #fp_backf.write('[rtj_nonpre] %s:\n' %jobid )
                    # print "     rtj_c_time: ", self.get_current_time()
                    #
                    ## self._start_rt_job(job, best_partition_dict_wcheckp[jobid])
                    ## self._start_rt_job_wOverhead(job, best_partition_dict_wcheckp[jobid], rtj_checkp_overhead)
                    ##
                    rtj_overheadP = 0 #cqm.get_current_checkp_overheadP( checkp_t_internval, self.get_current_time(), dsize_pnode, bw_temp_write )
                    self._start_rt_job_wOverhead(job, best_partition_dict_wcheckp[jobid], rtj_checkp_overhead + rtj_overheadP )
                    #
                    util_job = ComponentProxy("system").get_utilization_rate(0)
                    util_job_p = ComponentProxy("system").get_utilization_rate(5)
                    # print "---> RTJ_util: ", util_job
                    # print "---> RTJ_util_p: ", util_job_p
                    fp_backf.write('RTJ: %s: %f \n' %( self.get_current_time(), util_job ))
            else:
            ##elif not realtime_jobs:
                #elif len(realtime_jobs)==0 and self.rtj_blocking==False:
                for jobid in best_partition_dict:
                    job = self.jobs[int(jobid)]
                    #
                    # print "     rest_c_time: ", self.get_current_time()
                    rest_overheadP = cqm.get_current_checkp_overheadP_INT( checkp_t_internval, jobid, self.get_current_time(), dsize_pnode, bw_temp_write, fp_backf )
                    #
                    # print "[RE_OVER-115] job.jid: ", jobid
                    rest_overhead = cqm.get_restart_overhead(jobid)
                    # print "[RE_OVER-115] job.restart_over: ", rest_overhead
                    # print "[RE_OVER-115] job.rest_overP: ", rest_overheadP
                    fp_backf.write('RE_OVER-115: %s: %f \n' %( jobid, rest_overhead ))
                    # 
                    fp_pre_bj.write('%d: %d, %.3f,%.3f, %.3f \n' %(int(jobid), 0, rest_overhead, rest_overheadP, rest_overhead + rest_overheadP ) )
                    #
                    ## self._start_job(job, best_partition_dict[jobid])
                    self._start_job_wOverhead(job, best_partition_dict[jobid], rest_overhead + rest_overheadP )
                    ##
                    util_job = ComponentProxy("system").get_utilization_rate(0)
                    util_job_p = ComponentProxy("system").get_utilization_rate(5)
                    # print "---> REST_util: ", util_job
                    # print "---> REST_util_p: ", util_job_p
                    fp_backf.write('REST: %s: %f \n' %( self.get_current_time(), util_job ))

            '''
            # dwang:
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            if realtime_jobs: # and (best_partition_dict_wcheckp != best_partition_dict):
                if preempt_list_full: 
                    for pjob in preempt_list_full:
                        jobid = pjob['jobid'] 
                        #job = self.jobs[int(jobid)]
                        #
                        #global restart_jobs_global
                        #restart_jobs_global.append(job)
                        #
                        cqm.restart_job_add_queue(jobid)
                        print "[] -->> ADD_queue_restart_jobs() --------------->> "
                        ## 
                    
                        # ( --> _release_rt_partition() )
                        # ...
                        
                        # ( --> _os_restart() )
                        # ...
            '''
    schedule_jobs_wcheckp_v2p = locking(automatic(schedule_jobs_wcheckp_v2p,
        float(get_bgsched_config('schedule_jobs_interval', 10))))     

    ###
    # samnickolay
    def schedule_jobs_wcheckp_v2p_sam_v1(self, preempt, fp_backf, fp_pre_bj, dsize_pnode, bw_temp_write, bw_temp_read,
                                      checkp_t_internval, checkp_heur_opt, job_length_type):

        '''look at the queued jobs, and decide which ones to start
        This entire method completes prior to the job's timer starting
        in cqm.
       '''
        #

        # print "[sch] sch_j_v2() ... "
        # print "     c_time: ", self.get_current_time()
        # print "     c_RTJ_blocking: ", self.rtj_blocking
        # print "     c_util: ", ComponentProxy("system").get_utilization_rate(0)
        # print "     c_util_p: ", ComponentProxy("system").get_utilization_rate(5)
        fp_backf.write('--> c_time %s:\n' % self.get_current_time())
        fp_backf.write('--> c_util %f:\n' % ComponentProxy("system").get_utilization_rate(0))
        #
        if not self.active:
            return

        self.sync_data()

        # if we're missing information, don't bother trying to schedule jobs
        if not (self.queues.__oserror__.status and
                    self.jobs.__oserror__.status):
            self.sync_state.Fail()
            return
        self.sync_state.Pass()

        self.component_lock_acquire()
        try:
            # cleanup any reservations which have expired
            for res in self.reservations.values():
                if res.is_over():
                    self.logger.info("reservation %s has ended; removing" %
                                     (res.name))
                    self.logger.info("Res %s/%s: Ending reservation: %r" %
                                     (res.res_id, res.cycle_id, res.name))
                    # dbwriter.log_to_db(None, "ending", "reservation",
                    #        res)
                    del_reservations = self.reservations.q_del([
                        {'name': res.name}])

            # FIXME: this isn't a deepcopy.  it copies references to each reservation in the reservations dict.  is that really
            # sufficient?  --brt
            reservations_cache = self.reservations.copy()
        except:
            # just to make sure we don't keep the lock forever
            self.logger.error("error in schedule_jobs")
        self.component_lock_release()

        # clean up the started_jobs cached data
        # TODO: Make this tunable.
        # started_jobs are jobs that bgsched has prompted cqm to start
        # but may not have had job.run has been completed.
        now = self.get_current_time()
        for job_name in self.started_jobs.keys():
            if (now - self.started_jobs[job_name]) > 60:
                del self.started_jobs[job_name]

        active_queues = []
        spruce_queues = []
        res_queues = set()
        for item in reservations_cache.q_get([{'queue': '*'}]):
            if self.queues.has_key(item.queue):
                if self.queues[item.queue].state == 'running':
                    res_queues.add(item.queue)

        for queue in self.queues.itervalues():
            # print "[dw] queueName: ", queue.name
            if queue.name not in res_queues and queue.state == 'running':
                if queue.policy == "high_prio":
                    spruce_queues.append(queue)
                else:
                    active_queues.append(queue)

        # handle the reservation jobs that might be ready to go
        self._run_reservation_jobs(reservations_cache)

        # figure out stuff about queue equivalence classes
        res_info = {}
        pt_blocking_res = []
        for cur_res in reservations_cache.values():
            res_info[cur_res.name] = cur_res.partitions
            if cur_res.block_passthrough:
                pt_blocking_res.append(cur_res.name)

        # print "[dw] test_1 "
        try:
            ''' esjung: get a job from a system? -- no'''
            equiv = ComponentProxy("system").find_queue_equivalence_classes(
                res_info, [q.name for q in active_queues + spruce_queues],
                pt_blocking_res)
        except:
            self.logger.error("failed to connect to system component")
            return
        # print "[dw] test_2 "

        for eq_class in equiv:
            # recall that is_runnable is True for certain types of holds
            ''' esjung: get jobs with matching specs. '''
            temp_jobs = self.jobs.q_get([{'is_runnable': True, 'queue': queue.name} for queue in active_queues \
                                         if queue.name in eq_class['queues']])

            #
            temp_restart_jobs = self.jobs.q_get([{'is_restarted': True, 'queue': queue.name} for queue in active_queues \
                                                 if queue.name in eq_class['queues']])
            # print "[dw] len(temp_jobs): ", len(temp_jobs)
            active_jobs = []
            spruce_jobs = []
            for j in temp_jobs:
                # print "[dw] temp.queue: ", j.queue # dwang
                if not self.started_jobs.has_key(j.jobid):
                    ''' esjung: add to active job list '''
                    active_jobs.append(j)

            ''' esjung: follow the same way spruce jobs are implemented '''
            # preempt = 5
            # if preempt > 0:
            #     realtime_jobs = []
            #     for j in active_jobs:
            #         if j.user == 'realtime':
            #             realtime_jobs.append(j)
            ''' esjung '''

            # ###
            # # samnickolay
            # realtime_jobs = []
            # realtime_jobs_preempt = []
            # slowdown_threshold = 1.2
            #
            # # check if there are any realtime jobs
            # for j in active_jobs:
            #     if j.user == 'realtime':
            #         realtime_jobs.append(j)
            #
            #         if running_job_walltime_prediction:
            #             runtime_estimate = float(j.walltime_p)  # *Adj_Est*
            #         else:
            #             runtime_estimate = float(j.walltime)
            #
            #         # check for realtime jobs that have reached slowdown threshold and so need to preempt batch job
            #         temp_slowdown = (now + runtime_estimate * 60 - j.submittime) / (runtime_estimate * 60)
            #         if temp_slowdown > slowdown_threshold:
            #             realtime_jobs_preempt.append(j)
            #
            # # if there are realtime jobs that have reached threshold then schedule them with preemption
            # if realtime_jobs_preempt:
            #     active_jobs = realtime_jobs_preempt
            #     realtime_jobs = realtime_jobs_preempt
            # # else schedule the realtime jobs that haven't reached the threshold without preemption
            # elif realtime_jobs:
            #     active_jobs = realtime_jobs
            #     realtime_jobs = []
            #
            # # samnickolay
            # ###

            ###
            # samnickolay
            realtime_jobs = []
            slowdown_threshold = 1.1

            # check if there are any realtime jobs
            for j in active_jobs:
                if j.user == 'realtime':

                    if running_job_walltime_prediction:
                        runtime_estimate = float(j.walltime_p)  # *Adj_Est*
                    else:
                        runtime_estimate = float(j.walltime)

                    if job_length_type == 'walltime':
                        # check for realtime jobs that have reached slowdown threshold and so need to preempt batch job
                        temp_slowdown = (now - j.submittime + runtime_estimate * 60) / (runtime_estimate * 60)
                    elif job_length_type == 'actual':
                        # THIS IS A TEST - TO COMPUTE SLOWDOWN USING ACTUAL RUNTIME, REMOVE THIS WHEN DONE TESTING
                        from bqsim import orig_run_times
                        runtime_org = orig_run_times[str(j.get('jobid'))]
                        temp_slowdown = (now - j.submittime + runtime_org) / runtime_org
                    elif job_length_type == 'predicted':
                        # THIS IS A TEST - TO COMPUTE SLOWDOWN USING PREDICTED RUNTIME, REMOVE THIS WHEN DONE TESTING
                        from bqsim import predicted_run_times
                        predicted_runtime = predicted_run_times[str(j.get('jobid'))]
                        temp_slowdown = (now - j.get('submittime') + predicted_runtime) / predicted_runtime
                    else:
                        # print('Invalid argument for job_length_type: ', job_length_type)
                        exit(-1)

                    wait_time = now - j.submittime
                    wait_time_threshold = 10 * 60.0

                    if temp_slowdown >= slowdown_threshold:
                        pass
                    if wait_time >= wait_time_threshold:
                        pass

                    if temp_slowdown >= slowdown_threshold:
                        realtime_jobs.append(j)

                    # if temp_slowdown >= slowdown_threshold and wait_time >= wait_time_threshold:
                    #     realtime_jobs.append(j)

            # if there are realtime jobs that have reached threshold then schedule them with preemption
            if realtime_jobs:
                active_jobs = realtime_jobs
            # otherwise schedule the batch and realtime jobs below the slowdown threshold without preemption

            # samnickolay
            ###


            ''' dwang: restart_jobs '''
            # print "[dw] len(realtime_jobs): ", len(realtime_jobs)
            cqm = ComponentProxy("queue-manager")
            restart_count_25 = cqm.get_restart_size()
            # print "[dw] restart_count_221-4: ", restart_count_25
            # print "[dw] len(restart_jobs_221-4): ", len(temp_restart_jobs)
            global restart_jobs_global
            restart_jobs = []  # restart_jobs_global
            # print "[dw] len(global_restart_jobs_221-4): ", len(restart_jobs_global)
            #
            # if realtime_jobs:
            #     active_jobs = realtime_jobs
            #
            temp_jobs = self.jobs.q_get([{'is_runnable': True, 'queue': queue.name} for queue in spruce_queues \
                                         if queue.name in eq_class['queues']])

            for j in temp_jobs:
                if not self.started_jobs.has_key(j.jobid):
                    spruce_jobs.append(j)

            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            if spruce_jobs:
                # print "[dw] high_prio[] jobs ..."
                active_jobs = spruce_jobs

            # get the cutoff time for backfilling
            #
            # BRT: should we use 'has_resources' or 'is_active'?  has_resources returns to false once the resource epilogue
            # scripts have finished running while is_active only returns to false once the job (not just the running task) has
            # completely terminated.  the difference is likely to be slight unless the job epilogue scripts are heavy weight.
            temp_jobs = [job for job in self.jobs.q_get([{'has_resources': True}]) if job.queue in eq_class['queues']]
            end_times = []
            for job in temp_jobs:
                # take the max so that jobs which have gone overtime and are being killed
                # continue to cast a small backfilling shadow (we need this for the case
                # that the final job in a drained partition runs overtime -- which otherwise
                # allows things to be backfilled into the drained partition)

                ##*AdjEst*
                if running_job_walltime_prediction:
                    runtime_estimate = float(job.walltime_p)
                else:
                    runtime_estimate = float(job.walltime)

                end_time = max(float(job.starttime) + 60 * runtime_estimate, now + 5 * 60)
                end_times.append([job.location, end_time])

            for res_name in eq_class['reservations']:
                cur_res = reservations_cache[res_name]

                if not cur_res.cycle:
                    end_time = float(cur_res.start) + float(cur_res.duration)
                else:
                    done_after = float(cur_res.duration) - ((now - float(cur_res.start)) % float(cur_res.cycle))
                    if done_after < 0:
                        done_after += cur_res.cycle
                    end_time = now + done_after
                if cur_res.is_active():
                    for part_name in cur_res.partitions.split(":"):
                        end_times.append([[part_name], end_time])
            #
            # print "END_Times: ", end_times
            #

            if not active_jobs:
                continue
            active_jobs.sort(self.utilitycmp)

            # now smoosh lots of data together to be passed to the allocator in the system component
            job_location_args = []
            for job in active_jobs:
                forbidden_locations = set()
                pt_blocking_locations = set()

                for res_name in eq_class['reservations']:
                    cur_res = reservations_cache[res_name]
                    if cur_res.overlaps(self.get_current_time(), 60 * float(job.walltime) + SLOP_TIME):
                        forbidden_locations.update(cur_res.partitions.split(':'))
                        if cur_res.block_passthrough:
                            pt_blocking_locations.update(cur_res.partitions.split(':'))

                if job.queue:
                    temp_queue = job.queue
                else:
                    temp_queue = default
                ##
                job_location_args.append(
                    {'jobid': str(job.jobid),
                     'nodes': job.nodes,
                     'queue': temp_queue,  # job.queue, # dwang
                     'forbidden': list(forbidden_locations),
                     'pt_forbidden': list(pt_blocking_locations),
                     'utility_score': job.score,
                     'walltime': job.walltime,
                     'walltime_p': job.walltime_p,  # *AdjEst*
                     'attrs': job.attrs,
                     'user': job.user,
                     'geometry': job.geometry
                     })

            # dwang: checkpt/preempt ########################################## ##########################################
            ''' -> 1. get_preempt_list
                  -> 2. prempt_list_select
                  -> 3. job_checkpt
                  -> 4. job_kill
                  -> 5. partation_update
                -> 6.
            '''
            # best_partition_dict = ComponentProxy("system").find_job_location_wcheckp(job_location_args, end_times,fp_backf,0)
            # preempt_list = ComponentProxy("system").get_preempt_list(job_location_args, best_partition_dict, 0)

            # dwang: checkpt/preempt ########################################## ##########################################


            try:
                self.logger.debug("calling from main sched %s", eq_class)
                # dwang:

                rtj_job = []
                rest_job = []
                if realtime_jobs:
                    # best_partition_dict_wcheckp = ComponentProxy("system").find_job_location_wcheckp(job_location_args,
                    #                                                                                  end_times,
                    #                                                                                  fp_backf, 0)
                    #
                    best_partition_dict_wcheckp = ComponentProxy("system").find_job_location_wcheckp_sam_v1(job_location_args,
                                                                                                            end_times, fp_backf,
                                                                                                            0, job_length_type,
                                                                                                            checkp_t_internval, None)

                    # dwang
                    for jobid in best_partition_dict_wcheckp:
                        job = self.jobs[int(jobid)]
                        if job.user == 'realtime':
                            rtj_job.append(job)
                            # print "---> rtj_id: ", job.jobid
                            # print "     c_time: ", self.get_current_time()
                        else:
                            rest_job.append(job)
                else:
                    best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times,
                                                                                     fp_backf)
                    for jobid in best_partition_dict:
                        job = self.jobs[int(jobid)]
                        if job.user == 'realtime':
                            rtj_job.append(job)
                            # print "---> rtj_id: ", job.jobid
                        else:
                            rest_job.append(job)
                # print "----> len_rtj: ", len(rtj_job)
                # print "----> len_rest: ", len(rest_job)
                #
                # dwang
                ''' esjung: if realtime jobs, do differently 
                    1) should modify find_job_location() function
                       find_job_location(job_location_args, end_times, preemption=true)
                '''
                # print "[dw_diag] testM_3 "

                # if preempt > 0 and job_location_args.user == 'realtime':
                # pass
            except:
                self.logger.error("failed to connect to system component")
                self.logger.debug("%s", traceback.format_exc())
                best_partition_dict = {}

            # dwang:
            # checkp_t_internval = 5*60
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            preempt_list_full = []
            if realtime_jobs:  # and (best_partition_dict_wcheckp != best_partition_dict):
                if not best_partition_dict_wcheckp:
                    self.rtj_blocking = True
                    # print "[dw_bgsched] ----> RTJ_blocking  -------------------------------------------- START -------- "
                else:
                    #
                    if self.rtj_blocking:
                        self.rtj_blocking = False
                        # print "[dw_bgsched] ----> RTJ_blocking  ------------------------------------------ STOP ---------- "
                    #
                    # print "[dw_bgsched] ----> check job scheduling w/preempt ... "
                    for jobid in best_partition_dict_wcheckp:
                        job = self.jobs[int(jobid)]
                        fp_backf.write('[rtj_jid] %s:\n' % jobid)

                    preempt_list = ComponentProxy("system").get_preempt_list(best_partition_dict_wcheckp)
                    # print "             ----> preempt_size: ", len(preempt_list)
                    preempt_list_full = preempt_list

                    ##
                    dsize_restart_pnode = 4.0 / 1024
                    bw_temp = 1536 * 0.8
                    # restart_overhead_temp = kill_partsize * dsize_restart_pnode / bw_temp
                    rtj_checkp_overhead = 0.0
                    for pjob in preempt_list:
                        # print "---> PRE_job: ", pjob['jobid']
                        fp_backf.write('[rtj_pre] %s:\n' % pjob['jobid'])
                        #
                        fp_backf.write('[pre] %s:\n' % pjob['jobid'])
                        # ( --> checkpoint )
                        # ...

                        #
                        util_before = ComponentProxy("system").get_utilization_rate(0)
                        util_before_p = ComponentProxy("system").get_utilization_rate(5)
                        # --> kill job
                        jobid = pjob['jobid']
                        job = self.jobs[int(jobid)]
                        donetime, lefttime, partsize = self._kill_job_wOverhead(job, pjob['pname'], dsize_pnode,
                                                                                bw_temp_read)  # w_overhead_recording
                        # print "---> PRE_job_killed: ", pjob['jobid']
                        # print "[RE] jid_org: ", job.jobid
                        #
                        rtj_checkp_overhead += 0  # partsize * dsize_restart_pnode / bw_temp
                        #
                        ## RESTART -->>
                        cqm.restart_job_add_queue(jobid)
                        # print "[] -->> ADD_queue_restart_jobs() --------------->> "

                        ##
                        self.sync_data()
                        ##
                        util_after = ComponentProxy("system").get_utilization_rate(0)
                        util_after_p = ComponentProxy("system").get_utilization_rate(5)
                        # print "---> PRE_util_before: ", util_before
                        # print "---> PRE_util_after: ", util_after
                        # print "---> PRE_util_before_p: ", util_before_p
                        # print "---> PRE_util_after_p: ", util_after_p
                    # ...
                    # --> rtj_checkp_overhead_TOTAL
                    # print "[CHECKP] rtj_overhead_TOTAL: ", rtj_checkp_overhead
                    #
                    #
            # dwang
            # --> _start_rt_job()
            # print "[dw_bgsched] ----> len(realtime_jobs): ", len(realtime_jobs)
            if realtime_jobs:
                # print "[dw_bgsched] ----> start job scheduling w/preempt ... "
                for jobid in best_partition_dict_wcheckp:
                    job = self.jobs[int(jobid)]
                    #
                    # print "     rtj_c_time: ", self.get_current_time()
                    #
                    rtj_overheadP = 0  # cqm.get_current_checkp_overheadP( checkp_t_internval, self.get_current_time(), dsize_pnode, bw_temp_write )
                    self._start_rt_job_wOverhead(job, best_partition_dict_wcheckp[jobid],
                                                 rtj_checkp_overhead + rtj_overheadP)
                    #
                    util_job = ComponentProxy("system").get_utilization_rate(0)
                    util_job_p = ComponentProxy("system").get_utilization_rate(5)
                    # print "---> RTJ_util: ", util_job
                    # print "---> RTJ_util_p: ", util_job_p
                    fp_backf.write('RTJ: %s: %f \n' % (self.get_current_time(), util_job))
            else:
                for jobid in best_partition_dict:
                    job = self.jobs[int(jobid)]

                    # samnickolay
                    if job.user == 'realtime':
                        # print "     rtj_c_time: ", self.get_current_time()
                        #
                        rtj_overheadP = 0  # cqm.get_current_checkp_overheadP( checkp_t_internval, self.get_current_time(), dsize_pnode, bw_temp_write )
                        rtj_checkp_overhead = 0
                        self._start_rt_job_wOverhead(job, best_partition_dict[jobid],
                                                     rtj_checkp_overhead + rtj_overheadP)
                        #
                        util_job = ComponentProxy("system").get_utilization_rate(0)
                        util_job_p = ComponentProxy("system").get_utilization_rate(5)
                        # print "---> RTJ_util: ", util_job
                        # print "---> RTJ_util_p: ", util_job_p
                        fp_backf.write('RTJ: %s: %f \n' % (self.get_current_time(), util_job))
                    else:
                        #
                        # print "     rest_c_time: ", self.get_current_time()
                        rest_overheadP = cqm.get_current_checkp_overheadP_INT(checkp_t_internval, jobid,
                                                                              self.get_current_time(), dsize_pnode,
                                                                              bw_temp_write, fp_backf)
                        #
                        # print "[RE_OVER-115] job.jid: ", jobid
                        rest_overhead = cqm.get_restart_overhead(jobid)
                        # print "[RE_OVER-115] job.restart_over: ", rest_overhead
                        # print "[RE_OVER-115] job.rest_overP: ", rest_overheadP
                        fp_backf.write('RE_OVER-115: %s: %f \n' % (jobid, rest_overhead))
                        #
                        fp_pre_bj.write('%d: %d, %.3f,%.3f, %.3f \n' % (
                        int(jobid), 0, rest_overhead, rest_overheadP, rest_overhead + rest_overheadP))
                        #
                        self._start_job_wOverhead(job, best_partition_dict[jobid], rest_overhead + rest_overheadP)
                        ##
                        util_job = ComponentProxy("system").get_utilization_rate(0)
                        util_job_p = ComponentProxy("system").get_utilization_rate(5)
                        # print "---> REST_util: ", util_job
                        # print "---> REST_util_p: ", util_job_p
                        fp_backf.write('REST: %s: %f \n' % (self.get_current_time(), util_job))
    schedule_jobs_wcheckp_v2p_sam_v1 = locking(automatic(schedule_jobs_wcheckp_v2p_sam_v1,
                                                float(get_bgsched_config('schedule_jobs_interval', 10))))

    # samnickolay
    ###

    # dwang: 
    def schedule_jobs_wcheckp_v2p_app (self, preempt,fp_backf,fp_pre_bj, dsize_pnode, bw_temp_write, bw_temp_read, checkp_t_internval_pcent, checkp_heur_opt):
    # dwang
        '''look at the queued jobs, and decide which ones to start
        This entire method completes prior to the job's timer starting
        in cqm.
       '''
        #
        # print "[sch] sch_j_v2p_app() ... "
        # print "     c_time: ", self.get_current_time()
        # print "     c_RTJ_blocking: ", self.rtj_blocking
        # print "     c_util: ", ComponentProxy("system").get_utilization_rate(0)
        # print "     c_util_p: ", ComponentProxy("system").get_utilization_rate(5)
        fp_backf.write('--> c_time %s:\n' %self.get_current_time() )
        fp_backf.write('--> c_util %f:\n' %ComponentProxy("system").get_utilization_rate(0) )
        #
        if not self.active:
            return

        self.sync_data() 

        # if we're missing information, don't bother trying to schedule jobs
        if not (self.queues.__oserror__.status and 
                self.jobs.__oserror__.status):
            self.sync_state.Fail()
            return
        self.sync_state.Pass()

        self.component_lock_acquire()
        try:
            # cleanup any reservations which have expired
            for res in self.reservations.values():
                if res.is_over():
                    self.logger.info("reservation %s has ended; removing" % 
                            (res.name))
                    self.logger.info("Res %s/%s: Ending reservation: %r" % 
                             (res.res_id, res.cycle_id, res.name))
                    #dbwriter.log_to_db(None, "ending", "reservation", 
                    #        res) 
                    del_reservations = self.reservations.q_del([
                        {'name': res.name}])

            # FIXME: this isn't a deepcopy.  it copies references to each reservation in the reservations dict.  is that really
            # sufficient?  --brt
            reservations_cache = self.reservations.copy()
        except:
            # just to make sure we don't keep the lock forever
            self.logger.error("error in schedule_jobs")
        self.component_lock_release()

        # clean up the started_jobs cached data
        # TODO: Make this tunable.
        # started_jobs are jobs that bgsched has prompted cqm to start
        # but may not have had job.run has been completed.
        now = self.get_current_time()
        for job_name in self.started_jobs.keys():
            if (now - self.started_jobs[job_name]) > 60:
                del self.started_jobs[job_name]

        active_queues = []
        spruce_queues = []
        res_queues = set()
        for item in reservations_cache.q_get([{'queue':'*'}]):
            if self.queues.has_key(item.queue):
                if self.queues[item.queue].state == 'running':
                    res_queues.add(item.queue)

        for queue in self.queues.itervalues():
            # print "[dw] queueName: ", queue.name
            if queue.name not in res_queues and queue.state == 'running':
                if queue.policy == "high_prio":
                    spruce_queues.append(queue)
                else:
                    active_queues.append(queue)

        # handle the reservation jobs that might be ready to go
        self._run_reservation_jobs(reservations_cache)

        # figure out stuff about queue equivalence classes
        res_info = {}
        pt_blocking_res = []
        for cur_res in reservations_cache.values():
            res_info[cur_res.name] = cur_res.partitions
            if cur_res.block_passthrough:
                pt_blocking_res.append(cur_res.name)

        # print "[dw] test_1 "
        try:
            ''' esjung: get a job from a system? -- no'''
            equiv = ComponentProxy("system").find_queue_equivalence_classes(
                    res_info, [q.name for q in active_queues + spruce_queues],
                    pt_blocking_res)
        except:
            self.logger.error("failed to connect to system component")
            return
        # print "[dw] test_2 "

        for eq_class in equiv:
            # recall that is_runnable is True for certain types of holds
            ''' esjung: get jobs with matching specs. '''
            temp_jobs = self.jobs.q_get([{'is_runnable':True, 'queue':queue.name} for queue in active_queues \
                if queue.name in eq_class['queues']])
            
            #
            temp_restart_jobs = self.jobs.q_get([{'is_restarted':True, 'queue':queue.name} for queue in active_queues \
                                         if queue.name in eq_class['queues']])
            # print "[dw] len(temp_jobs): ", len(temp_jobs)
            active_jobs = []
            spruce_jobs = []
            for j in temp_jobs:
                # print "[dw] temp.queue: ", j.queue # dwang
                if not self.started_jobs.has_key(j.jobid):
                    ''' esjung: add to active job list '''
                    active_jobs.append(j)

            ''' esjung: follow the same way spruce jobs are implemented '''
            preempt = 5
            if preempt > 0:
                realtime_jobs = []
                for j in active_jobs:
                    if j.user == 'realtime':
                        realtime_jobs.append(j)
            ''' esjung '''
            
            ''' dwang: restart_jobs '''
            # print "[dw] len(realtime_jobs): ", len(realtime_jobs)
            cqm = ComponentProxy("queue-manager")
            restart_count_25 = cqm.get_restart_size()
            # print "[dw] restart_count_221-4: ", restart_count_25
            # print "[dw] len(restart_jobs_221-4): ", len(temp_restart_jobs)
            global restart_jobs_global
            restart_jobs = [] #restart_jobs_global
            # print "[dw] len(global_restart_jobs_221-4): ", len(restart_jobs_global)
            #
            if realtime_jobs:
                active_jobs = realtime_jobs
            '''
            elif restart_jobs:
                active_jobs = restart_jobs
                global restart_jobs_global
                restart_jobs_global = []
                print "[] -->> queue_restart_jobs() --------------->> "
            ''' 
            #
            '''
            for j in active_jobs:
                # print "[dw] j.queue: ", j.queue # dwang
                if j.queue == "high_prio": # dwang
                    spruce_jobs.append(j)  # dwang
            '''
            #
            temp_jobs = self.jobs.q_get([{'is_runnable':True, 'queue':queue.name} for queue in spruce_queues \
                if queue.name in eq_class['queues']])
            
            for j in temp_jobs:
                if not self.started_jobs.has_key(j.jobid):
                    spruce_jobs.append(j)

            # print "[dw] len(spruce_job): ", len(spruce_jobs) # dwang
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            if spruce_jobs:
                # print "[dw] high_prio[] jobs ..."
                active_jobs = spruce_jobs


            # get the cutoff time for backfilling
            #
            # BRT: should we use 'has_resources' or 'is_active'?  has_resources returns to false once the resource epilogue
            # scripts have finished running while is_active only returns to false once the job (not just the running task) has
            # completely terminated.  the difference is likely to be slight unless the job epilogue scripts are heavy weight.
            temp_jobs = [job for job in self.jobs.q_get([{'has_resources':True}]) if job.queue in eq_class['queues']]
            end_times = []
            for job in temp_jobs:
                # take the max so that jobs which have gone overtime and are being killed
                # continue to cast a small backfilling shadow (we need this for the case
                # that the final job in a drained partition runs overtime -- which otherwise
                # allows things to be backfilled into the drained partition)

                ##*AdjEst*
                if running_job_walltime_prediction:
                    runtime_estimate = float(job.walltime_p)
                else:
                    runtime_estimate = float(job.walltime)

                end_time = max(float(job.starttime) + 60 * runtime_estimate, now + 5*60)
                end_times.append([job.location, end_time])

            for res_name in eq_class['reservations']:
                cur_res = reservations_cache[res_name]

                if not cur_res.cycle:
                    end_time = float(cur_res.start) + float(cur_res.duration)
                else:
                    done_after = float(cur_res.duration) - ((now - float(cur_res.start)) % float(cur_res.cycle))
                    if done_after < 0:
                        done_after += cur_res.cycle
                    end_time = now + done_after
                if cur_res.is_active():
                    for part_name in cur_res.partitions.split(":"):
                        end_times.append([[part_name], end_time])
            #
            #print "END_Times: ", end_times
            #
            
            if not active_jobs:
                continue
            active_jobs.sort(self.utilitycmp)
            

            # now smoosh lots of data together to be passed to the allocator in the system component
            job_location_args = []
            for job in active_jobs:
                forbidden_locations = set()
                pt_blocking_locations = set()

                for res_name in eq_class['reservations']:
                    cur_res = reservations_cache[res_name]
                    if cur_res.overlaps(self.get_current_time(), 60 * float(job.walltime) + SLOP_TIME):
                        forbidden_locations.update(cur_res.partitions.split(':'))
                        if cur_res.block_passthrough:
                            pt_blocking_locations.update(cur_res.partitions.split(':'))
                
                if job.queue:
                    temp_queue = job.queue
                else:
                    temp_queue = default
                ##
                job_location_args.append( 
                    { 'jobid': str(job.jobid), 
                      'nodes': job.nodes,
                      'queue': temp_queue, #job.queue, # dwang
                      'forbidden': list(forbidden_locations),
                      'pt_forbidden': list(pt_blocking_locations),
                      'utility_score': job.score,
                      'walltime': job.walltime,
                      'walltime_p': job.walltime_p, #*AdjEst*
                      'attrs': job.attrs,
                      'user': job.user,
                      'geometry':job.geometry
                    } )
            
            # dwang: checkpt/preempt ########################################## ##########################################
            ''' -> 1. get_preempt_list
                  -> 2. prempt_list_select
                  -> 3. job_checkpt
                  -> 4. job_kill
                  -> 5. partation_update
                -> 6.
            '''
            #best_partition_dict = ComponentProxy("system").find_job_location_wcheckp(job_location_args, end_times,fp_backf,0)
            #preempt_list = ComponentProxy("system").get_preempt_list(job_location_args, best_partition_dict, 0)
            
            # dwang: checkpt/preempt ########################################## ##########################################


            try:
                self.logger.debug("calling from main sched %s", eq_class)
                # dwang:
                #best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times)
                #best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf)
                
                rtj_job = []
                rest_job = []
                if realtime_jobs:
                    best_partition_dict_wcheckp = ComponentProxy("system").find_job_location_wcheckp(job_location_args, end_times,fp_backf,0)
                    #best_partition_dict_wcheckp = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf,0)
                    # 
                    # dwang
                    for jobid in best_partition_dict_wcheckp:
                        job = self.jobs[int(jobid)]
                        if job.user == 'realtime':
                            rtj_job.append(job)
                            # print "---> rtj_id: ", job.jobid
                            # print "     c_time: ", self.get_current_time()
                        else:
                            rest_job.append(job)
                else:
                    best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf)
                    for jobid in best_partition_dict:
                        job = self.jobs[int(jobid)]
                        if job.user == 'realtime':
                            rtj_job.append(job)
                            # print "---> rtj_id: ", job.jobid
                        else:
                            rest_job.append(job)
                # print "----> len_rtj: ", len(rtj_job)
                # print "----> len_rest: ", len(rest_job)
                #
                
                #if realtime_jobs: # and (best_partition_dict_wcheckp != best_partition_dict):
                    #print "[dw_bqsim] best_partition_dict:         ", best_partition_dict
                    #print "[dw_bqsim] best_partition_dict_wcheckpt:", best_partition_dict_wcheckp
                    #if best_partition_dict_wcheckp == best_partition_dict:
                        #print "[dw_bqsim] BEST_PARTITION EQUAL ---- "
                
                # dwang
                ''' esjung: if realtime jobs, do differently 
                    1) should modify find_job_location() function
                       find_job_location(job_location_args, end_times, preemption=true)
                '''
                # print "[dw_diag] testM_3 "
                
                # if preempt > 0 and job_location_args.user == 'realtime':
                # pass
            except:
                self.logger.error("failed to connect to system component")
                self.logger.debug("%s", traceback.format_exc())
                best_partition_dict = {}

            # dwang:
            #checkp_t_internval = 5*60
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            preempt_list_full = [] 
            if realtime_jobs: # and (best_partition_dict_wcheckp != best_partition_dict):
                if not best_partition_dict_wcheckp:
                    self.rtj_blocking = True
                    # print "[dw_bgsched] ----> RTJ_blocking  -------------------------------------------- START -------- "
                else:
                    #
                    if self.rtj_blocking:
                        self.rtj_blocking = False
                        # print "[dw_bgsched] ----> RTJ_blocking  ------------------------------------------ STOP ---------- "
                    #
                    # print "[dw_bgsched] ----> check job scheduling w/preempt ... "
                    for jobid in best_partition_dict_wcheckp:
                        job = self.jobs[int(jobid)]
                        fp_backf.write('[rtj_jid] %s:\n' %jobid )

                    preempt_list = ComponentProxy("system").get_preempt_list( best_partition_dict_wcheckp )
                    # print "             ----> preempt_size: ", len(preempt_list)
                    preempt_list_full = preempt_list
                    
                    ##
                    dsize_restart_pnode = 4.0 / 1024
                    bw_temp = 1536 * 0.8
                    #restart_overhead_temp = kill_partsize * dsize_restart_pnode / bw_temp
                    rtj_checkp_overhead = 0.0
                    for pjob in preempt_list:
                        # print "---> PRE_job: ", pjob['jobid']
                        fp_backf.write('[rtj_pre] %s:\n' %pjob['jobid'] )
                        #
                        fp_backf.write('[pre] %s:\n' %pjob['jobid'] )
                        # ( --> checkpoint )
                        # ...
                    
                        #
                        util_before = ComponentProxy("system").get_utilization_rate(0)
                        util_before_p = ComponentProxy("system").get_utilization_rate(5)
                        # --> kill job
                        jobid = pjob['jobid']
                        #job = self.jobs[int(jobid)]
                        job = self.jobs[jobid]
                        ### self._kill_job(job, pjob['pname']) # w/o overhead_recording
                        donetime, lefttime, partsize = self._kill_job_wOverhead(job, pjob['pname'], dsize_pnode, bw_temp_read) # w overhead_recording
                        ### donetime, lefttime, partsize = self._kill_job_wPcheckP_app(job, pjob['pname'], dsize_pnode, bw_temp_read)
                        # print "---> PRE_job_killed: ", pjob['jobid']
                        # print "[RE] jid_org: ", job.jobid
                        #
                        rtj_checkp_overhead += 0 #partsize * dsize_restart_pnode / bw_temp
                        #
                        ## RESTART -->>
                        cqm.restart_job_add_queue(jobid)
                        # print "[] -->> ADD_queue_restart_jobs() --------------->> "
                        '''
                        global restart_jobs_global
                        restart_jobs_global.append(job)
                        ##
                        cqm.restart_job_add_queue(job.jobid)
                        '''
                        ##
                        self.sync_data() 
                        ##
                        util_after = ComponentProxy("system").get_utilization_rate(0)
                        util_after_p = ComponentProxy("system").get_utilization_rate(5)
                        # print "---> PRE_util_before: ", util_before
                        # print "---> PRE_util_after: ", util_after
                        # print "---> PRE_util_before_p: ", util_before_p
                        # print "---> PRE_util_after_p: ", util_after_p
                    # ...
                    # --> rtj_checkp_overhead_TOTAL
                    # print "[CHECKP] rtj_overhead_TOTAL: ", rtj_checkp_overhead
                    #
                    #
            # dwang
            # --> _start_rt_job()
            # print "[dw_bgsched] ----> len(realtime_jobs): ", len(realtime_jobs)
            ##if realtime_jobs and len(preempt_list_full) == 0:
            if realtime_jobs:
                # print "[dw_bgsched] ----> start job scheduling w/preempt ... "
                for jobid in best_partition_dict_wcheckp:
                    job = self.jobs[int(jobid)]
                    #
                    #fp_backf.write('[rtj_nonpre] %s:\n' %jobid )
                    # print "     rtj_c_time: ", self.get_current_time()
                    #
                    ## self._start_rt_job(job, best_partition_dict_wcheckp[jobid])
                    ## self._start_rt_job_wOverhead(job, best_partition_dict_wcheckp[jobid], rtj_checkp_overhead)
                    ##
                    rtj_overheadP = 0 # cqm.get_current_checkp_overheadP( checkp_t_internval, self.get_current_time(), dsize_pnode, bw_temp_write )
                    self._start_rt_job_wOverhead(job, best_partition_dict_wcheckp[jobid], rtj_checkp_overhead + rtj_overheadP )
                    #
                    util_job = ComponentProxy("system").get_utilization_rate(0)
                    util_job_p = ComponentProxy("system").get_utilization_rate(5)
                    # print "---> RTJ_util: ", util_job
                    # print "---> RTJ_util_p: ", util_job_p
                    fp_backf.write('RTJ: %s: %f \n' %( self.get_current_time(), util_job ))
            else:
            ##elif not realtime_jobs:
                #elif len(realtime_jobs)==0 and self.rtj_blocking==False:
                for jobid in best_partition_dict:
                    job = self.jobs[int(jobid)]
                    #
                    # print "     rest_c_time: ", self.get_current_time()
                    ### rest_overheadP = cqm.get_current_checkp_overheadP( checkp_t_internval, self.get_current_time(), dsize_pnode, bw_temp_write )
                    rest_overheadP = cqm.get_current_checkp_overheadP_app(jobid, self.get_current_time(), dsize_pnode, bw_temp_write )
                    #
                    # print "[RE_OVER-115] job.jid: ", jobid
                    rest_overhead = cqm.get_restart_overhead(jobid)
                    # print "[RE_OVER-115] job.restart_over: ", rest_overhead
                    fp_backf.write('RE_OVER-115: %s: %f \n' %( jobid, rest_overhead ))
                    #
                    fp_pre_bj.write('%d: %d, %.3f,%.3f, %.3f \n' %(int(jobid), 0, rest_overhead, rest_overheadP, rest_overhead + rest_overheadP ) )
                    #
                    #
                    ## self._start_job(job, best_partition_dict[jobid])
                    self._start_job_wOverhead(job, best_partition_dict[jobid], rest_overhead + rest_overheadP )
                    ##
                    util_job = ComponentProxy("system").get_utilization_rate(0)
                    util_job_p = ComponentProxy("system").get_utilization_rate(5)
                    # print "---> REST_util: ", util_job
                    # print "---> REST_util_p: ", util_job_p
                    fp_backf.write('REST: %s: %f \n' %( self.get_current_time(), util_job ))

            '''
            # dwang:
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            if realtime_jobs: # and (best_partition_dict_wcheckp != best_partition_dict):
                if preempt_list_full: 
                    for pjob in preempt_list_full:
                        jobid = pjob['jobid'] 
                        #job = self.jobs[int(jobid)]
                        #
                        #global restart_jobs_global
                        #restart_jobs_global.append(job)
                        #
                        cqm.restart_job_add_queue(jobid)
                        print "[] -->> ADD_queue_restart_jobs() --------------->> "
                        ## 
                    
                        # ( --> _release_rt_partition() )
                        # ...
                        
                        # ( --> _os_restart() )
                        # ...
            '''
    schedule_jobs_wcheckp_v2p_app = locking(automatic(schedule_jobs_wcheckp_v2p_app,
        float(get_bgsched_config('schedule_jobs_interval', 10))))    

    ###
    # samnickolay
    def schedule_jobs_wcheckp_v2p_app_sam_v1(self, preempt, fp_backf, fp_pre_bj, dsize_pnode, bw_temp_write, bw_temp_read,
                                      checkp_heur_opt, job_length_type, checkp_overhead_percent):
        # dwang
        '''look at the queued jobs, and decide which ones to start
        This entire method completes prior to the job's timer starting
        in cqm.
       '''
        #
        # print "[sch] sch_j_v2p_app() ... "
        # print "     c_time: ", self.get_current_time()
        # print "     c_RTJ_blocking: ", self.rtj_blocking
        # print "     c_util: ", ComponentProxy("system").get_utilization_rate(0)
        # print "     c_util_p: ", ComponentProxy("system").get_utilization_rate(5)
        fp_backf.write('--> c_time %s:\n' % self.get_current_time())
        fp_backf.write('--> c_util %f:\n' % ComponentProxy("system").get_utilization_rate(0))
        #
        if not self.active:
            return

        self.sync_data()

        # if we're missing information, don't bother trying to schedule jobs
        if not (self.queues.__oserror__.status and
                    self.jobs.__oserror__.status):
            self.sync_state.Fail()
            return
        self.sync_state.Pass()

        self.component_lock_acquire()
        try:
            # cleanup any reservations which have expired
            for res in self.reservations.values():
                if res.is_over():
                    self.logger.info("reservation %s has ended; removing" %
                                     (res.name))
                    self.logger.info("Res %s/%s: Ending reservation: %r" %
                                     (res.res_id, res.cycle_id, res.name))
                    # dbwriter.log_to_db(None, "ending", "reservation",
                    #        res)
                    del_reservations = self.reservations.q_del([
                        {'name': res.name}])

            # FIXME: this isn't a deepcopy.  it copies references to each reservation in the reservations dict.  is that really
            # sufficient?  --brt
            reservations_cache = self.reservations.copy()
        except:
            # just to make sure we don't keep the lock forever
            self.logger.error("error in schedule_jobs")
        self.component_lock_release()

        # clean up the started_jobs cached data
        # TODO: Make this tunable.
        # started_jobs are jobs that bgsched has prompted cqm to start
        # but may not have had job.run has been completed.
        now = self.get_current_time()
        for job_name in self.started_jobs.keys():
            if (now - self.started_jobs[job_name]) > 60:
                del self.started_jobs[job_name]

        active_queues = []
        spruce_queues = []
        res_queues = set()
        for item in reservations_cache.q_get([{'queue': '*'}]):
            if self.queues.has_key(item.queue):
                if self.queues[item.queue].state == 'running':
                    res_queues.add(item.queue)

        for queue in self.queues.itervalues():
            # print "[dw] queueName: ", queue.name
            if queue.name not in res_queues and queue.state == 'running':
                if queue.policy == "high_prio":
                    spruce_queues.append(queue)
                else:
                    active_queues.append(queue)

        # handle the reservation jobs that might be ready to go
        self._run_reservation_jobs(reservations_cache)

        # figure out stuff about queue equivalence classes
        res_info = {}
        pt_blocking_res = []
        for cur_res in reservations_cache.values():
            res_info[cur_res.name] = cur_res.partitions
            if cur_res.block_passthrough:
                pt_blocking_res.append(cur_res.name)

        # print "[dw] test_1 "
        try:
            ''' esjung: get a job from a system? -- no'''
            equiv = ComponentProxy("system").find_queue_equivalence_classes(
                res_info, [q.name for q in active_queues + spruce_queues],
                pt_blocking_res)
        except:
            self.logger.error("failed to connect to system component")
            return
        # print "[dw] test_2 "

        for eq_class in equiv:
            # recall that is_runnable is True for certain types of holds
            ''' esjung: get jobs with matching specs. '''
            temp_jobs = self.jobs.q_get([{'is_runnable': True, 'queue': queue.name} for queue in active_queues \
                                         if queue.name in eq_class['queues']])

            #
            temp_restart_jobs = self.jobs.q_get([{'is_restarted': True, 'queue': queue.name} for queue in active_queues \
                                                 if queue.name in eq_class['queues']])
            # print "[dw] len(temp_jobs): ", len(temp_jobs)
            active_jobs = []
            spruce_jobs = []
            for j in temp_jobs:
                # print "[dw] temp.queue: ", j.queue # dwang
                if not self.started_jobs.has_key(j.jobid):
                    ''' esjung: add to active job list '''
                    active_jobs.append(j)

            # ''' esjung: follow the same way spruce jobs are implemented '''
            # preempt = 5
            # if preempt > 0:
            #     realtime_jobs = []
            #     for j in active_jobs:
            #         if j.user == 'realtime':
            #             realtime_jobs.append(j)
            # ''' esjung '''

            ###
            # samnickolay
            realtime_jobs = []
            slowdown_threshold = 1.1

            # check if there are any realtime jobs
            for j in active_jobs:
                if j.user == 'realtime':

                    if running_job_walltime_prediction:
                        runtime_estimate = float(j.walltime_p)  # *Adj_Est*
                    else:
                        runtime_estimate = float(j.walltime)

                    if job_length_type == 'walltime':
                        # check for realtime jobs that have reached slowdown threshold and so need to preempt batch job
                        temp_slowdown = (now - j.submittime + runtime_estimate * 60) / (runtime_estimate * 60)
                    elif job_length_type == 'actual':
                        # THIS IS A TEST - TO COMPUTE SLOWDOWN USING ACTUAL RUNTIME, REMOVE THIS WHEN DONE TESTING
                        from bqsim import orig_run_times
                        runtime_org = orig_run_times[str(j.get('jobid'))]
                        temp_slowdown = (now - j.submittime + runtime_org) / runtime_org
                    elif job_length_type == 'predicted':
                        # THIS IS A TEST - TO COMPUTE SLOWDOWN USING PREDICTED RUNTIME, REMOVE THIS WHEN DONE TESTING
                        from bqsim import predicted_run_times
                        predicted_runtime = predicted_run_times[str(j.get('jobid'))]
                        temp_slowdown = (now - j.get('submittime') + predicted_runtime) / predicted_runtime
                    else:
                        # print('Invalid argument for job_length_type: ', job_length_type)
                        exit(-1)

                    wait_time = now - j.submittime
                    wait_time_threshold = 10 * 60.0

                    if temp_slowdown >= slowdown_threshold:
                        pass
                    if wait_time >= wait_time_threshold:
                        pass

                    if temp_slowdown >= slowdown_threshold:
                        realtime_jobs.append(j)

                        # if temp_slowdown >= slowdown_threshold and wait_time >= wait_time_threshold:
                        #     realtime_jobs.append(j)

            # if there are realtime jobs that have reached threshold then schedule them with preemption
            if realtime_jobs:
                active_jobs = realtime_jobs
                # otherwise schedule the batch and realtime jobs below the slowdown threshold without preemption

            # samnickolay
            ###

            ''' dwang: restart_jobs '''
            # print "[dw] len(realtime_jobs): ", len(realtime_jobs)
            cqm = ComponentProxy("queue-manager")
            restart_count_25 = cqm.get_restart_size()
            # print "[dw] restart_count_221-4: ", restart_count_25
            # print "[dw] len(restart_jobs_221-4): ", len(temp_restart_jobs)
            global restart_jobs_global
            restart_jobs = []  # restart_jobs_global
            # print "[dw] len(global_restart_jobs_221-4): ", len(restart_jobs_global)
            #
            if realtime_jobs:
                active_jobs = realtime_jobs
            '''
            elif restart_jobs:
                active_jobs = restart_jobs
                global restart_jobs_global
                restart_jobs_global = []
                print "[] -->> queue_restart_jobs() --------------->> "
            '''
            #
            '''
            for j in active_jobs:
                # print "[dw] j.queue: ", j.queue # dwang
                if j.queue == "high_prio": # dwang
                    spruce_jobs.append(j)  # dwang
            '''
            #
            temp_jobs = self.jobs.q_get([{'is_runnable': True, 'queue': queue.name} for queue in spruce_queues \
                                         if queue.name in eq_class['queues']])

            for j in temp_jobs:
                if not self.started_jobs.has_key(j.jobid):
                    spruce_jobs.append(j)

            # print "[dw] len(spruce_job): ", len(spruce_jobs) # dwang
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            if spruce_jobs:
                # print "[dw] high_prio[] jobs ..."
                active_jobs = spruce_jobs

            # get the cutoff time for backfilling
            #
            # BRT: should we use 'has_resources' or 'is_active'?  has_resources returns to false once the resource epilogue
            # scripts have finished running while is_active only returns to false once the job (not just the running task) has
            # completely terminated.  the difference is likely to be slight unless the job epilogue scripts are heavy weight.
            temp_jobs = [job for job in self.jobs.q_get([{'has_resources': True}]) if job.queue in eq_class['queues']]
            end_times = []
            for job in temp_jobs:
                # take the max so that jobs which have gone overtime and are being killed
                # continue to cast a small backfilling shadow (we need this for the case
                # that the final job in a drained partition runs overtime -- which otherwise
                # allows things to be backfilled into the drained partition)

                ##*AdjEst*
                if running_job_walltime_prediction:
                    runtime_estimate = float(job.walltime_p)
                else:
                    runtime_estimate = float(job.walltime)

                end_time = max(float(job.starttime) + 60 * runtime_estimate, now + 5 * 60)
                end_times.append([job.location, end_time])

            for res_name in eq_class['reservations']:
                cur_res = reservations_cache[res_name]

                if not cur_res.cycle:
                    end_time = float(cur_res.start) + float(cur_res.duration)
                else:
                    done_after = float(cur_res.duration) - ((now - float(cur_res.start)) % float(cur_res.cycle))
                    if done_after < 0:
                        done_after += cur_res.cycle
                    end_time = now + done_after
                if cur_res.is_active():
                    for part_name in cur_res.partitions.split(":"):
                        end_times.append([[part_name], end_time])
            #
            # print "END_Times: ", end_times
            #

            if not active_jobs:
                continue
            active_jobs.sort(self.utilitycmp)

            # now smoosh lots of data together to be passed to the allocator in the system component
            job_location_args = []
            for job in active_jobs:
                forbidden_locations = set()
                pt_blocking_locations = set()

                for res_name in eq_class['reservations']:
                    cur_res = reservations_cache[res_name]
                    if cur_res.overlaps(self.get_current_time(), 60 * float(job.walltime) + SLOP_TIME):
                        forbidden_locations.update(cur_res.partitions.split(':'))
                        if cur_res.block_passthrough:
                            pt_blocking_locations.update(cur_res.partitions.split(':'))

                if job.queue:
                    temp_queue = job.queue
                else:
                    temp_queue = default
                ##
                job_location_args.append(
                    {'jobid': str(job.jobid),
                     'nodes': job.nodes,
                     'queue': temp_queue,  # job.queue, # dwang
                     'forbidden': list(forbidden_locations),
                     'pt_forbidden': list(pt_blocking_locations),
                     'utility_score': job.score,
                     'walltime': job.walltime,
                     'walltime_p': job.walltime_p,  # *AdjEst*
                     'attrs': job.attrs,
                     'user': job.user,
                     'geometry': job.geometry
                     })

            # dwang: checkpt/preempt ########################################## ##########################################
            ''' -> 1. get_preempt_list
                  -> 2. prempt_list_select
                  -> 3. job_checkpt
                  -> 4. job_kill
                  -> 5. partation_update
                -> 6.
            '''
            # best_partition_dict = ComponentProxy("system").find_job_location_wcheckp(job_location_args, end_times,fp_backf,0)
            # preempt_list = ComponentProxy("system").get_preempt_list(job_location_args, best_partition_dict, 0)

            # dwang: checkpt/preempt ########################################## ##########################################


            try:
                self.logger.debug("calling from main sched %s", eq_class)
                # dwang:
                # best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times)
                # best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf)

                rtj_job = []
                rest_job = []
                if realtime_jobs:
                    # best_partition_dict_wcheckp = ComponentProxy("system").find_job_location_wcheckp(job_location_args,
                    #                                                                                  end_times,
                    #                                                                                  fp_backf, 0)
                    # best_partition_dict_wcheckp = ComponentProxy("system").find_job_location(job_location_args, end_times,fp_backf,0)
                    #
                    best_partition_dict_wcheckp = ComponentProxy("system").find_job_location_wcheckp_sam_v1(
                        job_location_args,
                        end_times,
                        fp_backf, 0, job_length_type, None, checkp_overhead_percent)

                    # dwang
                    for jobid in best_partition_dict_wcheckp:
                        job = self.jobs[int(jobid)]
                        if job.user == 'realtime':
                            rtj_job.append(job)
                            # print "---> rtj_id: ", job.jobid
                            # print "     c_time: ", self.get_current_time()
                        else:
                            rest_job.append(job)
                else:
                    best_partition_dict = ComponentProxy("system").find_job_location(job_location_args, end_times,
                                                                                     fp_backf)
                    for jobid in best_partition_dict:
                        job = self.jobs[int(jobid)]
                        if job.user == 'realtime':
                            rtj_job.append(job)
                            # print "---> rtj_id: ", job.jobid
                        else:
                            rest_job.append(job)
                # print "----> len_rtj: ", len(rtj_job)
                # print "----> len_rest: ", len(rest_job)
                #

                # if realtime_jobs: # and (best_partition_dict_wcheckp != best_partition_dict):
                # print "[dw_bqsim] best_partition_dict:         ", best_partition_dict
                # print "[dw_bqsim] best_partition_dict_wcheckpt:", best_partition_dict_wcheckp
                # if best_partition_dict_wcheckp == best_partition_dict:
                # print "[dw_bqsim] BEST_PARTITION EQUAL ---- "

                # dwang
                ''' esjung: if realtime jobs, do differently 
                    1) should modify find_job_location() function
                       find_job_location(job_location_args, end_times, preemption=true)
                '''
                # print "[dw_diag] testM_3 "

                # if preempt > 0 and job_location_args.user == 'realtime':
                # pass
            except:
                self.logger.error("failed to connect to system component")
                self.logger.debug("%s", traceback.format_exc())
                best_partition_dict = {}

            # dwang:
            # checkp_t_internval = 5*60
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            preempt_list_full = []
            if realtime_jobs:  # and (best_partition_dict_wcheckp != best_partition_dict):
                if not best_partition_dict_wcheckp:
                    self.rtj_blocking = True
                    # print "[dw_bgsched] ----> RTJ_blocking  -------------------------------------------- START -------- "
                else:
                    #
                    if self.rtj_blocking:
                        self.rtj_blocking = False
                        # print "[dw_bgsched] ----> RTJ_blocking  ------------------------------------------ STOP ---------- "
                    #
                    # print "[dw_bgsched] ----> check job scheduling w/preempt ... "
                    for jobid in best_partition_dict_wcheckp:
                        job = self.jobs[int(jobid)]
                        fp_backf.write('[rtj_jid] %s:\n' % jobid)

                    preempt_list = ComponentProxy("system").get_preempt_list(best_partition_dict_wcheckp)
                    # print "             ----> preempt_size: ", len(preempt_list)
                    preempt_list_full = preempt_list

                    ##
                    dsize_restart_pnode = 4.0 / 1024
                    bw_temp = 1536 * 0.8
                    # restart_overhead_temp = kill_partsize * dsize_restart_pnode / bw_temp
                    rtj_checkp_overhead = 0.0
                    for pjob in preempt_list:
                        # print "---> PRE_job: ", pjob['jobid']
                        fp_backf.write('[rtj_pre] %s:\n' % pjob['jobid'])
                        #
                        fp_backf.write('[pre] %s:\n' % pjob['jobid'])
                        # ( --> checkpoint )
                        # ...

                        #
                        util_before = ComponentProxy("system").get_utilization_rate(0)
                        util_before_p = ComponentProxy("system").get_utilization_rate(5)
                        # --> kill job
                        jobid = pjob['jobid']
                        # job = self.jobs[int(jobid)]
                        job = self.jobs[jobid]
                        ### self._kill_job(job, pjob['pname']) # w/o overhead_recording
                        donetime, lefttime, partsize = self._kill_job_wOverhead(job, pjob['pname'], dsize_pnode,
                                                                                bw_temp_read)  # w overhead_recording
                        ### donetime, lefttime, partsize = self._kill_job_wPcheckP_app(job, pjob['pname'], dsize_pnode, bw_temp_read)
                        # print "---> PRE_job_killed: ", pjob['jobid']
                        # print "[RE] jid_org: ", job.jobid
                        #
                        rtj_checkp_overhead += 0  # partsize * dsize_restart_pnode / bw_temp
                        #
                        ## RESTART -->>
                        cqm.restart_job_add_queue(jobid)
                        # print "[] -->> ADD_queue_restart_jobs() --------------->> "
                        '''
                        global restart_jobs_global
                        restart_jobs_global.append(job)
                        ##
                        cqm.restart_job_add_queue(job.jobid)
                        '''
                        ##
                        self.sync_data()
                        ##
                        util_after = ComponentProxy("system").get_utilization_rate(0)
                        util_after_p = ComponentProxy("system").get_utilization_rate(5)
                        # print "---> PRE_util_before: ", util_before
                        # print "---> PRE_util_after: ", util_after
                        # print "---> PRE_util_before_p: ", util_before_p
                        # print "---> PRE_util_after_p: ", util_after_p
                    # ...
                    # --> rtj_checkp_overhead_TOTAL
                    # print "[CHECKP] rtj_overhead_TOTAL: ", rtj_checkp_overhead
                    #
                    #
            # dwang
            # --> _start_rt_job()
            # print "[dw_bgsched] ----> len(realtime_jobs): ", len(realtime_jobs)
            ##if realtime_jobs and len(preempt_list_full) == 0:
            if realtime_jobs:
                # print "[dw_bgsched] ----> start job scheduling w/preempt ... "
                for jobid in best_partition_dict_wcheckp:
                    job = self.jobs[int(jobid)]
                    #
                    # fp_backf.write('[rtj_nonpre] %s:\n' %jobid )
                    # print "     rtj_c_time: ", self.get_current_time()
                    #
                    ## self._start_rt_job(job, best_partition_dict_wcheckp[jobid])
                    ## self._start_rt_job_wOverhead(job, best_partition_dict_wcheckp[jobid], rtj_checkp_overhead)
                    ##
                    rtj_overheadP = 0  # cqm.get_current_checkp_overheadP( checkp_t_internval, self.get_current_time(), dsize_pnode, bw_temp_write )
                    self._start_rt_job_wOverhead(job, best_partition_dict_wcheckp[jobid],
                                                 rtj_checkp_overhead + rtj_overheadP)
                    #
                    util_job = ComponentProxy("system").get_utilization_rate(0)
                    util_job_p = ComponentProxy("system").get_utilization_rate(5)
                    # print "---> RTJ_util: ", util_job
                    # print "---> RTJ_util_p: ", util_job_p
                    fp_backf.write('RTJ: %s: %f \n' % (self.get_current_time(), util_job))
            else:
                ##elif not realtime_jobs:
                # elif len(realtime_jobs)==0 and self.rtj_blocking==False:
                for jobid in best_partition_dict:
                    job = self.jobs[int(jobid)]

                    # samnickolay
                    if job.user == 'realtime':
                        # print "     rtj_c_time: ", self.get_current_time()
                        #
                        rtj_overheadP = 0  # cqm.get_current_checkp_overheadP( checkp_t_internval, self.get_current_time(), dsize_pnode, bw_temp_write )
                        rtj_checkp_overhead = 0
                        self._start_rt_job_wOverhead(job, best_partition_dict[jobid],
                                                     rtj_checkp_overhead + rtj_overheadP)
                        #
                        util_job = ComponentProxy("system").get_utilization_rate(0)
                        util_job_p = ComponentProxy("system").get_utilization_rate(5)
                        # print "---> RTJ_util: ", util_job
                        # print "---> RTJ_util_p: ", util_job_p
                        fp_backf.write('RTJ: %s: %f \n' % (self.get_current_time(), util_job))
                    else:
                        #
                        # print "     rest_c_time: ", self.get_current_time()
                        ### rest_overheadP = cqm.get_current_checkp_overheadP( checkp_t_internval, self.get_current_time(), dsize_pnode, bw_temp_write )
                        rest_overheadP = cqm.get_current_checkp_overheadP_app(jobid, self.get_current_time(), dsize_pnode,
                                                                              bw_temp_write)
                        #
                        # print "[RE_OVER-115] job.jid: ", jobid
                        rest_overhead = cqm.get_restart_overhead(jobid)
                        # print "[RE_OVER-115] job.restart_over: ", rest_overhead
                        fp_backf.write('RE_OVER-115: %s: %f \n' % (jobid, rest_overhead))
                        #
                        fp_pre_bj.write('%d: %d, %.3f,%.3f, %.3f \n' % (
                        int(jobid), 0, rest_overhead, rest_overheadP, rest_overhead + rest_overheadP))
                        #
                        #
                        ## self._start_job(job, best_partition_dict[jobid])
                        self._start_job_wOverhead(job, best_partition_dict[jobid], rest_overhead + rest_overheadP)
                        ##
                        util_job = ComponentProxy("system").get_utilization_rate(0)
                        util_job_p = ComponentProxy("system").get_utilization_rate(5)
                        # print "---> REST_util: ", util_job
                        # print "---> REST_util_p: ", util_job_p
                        fp_backf.write('REST: %s: %f \n' % (self.get_current_time(), util_job))

                    #
                    # print "     rest_c_time: ", self.get_current_time()
                    # ### rest_overheadP = cqm.get_current_checkp_overheadP( checkp_t_internval, self.get_current_time(), dsize_pnode, bw_temp_write )
                    # rest_overheadP = cqm.get_current_checkp_overheadP_app(jobid, self.get_current_time(), dsize_pnode,
                    #                                                       bw_temp_write)
                    # #
                    # print "[RE_OVER-115] job.jid: ", jobid
                    # rest_overhead = cqm.get_restart_overhead(jobid)
                    # print "[RE_OVER-115] job.restart_over: ", rest_overhead
                    # fp_backf.write('RE_OVER-115: %s: %f \n' % (jobid, rest_overhead))
                    # #
                    # fp_pre_bj.write('%d: %d, %.3f,%.3f, %.3f \n' % (
                    # int(jobid), 0, rest_overhead, rest_overheadP, rest_overhead + rest_overheadP))
                    # #
                    # #
                    # ## self._start_job(job, best_partition_dict[jobid])
                    # self._start_job_wOverhead(job, best_partition_dict[jobid], rest_overhead + rest_overheadP)
                    # ##
                    # util_job = ComponentProxy("system").get_utilization_rate(0)
                    # util_job_p = ComponentProxy("system").get_utilization_rate(5)
                    # print "---> REST_util: ", util_job
                    # print "---> REST_util_p: ", util_job_p
                    # fp_backf.write('REST: %s: %f \n' % (self.get_current_time(), util_job))

            '''
            # dwang:
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            if realtime_jobs: # and (best_partition_dict_wcheckp != best_partition_dict):
                if preempt_list_full: 
                    for pjob in preempt_list_full:
                        jobid = pjob['jobid'] 
                        #job = self.jobs[int(jobid)]
                        #
                        #global restart_jobs_global
                        #restart_jobs_global.append(job)
                        #
                        cqm.restart_job_add_queue(jobid)
                        print "[] -->> ADD_queue_restart_jobs() --------------->> "
                        ## 

                        # ( --> _release_rt_partition() )
                        # ...

                        # ( --> _os_restart() )
                        # ...
            '''

    schedule_jobs_wcheckp_v2p_app = locking(automatic(schedule_jobs_wcheckp_v2p_app,
                                                      float(get_bgsched_config('schedule_jobs_interval', 10))))

    # samnickolay
    ###



    def enable(self, user_name):
        """Enable scheduling"""
        self.logger.info("%s enabling scheduling", user_name)
        self.active = True
    enable = exposed(enable)

    def disable(self, user_name):
        """Disable scheduling"""
        self.logger.info("%s disabling scheduling", user_name)
        self.active = False
    disable = exposed(disable)

    def sched_status(self):
        return self.active
    sched_status = exposed(sched_status)

    def set_res_id(self, id_num):
        """Set the reservation id number."""
        self.id_gen.set(id_num)
        logger.info("Reset res_id generator to %s." % id_num)

    set_res_id = exposed(set_res_id)

    def set_cycle_id(self, id_num):
        """Set the cycle id number."""
        self.cycle_id_gen.set(id_num)
        logger.info("Reset cycle_id generator to %s." % id_num)

    set_cycle_id = exposed(set_cycle_id)

    def force_res_id(self, id_num):
        """Override the id-generator and change the resid to id_num"""
        self.id_gen.idnum = id_num - 1
        logger.warning("Forced res_id generator to %s." % id_num)

    force_res_id = exposed(force_res_id)

    def force_cycle_id(self, id_num):
        """Override the id-generator and change the cycleid to id_num"""
        self.cycle_id_gen.idnum = id_num - 1
        logger.warning("Forced cycle_id generator to %s." % id_num)

    force_cycle_id = exposed(force_cycle_id)

    def get_next_res_id(self):
        """Get what the next resid number would be"""
        return self.id_gen.idnum + 1
    get_next_res_id = exposed(get_next_res_id)

    def get_next_cycle_id(self):
        """get what the next cycleid number would be"""
        return self.cycle_id_gen.idnum + 1
    get_next_cycle_id = exposed(get_next_cycle_id)

    def __flush_msg_queue(self):
        """Send queued messages to the database-writer component"""
        return dbwriter.flush_queue()
    __flush_msg_queue = automatic(__flush_msg_queue, 
                float(get_bgsched_config('db_flush_interval', 10)))
