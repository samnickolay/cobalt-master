#!/usr/bin/env python

'''Cobalt Queue Simulator (for Blue Gene systems) library'''

import ConfigParser
import copy
import logging
import math
import os
import os.path
import random
import signal
import sys
import time
from numpy import arange

from ConfigParser import SafeConfigParser, NoSectionError, NoOptionError
from datetime import datetime

import Cobalt
import Cobalt.Cqparse
import Cobalt.Util

from Cobalt.Components.qsim_base import *
from Cobalt.Components.base import exposed, query, automatic, locking
from Cobalt.Components.cqm import QueueDict, Queue
from Cobalt.Components.simulator import Simulator
from Cobalt.Data import Data, DataList
from Cobalt.Exceptions import ComponentLookupError
from Cobalt.Proxy import ComponentProxy, local_components
from Cobalt.Server import XMLRPCServer, find_intended_location
# from objc._objc import NULL

REMOTE_QUEUE_MANAGER = "cluster-queue-manager"

WALLTIME_AWARE_CONS = False

MACHINE_ID = 0
#MACHINE_NAME = "Intrepid"
MACHINE_NAME = "Mira" # esjung
DEFAULT_MAX_HOLDING_SYS_UTIL = 0.6
SELF_UNHOLD_INTERVAL = 0
AT_LEAST_HOLD = 600
MIDPLANE_SIZE = 512
#TOTAL_NODES = 40960 #INTREPID
TOTAL_NODES = 49152 #MIRA
#TOTAL_MIDPLANE = 80 #INTREPID
TOTAL_MIDPLANE = 96 #MIRA
YIELD_THRESHOLD = 0

BESTFIT_BACKFILL = False
SJF_BACKFILL = True

MIN_WALLTIME = 60
#MAX_WALLTIME = 43200 #INTREPID -- 12 hours
MAX_WALLTIME = 259200 #MIRA -- 72 Hours (reservations can do this, 24 for normal operation)
BALANCE_FACTOR = 1

#
rtj_id = []
#
#
walltimes = {} # in seconds (float)
walltime_runtime_differences = {} # in seconds (float)
orig_run_times = {}
## sam nickolay
predicted_run_times = {}
# rtj_ids_for_baseline = []
utilization_records = []
jobs_start_times = {}

#

preempt_cost_runtime = 0
#preempt_waste_runtime_checkpoint = 0
#preempt_waste_runtime_restart = 0 
#
rtj_resv_id = [] 
rtj_resv_part_dict = dict() #set() 
hpResv_drain_partitions = set()
#

class BGQsim(Simulator):
    '''Cobalt Queue Simulator for Blue Gene systems'''

    implementation = "qsim"
    name = "queue-manager"
    alias = "system"
    logger = logging.getLogger(__name__)

    def __init__(self, *args, **kwargs):

        Simulator.__init__(self, *args, **kwargs)
        
        # dwang:
        global rtj_id
        rtj_id = []
        # print("[dw_BGQsim] simu_times: %d. " %kwargs.get("times"))
        # print("[dw_BGQsim] simu_name: %s. " %kwargs.get("name"))
        # print("[dw_BGQsim] ckp_intrv_pcent: %f." %kwargs.get("intv_pcent"))
        # dwang
        
        #initialize partitions
        self.sleep_interval = kwargs.get("sleep_interval", 0)

        self.fraction = kwargs.get("BG_Fraction", 1)
        self.sim_start = kwargs.get("bg_trace_start", 0)
        self.sim_end = kwargs.get("bg_trace_end", sys.maxint)
        self.anchor = kwargs.get("Anchor", 0)
        self.backfill = kwargs.get("backfill", "ff")

###--------Partition related
        partnames = self._partitions.keys()
        self.init_partition(partnames)
        self.inhibit_small_partitions()

        self.total_nodes = TOTAL_NODES
        self.total_midplane = TOTAL_MIDPLANE

        self.part_size_list = []

        for part in self.partitions.itervalues():
            if int(part.size) not in self.part_size_list:
                if part.size >= MIDPLANE_SIZE:
                    self.part_size_list.append(int(part.size))
        self.part_size_list.sort()

        self.cached_partitions = self.partitions
        self._build_locations_cache()

###-------Job related
        self.workload_file =  kwargs.get("bgjob")
        self.output_log = MACHINE_NAME + "-" + kwargs.get("outputlog", "")

        self.event_manager = ComponentProxy("event-manager")

        self.time_stamps = [('I', '0', 0, {})]
        self.cur_time_index = 0
        ###
        # samnickolay
        # self.queues = SimQueueDict(policy=None)

        self.queues = SimQueueDict(policy=kwargs.get("utility_function"))
        # if "policy" in kwargs:
        #     self.queues = SimQueueDict(policy=kwargs.get("policy"))
        # else:
        #     self.queues = SimQueueDict(policy="default")
        # samnickolay
        ###
        self.unsubmitted_job_spec_dict = {}   #{jobid_stringtype: jobspec}

        self.num_running = 0
        self.num_waiting = 0
        self.num_busy = 0
        self.num_end = 0
        self.total_job = 0
        
        #dwang:
        self.first_job_start = 0
        self.preempt_waste_runtime_checkpoint = 0
        self.preempt_waste_runtime_restart = 0
        self.preempt_waste_runtime_rework = 0
        self.time_last_Pcheckp = -1 # before 1st initialization 
        self.Pcheckp_specs = [] # for application-based pChcekp 
        #dwang

####------Walltime prediction
        self.predict_scheme = kwargs.get("predict", False)

        if self.predict_scheme:
            self.walltime_prediction = True
            self.predict_queue = bool(int(self.predict_scheme[0]))
            self.predict_backfill = bool(int(self.predict_scheme[1]))
            self.predict_running = bool(int(self.predict_scheme[2]))
        else:
            self.walltime_prediction = False
            self.predict_queue = False
            self.predict_backfill = False
            self.predict_running = False

        histm_alive = False
        try:
            histm_alive = ComponentProxy("history-manager").is_alive()
        except:
            #self.logger.error("failed to connect to histm component", exc_info=True)
            histm_alive = False

        if histm_alive:
            self.history_manager = ComponentProxy("history-manager")
        else:
            self.walltime_prediction = False

#####init jobs (should be after walltime prediction initializing stuff)
        realtime_tup = kwargs.get("realtime")
        if realtime_tup:
            self.init_queues(int(realtime_tup[0]), int(realtime_tup[1]), int(realtime_tup[2]))
        else:
            if kwargs.get("checkpoint") == "v2p_app_sam_v1":
                self.init_queues(0, 0, 0, kwargs.get("name"), kwargs.get("times"), kwargs.get("rt_percent"),
                                 -1, kwargs.get("rt_job_categories"), kwargs.get("times"), kwargs.get("checkp_overhead_percent"),
                                 kwargs.get("checkp_dsize"), kwargs.get("checkp_w_bandwidth"))
            else:
                self.init_queues(0,0,0,kwargs.get("name"),kwargs.get("times"),kwargs.get("rt_percent"),
                                kwargs.get("intv_pcent"), kwargs.get("rt_job_categories"), kwargs.get("times"))

#####------walltime-aware spatial scheduling
        self.walltime_aware_cons = False
        self.walltime_aware_aggr = False
        self.wass_scheme = kwargs.get("wass", None)

        if self.wass_scheme == "both":
            self.walltime_aware_cons = True
            self.walltime_aware_aggr = True
        elif self.wass_scheme == "cons":
            self.walltime_aware_cons = True
        elif self.wass_scheme == "aggr":
            self.walltime_aware_aggr = True

###-------CoScheduling start###
        self.cosched_scheme_tup = kwargs.get("coscheduling", (0,0))

        self.mate_vicinity = kwargs.get("vicinity", 0)

        self.cosched_scheme = self.cosched_scheme_tup[0]
        self.cosched_scheme_remote = self.cosched_scheme_tup[1]

        valid_cosched_schemes = ["hold", "yield"]

        if self.cosched_scheme in valid_cosched_schemes and self.cosched_scheme_remote in valid_cosched_schemes:
            self.coscheduling = True
        else:
            self.coscheduling = False

        #key=local job id, value=remote mated job id
        self.mate_job_dict = {}
        #key = jobid, value = nodelist  ['part-or-node-name','part-or-node-name' ]
        self.job_hold_dict = {}

        #record holding job's holding time   jobid:first hold (sec)
        self.first_hold_time_dict = {}

        #record yield jobs's first yielding time, for calculating the extra waiting time
        self.first_yield_hold_time_dict = {}

        #record yield job ids. update dynamically
        self.yielding_job_list = []

        self.cluster_job_trace = kwargs.get("cjob", None)
        if not self.cluster_job_trace:
            self.coscheduling = False

        self.jobid_qtime_pairs = []

        if self.coscheduling:
            self.init_jobid_qtime_pairs()
            # 'disable' coscheduling for a while until cqsim triggers the remote function
            # to initialize mate job dice successfully
            self.coscheduling = False

        self.max_holding_sys_util = DEFAULT_MAX_HOLDING_SYS_UTIL

####----reservation related
        self.reservations = {}
        self.reserve_ratio = kwargs.get("reserve_ratio", 0)
        if self.reserve_ratio > 0:
            self.init_jobid_qtime_pairs()
            self.init_reservations_by_ratio(self.reserve_ratio)

####----log and other
        #initialize PBS-style logger
        self.pbslog = PBSlogger(self.output_log)

        #initialize debug logger
        if self.output_log:
            self.dbglog = PBSlogger(self.output_log+"-debug")
        else:
            self.dbglog = PBSlogger(".debug")

        #finish tag
        self.finished = False

        #register local alias "system" for this component
        local_components["system"] = self

        #initialize capacity loss
        self.capacity_loss = 0

        self.user_utility_functions = {}
        self.builtin_utility_functions = {}

        self.define_builtin_utility_functions()
        self.define_user_utility_functions()

        self.rack_matrix = []
        self.reset_rack_matrix()

        self.batch = kwargs.get("batch", False)

######adaptive metric-aware cheduling
        self.metric_aware = kwargs.get("metrica", False)
        self.balance_factor = float(kwargs.get("balance_factor"))
        self.window_size = kwargs.get("window_size", 1)

        self.history_wait = {}
        self.history_slowdown = {}
        self.history_utilization = {}

        self.delivered_node_hour = 0
        self.delivered_node_hour2 = 0
        self.jobcount = 0
        self.counted_jobs = []
        self.num_started = 0
        self.started_job_dict = {}
        self.queue_depth_data = []
        self.adaptive = kwargs.get("adaptive", False)
        if self.adaptive:
            print "adaptive scheme=", self.adaptive

####----print some configuration
        if self.wass_scheme:
            print "walltime aware job allocation enabled, scheme = ", self.wass_scheme

        if self.walltime_prediction:
            print "walltime prediction enabled, scheme = ", self.predict_scheme

        if self.fraction != 1:
            print "job arrival intervals adjusted, fraction = ", self.fraction

        if not self.cluster_job_trace:
            #Var = raw_input("press any Enter to continue...")
            pass

##### simulation related
    def get_current_time(self):
        '''this function overrides get_current_time() in bgsched, bg_base_system, and cluster_base_system'''
        return  self.event_manager.get_current_time()

    def get_current_time_sec(self):
        return  self.event_manager.get_current_time()

    def get_current_time_date(self):
        return self.event_manager.get_current_date_time()

    def insert_time_stamp(self, timestamp, type, info):
        '''insert time stamps in the same order'''
        if type not in SET_event:
            print "invalid event type,", type
            return

        evspec = {}
        evspec['jobid'] = info.get('jobid', 0)
        evspec['type'] = type
        evspec['datetime'] = sec_to_date(timestamp)
        evspec['unixtime'] = timestamp
        evspec['machine'] = MACHINE_ID
        evspec['location'] = info.get('location', [])

        self.event_manager.add_event(evspec)

    def log_job_event(self, eventtype, timestamp, spec):
        '''log job events(Queue,Start,End) to PBS-style log'''

        ###
        # samnickolay
        if eventtype == 'S':  # start running
            global jobs_start_times
            time_tuple = (date_to_sec(timestamp), timestamp )
            if spec['jobid'] in jobs_start_times:
                jobs_start_times[spec['jobid']].append(time_tuple)
            else:
                jobs_start_times[spec['jobid']] = [time_tuple]

        #
        ###


        def len2 (_input):
            _input = str(_input)
            if len(_input) == 1:
                return "0" + _input
            else:
                return _input
        if eventtype == 'Q':  #submitted(queued) for the first time
            message = "%s;Q;%s;queue=%s" % (timestamp, spec['jobid'], spec['queue'])
        elif eventtype == 'R':  #resume running after failure recovery
            message = "%s;R;%s" % (timestamp, ":".join(spec['location']))
        else:
            wall_time = spec['walltime']
            walltime_minutes = len2(int(float(wall_time)) % 60)
            walltime_hours = len2(int(float(wall_time)) // 60)
            log_walltime = "%s:%s:00" % (walltime_hours, walltime_minutes)
            if eventtype == 'S':  #start running
                message = "%s;S;%s;queue=%s qtime=%s Resource_List.nodect=%s Resource_List.walltime=%s start=%s exec_host=%s" % \
                (timestamp, spec['jobid'], spec['queue'], spec['submittime'],
                 spec['nodes'], log_walltime, spec['start_time'], ":".join(spec['location']))
                #dbgmsg = "%s:Start:%s:%s" % (timestamp, spec['jobid'], ":".join(spec['location']))
                #self.dbglog.LogMessage(dbgmsg)
            elif eventtype == 'H':  #hold some resources
                message = "%s;H;%s;queue=%s qtime=%s Resource_List.nodect=%s Resource_List.walltime=%s exec_host=%s" % \
                (timestamp, spec['jobid'], spec['queue'], spec['submittime'],
                 spec['nodes'], log_walltime, ":".join(spec['location']))
            elif eventtype == "U":  #unhold some resources
                message = "%s;U;%s;host=%s" % \
                (timestamp, spec['jobid'], ":".join(spec['location']))
            elif eventtype == 'E':  #end
                first_yield_hold = self.first_yield_hold_time_dict.get(int(spec['jobid']), 0)
                if first_yield_hold > 0:
                    overhead = spec['start_time'] - first_yield_hold
                else:
                    overhead = 0
                message = "%s;E;%s;queue=%s qtime=%s Resource_List.nodect=%s Resource_List.walltime=%s start=%s end=%f exec_host=%s runtime=%s hold=%s overhead=%s" % \
                (timestamp, spec['jobid'], spec['queue'], spec['submittime'], spec['nodes'], log_walltime, spec['start_time'],
                 round(float(spec['end_time']), 1), ":".join(spec['location']),
                 spec['runtime'], spec['hold_time'], overhead)
            else:
                print "invalid event type, type=", eventtype
                return
        self.pbslog.LogMessage(message)


 ####reservation related

    def init_starttime_jobid_pairs(self):
        '''used for initializing reservations'''
        pair_list = []

        for id, spec in self.unsubmitted_job_spec_dict.iteritems():
            start = spec['start_time']
            pair_list.append((float(start), int(id)))

        def _stimecmp(tup1, tup2):
            return cmp(tup1[0], tup2[0])

        pair_list.sort(_stimecmp)

        return pair_list

    def init_reservations_by_ratio(self, ratio):
        '''init self.reservations dictionary'''

        if ratio <= 0.5:
            step = int(1.0 / ratio)
            reverse_step = 1
        else:
            step = 1
            reverse_step = int(1.0/(1-ratio))

        i = 0
        temp_dict = {}
        start_time_pairs = self.init_starttime_jobid_pairs()
        for item in start_time_pairs:
            #remote_item = self.remote_jobid_qtime_pairs[i]
            i += 1

            if step > 1 and i % step != 0:
                continue

            if reverse_step > 1 and i % reverse_step == 0:
                continue

            jobid = item[1]
            reserved_time = item[0]
            jobspec = self.unsubmitted_job_spec_dict[str(jobid)]

            nodes = int(jobspec['nodes'])
            if nodes < 512 or nodes> 16384:
                continue

            reserved_location = jobspec['location']
            self.reservations[jobid] = (reserved_time, reserved_location)

            self.insert_time_stamp(reserved_time, "S", {'jobid':jobid})

        print "totally reserved jobs: ", len(self.reservations.keys())

    def reservation_violated(self, expect_end, location):
        '''test if placing a job with current expected end time (expect_end)
        on partition (location) will violate any reservation'''
        violated = False
        for resrv in self.reservations.values():
            start = resrv[0]
            if expect_end < start:
                continue

            reserved_partition = resrv[1]
            if self.location_conflict(location, reserved_partition):
                #print "location conflict:", location, reserved_partition
                violated = True

        return violated

    def location_conflict(self, partname1, partname2):
        '''test if partition 1 is parent or children or same of partition2 '''
        conflict = False

        p = self._partitions[partname2]
        #print partname1, partname2, p.children, p.parents
        if partname1==partname2 or partname1 in p.parents or partname1 in p.parents:
            conflict = True
        return conflict

##### job/queue related
    def _get_queuing_jobs(self):
        jobs = [job for job in self.queues.get_jobs([{'is_runnable':True}])]
        return jobs
    queuing_jobs = property(_get_queuing_jobs)

    def _get_running_jobs(self):
        return [job for job in self.queues.get_jobs([{'has_resources':True}])]
    running_jobs = property(_get_running_jobs)


    # dwang:
    # def init_queues(self, frequency, duration, nodes):
    def init_queues(self, frequency, duration, nodes, simu_name, simu_tid, simu_rt_percent,
                    simu_checkp_t_internval_pcent, rt_job_categories, trial_number,
                    checkp_overhead_percent=-1, checkp_dsize=-1, checkp_w_bandwidth=-1):
    # dwang
        '''parses the work load log file, initializes queues and sorted time
        stamp list'''

        print "Initializing BG jobs, one moment please..." 
        raw_jobs = parse_work_load(self.workload_file)
        # raw_jobs = parse_work_load_intrepid(self.workload_file)
        print " [] raw_jobs NUM: ", len(raw_jobs)
        ### esjung
        ## if frequency > 0:
            ## starttime = date_to_sec('01/01/2014 00:00:01')
            ## realtime_jobs = generate_realtime_job(starttime, 31, frequency, duration, nodes)
            ## raw_jobs = merge_job(raw_jobs, realtime_jobs)
        ### esjung

        realtime_info_in_job_trace = False
        # check the parsed jobs to see if they are already marked as realtime or batch (realtime=True, realtime=False)
        # this is the case for the small simulation trace experiments
        # in this case we don't need to choose which jobs are realtime or batch
        if 'realtime' in raw_jobs.values()[0]:
            realtime_info_in_job_trace = True
            realtime_job_ids = [job_id for job_id, raw_job in raw_jobs.iteritems() if raw_job['realtime'] == 'True']

        specs = []
        tag = 0
        temp_num = 0
        for key in raw_jobs:
            #__0508:
            temp_num = temp_num + 1
            # print "[Init] temp_num: ", temp_num
            #_0508
            spec = {}
            tmp = raw_jobs[key]
            spec['jobid'] = tmp.get('jobid')
            spec['queue'] = tmp.get('queue')
            spec['user'] = tmp.get('user')
            # print "[] raw_jobid: ", tmp.get('jobid')

         #    #convert submittime from "%m/%d/%Y %H:%M:%S" to Unix time sec
         #    format_sub_time = tmp.get('submittime')
         #    if format_sub_time:
         #        # dwang_0507:
         #        # qtime = date_to_sec(format_sub_time)
         #        qtime = float(format_sub_time)
         #        # dwang_0507
         #        if qtime < self.sim_start or qtime > self.sim_end:
         #    	    print "[raw_j] qtime Error !"
         #            continue
         #        spec['submittime'] = qtime
         #        #spec['submittime'] = float(tmp.get('qtime'))
         #        spec['first_subtime'] = spec['submittime']  #set the first submit time
         #    else:
        	# print "[raw_j] submittime Error !"
         #        continue
        #
         #    # spec['user'] = tmp.get('user')
         #    spec['project'] = tmp.get('account')
        #
         #    #convert walltime from 'hh:mm:ss' to float of minutes
         #    format_walltime = tmp.get('Resource_List.walltime')
         #    spec['walltime'] = 0
         #    if format_walltime:
         #        # dwang_0507:
         #        '''
         #        segs = format_walltime.split(':')
         #        walltime_minuntes = int(segs[0])*60 + int(segs[1])
         #        spec['walltime'] = str(int(segs[0])*60 + int(segs[1]))
         #        '''
         #        spec['walltime'] = float(format_walltime)/60
         #        # dwang_0507
         #    else:  #invalid job entry, discard
        	# print "[raw_j] walltime Error !"
         #        continue
        #
         #    if tmp.get('runtime'):
         #        # dwang_0507:
         #        spec['runtime'] = float( tmp.get('runtime') )
         #        spec['runtime_org'] = float( tmp.get('runtime') )
         #        # spec['runtime'] = tmp.get('runtime')
         #    elif tmp.get('start') and tmp.get('end'):
         #        act_run_time = float(tmp.get('end')) - float(tmp.get('start'))
         #        if act_run_time <= 0:
         #            continue
         #        if act_run_time / (float(spec['walltime'])*60) > 1.1:
         #            act_run_time = float(spec['walltime'])*60
         #        #spec['runtime'] = str(round(act_run_time, 1))
         #        spec['runtime'] = round(act_run_time, 1)
         #    else:
         #        continue
         #        # dwang_0507
        #
         #    # if tmp.get('Resource_List.nodect'):
         #        # spec['nodes'] = tmp.get('Resource_List.nodect')
         #    if tmp.get('nodes'):
         #        spec['nodes'] = int(tmp.get('nodes'))/4
         #        if int(tmp.get('nodes')) < 4:
        #     spec['nodes'] = 1
        # if int(spec['nodes']) == TOTAL_NODES:
         #    	    print "[raw_j] total_nodes Error !"
         #            continue
         #    else:  #invalid job entry, discard
        	# print "[raw_j] nodes Error !"
         #        continue
        #
         #    if self.walltime_prediction: #*AdjEst*
         #        if tmp.has_key('walltime_p'):
         #            spec['walltime_p'] = int(tmp.get('walltime_p')) / 60 #convert from sec (in log) to min, in line with walltime
         #        else:
         #            ap = self.get_walltime_Ap(spec)
         #            spec['walltime_p'] = int(spec['walltime']) * ap
         #    else:
         #        spec['walltime_p'] = int(spec['walltime'])
        #
         #    spec['state'] = 'invisible'
         #    spec['start_time'] = '0'
         #    spec['end_time'] = '0'
         #    # spec['queue'] = "default"
         #    spec['has_resources'] = False
         #    spec['is_runnable'] = False
         #    spec['location'] =tmp.get('exec_host', '')  #used for reservation jobs only
         #    spec['start_time'] = tmp.get('start', 0)  #used for reservation jobs only
         #    # dwang:
         #    spec['restart_overhead'] = 0.0
         #    #
        #
         #    #add the job spec to the spec list
         #    specs.append(spec)
            # convert submittime from "%m/%d/%Y %H:%M:%S" to Unix time sec
            format_sub_time = tmp.get('submittime')
            if format_sub_time:
                qtime = date_to_sec(format_sub_time)
                if qtime < self.sim_start or qtime > self.sim_end:
                    continue
                spec['submittime'] = qtime
                # spec['submittime'] = float(tmp.get('qtime'))
                spec['first_subtime'] = spec['submittime']  # set the first submit time
            else:
                continue

            # spec['user'] = tmp.get('user')
            spec['project'] = tmp.get('account')

            # convert walltime from 'hh:mm:ss' to float of minutes
            format_walltime = tmp.get('Resource_List.walltime')
            spec['walltime'] = 0
            if format_walltime:
                segs = format_walltime.split(':')
                walltime_minuntes = int(segs[0]) * 60 + int(segs[1])
                spec['walltime'] = str(int(segs[0]) * 60 + int(segs[1]))
            else:  # invalid job entry, discard
                continue

            if tmp.get('runtime'):
                spec['runtime'] = tmp.get('runtime')
            elif tmp.get('start') and tmp.get('end'):
                act_run_time = float(tmp.get('end')) - float(tmp.get('start'))
                if act_run_time <= 0:
                    continue
                if act_run_time / (float(spec['walltime']) * 60) > 1.1:
                    act_run_time = float(spec['walltime']) * 60
                spec['runtime'] = str(round(act_run_time, 1))
            else:
                continue

            if tmp.get('Resource_List.nodect'):
                spec['nodes'] = tmp.get('Resource_List.nodect')
                if int(spec['nodes']) == TOTAL_NODES:
                    continue
            else:  # invalid job entry, discard
                continue

            if self.walltime_prediction:  # *AdjEst*
                if tmp.has_key('walltime_p'):
                    spec['walltime_p'] = int(
                        tmp.get('walltime_p')) / 60  # convert from sec (in log) to min, in line with walltime
                else:
                    ap = self.get_walltime_Ap(spec)
                    spec['walltime_p'] = int(spec['walltime']) * ap
            else:
                spec['walltime_p'] = int(spec['walltime'])

            spec['state'] = 'invisible'
            spec['start_time'] = '0'
            spec['end_time'] = '0'
            # spec['queue'] = "default"
            spec['has_resources'] = False
            spec['is_runnable'] = False
            spec['location'] = tmp.get('exec_host', '')  # used for reservation jobs only
            spec['start_time'] = tmp.get('start', 0)  # used for reservation jobs only
            # dwang:
            spec['restart_overhead'] = 0.0
            #

            # add the job spec to the spec list
            specs.append(spec)

        #_0508:
        print "[Init_2] len(specs): ", len(specs)
        #_0508
        #

        # --> highP
        '''
        #highp_job = math.ceil( 0.1 * len(specs) )
        if not simu_rt_percent:
            highp_job = 0
        else:
            highp_job = math.ceil( float(simu_rt_percent/100.0) * len(specs) )
        #highp_job = 0
        print "[dw] len(specs): ", len(specs)
        print "[dw] highp_job: ", highp_job
        # -------------------->
        highp_id = random.sample(range(len(specs)),int(highp_job))
        '''
        highp_id = [];
        #
        # hpid_fname = './_hpid_' + simu_name + '/hpid_' + simu_name + '_' + str(simu_tid) + '.txt'
        #
        # print "[dw] simu_rt_percent: ", simu_rt_percent
        # if simu_rt_percent == 5:
        #     hpid_fname = './_hpid_intre_hp_005/hpid_intre_hp_005_' + str(simu_tid) + '.txt'
        #     #hpid_fname = './_hpid_dec4_hp_005/hpid_dec4_hp_005_' + str(simu_tid) + '.txt'
        # elif simu_rt_percent == 10:
        #     hpid_fname = './_hpid_intre_hp_01/hpid_intre_hp_01_' + str(simu_tid) + '.txt'
        #     #hpid_fname = './_hpid_dec4_hp_01/hpid_dec4_hp_01_' + str(simu_tid) + '.txt'
        # elif simu_rt_percent == 20:
        #     hpid_fname = './_hpid_intre_hp_02/hpid_intre_hp_02_' + str(simu_tid) + '.txt'
        #     #hpid_fname = './_hpid_dec4_hp_02/hpid_dec4_hp_02_' + str(simu_tid) + '.txt'
        # elif simu_rt_percent == 30:
        #     hpid_fname = './_hpid_intre_hp_03/hpid_intre_hp_03_' + str(simu_tid) + '.txt'
        #     #hpid_fname = './_hpid_dec4_hp_03/hpid_dec4_hp_03_' + str(simu_tid) + '.txt'
        # elif simu_rt_percent == 40:
        #     hpid_fname = './_hpid_intre_hp_04/hpid_intre_hp_04_' + str(simu_tid) + '.txt'
        #     #hpid_fname = './_hpid_dec4_hp_04/hpid_dec4_hp_04_' + str(simu_tid) + '.txt'
        # elif simu_rt_percent == 50:
        #     hpid_fname = './_hpid_intre_hp_05/hpid_intre_hp_05_' + str(simu_tid) + '.txt'
        #     #hpid_fname = './_hpid_dec4_hp_05/hpid_dec4_hp_05_' + str(simu_tid) + '.txt'
        # #
        # print "[dw] hpid_fname: ", hpid_fname
        # fp_highp = open( hpid_fname, 'r')
        # data = fp_highp.readlines()
        # for line in data:
        #     print line
        #     words = line.split(',')
        #     for word_elem in words:
        #         print int(word_elem)
        # 	highp_id.append(int(word_elem))
        # #
        # print "[dw] highp_id: ", highp_id

        highPriorityJobs = []
        # if the job trace contains realtime/batch indicators
        if realtime_info_in_job_trace is True:
            for job_temp in specs:
                if job_temp['jobid'] in realtime_job_ids:
                    highPriorityJobs.append(job_temp)

        # if the job trace didn't contain realtime/batch indicators then randomly set jobs as realtime/batch
        elif realtime_info_in_job_trace is False:

            ###
            # samnickolay
            # select simu_rt_percent of the jobs as realtime jobs using random seed
            randomNumberGeneratorInstance = random.Random()
            # seed = trial_number * 25 + 5
            seed = trial_number + 1000
            randomNumberGeneratorInstance.seed(seed)

            if simu_rt_percent is None:
                simu_rt_percent = 0

            number_rt_jobs = int(simu_rt_percent / 100.0 * len(specs))

            while len(highPriorityJobs) < number_rt_jobs:
                job_temp = specs[randomNumberGeneratorInstance.randint(0, len(specs)-1)]
                if job_temp in highPriorityJobs:
                    continue

                if  rt_job_categories == 'all':
                    pass
                elif rt_job_categories == 'short':
                    if float(job_temp.get('walltime')) > 120:
                        continue
                elif rt_job_categories == 'narrow': # only narrow jobs
                    if float(job_temp.get('nodes')) > 4096:
                        continue
                elif rt_job_categories == 'short-or-narrow': # exclude long-wide
                    if float(job_temp.get('nodes')) > 4096 and float(job_temp.get('walltime')) > 120:
                        continue
                elif rt_job_categories == 'short-and-narrow': # only short-narrow jobs
                    if float(job_temp.get('nodes')) > 4096 or float(job_temp.get('walltime')) > 120:
                        continue
                elif rt_job_categories == 'corehours':
                    if float(job_temp.get('nodes')) * float(job_temp.get('walltime')) > 491520:
                        continue
                else:
                    print('Invalid argument for rt_job_categories: ', rt_job_categories)
                    exit(-1)

                # if float(job_temp.get('nodes')) > 4096:
                #     continue
                # if float(job_temp.get('walltime')) > 120:
                #     continue
                # if float(job_temp.get('nodes')) > 4096 and float(job_temp.get('walltime')) > 120:
                #     continue
                highPriorityJobs.append(job_temp)

        # highPriorityJobs = randomNumberGeneratorInstance.sample(specs, number_rt_jobs)

        rt_narrow_short = []
        rt_narrow_long = []
        rt_wide_short = []
        rt_wide_long = []

        batch_narrow_short = []
        batch_narrow_long = []
        batch_wide_short = []
        batch_wide_long = []

        for job_temp in highPriorityJobs:
            if float(job_temp.get('nodes')) <= 4096:  # if job is narrow
                if float(job_temp.get('walltime')) <= 120:  # if job is short
                    rt_narrow_short.append(job_temp)
                else:  # if job is long
                    rt_narrow_long.append(job_temp)
            else:  # if job is wide
                if float(job_temp.get('walltime')) <= 120:  # if job is short
                    rt_wide_short.append(job_temp)
                else:  # if job is long
                    rt_wide_long.append(job_temp)

        for job_temp in specs:
            if job_temp in highPriorityJobs:
                continue

            if float(job_temp.get('nodes')) <= 4096:  # if job is narrow
                if float(job_temp.get('walltime')) <= 120:  # if job is short
                    batch_narrow_short.append(job_temp)
                else:  # if job is long
                    batch_narrow_long.append(job_temp)
            else:  # if job is wide
                if float(job_temp.get('walltime')) <= 120:  # if job is short
                    batch_wide_short.append(job_temp)
                else:  # if job is long
                    batch_wide_long.append(job_temp)

        print('\nrealtime jobs count:', len(highPriorityJobs))

        print('realtime jobs count - narrow short:', len(rt_narrow_short))
        print('realtime jobs count - narrow long:', len(rt_narrow_long))
        print('realtime jobs count - wide short:', len(rt_wide_short))
        print('realtime jobs count - wide long:', len(rt_wide_long))

        print('\nbatch jobs count:', len(specs) - len(highPriorityJobs))

        print('batch jobs count - narrow short:', len(batch_narrow_short))
        print('batch jobs count - narrow long:', len(batch_narrow_long))
        print('batch jobs count - wide short:', len(batch_wide_short))
        print('batch jobs count - wide long:', len(batch_wide_long))

        wide_count = 0
        long_count = 0
        both_count = 0

        for job_temp in specs:
            if job_temp.get('nodes') > 4096 and float(job_temp.get('walltime')) > 120:
                both_count += 1
            elif job_temp.get('nodes') > 4096:
                wide_count += 1
            elif float(job_temp.get('walltime')) > 120:
                long_count += 1

        print('Counts: both: {}, wide: {}, long: {}'.format(both_count, wide_count, long_count))

        # if "baseline" in simu_name or "highpQ" in simu_name:
        #     number_rt_jobs = 0
        #     global rtj_ids_for_baseline
        #     rtj_ids_for_baseline = [j['jobid'] for j in highPriorityJobs]
        #     highPriorityJobs = []

        highp_id = [int(j['jobid']) for j in highPriorityJobs]

    # samnickolay
        ###

        #

        ####### samnickolay
        ##### Code to run synthetic job data
        # job_data = [
        #     (1, 0, 160, 1024),
        #     (2, 22, 170, 1024),
        #     (3, 52, 14, 512),
        #     (4, 60, 63, 1024),
        #     (5, 10, 10, 512),
        #
        #     (6, 5, 14, 512),
        #     (7, 11, 98, 512),
        #     (8, 12, 61, 1024)
        # ]
        # highp_id = [6, 7, 8]
        # specs = []
        # for jobid, submit_time, runtime, nodes in job_data:
        #     spec = {}
        #
        #     spec['jobid'] = str(jobid)
        #     spec['queue'] = 'default'
        #     spec['user'] = ''
        #     spec['project'] = ''
        #     spec['submittime'] = float(submit_time) * 60.0
        #     spec['first_subtime'] = spec['submittime']
        #     spec['runtime'] = float(runtime)  * 60.0
        #     spec['walltime'] = str(int(runtime * 1.5))
        #     spec['walltime_p'] = int(spec['walltime'])
        #     spec['nodes'] = str(nodes*48)
        #
        #     spec['state'] = 'invisible'
        #     spec['start_time'] = '0'
        #     spec['end_time'] = '0'
        #     # spec['queue'] = "default"
        #     spec['has_resources'] = False
        #     spec['is_runnable'] = False
        #     spec['location'] = ''
        #     spec['start_time'] = ''
        #     # dwang:
        #     spec['restart_overhead'] = 0.0
        #     #
        #
        #     # add the job spec to the spec list
        #     specs.append(spec)
        ####### samnickolay

        # load in predicted_run_times
        with open("predicted_run_times-dec4.txt") as predicted_run_times_file:
            for line in predicted_run_times_file.readlines():
                entry, predicted_run_time = line.split(',')
                predicted_run_times[entry] = float(predicted_run_time)

        for j_spec in range(0, len(specs)):
            walltimes[specs[j_spec].get('jobid')] = float(specs[j_spec].get('walltime')) * 60.0

            walltime_runtime_differences[specs[j_spec].get('jobid')] = \
                float(specs[j_spec].get('walltime')) * 60.0 - float(specs[j_spec].get('runtime'))

            orig_run_times[specs[j_spec].get('jobid')] = float(specs[j_spec].get('runtime'))
            job_id = specs[j_spec].get('jobid')

        ###
         
        # -------------------->
        #
        # --> logRec_highP
        count_highp = 0;
        #
        #highp_fname = './_rec/highP_' + simu_name + '_' + str(simu_tid) + '.txt'

        highp_dname = './_rec_' + simu_name + '/'

        import os
        if not os.path.exists(highp_dname):
            os.mkdir(highp_dname)

        highp_fname = './_rec_' + simu_name + '/highP_' + simu_name + '_' + str(simu_tid) + '.txt'
        fp_highp = open( highp_fname,'w+' )
        fp_highp.write('id: jobid, submittime, runtime, walltime, nodes \n')
        #
        for j_highp in highp_id:
            # print "[dw] hp_count: ", count_highp
            # print "[dw] j_highp: ", j_highp
            # print "[dw] len_specs: ", len(specs)
            # 
            for j_spec in range(0,len(specs)):
            # 
                if int(specs[j_spec].get('jobid')) == j_highp: 
                    # print "[dw] j_match ... ...", j_spec
                    specs[j_spec]['user'] = "realtime"
                    # print "[dw] j_spec: ", specs[j_spec]['user']
                    # 
                    global rtj_id
                    rtj_id.append( specs[j_spec].get('jobid') )
                    #
                    fp_highp.write('%d: %s, %s, %s, %s, %s, %s \n' %(count_highp, specs[j_spec].get('jobid'), specs[j_spec].get('submittime'), specs[j_spec].get('start_time'), specs[j_spec].get('runtime'), specs[j_spec].get('walltime'), specs[j_spec].get('nodes') ) )
            '''
            # 
            specs[j_highp]['user'] = "realtime" 
            global rtj_id
            rtj_id.append( specs[j_highp].get('jobid') )
            fp_highp.write('%d: %s, %s, %s, %s, %s, %s \n' %(count_highp, specs[j_highp].get('jobid'), specs[j_highp].get('submittime'), specs[j_highp].get('start_time'), specs[j_highp].get('runtime'), specs[j_highp].get('walltime'), specs[j_highp].get('nodes') ) )
            #  
            ''' 
            # <-- logRec_highP
            count_highp += 1
        ######################
        '''
        global rtj_id
        rtj_id = ['375282','374623','372859','373743','373878','372882','372845','373502','374109','373198','374495','372945','372812','372844','375382','373641','375387','373194','373649','372624','373648','374725','372622','372626','372821','372877','374355','373446','373784','373640','373447','374141','374437','374387','372922','373444','373836','374113','374608','375284','374586']
        for j_spec in range(0,len(specs)):
            spec_temp = specs[j_spec] 
            if spec_temp.get('jobid') in rtj_id:
                print "[rtj_rec] ", spec_temp.get('jobid')
                spec_temp['user'] = 'realtime'
                specs[j_spec] = spec_temp
                #
                fp_highp.write('%d: %s, %s, %s, %s, %s \n' %(j_spec, spec_temp.get('jobid'), spec_temp.get('submittime'), spec_temp.get('runtime'), spec_temp.get('walltime'), spec_temp.get('nodes') ) )
            # <-- logRec_highP
        ###
        global rtj_id
        rtj_id =  ['373199', '373059', '375397', '375277', '374412', '369082', '372846', '374725', '372725', '373778', '374390', '373305', '374780', '374470', '374274', '373743', '373882', '373634', '374614', '372933', '372597', '373307', '372851', '373881', '372849', '375750', '374018', '374642', '373309', '372927', '372831', '372631', '373782', '375282', '372914', '374100', '373265', '373887', '373831', '373641', '372726', '373780', '374329', '375330', '373193', '368819', '373242', '373753', '373832', '372861', '373200', '373763', '368954', '373444', '372829', '372618', '373235', '372875', '374641', '372878', '372873', '373612', '373878', '372938', '374035', '372557', '372827', '368180', '374475', '373643', '372832', '374463', '372943', '374026', '373269', '372616', '374349', '372626', '373447', '373726', '374304', '372791', '372758', '373583', '373038', '373502', '374391', '372797', '374116', '373445', '372876', '372756', '374586', '372740', '372742', '373633', '374043', '374098', '374610', '372814', '374102', '375412', '372858', '372736', '373276', '373781', '374051', '372854', '373648', '373981', '374370', '373644', '374643', '374223', '374448', '373892', '374612', '374279', '372723', '372617', '374139', '375420', '372850', '374639', '374615', '374860', '368952', '373650', '373274', '373504', '374327', '374483', '373993', '373639', '372979', '374472', '374040', '373952', '374566', '368854', '375355', '372796', '374355', '374351', '374265', '374293', '373198', '372664', '372733', '372622', '374495', '373646', '372566', '374604', '372839', '374366', '375382', '373236', '372855', '375518', '374110', '374839', '368866', '375395', '374636', '374063', '373339', '372556', '372615', '374437', '374807', '373244', '375392', '372936', '372815', '372817', '372830', '373349', '373876', '373194', '372868', '374496', '372844', '373779', '372882', '372838', '372794', '373278', '373227', '372772', '375398', '374353', '372860', '375306', '373877', '372605', '372760', '373055', '375582', '372840', '372945', '373037']
        for j_spec in range(0,len(specs)):
            spec_temp = specs[j_spec]
            if spec_temp.get('jobid') in rtj_id:
                print "[rtj_rec] ", spec_temp.get('jobid')
                spec_temp['user'] = 'realtime'
                specs[j_spec] = spec_temp
                #
                fp_highp.write('%d: %s, %s, %s, %s, %s \n' %(j_spec, spec_temp.get('jobid'), spec_temp.get('submittime'), spec_temp.get('runtime'), spec_temp.get('walltime'), spec_temp.get('nodes') ) )
        # <-- logRec_highP
        '''
        # 
        print "len(rtj_id): ", len(rtj_id)
        print "rtj_id: ", rtj_id
        # <-- highP
        # --> logRec
        #
        if simu_tid == 0:
            #rec_fname = './_rec/rec_' + simu_name + '.txt'
            rec_fname = './_rec_' + simu_name + '/rec_' + simu_name + '.txt'
            #fp_rec = open('./_rec/rec_dec-4.txt','w+')
            fp_rec = open( rec_fname,'w+' )
            fp_rec.write('id: jobid, submittime, runtime, walltime, nodes \n')
            for i in range(0, len(specs)):
                fp_rec.write('%d: %s, %s, %s, %s, %s, %s \n' %(i, specs[i].get('jobid'), specs[i].get('submittime'), specs[i].get('runtime'), specs[i].get('walltime'), specs[i].get('nodes'), specs[i].get('start_time') ))
        # <-- logRec
        
        #adjust workload density and simulation start time
        if self.fraction != 1 or self.anchor !=0 :
            tune_workload(specs, self.fraction, self.anchor)

        # dwang: 
        ''' 
        print "simulation time span:"
        print "first job submitted:", sec_to_date(specs[0].get('submittime'))
        print "last job submitted:", sec_to_date(specs[len(specs)-1].get('submittime'))

        self.total_job = len(specs)
        print "total job number:", self.total_job
        ''' 
        # dwang 

        #for i in range(0,self.total_job):
        #    print "_user: ", specs[i].get('user')

        specs.sort(subtimecmp)
        # dwang: 
        ### specs = specs[0:100] # job slice #
        # 
        print "simulation time span:"
        # print "first job submitted:", sec_to_date(specs[0].get('submittime'))
        # print "last job submitted:", sec_to_date(specs[len(specs)-1].get('submittime'))

        self.total_job = len(specs)
        print "total job number:", self.total_job
        # dwang  
        #self.add_jobs(specs)
        self.first_job_start = specs[0].get('submittime') 
        self.unsubmitted_job_spec_dict = self.init_unsubmitted_dict(specs)
        self.event_manager.add_init_events(specs, MACHINE_ID)

        ###
        # samnickolay

        # # test - how many jobs have runtimes longer than there walltime
        # for j_spec in specs:
        #     if float(j_spec.get('walltime'))*60.0 < float(j_spec.get('runtime')):
        #         print((j_spec.get('jobid'), float(j_spec.get('walltime')) * 60.0, j_spec.get('runtime'),
        #                float(j_spec.get('runtime')) - 60.0 *float(j_spec.get('walltime'))))

        ### dwang:
        print "[init_queues] Pcheckp_interv_pcent: ", simu_checkp_t_internval_pcent
        for j_spec in specs:
            checkp_spec = {}
            checkp_spec['jobid'] = j_spec.get('jobid')

            if checkp_overhead_percent == -1:
                checkp_spec['Pcheckp_interv'] = simu_checkp_t_internval_pcent / 100.0 * float(j_spec.get('walltime')) * 60
            else:
                bw_temp = min( float(j_spec.get('nodes'))/128.0*4.0, checkp_w_bandwidth)
                checkp_overhead_time = float(j_spec.get('nodes')) * (checkp_dsize/1024.0) / bw_temp
                number_checkpoints = math.floor((float(j_spec.get('walltime')) * 60 * checkp_overhead_percent / 100.0) /
                                                checkp_overhead_time)
                # number_checkpoints = max(number_checkpoints, 0.1)
                if number_checkpoints > 0:
                    # for cases where runtime > walltime, jobs were checkpointing an extra time
                    # since runtime only ever exceeds walltime by 2 minutes, only need to offset it by 2 minutes
                    walltime_increased = float(j_spec.get('walltime')) * 60 + 2.0 * 60.0
                    checkp_spec['Pcheckp_interv'] = 1.0 / (number_checkpoints+1.0) * walltime_increased

                else:
                    checkp_spec['Pcheckp_interv'] = 10.0 * float(j_spec.get('walltime')) * 60

            checkp_spec['time_last_Pcheckp'] = -1
            self.Pcheckp_specs.append(checkp_spec)
        print "[Pcheckp_specs] len: ", len(self.Pcheckp_specs)
        ###

        return 0

    def add_queues(self, specs):
        '''add queues'''
        return self.queues.add_queues(specs)
    add_queues = exposed(query(add_queues))

    def get_queues(self, specs):
        '''get queues'''
        return self.queues.get_queues(specs)
    get_queues = exposed(query(get_queues))

    def init_unsubmitted_dict(self, specs):
        #jobdict = {}
        specdict = {}
        for spec in specs:
            jobid = str(spec['jobid'])
            #new_job = Job(spec)
            #jobdict[jobid] = new_job
            specdict[jobid] = spec
        return specdict

    def get_live_job_by_id(self, jobid):
        '''get waiting or running job instance by jobid'''
        job = None
        joblist = self.queues.get_jobs([{'jobid':int(jobid)}])
        if joblist:
            job = joblist[0]
        return job
    get_live_job_by_id = exposed(get_live_job_by_id)

    def add_jobs(self, specs):
        '''Add a job'''
        response = self.queues.add_jobs(specs)
        return response
    add_jobs = exposed(query(add_jobs))

    def get_jobs(self, specs):
        '''get a list of jobs, each time triggers time stamp increment and job
        states update'''

        jobs = []

        if self.event_manager.get_go_next():
            #enter a scheduling iteration

            #clear yielding job list
            del self.yielding_job_list[:]

            cur_event = self.event_manager.get_current_event_type()
            cur_event_job = self.event_manager.get_current_event_job

            if cur_event == "S":
                #start reserved job at this time point
                self.run_reserved_jobs()

            if cur_event in ["Q", "E"]:
                #scheduling related events
                self.update_job_states(specs, {}, cur_event)

            self.compute_utility_scores()

            #unhold holding job. MUST be after compute_utility_scores()
            if cur_event == "U":
                cur_job = self.event_manager.get_current_event_job()
                if cur_job in self.job_hold_dict.keys():
                    self.unhold_job(cur_job)
                else:
                    #if the job not in job_hold_dict, do nothing. the job should have already started
                    return []

            if cur_event == "C":
                 if self.job_hold_dict.keys():
                    self.unhold_all()

        self.event_manager.set_go_next(True)

        jobs = self.queues.get_jobs([{'tag':"job"}])

        if self.yielding_job_list:
            jobs = [job for job in jobs if job.jobid not in self.yielding_job_list]

        #before handling the jobs to scheduler, rule out the jobs already having reservations
        if self.reservations:
            jobs = [job for job in jobs if job.jobid not in self.reservations.keys()]

        return jobs
    get_jobs = exposed(query(get_jobs))


    def update_job_states(self, specs, updates, cur_event):
        '''update the state of the jobs associated to the current time stamp'''
        ids_str = str(self.event_manager.get_current_event_job())

        ids = ids_str.split(':')
        #print "current event=", cur_event, " ", ids
        for Id in ids:
            
            if cur_event == "Q":  # Job (Id) is submitted
                tempspec = self.unsubmitted_job_spec_dict.get(Id, None)
                if tempspec == None:
                    continue

                tempspec['state'] = "queued"   #invisible -> queued
                tempspec['is_runnable'] = True   #False -> True
                # dwang:
                # print "j_submit [RE_OVER] job.restart_over: ", tempspec.get('restart_overhead')
                #tempspec['restart_overhead'] = float(tempspec.get('restart_overhead'))
                #
                self.queues.add_jobs([tempspec])
                self.num_waiting += 1

                self.log_job_event("Q", self.get_current_time_date(), tempspec)
                #del self.unsubmitted_job_spec_dict[Id]
                ###
                temp_id = tempspec.get('jobid')
                joblist = self.queues.get_jobs([{'jobid':int(temp_id)}])
                if joblist:
                    job_temp = joblist[0]
                    # print "j_submit [RE_OVER] job.restart_over_RE: ", job_temp.get('restart_overhead')
                ###
            elif cur_event=="E":  # Job (Id) is completed
                completed_job = self.get_live_job_by_id(Id)
                
                flag_killed = 0
                if completed_job == None:
                    continue
                else:
                    for j_temp in self.started_job_dict.itervalues():
                        if j_temp.get('job_killed'):
                            # print "[dw_bqsim] killed_j_pool: ", j_temp.get('jobid')
                            if int(j_temp.get('jobid')) == int(completed_job.get("jobid")):
                                # print "[dw_bqsim] killed_j_MATCH ..."
                                flag_killed = 1
                                break
                    #
                    # print "[JOB_ending] job_id: ", completed_job.get("jobid")
                    # print "[JOB_ending] j_partition: ", completed_job.location

                #
                if flag_killed:
                    break
                
                # print "[JOB_ending] ---> release partition "
                #release partition
                for partition in completed_job.location:
                    self.release_partition(partition)

                partsize = int(self._partitions[partition].size)
                self.num_busy -= partsize

                #log the job end event
                jobspec = completed_job.to_rx()
                #print "end jobspec=", jobspec
                if jobspec['end_time']:
                    end = float(jobspec['end_time'])
                else:
                    end = 0
                end_datetime = sec_to_date(end)
                self.log_job_event("E", end_datetime, jobspec)

                #delete the job instance from self.queues
                self.queues.del_jobs([{'jobid':int(Id)}])
                self.num_running -= 1
                self.num_end += 1

        if not self.cluster_job_trace and not self.batch:
            os.system('clear')
            self.print_screen(cur_event)

        return 0

    def run_reserved_jobs(self):
        #handle reserved job (first priority)
        jobid = int(self.event_manager.get_current_event_job())

        if jobid in self.reservations.keys():
            reserved_location = self.reservations.get(jobid)[1]
            self.start_reserved_job(jobid, [reserved_location])

    def start_reserved_job(self, jobid, nodelist):
       # print "%s: start reserved job %s at %s" % (self.get_current_time_date(), jobid, nodelist)
        self.start_job([{'jobid':int(jobid)}], {'location': nodelist})
        del self.reservations[jobid]

    def run_jobs(self, specs, nodelist, user_name=None, resid=None, walltime=None):
        '''run a queued job, by updating the job state, start_time and
        end_time, invoked by bgsched'''
        #print "run job ", specs, " on nodes", nodelist
        if specs == None:
            return 0

        for spec in specs: 
            action = "start"
            dbgmsg = ""

            if self.coscheduling:
                local_job_id = spec.get('jobid') #int
                #check whether there is a mate job

                mate_job_id = self.mate_job_dict.get(local_job_id, 0)

                #if mate job exists, get the status of the mate job
                if mate_job_id > 0:
                    remote_status = self.get_mate_jobs_status_local(mate_job_id).get('status', "unknown!")
                    dbgmsg += "local=%s;mate=%s;mate_status=%s" % (local_job_id, mate_job_id, remote_status)

                    if remote_status in ["queuing", "unsubmitted"]:
                        if self.cosched_scheme == "hold": # hold resource if mate cannot run, favoring job
                            action = "start_both_or_hold"
                        if self.cosched_scheme == "yield": # give up if mate cannot run, favoring sys utilization
                            action = "start_both_or_yield"
                    if remote_status == "holding":
                        action = "start_both"

                #to be inserted co-scheduling handling code
                else:
                    pass

            if action == "start":
                #print "BQSIM-normal start job %s on nodes %s" % (spec['jobid'], nodelist)
                # 
                # print "[dw_bqsim] p_START run_jobs(): ", self.get_current_time_sec()
                #
                p = self.cached_partitions[nodelist[0]]
                # print "[dw_bqsim] p_STATE_before run_jobs(): ", p.state
                #
                for p_parents_name in p.parents:
                    p_parents = self.cached_partitions[p_parents_name]
                    # print "[dw_bqsim] ppa_name_before run_jobs(): ", p_parents.name
                    # print "[dw_bqsim] ppa_STATE_before run_jobs(): ", p_parents.state
                #
                #
                for p_children_name in p.children:
                    p_children = self.cached_partitions[p_children_name]
                    # print "[dw_bqsim] pch_name_before run_jobs(): ", p_children.name
                    # print "[dw_bqsim] pch_STATE_before run_jobs(): ", p_children.state
                #
                self.start_job([spec], {'location': nodelist})
                # 
                # print "[dw_bqsim] p_id run_jobs(): ", spec.get('jobid')
                # print "[dw_bqsim] p_START run_jobs(): ", self.get_current_time_sec()
                #
                # print "[dw_bqsim] p_name_after run_jobs(): ", p.name
                # print "[dw_bqsim] p_STATE_after run_jobs(): ", p.state
                #
                for p_children_name in p.children:
                    p_children = self.cached_partitions[p_children_name]
                    # print "[dw_bqsim] pch_name_after run_jobs(): ", p_children.name
                    # print "[dw_bqsim] pch_STATE_after run_jobs(): ", p_children.state
                #
            elif action == "start_both_or_hold":
                #print "try to hold job %s on location %s" % (local_job_id, nodelist)
                mate_job_can_run = False

                #try to invoke a scheduling iteration to see if remote yielding job can run now
                try:
                    mate_job_can_run = ComponentProxy(REMOTE_QUEUE_MANAGER).try_to_run_mate_job(mate_job_id)
                except:
                    self.logger.error("failed to connect to remote queue-manager component!")

                if mate_job_can_run:
                    #now that mate has been started, start local job
                    self.start_job([spec], {'location': nodelist})
                    dbgmsg += " ###start both"
                else:
                    self.hold_job(spec, {'location': nodelist})
            elif action == "start_both":
                #print "start both mated jobs %s and %s" % (local_job_id, mate_job_id)
                self.start_job([spec], {'location': nodelist})
                ComponentProxy(REMOTE_QUEUE_MANAGER).run_holding_job([{'jobid':mate_job_id}])
            elif action == "start_both_or_yield":
                #print "BQSIM: In order to run local job %s, try to run mate job %s" % (local_job_id, mate_job_id)
                mate_job_can_run = False

                #try to invoke a scheduling iteration to see if remote yielding job can run now
                try:
                    mate_job_can_run = ComponentProxy(REMOTE_QUEUE_MANAGER).try_to_run_mate_job(mate_job_id)
                except:
                    self.logger.error("failed to connect to remote queue-manager component!")

                if mate_job_can_run:
                    #now that mate has been started, start local job
                    self.start_job([spec], {'location': nodelist})
                    dbgmsg += " ###start both"
                else:
                    #mate job cannot run, give up the turn. mark the job as yielding.
                    job_id = spec.get('jobid')
                    self.yielding_job_list.append(job_id)  #int
                    #record the first time this job yields
                    if not self.first_yield_hold_time_dict.has_key(job_id):
                        self.first_yield_hold_time_dict[job_id] = self.get_current_time_sec()
                        #self.dbglog.LogMessage("%s: job %s first yield" % (self.get_current_time_date(), job_id))

                    #self.release_allocated_nodes(nodelist)
            if len(dbgmsg) > 0:
                #self.dbglog.LogMessage(dbgmsg)
                pass

            if self.walltime_aware_aggr:
                self.run_matched_job(spec['jobid'], nodelist[0])

        #set tag false, enable scheduling another job at the same time
        self.event_manager.set_go_next(False)
        #self.print_screen()

        return len(specs)
    run_jobs = exposed(run_jobs)


    def run_rt_jobs(self, specs, nodelist, user_name=None, resid=None, walltime=None):
        '''run a queued job, by updating the job state, start_time and
        end_time, invoked by bgsched'''

        # print "[dw_rtj] run_rt_jobs() start ... "
        #print "run job ", specs, " on nodes", nodelist
        if specs == None:
            return 0
        for spec in specs:
            action = "start"
            dbgmsg = ""
            if self.coscheduling:
                local_job_id = spec.get('jobid') #int
                #check whether there is a mate job

                mate_job_id = self.mate_job_dict.get(local_job_id, 0)

                #if mate job exists, get the status of the mate job
                if mate_job_id > 0:
                    remote_status = self.get_mate_jobs_status_local(mate_job_id).get('status', "unknown!")
                    dbgmsg += "local=%s;mate=%s;mate_status=%s" % (local_job_id, mate_job_id, remote_status)

                    if remote_status in ["queuing", "unsubmitted"]:
                        if self.cosched_scheme == "hold": # hold resource if mate cannot run, favoring job
                            action = "start_both_or_hold"
                        if self.cosched_scheme == "yield": # give up if mate cannot run, favoring sys utilization
                            action = "start_both_or_yield"
                    if remote_status == "holding":
                        action = "start_both"

                #to be inserted co-scheduling handling code
                else:
                    pass

            if action == "start":
                #print "BQSIM-normal start job %s on nodes %s" % (spec['jobid'], nodelist)
                # print "[dw_rtj] run_rt_jobs() -> start_rt_jobs() ... "
                #
                # print "[dw_bqsim] p_START run_rt_jobs(): ", self.get_current_time_sec()
                #
                self.start_rt_job([spec], {'location': nodelist})
                # 
                # print "[dw_bqsim] p_START run_rt_jobs(): ", self.get_current_time_sec()
                #print "[dw_bqsim] p_WAIT run_rt_jobs(): ", now - str2snum(rt_subtime)
                #
                p = self._partitions[nodelist[0]]
                # print "[dw_bqsim] p_name_after run_rt_jobs(): ", p.name
                # print "[dw_bqsim] p_STATE_after run_rt_jobs(): ", p.state
                #
                for p_children_name in p.children:
                    p_children = self.cached_partitions[p_children_name]
                    # print "[dw_bqsim] pch_name_after run_rt_jobs(): ", p_children.name
                    # print "[dw_bqsim] pch_STATE_after run_rt_jobs(): ", p_children.state
                #
            elif action == "start_both_or_hold":
                #print "try to hold job %s on location %s" % (local_job_id, nodelist)
                mate_job_can_run = False

                #try to invoke a scheduling iteration to see if remote yielding job can run now
                try:
                    mate_job_can_run = ComponentProxy(REMOTE_QUEUE_MANAGER).try_to_run_mate_job(mate_job_id)
                except:
                    self.logger.error("failed to connect to remote queue-manager component!")

                if mate_job_can_run:
                    #now that mate has been started, start local job
                    self.start_rt_job([spec], {'location': nodelist})
                    dbgmsg += " ###start both"
                else:
                    self.hold_job(spec, {'location': nodelist})
            elif action == "start_both":
                #print "start both mated jobs %s and %s" % (local_job_id, mate_job_id)
                self.start_rt_job([spec], {'location': nodelist})
                ComponentProxy(REMOTE_QUEUE_MANAGER).run_holding_job([{'jobid':mate_job_id}])
            elif action == "start_both_or_yield":
                #print "BQSIM: In order to run local job %s, try to run mate job %s" % (local_job_id, mate_job_id)
                mate_job_can_run = False

                #try to invoke a scheduling iteration to see if remote yielding job can run now
                try:
                    mate_job_can_run = ComponentProxy(REMOTE_QUEUE_MANAGER).try_to_run_mate_job(mate_job_id)
                except:
                    self.logger.error("failed to connect to remote queue-manager component!")

                if mate_job_can_run:
                    #now that mate has been started, start local job
                    self.start_rt_job([spec], {'location': nodelist})
                    dbgmsg += " ###start both"
                else:
                    #mate job cannot run, give up the turn. mark the job as yielding.
                    job_id = spec.get('jobid')
                    self.yielding_job_list.append(job_id)  #int
                    #record the first time this job yields
                    if not self.first_yield_hold_time_dict.has_key(job_id):
                        self.first_yield_hold_time_dict[job_id] = self.get_current_time_sec()
                        #self.dbglog.LogMessage("%s: job %s first yield" % (self.get_current_time_date(), job_id))

                    #self.release_allocated_nodes(nodelist)
            if len(dbgmsg) > 0:
                #self.dbglog.LogMessage(dbgmsg)
                pass

            if self.walltime_aware_aggr:
                self.run_matched_job(spec['jobid'], nodelist[0])

        #set tag false, enable scheduling another job at the same time
        self.event_manager.set_go_next(False)
        #self.print_screen()

        return len(specs)
    run_rt_jobs = exposed(run_rt_jobs)

    # dwang: 
    def run_jobs_wOverhead(self, specs, nodelist, rest_overhead, user_name=None, resid=None, walltime=None ):
        '''run a queued job, by updating the job state, start_time and
        end_time, invoked by bgsched'''
        #print "run job ", specs, " on nodes", nodelist
        if specs == None:
            return 0

        for spec in specs: 
            action = "start"
            dbgmsg = ""

            if self.coscheduling:
                local_job_id = spec.get('jobid') #int
                #check whether there is a mate job

                mate_job_id = self.mate_job_dict.get(local_job_id, 0)

                #if mate job exists, get the status of the mate job
                if mate_job_id > 0:
                    remote_status = self.get_mate_jobs_status_local(mate_job_id).get('status', "unknown!")
                    dbgmsg += "local=%s;mate=%s;mate_status=%s" % (local_job_id, mate_job_id, remote_status)

                    if remote_status in ["queuing", "unsubmitted"]:
                        if self.cosched_scheme == "hold": # hold resource if mate cannot run, favoring job
                            action = "start_both_or_hold"
                        if self.cosched_scheme == "yield": # give up if mate cannot run, favoring sys utilization
                            action = "start_both_or_yield"
                    if remote_status == "holding":
                        action = "start_both"

                #to be inserted co-scheduling handling code
                else:
                    pass

            if action == "start":
                #print "BQSIM-normal start job %s on nodes %s" % (spec['jobid'], nodelist)
                # 
                # print "[dw_bqsim] p_START run_jobs(): ", self.get_current_time_sec()
                #
                p = self.cached_partitions[nodelist[0]]
                # print "[dw_bqsim] p_STATE_before run_jobs(): ", p.state
                #
                for p_parents_name in p.parents:
                    p_parents = self.cached_partitions[p_parents_name]
                    # print "[dw_bqsim] ppa_name_before run_jobs(): ", p_parents.name
                    # print "[dw_bqsim] ppa_STATE_before run_jobs(): ", p_parents.state
                #
                #
                for p_children_name in p.children:
                    p_children = self.cached_partitions[p_children_name]
                    # print "[dw_bqsim] pch_name_before run_jobs(): ", p_children.name
                    # print "[dw_bqsim] pch_STATE_before run_jobs(): ", p_children.state
                ## 
                self.start_job_wOverhead([spec], {'location': nodelist}, rest_overhead)
                ## 
                # print "[dw_bqsim] p_id run_jobs(): ", spec.get('jobid')
                # print "[dw_bqsim] p_START run_jobs(): ", self.get_current_time_sec()
                #
                # print "[dw_bqsim] p_name_after run_jobs(): ", p.name
                # print "[dw_bqsim] p_STATE_after run_jobs(): ", p.state
                #
                for p_children_name in p.children:
                    p_children = self.cached_partitions[p_children_name]
                    # print "[dw_bqsim] pch_name_after run_jobs(): ", p_children.name
                    # print "[dw_bqsim] pch_STATE_after run_jobs(): ", p_children.state
                #
            elif action == "start_both_or_hold":
                #print "try to hold job %s on location %s" % (local_job_id, nodelist)
                mate_job_can_run = False

                #try to invoke a scheduling iteration to see if remote yielding job can run now
                try:
                    mate_job_can_run = ComponentProxy(REMOTE_QUEUE_MANAGER).try_to_run_mate_job(mate_job_id)
                except:
                    self.logger.error("failed to connect to remote queue-manager component!")

                if mate_job_can_run:
                    #now that mate has been started, start local job
                    self.start_job([spec], {'location': nodelist})
                    dbgmsg += " ###start both"
                else:
                    self.hold_job(spec, {'location': nodelist})
            elif action == "start_both":
                #print "start both mated jobs %s and %s" % (local_job_id, mate_job_id)
                self.start_job([spec], {'location': nodelist})
                ComponentProxy(REMOTE_QUEUE_MANAGER).run_holding_job([{'jobid':mate_job_id}])
            elif action == "start_both_or_yield":
                #print "BQSIM: In order to run local job %s, try to run mate job %s" % (local_job_id, mate_job_id)
                mate_job_can_run = False

                #try to invoke a scheduling iteration to see if remote yielding job can run now
                try:
                    mate_job_can_run = ComponentProxy(REMOTE_QUEUE_MANAGER).try_to_run_mate_job(mate_job_id)
                except:
                    self.logger.error("failed to connect to remote queue-manager component!")

                if mate_job_can_run:
                    #now that mate has been started, start local job
                    self.start_job([spec], {'location': nodelist})
                    dbgmsg += " ###start both"
                else:
                    #mate job cannot run, give up the turn. mark the job as yielding.
                    job_id = spec.get('jobid')
                    self.yielding_job_list.append(job_id)  #int
                    #record the first time this job yields
                    if not self.first_yield_hold_time_dict.has_key(job_id):
                        self.first_yield_hold_time_dict[job_id] = self.get_current_time_sec()
                        #self.dbglog.LogMessage("%s: job %s first yield" % (self.get_current_time_date(), job_id))

                    #self.release_allocated_nodes(nodelist)
            if len(dbgmsg) > 0:
                #self.dbglog.LogMessage(dbgmsg)
                pass

            if self.walltime_aware_aggr:
                self.run_matched_job(spec['jobid'], nodelist[0])

        #set tag false, enable scheduling another job at the same time
        self.event_manager.set_go_next(False)
        #self.print_screen()

        return len(specs)
    run_jobs_wOverhead = exposed(run_jobs_wOverhead) 
    # dwang 

    # dwang: 
    def run_rt_jobs_wOverhead(self, specs, nodelist, rtj_overhead, user_name=None, resid=None, walltime=None):
        '''run a queued job, by updating the job state, start_time and
        end_time, invoked by bgsched'''

        # print "[dw_rtj] run_rt_jobs() start ... "
        #print "run job ", specs, " on nodes", nodelist
        if specs == None:
            return 0
        for spec in specs:
            action = "start"
            dbgmsg = ""
            if self.coscheduling:
                local_job_id = spec.get('jobid') #int
                #check whether there is a mate job

                mate_job_id = self.mate_job_dict.get(local_job_id, 0)

                #if mate job exists, get the status of the mate job
                if mate_job_id > 0:
                    remote_status = self.get_mate_jobs_status_local(mate_job_id).get('status', "unknown!")
                    dbgmsg += "local=%s;mate=%s;mate_status=%s" % (local_job_id, mate_job_id, remote_status)

                    if remote_status in ["queuing", "unsubmitted"]:
                        if self.cosched_scheme == "hold": # hold resource if mate cannot run, favoring job
                            action = "start_both_or_hold"
                        if self.cosched_scheme == "yield": # give up if mate cannot run, favoring sys utilization
                            action = "start_both_or_yield"
                    if remote_status == "holding":
                        action = "start_both"

                #to be inserted co-scheduling handling code
                else:
                    pass

            if action == "start":
                #print "BQSIM-normal start job %s on nodes %s" % (spec['jobid'], nodelist)
                # print "[dw_rtj] run_rt_jobs() -> start_rt_jobs() ... "
                # 
                # print "[dw_bqsim] p_START run_rt_jobs(): ", self.get_current_time_sec()
                self.start_rt_job_wOverhead([spec], {'location': nodelist}, rtj_overhead)
                # 
                # print "[dw_bqsim] p_START run_rt_jobs(): ", self.get_current_time_sec()
                #print "[dw_bqsim] p_WAIT run_rt_jobs(): ", now - str2snum(rt_subtime)
                #
                p = self._partitions[nodelist[0]]
                # print "[dw_bqsim] p_name_after run_rt_jobs(): ", p.name
                # print "[dw_bqsim] p_STATE_after run_rt_jobs(): ", p.state
                #
                for p_children_name in p.children:
                    p_children = self.cached_partitions[p_children_name]
                    # print "[dw_bqsim] pch_name_after run_rt_jobs(): ", p_children.name
                    # print "[dw_bqsim] pch_STATE_after run_rt_jobs(): ", p_children.state
                #
            elif action == "start_both_or_hold":
                #print "try to hold job %s on location %s" % (local_job_id, nodelist)
                mate_job_can_run = False

                #try to invoke a scheduling iteration to see if remote yielding job can run now
                try:
                    mate_job_can_run = ComponentProxy(REMOTE_QUEUE_MANAGER).try_to_run_mate_job(mate_job_id)
                except:
                    self.logger.error("failed to connect to remote queue-manager component!")

                if mate_job_can_run:
                    #now that mate has been started, start local job
                    self.start_rt_job([spec], {'location': nodelist})
                    dbgmsg += " ###start both"
                else:
                    self.hold_job(spec, {'location': nodelist})
            elif action == "start_both":
                #print "start both mated jobs %s and %s" % (local_job_id, mate_job_id)
                self.start_rt_job([spec], {'location': nodelist})
                ComponentProxy(REMOTE_QUEUE_MANAGER).run_holding_job([{'jobid':mate_job_id}])
            elif action == "start_both_or_yield":
                #print "BQSIM: In order to run local job %s, try to run mate job %s" % (local_job_id, mate_job_id)
                mate_job_can_run = False

                #try to invoke a scheduling iteration to see if remote yielding job can run now
                try:
                    mate_job_can_run = ComponentProxy(REMOTE_QUEUE_MANAGER).try_to_run_mate_job(mate_job_id)
                except:
                    self.logger.error("failed to connect to remote queue-manager component!")

                if mate_job_can_run:
                    #now that mate has been started, start local job
                    self.start_rt_job([spec], {'location': nodelist})
                    dbgmsg += " ###start both"
                else:
                    #mate job cannot run, give up the turn. mark the job as yielding.
                    job_id = spec.get('jobid')
                    self.yielding_job_list.append(job_id)  #int
                    #record the first time this job yields
                    if not self.first_yield_hold_time_dict.has_key(job_id):
                        self.first_yield_hold_time_dict[job_id] = self.get_current_time_sec()
                        #self.dbglog.LogMessage("%s: job %s first yield" % (self.get_current_time_date(), job_id))

                    #self.release_allocated_nodes(nodelist)
            if len(dbgmsg) > 0:
                #self.dbglog.LogMessage(dbgmsg)
                pass

            if self.walltime_aware_aggr:
                self.run_matched_job(spec['jobid'], nodelist[0])

        #set tag false, enable scheduling another job at the same time
        self.event_manager.set_go_next(False)
        #self.print_screen()

        return len(specs)
    run_rt_jobs_wOverhead = exposed(run_rt_jobs_wOverhead)
    # dwang 


    # dwang: 
    def test_run_jobs(self, specs, nodelist, user_name=None, resid=None, walltime=None):
        print "[dw_bqsim] test_run_jobs() ..."

        def _test_run_jobs(job, nodes):
            print "[dw_bqsim] _test_run_jobs() ..."
    test_run_jobs = exposed(test_run_jobs)
    
    
    def kill_job(self, job, partition_name):
        print "[dw_bqsim] kill_job_updated() ..."
        #
        #job_temp = job.to_rx()
        #jobspec = self.get_live_job_by_id(job.jobid)
        job_temp = self.get_live_job_by_id(job.jobid)
        #
        end = self.get_current_time()
        end_datetime = sec_to_date(end)
        #
        
        global preempt_cost_runtime
        preempt_cost_runtime += int(job_temp.get('nodes')) * float(end - job_temp.get('start_time'))
        #
        print "4_jkill submit_time: ", job_temp.get('submittime')
        print "4_jkill start_time: ", job_temp.get('start_time')
        print "4_jkill location: ", job_temp.get('location')
        print "4_jkill walltime: ", end - job_temp.get('start_time')
        print "4_jkill queue: ", job_temp.get('queue')
        print "4_jkill nodes: ", job_temp.get('nodes')
        print "4_jkill runtime: ", job_temp.get('runtime')
        print "4_jkill hold_time: ", job_temp.get('hold_time')
        print "4_jkill remain_time: ", 0
        #
        jobspec = {'jobid':str(job_temp.get('jobid')), 'submittime': job_temp.get('submittime'),
                    'start_time': job_temp.get('start_time'), 'walltime': end-job_temp.get('start_time'),
                    'end_time': end, 'location': job_temp.get('location'), 'queue': job_temp.get('queue'), 'nodes': job_temp.get('nodes'),
            'runtime': job_temp.get('runtime'),
            'runtime_org': job_temp.get('runtime'),
            'hold_time': job_temp.get('hold_time'),
            'job_killed': True,
            'walltime_org': job_temp.get('walltime'),
            'start_time_org': job_temp.get('start_time') }
        #
        ##self.update_job_states(jobspec, {}, "E")
        self.started_job_dict[str(jobspec['jobid'])] = jobspec
        print "w/_jkill end: ", end
        ##
        ###self.insert_time_stamp( end, "E", jobspec )
        ##
        #release partition
        completed_job = job_temp
        for partition in completed_job.location:
            self.release_partition(partition)
            print "_jkill partition: ", partition
            partsize = int(self._partitions[partition].size)
            self.num_busy -= partsize
                
        #log the job end event
        #jobspec = completed_job.to_rx()
        #print "end jobspec=", jobspec
        #self.log_job_event("E", end_datetime, jobspec)
                
        #delete the job instance from self.queues
        self.queues.del_jobs([{'jobid':int(job.jobid)}])
        self.num_running -= 1
        self.num_end += 1

        #
        print "_jkill del_event() _1 ... "
        self.event_manager.del_event(int(job.jobid))
        print "_jkill del_event() _2 ... "
    kill_job = exposed(kill_job)


    def kill_job_wOverhead(self, job, partition_name, dsize_pnode, bw_temp_read):
        # print "[dw_bqsim] kill_job_updated() ..."
        #
        #job_temp = job.to_rx()
        #jobspec = self.get_live_job_by_id(job.jobid)
        job_temp = self.get_live_job_by_id(job.jobid)
        #
        end = self.get_current_time()
        end_datetime = sec_to_date(end)
        #
        
        global preempt_cost_runtime
        preempt_cost_runtime += int(job_temp.get('nodes')) * float(end - job_temp.get('start_time'))
        #
        # print "4_jkill submit_time: ", job_temp.get('submittime')
        # print "4_jkill start_time: ", job_temp.get('start_time')
        # print "4_jkill location: ", job_temp.get('location')
        # print "4_jkill walltime: ", end - job_temp.get('start_time')
        # print "4_jkill queue: ", job_temp.get('queue')
        # print "4_jkill nodes: ", job_temp.get('nodes')
        # print "4_jkill runtime: ", job_temp.get('runtime')
        # print "4_jkill hold_time: ", job_temp.get('hold_time')
        # print "4_jkill remain_time: ", 0
        ##
        kill_partsize = 0
        #calculate release_partition
        completed_job = job_temp
        for partition in completed_job.location:
            partsize = int(self._partitions[partition].size)
            kill_partsize += partsize
        ##
        #dsize_restart_pnode = 4.0 / 1024
        #bw_temp = 1536 * 0.8
        dsize_restart_pnode = dsize_pnode/1024.0
        ###bw_temp = bw_temp_read
        bw_temp = min( kill_partsize/128 * 4, bw_temp_read )
        restart_overhead_temp = kill_partsize * dsize_restart_pnode / bw_temp
        # restart_overhead_temp = 0
        
        ##
        # print "[XXX] end_type: ", type(end)
        # print "[XXX] starttime_type: ", type(job_temp.get('start_time'))
        # print "[XXX] walltime_type: ", type(job_temp.get('walltime'))
        # print "[XXX] runtime_type: ", type(job_temp.get('runtime'))
        ##
        jobspec = {'jobid':str(job_temp.get('jobid')), 'submittime': job_temp.get('submittime'), 'start_time': job_temp.get('start_time'),
                    'walltime': end-job_temp.get('start_time'), 'end_time': end, 'location': job_temp.get('location'),
                    'queue': job_temp.get('queue'),'nodes': job_temp.get('nodes'), 'hold_time': job_temp.get('hold_time'),
                    'job_killed': True,'walltime_org': str((float(job_temp.get('walltime'))*60 - (end - job_temp.get('start_time')))/60),
                    'runtime': str(float(job_temp.get('runtime'))- (end - job_temp.get('start_time'))),
                    'runtime_org': job_temp.get('runtime'),
                    'start_time_org': job_temp.get('start_time'),
                    'restart_overhead': restart_overhead_temp }
        #
        ##self.update_job_states(jobspec, {}, "E")
        self.started_job_dict[str(jobspec['jobid'])] = jobspec
        # print "w/_jkill end: ", end
        ##
        ###self.insert_time_stamp( end, "E", jobspec )
        #
        #release partition
        completed_job = job_temp
        for partition in completed_job.location:
            self.release_partition(partition)
            # print "_jkill partition: ", partition
            partsize = int(self._partitions[partition].size)
            self.num_busy -= partsize
            #
            # kill_partsize += partsize
            #
            
        ## waste_cost
        self.preempt_waste_runtime_checkpoint += kill_partsize * restart_overhead_temp
        self.preempt_waste_runtime_restart += kill_partsize * restart_overhead_temp
        ##

        #log the job end event
        #jobspec = completed_job.to_rx()
        #print "end jobspec=", jobspec
        #self.log_job_event("E", end_datetime, jobspec)
                
        #delete the job instance from self.queues
        self.queues.del_jobs([{'jobid':int(job.jobid)}])
        self.num_running -= 1
        self.num_end += 1
        
        #
        # print "_jkill del_event() _1 ... "
        self.event_manager.del_event(int(job.jobid))
        # print "_jkill del_event() _2 ... "
        ##
        
        donetime = float(end) - float(job_temp.get('start_time'))
        lefttime = float(job_temp.get('runtime')) - donetime 
        partsize = kill_partsize
        return donetime, lefttime, partsize 
    kill_job_wOverhead = exposed(kill_job_wOverhead)


    def kill_job_wPcheckP(self, job, partition_name, checkp_t_internval, dsize_pnode, bw_temp_read, fp_backf):
        print "[dw_bqsim] kill_job_wPcheckP() ..."
        #
        #job_temp = job.to_rx()
        #jobspec = self.get_live_job_by_id(job.jobid)
        job_temp = self.get_live_job_by_id(job.jobid)
        #
        end = self.get_current_time()
        end_datetime = sec_to_date(end)
        #
        
        global preempt_cost_runtime
        preempt_cost_runtime += int(job_temp.get('nodes')) * float(end - job_temp.get('start_time'))
        #
        
        restart_overhead_temp = 0.0
        ###
        total_start_time = self.first_job_start
        '''
        j_count = 0
        total_start_time = 0.0
        for job in self.started_job_dict.itervalues():
            j_start_time = float(job.get('start_time')) 
            if j_count == 0:
                total_start_time = j_start_time
            if j_start_time < total_start_time: #dwang
                total_start_time = j_start_time #dwang
            j_count += 1 
        '''
        id_tInterval_all= math.floor( float(job_temp.get('runtime'))/checkp_t_internval )
        id_tInterval_tillNow = math.floor( (end - job_temp.get('start_time'))/checkp_t_internval )
        last_Pcheckp_time = job_temp.get('start_time') + int(id_tInterval_tillNow) * checkp_t_internval
        
        ##
        #dsize_restart_pnode = 4.0 / 1024
        #bw_temp = 1536 * 0.8
        dsize_restart_pnode = dsize_pnode/1024.0
        ###bw_temp = bw_temp_read
        ###

        print "4_jkill submit_time: ", job_temp.get('submittime')
        print "4_jkill start_time: ", job_temp.get('start_time')
        print "4_jkill location: ", job_temp.get('location')
        print "4_jkill walltime: ", job_temp.get('walltime')
        print "4_jkill walltime_wcheckp_3: ", (float(job_temp.get('walltime'))*60 - (last_Pcheckp_time - job_temp.get('start_time')))/60
        print "4_jkill queue: ", job_temp.get('queue')
        print "4_jkill nodes: ", job_temp.get('nodes')
        print "4_jkill runtime: ", job_temp.get('runtime')
        print "4_jkill runtime_wcheckp_3: ", float(job_temp.get('runtime'))- (last_Pcheckp_time - job_temp.get('start_time'))
        print "4_jkill hold_time: ", job_temp.get('hold_time')
        print "4_jkill done_time: ", end - job_temp.get('start_time')
        ##
        kill_partsize = 0
        #calculate release_partition
        completed_job = job_temp
        for partition in completed_job.location:
            partsize = int(self._partitions[partition].size)
            kill_partsize += partsize
        ##
        bw_temp = min( kill_partsize/128 * 4, bw_temp_read )
        restart_overhead_temp = kill_partsize * dsize_restart_pnode / bw_temp
        
        # jobspec = {'jobid':str(job_temp.get('jobid')), 'submittime': job_temp.get('submittime'), 'start_time': job_temp.get('start_time'),
        #    'walltime': end-job_temp.get('start_time'), 'end_time': end, 'location': job_temp.get('location'),
        #        'queue': job_temp.get('queue'),'nodes': job_temp.get('nodes'), 'hold_time': job_temp.get('hold_time'),
        #            'job_killed': True,'walltime_org': str((float(job_temp.get('walltime'))*60 - (end - job_temp.get('start_time')))/60),
        #            'runtime': str(float(job_temp.get('runtime'))- (end - job_temp.get('start_time'))),
        #            'runtime_org': job_temp.get('runtime'),
        #            'start_time_org': job_temp.get('start_time'),
        #            'restart_overhead': restart_overhead_temp }
        ##
        jobspec = {'jobid':str(job_temp.get('jobid')), 'submittime': job_temp.get('submittime'), 'start_time': job_temp.get('start_time'),
                    #'walltime': end-job_temp.get('start_time'),
                    'walltime': str((float(job_temp.get('walltime')) - (last_Pcheckp_time - job_temp.get('start_time')))/60),
                    'end_time': end, 'location': job_temp.get('location'), 'queue': job_temp.get('queue'),
                    'nodes': job_temp.get('nodes'), 'hold_time': job_temp.get('hold_time'), 'job_killed': True,
                    #'walltime_org': job_temp.get('walltime'), 'runtime': job_temp.get('runtime'),
                    'walltime_org': str((float(job_temp.get('walltime'))*60 - (last_Pcheckp_time - job_temp.get('start_time')))/60),
                    'runtime': str(float(job_temp.get('runtime'))- (last_Pcheckp_time - job_temp.get('start_time'))),
                    'runtime_org': job_temp.get('runtime'),
                    'start_time_org': job_temp.get('start_time'),
                    'restart_overhead': restart_overhead_temp } 
        #
        ##self.update_job_states(jobspec, {}, "E")
        self.started_job_dict[str(jobspec['jobid'])] = jobspec
        print "w/_jkill end: ", end
        print "w/_jkill runtime: ", jobspec.get('runtime')
        ##
        ###self.insert_time_stamp( end, "E", jobspec )
        #
        #release partition
        completed_job = job_temp
        for partition in completed_job.location:
            self.release_partition(partition)
            print "_jkill partition: ", partition 
                
            partsize = int(self._partitions[partition].size)
            self.num_busy -= partsize
            #
            # kill_partsize += partsize
            #

        ## waste_cost
        print "[CKP_UTIL_2] jkill id: ", job.jobid
        print "[CKP_UTIL_2] jkill sub: ", id_tInterval_all - id_tInterval_tillNow
        print "[CKP_UTIL_2] jkill pSize: ", kill_partsize
        print "[CKP_UTIL_2] jkill reOverhead: ", restart_overhead_temp
        #
        self.preempt_waste_runtime_checkpoint -= kill_partsize * (id_tInterval_all - id_tInterval_tillNow) * restart_overhead_temp
        print "[CKP_UTIL_2] SUB_ckp_cost: ", kill_partsize * (id_tInterval_all - id_tInterval_tillNow) * restart_overhead_temp
        print "[CKP_UTIL_2] cur_ckp_cost: ", self.preempt_waste_runtime_checkpoint
        ##
        fp_backf.write('- %s: %d, %f, %f, %f \n' %( job.jobid, kill_partsize, restart_overhead_temp, kill_partsize * (id_tInterval_all - id_tInterval_tillNow) * restart_overhead_temp,
                                                   self.preempt_waste_runtime_checkpoint ))
        #
        self.preempt_waste_runtime_restart += kill_partsize * restart_overhead_temp
        self.preempt_waste_runtime_rework += kill_partsize * (end - last_Pcheckp_time)
        ##
        
        #log the job end event
        #jobspec = completed_job.to_rx()
        #print "end jobspec=", jobspec
        #self.log_job_event("E", end_datetime, jobspec)
                
        #delete the job instance from self.queues
        self.queues.del_jobs([{'jobid':int(job.jobid)}])
        self.num_running -= 1
        self.num_end += 1 
        #
        print "_jkill del_event() _1 ... "
        self.event_manager.del_event(int(job.jobid))
        print "_jkill del_event() _2 ... "
        ##
        
        ##
        donetime = float(end) - float(job_temp.get('start_time'))
        lefttime = float(job_temp.get('runtime')) - donetime 
        partsize = kill_partsize
        return donetime, lefttime, partsize 
    kill_job_wPcheckP = exposed(kill_job_wPcheckP)



    def kill_job_wPcheckP_app(self, job, partition_name, dsize_pnode, bw_temp_read):
        print "[dw_bqsim] kill_job_wPcheckP() ..."
        #
        #job_temp = job.to_rx()
        #jobspec = self.get_live_job_by_id(job.jobid)
        job_temp = self.get_live_job_by_id(job.jobid)
        #
        end = self.get_current_time()
        end_datetime = sec_to_date(end)
        #
        
        global preempt_cost_runtime
        preempt_cost_runtime += int(job_temp.get('nodes')) * float(end - job_temp.get('start_time'))
        #
        
        restart_overhead_temp = 0.0
        ###
        total_start_time = self.first_job_start
        ###
        temp_last_Pcheckp = 0 
        temp_checkp_interv = 0 
        for jj in range(0,len(self.Pcheckp_specs)): 
            if int(job.jobid) == int(self.Pcheckp_specs[jj].get('jobid')):
                print "[Pcheckp_app] jid_match ..."
                temp_last_Pcheckp = self.Pcheckp_specs[jj].get('time_last_Pcheckp')
                temp_checkp_interv = self.Pcheckp_specs[jj].get('Pcheckp_interv')
        ### 

        id_tInterval_all= math.floor( float(job_temp.get('runtime'))/temp_checkp_interv )
        id_tInterval_tillNow = math.floor( (end - job_temp.get('start_time'))/temp_checkp_interv )
        last_Pcheckp_time = job_temp.get('start_time') + int(id_tInterval_tillNow) * temp_checkp_interv
        '''
        last_Pcheckp_time = temp_last_Pcheckp
        '''        
        ##
        #dsize_restart_pnode = 4.0 / 1024
        #bw_temp = 1536 * 0.8
        dsize_restart_pnode = dsize_pnode/1024.0
        ###bw_temp = bw_temp_read
        ###

        print "4_jkill submit_time: ", job_temp.get('submittime')
        print "4_jkill start_time: ", job_temp.get('start_time')
        print "4_jkill location: ", job_temp.get('location')
        print "4_jkill walltime: ", end - job_temp.get('start_time')
        print "4_jkill queue: ", job_temp.get('queue')
        print "4_jkill nodes: ", job_temp.get('nodes')
        print "4_jkill runtime: ", job_temp.get('runtime')
        print "4_jkill hold_time: ", job_temp.get('hold_time')
        print "4_jkill remain_time: ", 0
        ##
        kill_partsize = 0
        #calculate release_partition
        completed_job = job_temp
        for partition in completed_job.location:
            partsize = int(self._partitions[partition].size)
            kill_partsize += partsize
        ##
        bw_temp = min( kill_partsize/128 * 4, bw_temp_read )
        restart_overhead_temp = kill_partsize * dsize_restart_pnode / bw_temp
        ##
        jobspec = {'jobid':str(job_temp.get('jobid')), 'submittime': job_temp.get('submittime'), 'start_time': job_temp.get('start_time'),
                    #'walltime': end-job_temp.get('start_time'),
                    'walltime': str((float(job_temp.get('walltime')) - (last_Pcheckp_time - job_temp.get('start_time')))/60),
                    'end_time': end, 'location': job_temp.get('location'), 'queue': job_temp.get('queue'),
                    'nodes': job_temp.get('nodes'), 'hold_time': job_temp.get('hold_time'), 'job_killed': True,
                    #'walltime_org': job_temp.get('walltime'), 'runtime': job_temp.get('runtime'),
                    'walltime_org': str((float(job_temp.get('walltime'))*60 - (last_Pcheckp_time - job_temp.get('start_time')))/60),
                    'runtime': str(float(job_temp.get('runtime'))- (last_Pcheckp_time - job_temp.get('start_time'))),
                    'runtime_org': job_temp.get('runtime'),
                    'start_time_org': job_temp.get('start_time'),
                    'restart_overhead': restart_overhead_temp } 
        #
        ##self.update_job_states(jobspec, {}, "E")
        self.started_job_dict[str(jobspec['jobid'])] = jobspec
        print "w/_jkill end: ", end
        ##
        ###self.insert_time_stamp( end, "E", jobspec )
        #
        #release partition
        completed_job = job_temp
        for partition in completed_job.location:
            self.release_partition(partition)
            print "_jkill partition: ", partition 
                
            partsize = int(self._partitions[partition].size)
            self.num_busy -= partsize
            #
            # kill_partsize += partsize
            #

        ## waste_cost
        self.preempt_waste_runtime_checkpoint -= kill_partsize * (id_tInterval_all - id_tInterval_tillNow) * restart_overhead_temp
        # 
        self.preempt_waste_runtime_restart += kill_partsize * restart_overhead_temp
        self.preempt_waste_runtime_rework += kill_partsize * (end - last_Pcheckp_time)
        #
        print "[preempt_rework] type_end : ", type(end)
        print "[preempt_rework] _end : ", end
        print "[preempt_rework] type_last_Pcheckp_time: ", type(last_Pcheckp_time)
        print "[preempt_rework] _last_Pcheckp_time: ", last_Pcheckp_time
        print "[preempt_rework] (end - last_Pcheckp_time): ", (end - last_Pcheckp_time)
        print "[preempt_rework] kill_partsize: ", kill_partsize
        print "[preempt_rework] preempt_waste_runtime_rework: ", kill_partsize * (end - last_Pcheckp_time)
        ##
        
        #log the job end event
        #jobspec = completed_job.to_rx()
        #print "end jobspec=", jobspec
        #self.log_job_event("E", end_datetime, jobspec)
                
        #delete the job instance from self.queues
        self.queues.del_jobs([{'jobid':int(job.jobid)}])
        self.num_running -= 1
        self.num_end += 1 
        #
        print "_jkill del_event() _1 ... "
        self.event_manager.del_event(int(job.jobid))
        print "_jkill del_event() _2 ... "
        ##
        
        ##
        donetime = float(end) - float(job_temp.get('start_time'))
        lefttime = float(job_temp.get('runtime')) - donetime 
        partsize = kill_partsize
        return donetime, lefttime, partsize 
    kill_job_wPcheckP_app = exposed(kill_job_wPcheckP_app)



    def restart_job_add_queue(self, jobid):
        # print "[dw_bqsim] restart_job_updated() ..."
        
        # print "[dw_bqsim] restart_target: ", jobid
        for j_temp in self.started_job_dict.itervalues():
            if j_temp.get('job_killed'):
                # print "[dw_bqsim] restart_pool: ", j_temp.get('jobid')
                if int(j_temp.get('jobid')) == int(jobid):
                    # print "[dw_bqsim] restart_MATCH ..."
                    tempspec = j_temp
                    #
                    #tempspec['state'] = "queued"   #invisible -> queued
                    #tempspec['is_runnable'] = True   #False -> True
                    tempspec['walltime'] = (j_temp.get('walltime_org'))
                    tempspec['walltime_p'] = (j_temp.get('walltime_org'))
                    tempspec['runtime'] = float(j_temp.get('runtime'))
                    tempspec['remain_time'] = float(j_temp.get('runtime'))
                    # print "rs_addq walltime_2: ", tempspec['walltime']
                    # print "rs_addq walltime_p_2: ", tempspec['walltime_p']
                    # print "rs_addq runtime: ", tempspec['runtime']
                    # print "rs_addq remain_time_2: ", tempspec['remain_time']
                    ###
                    ## tempspec['job_restarted'] = True   #False -> True
                    ##
                    tempspec['start_time_org'] = float(j_temp.get('start_time_org'))
                    tempspec['runtime_org'] = float(j_temp.get('runtime_org'))
                    tempspec['job_killed'] = float(j_temp.get('job_killed'))
                    #
                    queuetime = self.get_current_time_sec()
                    #tempspec['start_time'] = queuetime #start
                    #tempspec['end_time'] = queuetime + j_temp.get('runtime')
                    tempspec['queue'] = j_temp.get('queue')
                    tempspec['submittime'] = float(j_temp.get('submittime'))
                    ##
                    tempspec['state'] = 'invisible'
                    tempspec['start_time'] = '0'
                    tempspec['end_time'] = '0'
                    # spec['queue'] = "default"
                    tempspec['has_resources'] = False
                    tempspec['is_runnable'] = False
                    ##
                    '''
                    self.queues.add_jobs([tempspec])
                    self.num_waiting += 1
                    '''
                    ##
                    self.insert_time_stamp( queuetime, "Q", tempspec )
                    #
                    self.unsubmitted_job_spec_dict[jobid] = tempspec
                    #self.log_job_event("Q", queuetime, tempspec)
                    #self.insert_time_stamp( queuetime, "S", tempspec )
    restart_job_add_queue = exposed(restart_job_add_queue)


    def restart_job_add_queue_wcheckp(self, jobid, checkp_overhead):
        # print "[dw_bqsim] restart_job_add_queue_wcheckp() ..."
        
        # print "[dw_bqsim] restart_target: ", jobid
        for j_temp in self.started_job_dict.itervalues():
            if j_temp.get('job_killed'):
                # print "[dw_bqsim] restart_pool: ", j_temp.get('jobid')
                if int(j_temp.get('jobid')) == int(jobid):
                    # print "[dw_bqsim] restart_MATCH ..."
                    tempspec = j_temp
                    #
                    #tempspec['state'] = "queued"   #invisible -> queued
                    #tempspec['is_runnable'] = True   #False -> True
                    tempspec['walltime'] = float(j_temp.get('walltime_org'))
                    tempspec['walltime_p'] = float(j_temp.get('walltime_org'))
                    tempspec['runtime'] = float(j_temp.get('runtime'))
                    ###
                    ## tempspec['job_restarted'] = True   #False -> True
                    ##
                    tempspec['start_time_org'] = float(j_temp.get('start_time_org'))
                    #
                    queuetime = self.get_current_time_sec() + checkp_overhead
                    #tempspec['start_time'] = queuetime #start
                    #tempspec['end_time'] = queuetime + j_temp.get('runtime')
                    tempspec['queue'] = j_temp.get('queue')
                    tempspec['submittime'] = float(j_temp.get('submittime'))
                    ##
                    tempspec['state'] = 'invisible'
                    tempspec['start_time'] = '0'
                    tempspec['end_time'] = '0'
                    # spec['queue'] = "default"
                    tempspec['has_resources'] = False
                    tempspec['is_runnable'] = False
                    ##
                    '''
                    self.queues.add_jobs([tempspec])
                    self.num_waiting += 1
                    '''
                    ##
                    self.insert_time_stamp( queuetime, "Q", tempspec )
                    #
                    self.unsubmitted_job_spec_dict[jobid] = tempspec
                    #self.log_job_event("Q", queuetime, tempspec)
                    #self.insert_time_stamp( queuetime, "S", tempspec )
    restart_job_add_queue_wcheckp = exposed(restart_job_add_queue_wcheckp)


    #dwang:
    def get_restart_overhead(self, jobid):
        # print "[dw_bqsim] get_restart_overhead() ..."
        #
        overhead_temp = 0.0
        #
        for j_temp in self.started_job_dict.itervalues():
            if j_temp.get('job_killed'):
                # print "[dw_bqsim] restart_overhead_pool: ", j_temp.get('jobid')
                if int(j_temp.get('jobid')) == int(jobid):
                    # print "[dw_bqsim] restart_overhead_MATCH ..."
                    overhead_temp = j_temp.get('restart_overhead')
        return overhead_temp
    get_restart_overhead = exposed(get_restart_overhead)
    #dwang 

    #dwang:
    def get_current_checkp_overheadP_OOD(self, checkp_t_internval, t_current, dsize_pnode, bw_temp_write):
        # print "[dw_bqsim] get_checkp_overheadP() ..."
        #
        current_overhead_temp = 0.0  
        #  
        total_start_time = self.first_job_start 
        '''
        total_start_time = 0.0
        j_count = 0
        for job in self.started_job_dict.itervalues():
            j_start_time = float(job.get('start_time')) 
            if j_count == 0:
                total_start_time = j_start_time
            if j_start_time < total_start_time: #dwang
                total_start_time = j_start_time #dwang
            j_count += 1 
        ''' 
        print "[overheadP] total_start_time:", total_start_time

        ##
        #dsize_restart_pnode = 4.0 / 1024
        #bw_temp = 1536 * 0.8
        dsize_restart_pnode = dsize_pnode/1024.0
        ### bw_temp = bw_temp_write
        #restart_overhead_temp = kill_partsize * dsize_restart_pnode / bw_temp
        ##
        
        id_tInterval_tillNow = math.floor( (t_current - total_start_time)/checkp_t_internval )
        print "[overheadP] t_current:", t_current
        print "[overheadP] t sub:", (t_current - total_start_time)
        print "[overheadP] t sub div:", (t_current - total_start_time)/checkp_t_internval
        print "[overheadP] id_tInterval_tillNow:", id_tInterval_tillNow
        #
        for id_tInterval in range(1, int(id_tInterval_tillNow)+1):
            busynodes_temp = math.ceil(self.get_utilization_rate_SE( total_start_time + id_tInterval * checkp_t_internval, 5 ) * TOTAL_NODES)
            #
            bw_temp = min( busynodes_temp/128 * 4, bw_temp_write )
            #
            if busynodes_temp > 0:
                current_overhead_temp += busynodes_temp * dsize_restart_pnode / bw_temp
            ## waste_cost
            # self.preempt_waste_runtime_checkpoint += busynodes_temp * current_overhead_temp
            ##
        #
        print "[overheadP] current_overhead_temp:", current_overhead_temp
        return current_overhead_temp 
    get_current_checkp_overheadP_OOD = exposed(get_current_checkp_overheadP_OOD)
    #dwang 

    #dwang:
    def get_current_checkp_overheadP(self, checkp_t_internval, t_current, dsize_pnode, bw_temp_write):
        # print "[dw_bqsim] get_checkp_overheadP() ..."
        #
        current_overhead_temp = 0.0  
        #  
        if self.time_last_Pcheckp == -1: 
            self.time_last_Pcheckp = self.first_job_start  
        '''
        total_start_time = 0.0
        j_count = 0
        for job in self.started_job_dict.itervalues():
            j_start_time = float(job.get('start_time')) 
            if j_count == 0:
                total_start_time = j_start_time
            if j_start_time < total_start_time: #dwang
                total_start_time = j_start_time #dwang
            j_count += 1 
        ''' 
        #print "[overheadP] total_start_time:", total_start_time

        ##
        #dsize_restart_pnode = 4.0 / 1024
        #bw_temp = 1536 * 0.8
        dsize_restart_pnode = dsize_pnode/1024.0
        ### bw_temp = bw_temp_write
        #restart_overhead_temp = kill_partsize * dsize_restart_pnode / bw_temp
        ##
        
        id_tInterval_fromLast = math.floor( (t_current - self.time_last_Pcheckp)/checkp_t_internval )
        print "[overheadP] t_current:", t_current
        print "[overheadP] t sub:", (t_current - self.time_last_Pcheckp)
        print "[overheadP] t sub div:", (t_current - self.time_last_Pcheckp)/checkp_t_internval
        print "[overheadP] id_tInterval_fromLast:", id_tInterval_fromLast
        #
        for id_tInterval in range(1, int(id_tInterval_fromLast)+1):
            busynodes_temp = math.ceil(self.get_utilization_rate_SE( self.time_last_Pcheckp + id_tInterval * checkp_t_internval, 5 ) * TOTAL_NODES)
            #
            bw_temp = min( busynodes_temp/128 * 4, bw_temp_write )
            #
            if busynodes_temp > 0:
                current_overhead_temp += busynodes_temp * dsize_restart_pnode / bw_temp
            ## waste_cost
            # self.preempt_waste_runtime_checkpoint += busynodes_temp * current_overhead_temp
            ##
        self.time_last_Pcheckp = t_current 
        #
        print "[overheadP] current_overhead_temp:", current_overhead_temp
        return current_overhead_temp 
    get_current_checkp_overheadP = exposed(get_current_checkp_overheadP)
    #dwang 



    #dwang:
    def get_current_checkp_overheadP_INT(self, checkp_t_internval, jobid, t_current, dsize_pnode, bw_temp_write, fp_backf):
        # print "[dw_bqsim] get_checkp_overheadP() ..., bw_temp_write = ", bw_temp_write
        #
        current_overhead_temp = 0.0  
        #  
        if self.time_last_Pcheckp == -1: 
            self.time_last_Pcheckp = self.first_job_start
        #print "[overheadP] total_start_time:", total_start_time
        
        ##
        '''
        for spec_temp in self.unsubmitted_job_spec_dict.iteritems():
            if jobid == spec_temp['jobid']:
        '''
        ##jobspec = self.unsubmitted_job_spec_dict[str(jobid)]
        joblist = self.queues.get_jobs([{'jobid':int(jobid)}])
        jobspec = joblist[0]
        print "[INT_temp_2] j_id: ", jobid
        jb_nodes = jobspec.get('nodes') #jobspec['nodes']
        jb_runtime = jobspec.get('runtime') #jobspec['runtime']
        print "[INT_temp_2] j_nodes: ", jb_nodes
        print "[INT_temp_2] j_runtime: ", jb_runtime
        ##
        #dsize_restart_pnode = 4.0 / 1024
        #bw_temp = 1536 * 0.8
        dsize_restart_pnode = dsize_pnode/1024.0
        ### bw_temp = bw_temp_write
        #restart_overhead_temp = kill_partsize * dsize_restart_pnode / bw_temp
        bw_temp = min( float(jb_nodes)/128 * 4, bw_temp_write )
	print "bw_temp_min_1: ", float(jb_nodes)/128 * 4 
	print "bw_temp_min_2: ", float(jb_nodes)/float(128) * 4 
        ##
        
        tInterval_druntime = math.floor( float(jb_runtime)/checkp_t_internval )
        print "[CKP_UTIL_2] tInterval_druntime: ", tInterval_druntime
        #
        current_overhead_temp = tInterval_druntime * int(jb_nodes) * dsize_restart_pnode / bw_temp
        ## waste_cost
        self.preempt_waste_runtime_checkpoint += int(jb_nodes) * current_overhead_temp
        print "[CKP_UTIL_2] ADD_ckp_cost: ", int(jb_nodes) * current_overhead_temp
        print "[CKP_UTIL_2] cur_ckp_cost: ", self.preempt_waste_runtime_checkpoint
        ##
        fp_backf.write('+ %s: %d, %f, %f, %f \n' %( jobid, int(jb_nodes), current_overhead_temp, int(jb_nodes) * current_overhead_temp, self.preempt_waste_runtime_checkpoint ))
        #
        print "[overheadP] current_overhead_temp:", current_overhead_temp
        return current_overhead_temp 
    get_current_checkp_overheadP_INT = exposed(get_current_checkp_overheadP_INT)
    #dwang 



    #dwang:
    def get_current_checkp_overheadP_app(self, jobid, t_current, dsize_pnode, bw_temp_write):
        # print "[dw_bqsim] get_checkp_overheadP() ..."
        #
        current_overhead_temp = 0.0  
        ###  
        temp_last_Pcheckp = 0 
        temp_checkp_interv = 0 
        temp_index = 0
        for jj in range(0,len(self.Pcheckp_specs)): 
            if int(jobid) == int(self.Pcheckp_specs[jj].get('jobid')):
                if self.Pcheckp_specs[jj].get('time_last_Pcheckp') == -1:
                    temp_spec = self.Pcheckp_specs[jj]
                    temp_spec['time_last_Pcheckp'] = self.first_job_start
                    self.Pcheckp_specs[jj] = temp_spec
                temp_last_Pcheckp = self.Pcheckp_specs[jj].get('time_last_Pcheckp')
                temp_checkp_interv = self.Pcheckp_specs[jj].get('Pcheckp_interv')
                temp_index = jj
                temp_spec = self.Pcheckp_specs[jj]
                break
        # print "[overheadP_app] temp_last_Pcheckp:", temp_last_Pcheckp
        # print "[overheadP_app] temp_checkp_interv:", temp_checkp_interv
        # print "[overheadP_app] temp_index:", temp_index
    
        ###
        #print "[overheadP] total_start_time:", total_start_time

        ##
        #dsize_restart_pnode = 4.0 / 1024
        #bw_temp = 1536 * 0.8
        dsize_restart_pnode = dsize_pnode/1024.0
        ### bw_temp = bw_temp_write
        #restart_overhead_temp = kill_partsize * dsize_restart_pnode / bw_temp
        ##
        
        '''
        id_tInterval_fromLast = math.floor( (t_current - self.time_last_Pcheckp)/checkp_t_internval )
        print "[overheadP] t_current:", t_current
        print "[overheadP] t sub:", (t_current - self.time_last_Pcheckp)
        print "[overheadP] t sub div:", (t_current - self.time_last_Pcheckp)/checkp_t_internval
        print "[overheadP] id_tInterval_fromLast:", id_tInterval_fromLast
        '''
        ############################## app_based overhead -->
        '''
        id_tInterval_fromLast = math.floor( (t_current - temp_last_Pcheckp)/temp_checkp_interv )
        print "[overheadP_app] t_current:", t_current
        print "[overheadP_app] t_last_checkP:", temp_last_Pcheckp
        print "[overheadP_app] t sub:", (t_current - temp_last_Pcheckp)
        print "[overheadP_app] t sub div:", (t_current - temp_last_Pcheckp)/temp_checkp_interv
        print "[overheadP_app] id_tInterval_fromLast:", id_tInterval_fromLast
        #
        for id_tInterval in range(1, int(id_tInterval_fromLast)+1):
            busynodes_temp = math.ceil(self.get_utilization_rate_SE( temp_last_Pcheckp + id_tInterval * temp_checkp_interv, 5 ) * TOTAL_NODES)
            #
            bw_temp = min( busynodes_temp/128 * 4, bw_temp_write )
            #
            if busynodes_temp > 0:
                current_overhead_temp += busynodes_temp * dsize_restart_pnode / bw_temp
                ## waste_cost
                self.preempt_waste_runtime_checkpoint += busynodes_temp * current_overhead_temp
                #
                print "[overheadP_app] busynodes_TEMP:", busynodes_temp
                print "[overheadP_app] preempt_waste_runtime_checkpoint_TEMP:", busynodes_temp * current_overhead_temp
                print "[overheadP_app] preempt_waste_runtime_checkpoint_CURRENT:", self.preempt_waste_runtime_checkpoint
                ##
        temp_spec['time_last_Pcheckp'] = t_current
        self.Pcheckp_specs[temp_index] = temp_spec
        print "[overheadP_app] new_t_last_checkP:", self.Pcheckp_specs[temp_index].get('time_last_Pcheckp')
        #
        print "[overheadP_app] current_overhead_temp:", current_overhead_temp
        '''
        # ----->
        joblist = self.queues.get_jobs([{'jobid':int(jobid)}])
        jobspec = joblist[0]
        # print "[APP_temp_2] j_id: ", jobid
        jb_nodes = jobspec.get('nodes') #jobspec['nodes']
        jb_runtime = jobspec.get('runtime') #jobspec['runtime']
        # print "[APP_temp_2] j_nodes: ", jb_nodes
        # print "[APP_temp_2] j_runtime: ", jb_runtime
        ##
        #dsize_restart_pnode = 4.0 / 1024
        #bw_temp = 1536 * 0.8
        dsize_restart_pnode = dsize_pnode/1024.0
        ### bw_temp = bw_temp_write
        #restart_overhead_temp = kill_partsize * dsize_restart_pnode / bw_temp
        bw_temp = min( float(jb_nodes)/128 * 4, bw_temp_write )
        ##
        
        tInterval_druntime = math.floor( float(jb_runtime)/temp_checkp_interv )
        # print "[overheadP_app] tInterval_fromLast:", tInterval_druntime
        #
        current_overhead_temp = tInterval_druntime * int(jb_nodes) * dsize_restart_pnode / bw_temp
        ## waste_cost
        self.preempt_waste_runtime_checkpoint += int(jb_nodes) * current_overhead_temp
        ##
        #
        # print "[overheadP_app] current_overhead_temp:", current_overhead_temp
        ############################## app_based overhead
        return current_overhead_temp 
    get_current_checkp_overheadP_app = exposed(get_current_checkp_overheadP_app)
    #dwang 

    #
    def get_restart_size(self):
        restart_count = 0
        for job in self.started_job_dict.itervalues():
            if job.get('job_killed'):
                restart_count += 1
        return restart_count
    get_restart_size = exposed(get_restart_size)
    
    #
    def get_restart_jobs(self):
        restart_list = []
        for job in self.started_job_dict.itervalues():
            if job.get('job_killed'):
                restart_list.append(job)
        return restart_list
    get_restart_jobs = exposed(get_restart_jobs)

    #
    def get_restart_job_list(self):
        restart_list = []
        for job in self.started_job_dict.itervalues():
            if job.get('job_killed'):
                restart_list.append(job.get('jobid'))
        return restart_list
    get_restart_job_list = exposed(get_restart_job_list)



    def kill_job_ood2(self, specs, updates):
        '''update the job state and start_time and end_time when cqadm --run
            is issued to a group of jobs'''
        
        print "[dw_bqsim] kill_job_updated_2() ..."
        #
        #release partition
        completed_job = job_temp
        for partition in completed_job.location:
            self.release_partition(partition)
            print "_jkill partition: ", partition
            
            partsize = int(self._partitions[partition].size)
            self.num_busy -= partsize
        
        #log the job end event
        #jobspec = completed_job.to_rx()
        #print "end jobspec=", jobspec
        #self.log_job_event("E", end_datetime, jobspec)
        
        #delete the job instance from self.queues
        self.queues.del_jobs([{'jobid':int(job.jobid)}])
        self.num_running -= 1
        self.num_end += 1

        
        def _kill_rest_job(job, newattr):
            '''callback function to update job start/end time'''
            temp = job.to_rx()
            newattr = self.kill_job_updates(temp, newattr)
            '''
            temp.update(newattr)
            job.update(newattr)
            self.log_job_event('S', self.get_current_time_date(), temp)
            '''
        #
        # print "[dw_rtj] start_rt_job() before_return ... "
        return self.queues.get_jobs(specs, _kill_rest_job, updates)
    kill_job_ood2 = exposed(kill_job_ood2)
    # dwang



    def start_job(self, specs, updates):
        '''update the job state and start_time and end_time when cqadm --run
        is issued to a group of jobs'''
        #
        
        #
        start_holding = False
        for spec in specs:
            # print "[] jobspec restart_overhead: ", spec.get('restart_overhead')
            if self.job_hold_dict.has_key(spec['jobid']):
                start_holding = True

        partitions = updates['location']
        for partition in partitions:
            if not start_holding:
                self.reserve_partition(partition)
            p = self._partitions[partition]
            # print "[dw_bqsim] rest_state: ", p.state
            partsize = int(self._partitions[partition].size)
            self.num_busy += partsize

        self.num_running += 1
        self.num_waiting -= 1

        def _start_job(job, newattr):
            '''callback function to update job start/end time'''
            temp = job.to_rx()
            newattr = self.run_job_updates(temp, newattr)
            temp.update(newattr)
            job.update(newattr)
            self.log_job_event('S', self.get_current_time_date(), temp)

        return self.queues.get_jobs(specs, _start_job, updates)


    # dwang: 
    def start_rt_job(self, specs, updates):
        '''update the job state and start_time and end_time when cqadm --run
        is issued to a group of jobs'''

        # print "[dw_rtj] start_rt_job() start ... "
        
        #
        #check_pname = 'MIR-08800-3BBF1-2048'
        #check_p = self._partitions[check_pname]
        #print "[dw start_rtj] check_pname: ", check_p.name
        #print "[dw start_rtj] check_state: ", check_p.state
        #
        start_holding = False
        for spec in specs:
            # print "[] jobspec restart_overhead: ", spec.get('restart_overhead')
            if self.job_hold_dict.has_key(spec['jobid']):
                start_holding = True

        partitions = updates['location']
        for partition in partitions:
            if not start_holding:
                # print "[dw_rtj] start_rt_job() -> reserve_rtj_partition() ... "
                self.reserve_rtj_partition(partition)
            partsize = int(self._partitions[partition].size)
            self.num_busy += partsize

        self.num_running += 1
        self.num_waiting -= 1

        def _start_rt_job(job, newattr):
            '''callback function to update job start/end time'''
            temp = job.to_rx()
            newattr = self.run_job_updates(temp, newattr)
            temp.update(newattr)
            job.update(newattr)
            self.log_job_event('S', self.get_current_time_date(), temp)
            #
        
        # print "[dw_rtj] start_rt_job() before_return ... "
        return self.queues.get_jobs(specs, _start_rt_job, updates)
    # dwang 

    # dwang: 
    def start_job_wOverhead(self, specs, updates, rest_overhead):
        '''update the job state and start_time and end_time when cqadm --run
        is issued to a group of jobs'''
        #
        
        #
        start_holding = False
        for spec in specs:
            # print "[] jobspec restart_overhead: ", spec.get('restart_overhead')
            if self.job_hold_dict.has_key(spec['jobid']):
                start_holding = True

        partitions = updates['location']
        for partition in partitions:
            if not start_holding:
                self.reserve_partition(partition)
            p = self._partitions[partition]
            # print "[dw_bqsim] rest_state: ", p.state
            partsize = int(self._partitions[partition].size)
            self.num_busy += partsize

        self.num_running += 1
        self.num_waiting -= 1

        def _start_job(job, newattr):
            '''callback function to update job start/end time'''
            temp = job.to_rx()
            newattr = self.run_job_updates_wOverhead(temp, newattr, rest_overhead)
            temp.update(newattr)
            job.update(newattr)
            self.log_job_event('S', self.get_current_time_date(), temp)

        return self.queues.get_jobs(specs, _start_job, updates)
    # dwang 

    # dwang: 
    def start_rt_job_wOverhead(self, specs, updates, rtj_overhead):
        '''update the job state and start_time and end_time when cqadm --run
        is issued to a group of jobs'''

        # print "[dw_rtj] start_rt_job() start ... "
        
        #
        #check_pname = 'MIR-08800-3BBF1-2048'
        #check_p = self._partitions[check_pname]
        #print "[dw start_rtj] check_pname: ", check_p.name
        #print "[dw start_rtj] check_state: ", check_p.state
        #
        start_holding = False
        for spec in specs:
            # print "[] jobspec restart_overhead: ", spec.get('restart_overhead')
            if self.job_hold_dict.has_key(spec['jobid']):
                start_holding = True

        partitions = updates['location']
        for partition in partitions:
            if not start_holding:
                # print "[dw_rtj] start_rt_job() -> reserve_rtj_partition() ... "
                self.reserve_rtj_partition(partition)
            partsize = int(self._partitions[partition].size)
            self.num_busy += partsize

        self.num_running += 1
        self.num_waiting -= 1

        def _start_rt_job(job, newattr):
            '''callback function to update job start/end time'''
            temp = job.to_rx()
            newattr = self.run_job_updates_wOverhead(temp, newattr, rtj_overhead)
            temp.update(newattr)
            job.update(newattr)
            self.log_job_event('S', self.get_current_time_date(), temp)
            #
        
        # print "[dw_rtj] start_rt_job() before_return ... "
        return self.queues.get_jobs(specs, _start_rt_job, updates)
    # dwang

    def run_job_updates(self, jobspec, newattr):
        ''' return the state updates (including state queued -> running,
        setting the start_time, end_time)'''
        updates = {}
      
        #
        start = self.get_current_time_sec()
        updates['start_time'] = start
        updates['starttime'] = start

        updates['state'] = 'running'
        updates['system_state'] = 'running'
        updates['is_runnable'] = False
        updates['has_resources'] = True
        # dwang:
        updates['job_killed'] = False
        updates['job_restarted'] = False
        # dwang 
        if jobspec['last_hold'] > 0:
            updates['hold_time'] = jobspec['hold_time'] + self.get_current_time_sec() - jobspec['last_hold']

        #determine whether the job is going to fail before completion
        location = newattr['location']
        duration = jobspec['remain_time']

        end = start + duration
        updates['end_time'] = end
        self.insert_time_stamp(end, "E", {'jobid':jobspec['jobid']})

        updates.update(newattr)

        #self.update_jobdict(str(jobid), 'start_time', start)
        #self.update_jobdict(str(jobid), 'end_time', end)
        #self.update_jobdict(str(jobid), 'location', location)
        self.num_started += 1
        #print "start job %s" % self.num_started
        partsize = int(location[0].split('-')[-1])
        #print "now=%s, jobid=%s, start=%s, end=%s, partsize=%s" % (self.get_current_time_date(), jobspec['jobid'], sec_to_date(start), sec_to_date(end), partsize)

        ## started_job_spec = {'jobid':str(jobspec['jobid']), 'submittime': jobspec['submittime'], 'start_time': start, 'end_time': end, 'location': location, 'partsize': partsize}
        started_job_spec = {'jobid':str(jobspec['jobid']), 'submittime': jobspec['submittime'], 'start_time': start, 'end_time': end, 'location': location, 'partsize': partsize, 'runtime': duration, 'runtime_org': duration, 'job_killed': False, 'job_restarted': False}
        self.started_job_dict[str(jobspec['jobid'])] = started_job_spec
        self.delivered_node_hour2 += (end-start)* partsize / 3600.0
        return updates

    # dwang:  
    def run_job_updates_wOverhead(self, jobspec, newattr, overhead):
        ''' return the state updates (including state queued -> running,
        setting the start_time, end_time)'''
        updates = {}
      
        #
        start = self.get_current_time_sec() #+ overhead
        updates['start_time'] = start
        updates['starttime'] = start

        updates['state'] = 'running'
        updates['system_state'] = 'running'
        updates['is_runnable'] = False
        updates['has_resources'] = True
        #
        # dwang:
        updates['job_killed'] = False
        updates['job_restarted'] = False
        # dwang 
        if jobspec['last_hold'] > 0:
            updates['hold_time'] = jobspec['hold_time'] + self.get_current_time_sec() - jobspec['last_hold']

        #determine whether the job is going to fail before completion
        location = newattr['location']
        # duration = jobspec['remain_time']
        #####
        flag_re_du = 0
        restart_runtime_org = 0
        ##for jid,j_temp in self.started_job_dict.iteritems():
        for j_temp in self.started_job_dict.itervalues():
            if j_temp.get('job_killed'):
                # print "[re_du] restart_pool: ", j_temp.get('jobid')
                if int(j_temp.get('jobid')) == int(jobspec['jobid']):
                    # print "[re_du] restart_MATCH_2 ..."
                    flag_re_du = 1
                    duration = j_temp['runtime']
                    #
                    restart_runtime_org = j_temp['runtime_org']
                    # print "[re_du] rt_org_temp ...", j_temp['runtime_org']
                    # print "[re_du] rt_org_spec ...", jobspec['runtime_org']
                    #
                    #
        if flag_re_du == 0:
            duration = jobspec['remain_time']
        #####

        end = start + duration + overhead
        updates['end_time'] = end
        self.insert_time_stamp(end, "E", {'jobid':jobspec['jobid']})
        
        updates.update(newattr)

        #self.update_jobdict(str(jobid), 'start_time', start)
        #self.update_jobdict(str(jobid), 'end_time', end)
        #self.update_jobdict(str(jobid), 'location', location)
        self.num_started += 1
        #print "start job %s" % self.num_started
        partsize = int(location[0].split('-')[-1])
        #print "now=%s, jobid=%s, start=%s, end=%s, partsize=%s" % (self.get_current_time_date(), jobspec['jobid'], sec_to_date(start), sec_to_date(end), partsize)

        if flag_re_du == 1:
            started_job_spec = {'jobid':str(jobspec['jobid']), 'submittime': jobspec['submittime'], 'start_time': start, 'end_time': end, 'location': location, 'partsize': partsize, 'runtime': duration,'runtime_org': restart_runtime_org, 'job_killed': False, 'job_restarted': True}
        else:
            started_job_spec = {'jobid':str(jobspec['jobid']), 'submittime': jobspec['submittime'], 'start_time': start, 'end_time': end, 'location': location, 'partsize': partsize, 'runtime': duration, 'runtime_org': jobspec['runtime'], 'job_killed': False, 'job_restarted': False}
        self.started_job_dict[str(jobspec['jobid'])] = started_job_spec
        self.delivered_node_hour2 += (end-start)* partsize / 3600.0
        return updates
    # dwang 


    def update_jobdict(self, jobid, _key, _value):
        '''update self.unsubmitted_jobdict'''
        self.unsubmitted_job_spec_dict[jobid][_key] = _value
        if jobid == '280641':
            print "update job %s=, _key=%s, _value=%s, afterupdate=%s" % (jobid, _key, _value, self.unsubmitted_job_spec_dict[jobid][_key])

##### system related
    def init_partition(self, namelist):
	print "[] init_partition() ..."
	# 
        '''add all paritions and apply activate and enable'''
        func = self.add_partitions
        args = ([{'tag':'partition', 'name':partname, 'size':"*",
                  'functional':False, 'scheduled':False, 'queue':"*",
                  'deps':[]} for partname in namelist],)
        apply(func, args)

        func = self.set_partitions
        args = ([{'tag':'partition', 'name':partname} for partname in namelist],
                {'scheduled':True, 'functional': True})
        apply(func, args)
	# 
	print "[] part_size: ", len(self._partitions)
	# 


    def inhibit_small_partitions(self):
        '''set all partition less than 512 nodes not schedulable and functional'''
        namelist = []
        for partition in self._partitions.itervalues():
            if partition.size < MIDPLANE_SIZE:
                namelist.append(partition.name)
        func = self.set_partitions
        args = ([{'tag':'partition', 'name':partname} for partname in namelist],
                {'scheduled':False})
        apply(func, args)


    def _find_job_location(self, args, drain_partitions=set(), taken_partition=set(), backfilling=False):
        jobid = args['jobid']
        nodes = args['nodes']
        queue = args['queue']
        utility_score = args['utility_score']
        walltime = args['walltime']
        walltime_p = args['walltime_p']  #*AdjEst*
        forbidden = args.get("forbidden", [])
        required = args.get("required", [])

        best_score = sys.maxint
        best_partition = None

        available_partitions = set()

        requested_location = None
        if args['attrs'].has_key("location"):
            requested_location = args['attrs']['location']

        if required:
            # whittle down the list of required partitions to the ones of the proper size
            # this is a lot like the stuff in _build_locations_cache, but unfortunately,
            # reservation queues aren't assigned like real queues, so that code doesn't find
            # these
            for p_name in required:
                available_partitions.add(self.cached_partitions[p_name])
                available_partitions.update(self.cached_partitions[p_name]._children)

            possible = set()
            for p in available_partitions:
                possible.add(p.size)

            desired_size = 64
            job_nodes = int(nodes)
            for psize in sorted(possible):
                if psize >= job_nodes:
                    desired_size = psize
                    break

            for p in available_partitions.copy():
                if p.size != desired_size:
                    available_partitions.remove(p)
                elif p.name in self._not_functional_set:
                    available_partitions.remove(p)
                elif requested_location and p.name != requested_location:
                    available_partitions.remove(p)
        else:
	    # dwang:
	    #print "[dw_bgqsim] possible_location: ", self.possible_locations(nodes, queue) 
	    # dwang: 
            for p in self.possible_locations(nodes, queue):
                skip = False
                for bad_name in forbidden:
                    if p.name == bad_name or bad_name in p.children or bad_name in p.parents:
                        skip = True
                        break

                if not skip:
                    if (not requested_location) or (p.name == requested_location):
                        available_partitions.add(p)

        available_partitions -= drain_partitions
        now = self.get_current_time()
        available_partitions = list(available_partitions)
        available_partitions.sort(key=lambda d: (d.name))
        best_partition_list = []
        best_partition_score = []

	# dwang: 
	# print "[dw_bgqsim_base] available_location: ", available_partitions  
        pcal_base = 0
	# dwang: 
        for partition in available_partitions: 
            pcal_base = pcal_base + 1 
            # 
            # print "[dw_bgqsim_base] pcal_base: ", pcal_base
            # print "[dw_bgqsim_base] partition: ", partition
            #
            # if the job needs more time than the partition currently has available, look elsewhere
            if self.predict_backfill:
                runtime_estimate = float(walltime_p)   # *Adj_Est*
            else:
                runtime_estimate = float(walltime)

            if backfilling:
                if 60*runtime_estimate > (partition.backfill_time - now):
                    # print "[dw_bgqsim_base] __backfilling ... "
                    continue

            if self.reserve_ratio > 0:
                if self.reservation_violated(self.get_current_time_sec() + 60*runtime_estimate, partition.name): 
                    # print "[dw_bgqsim_base] __reserve_ratio ... "
                    continue

            #
            if partition.state == "idle":
                flag_rtj = 0
                # let's check the impact on partitions that would become blocked
                score = 0
                for p in partition.parents: 
		    pp = self._partitions[p]
		    # dwang: 
                    if self.cached_partitions[p].state == "idle" and self.cached_partitions[p].scheduled:
                        score += 1
		    ##
            	    '''
		    # dwang: 
		    for pchild_name in pp.children: 
                        pchild = self._partitions[pchild_name]
                        if pchild.state == "rtj":
                            score += 5
            	    '''
		    # dwang  
                #
                for p_children_name in partition.children:
                    p_children = self._partitions[p_children_name]
                    if (p_children.state == "rtj"): #or (p_children.state == "temp_blocked"):
                        flag_rtj = 1
                        # print "[dw_bgqsim_base] __p_children_rtj ... "
                        break
                #
                if flag_rtj: 
                    # print "[dw_bgqsim_base] __flag_rtj ... "
                    continue
                    ##break
                #
                for p_parents_name in partition.parents:
                    p_parents = self._partitions[p_parents_name]
                    ppt_str = str(p_parents.state)
                    if (p_parents.state == "rtj"): #or (p_parents.state == "temp_blocked") or ('blocked' in ppt_str):
                        flag_rtj = 1
                        # print "[dw_bgqsim_base] __p_parent_rtj ... "
                        break
                if flag_rtj:
                    continue
                    ##break
                #

                # dwang:
                # print "[dw_bgqsim_base] score: ", score
                # dwang
                # the lower the score, the fewer new partitions will be blocked by this selection
                if score < best_score:
                    best_score = score
                    best_partition = partition

                    best_partition_list[:] = []
                    best_partition_list.append(partition)
                    best_partition_score.append(score)
                #record equavalent partitions that have same best score
                elif score == best_score:
                    best_partition_list.append(partition)
            else: 
                # print "[dw_bgqsim_base] __part_not_idle ... "
                pass
                    
        # # dwang:
        # if len(best_partition_list):
        #     print "[dw_bgqsim_base] jobid: ", jobid
        #     print "[dw_bgqsim_base] best_partition_list: ", best_partition_list
        #     print "[dw_bgqsim_base] best_partition_score: ", best_partition_score
        # # dwang
        
        if self.walltime_aware_cons and len(best_partition_list) > 1:
            #print "best_partition_list=", [part.name for part in best_partition_list]
            #walltime aware job allocation (conservative)
            least_diff = MAXINT
            for partition in best_partition_list:
                nbpart = self.get_neighbor_by_partition(partition.name)
                if nbpart:
                    nbjob = self.get_running_job_by_partition(nbpart)
                    if nbjob:
                        nbjob_remain_length = nbjob.starttime + 60*float(nbjob.walltime) - self.get_current_time_sec()
                        diff = abs(60*float(walltime) - nbjob_remain_length)
                        if diff < least_diff:
                            least_diff = diff
                            best_partition = partition
                        msg = "jobid=%s, partition=%s, neighbor part=%s, neighbor job=%s, diff=%s" % (jobid, partition.name, nbpart, nbjob.jobid, diff)
                        #self.dbglog.LogMessage(msg)
            msg = "------------job %s allocated to best_partition %s-------------" % (jobid,  best_partition.name)
            #self.dbglog.LogMessage(msg)

        if best_partition:
            # print "[dw_bqsim] bpart_rest : ", best_partition.name
            # print "[dw_bqsim] bpart_rest STATE: ", best_partition.state
            #
            running_job = self.get_running_job_by_partition(best_partition.name)
            # if running_job:
            #     print "[dw_bqsim] bpart_rest RUNNING: ", running_job.jobid
            # else:
            #     print "[dw_bqsim] bpart_rest RUNNING: NONE "
            # #
            # print "[dw_bqsim] bpart_rest SCHEDULED: ", self.cached_partitions[best_partition.name].scheduled
            #
            #
            for p_children_name in best_partition.children:
                p_children = self._partitions[p_children_name]
                # print "[dw_bqsim] bpart_rest_ch : ", p_children.name
                # print "[dw_bqsim] bpart_rest_ch STATE: ", p_children.state
                #
                ch_running_job = self.get_running_job_by_partition(best_partition.name)
                # if ch_running_job:
                #     print "[dw_bqsim] bpart_rest_ch RUNNING: ", ch_running_job.jobid
                # else:
                #     print "[dw_bqsim] bpart_rest_ch RUNNING: NONE "
                # #
                # print "[dw_bqsim] bpart_rest_ch SCHEDULED: ", self.cached_partitions[p_children_name].scheduled
            #
            return {jobid: [best_partition.name]}


    def _find_job_location_wdrain(self, args, drain_partitions, taken_partition=set(), backfilling=False):
        # 
        print "[_find_job_location_wdrain] _find_job_location_wdrain() ... "
        # 
        jobid = args['jobid']
        nodes = args['nodes']
        queue = args['queue']
        utility_score = args['utility_score']
        walltime = args['walltime']
        walltime_p = args['walltime_p']  #*AdjEst*
        forbidden = args.get("forbidden", [])
        required = args.get("required", [])

        best_score = sys.maxint
        best_partition = None

        available_partitions = set()

        requested_location = None
        if args['attrs'].has_key("location"):
            requested_location = args['attrs']['location']

        if required:
            # whittle down the list of required partitions to the ones of the proper size
            # this is a lot like the stuff in _build_locations_cache, but unfortunately,
            # reservation queues aren't assigned like real queues, so that code doesn't find
            # these
            for p_name in required:
                available_partitions.add(self.cached_partitions[p_name])
                available_partitions.update(self.cached_partitions[p_name]._children)

            possible = set()
            for p in available_partitions:
                possible.add(p.size)

            desired_size = 64
            job_nodes = int(nodes)
            for psize in sorted(possible):
                if psize >= job_nodes:
                    desired_size = psize
                    break

            for p in available_partitions.copy():
                if p.size != desired_size:
                    available_partitions.remove(p)
                elif p.name in self._not_functional_set:
                    available_partitions.remove(p)
                elif requested_location and p.name != requested_location:
                    available_partitions.remove(p)
        else:
        # dwang:
        #print "[dw_bgqsim] possible_location: ", self.possible_locations(nodes, queue) 
        # dwang: 
            for p in self.possible_locations(nodes, queue):
                skip = False
                for bad_name in forbidden:
                    if p.name == bad_name or bad_name in p.children or bad_name in p.parents:
                        skip = True
                        break

                if not skip:
                    if (not requested_location) or (p.name == requested_location):
                        available_partitions.add(p)

        # dwang: 
        print "[_find_job_location_wdrain] len(available_partitions) w/o_drain: ", len(available_partitions)
        available_partitions -= drain_partitions 
        print "[_find_job_location_wdrain] len(available_partitions) w/_drain: ", len(available_partitions)
        # dwang 
        now = self.get_current_time()
        available_partitions = list(available_partitions)
        available_partitions.sort(key=lambda d: (d.name))
        best_partition_list = []
        best_partition_score = []

    # dwang:
    #print "[dw_bgqsim] available_location: ", available_partitions  
    # dwang: 
        for partition in available_partitions:
            # if the job needs more time than the partition currently has available, look elsewhere
            if self.predict_backfill:
                runtime_estimate = float(walltime_p)   # *Adj_Est*
            else:
                runtime_estimate = float(walltime)

            if backfilling:
                if 60*runtime_estimate > (partition.backfill_time - now):
                    continue

            if self.reserve_ratio > 0:
                if self.reservation_violated(self.get_current_time_sec() + 60*runtime_estimate, partition.name):
                   continue

            # 
            ## print "[_find_job_location_wdrain] partition.state: ", partition.state 
            #
            if partition.state == "idle":
                flag_rtj = 0
                # let's check the impact on partitions that would become blocked
                score = 0
                for p in partition.parents: 
            	    pp = self._partitions[p]
            # dwang: 
                    if self.cached_partitions[p].state == "idle" and self.cached_partitions[p].scheduled:
                        score += 1
            ##
                    '''
            # dwang: 
            for pchild_name in pp.children: 
                        pchild = self._partitions[pchild_name]
                        if pchild.state == "rtj":
                            score += 5
                    '''
            # dwang  
                #
                for p_children_name in partition.children:
                    p_children = self._partitions[p_children_name]
                    if (p_children.state == "rtj"): #or (p_children.state == "temp_blocked"):
                        flag_rtj = 1
                        break
                #
                if flag_rtj:
                    continue
                    ##break
                #
                for p_parents_name in partition.parents:
                    p_parents = self._partitions[p_parents_name]
                    ppt_str = str(p_parents.state)
                    if (p_parents.state == "rtj"): #or (p_parents.state == "temp_blocked") or ('blocked' in ppt_str):
                        flag_rtj = 1
                        break
                if flag_rtj:
                    continue
                    ##break
                #

                # the lower the score, the fewer new partitions will be blocked by this selection
                print "[_find_job_location_wdrain] score: ", score 
                print "[_find_job_location_wdrain] partition: ", partition  
                # 
                if score < best_score:
                    best_score = score
                    best_partition = partition

                    best_partition_list[:] = []
                    best_partition_list.append(partition)
                    best_partition_score.append(score)
                #record equavalent partitions that have same best score
                elif score == best_score:
                    best_partition_list.append(partition)
                    
        # dwang:
        print "[_find_job_location_wdrain] best_partition_list: ", best_partition_list
        #print "[dw_bgqsim] best_partition_score: ", best_partition_score
        # dwang
        
        if self.walltime_aware_cons and len(best_partition_list) > 1:
            #print "best_partition_list=", [part.name for part in best_partition_list]
            #walltime aware job allocation (conservative)
            least_diff = MAXINT
            for partition in best_partition_list:
                nbpart = self.get_neighbor_by_partition(partition.name)
                if nbpart:
                    nbjob = self.get_running_job_by_partition(nbpart)
                    if nbjob:
                        nbjob_remain_length = nbjob.starttime + 60*float(nbjob.walltime) - self.get_current_time_sec()
                        diff = abs(60*float(walltime) - nbjob_remain_length)
                        if diff < least_diff:
                            least_diff = diff
                            best_partition = partition
                        msg = "jobid=%s, partition=%s, neighbor part=%s, neighbor job=%s, diff=%s" % (jobid, partition.name, nbpart, nbjob.jobid, diff)
                        #self.dbglog.LogMessage(msg)
            msg = "------------job %s allocated to best_partition %s-------------" % (jobid,  best_partition.name)
            #self.dbglog.LogMessage(msg)

        if best_partition:
            print "[dw_bqsim] bpart_rest : ", best_partition.name
            print "[dw_bqsim] bpart_rest STATE: ", best_partition.state
            #
            running_job = self.get_running_job_by_partition(best_partition.name)
            if running_job:
                print "[dw_bqsim] bpart_rest RUNNING: ", running_job.jobid
            else:
                print "[dw_bqsim] bpart_rest RUNNING: NONE "
            #
            print "[dw_bqsim] bpart_rest SCHEDULED: ", self.cached_partitions[best_partition.name].scheduled
            #
            #
            for p_children_name in best_partition.children:
                p_children = self._partitions[p_children_name]
                print "[dw_bqsim] bpart_rest_ch : ", p_children.name
                print "[dw_bqsim] bpart_rest_ch STATE: ", p_children.state
                #
                ch_running_job = self.get_running_job_by_partition(best_partition.name)
                if ch_running_job:
                    print "[dw_bqsim] bpart_rest_ch RUNNING: ", ch_running_job.jobid
                else:
                    print "[dw_bqsim] bpart_rest_ch RUNNING: NONE "
                #
                print "[dw_bqsim] bpart_rest_ch SCHEDULED: ", self.cached_partitions[p_children_name].scheduled
            #
            return {jobid: [best_partition.name]}



    def _find_job_location_resv(self, args, drain_partitions=set(), taken_partition=set(), backfilling=False): 
        # 
        print "[] _find_job_location_resv() jobid: ", args['jobid'] 
        print "[] _find_job_location_resv() RTJ_nodes: ", args['nodes'] 
        #
        jobid = args['jobid']
        nodes = args['nodes']
        queue = args['queue']
        utility_score = args['utility_score']
        walltime = args['walltime']
        walltime_p = args['walltime_p']  #*AdjEst*
        forbidden = args.get("forbidden", [])
        required = args.get("required", [])

        best_score = sys.maxint
        best_partition = None

        available_partitions = set()
        # dwang:
        full_partitions = set()  
        idle_partitions = set()  
        # dwang 

        requested_location = None
        if args['attrs'].has_key("location"):
            requested_location = args['attrs']['location']

        if required:
            # whittle down the list of required partitions to the ones of the proper size
            # this is a lot like the stuff in _build_locations_cache, but unfortunately,
            # reservation queues aren't assigned like real queues, so that code doesn't find
            # these
            for p_name in required:
                available_partitions.add(self.cached_partitions[p_name])
                available_partitions.update(self.cached_partitions[p_name]._children)

            possible = set()
            for p in available_partitions:
                possible.add(p.size)

            desired_size = 64
            job_nodes = int(nodes)
            for psize in sorted(possible):
                if psize >= job_nodes:
                    desired_size = psize
                    break

            for p in available_partitions.copy():
                if p.size != desired_size:
                    available_partitions.remove(p)
                elif p.name in self._not_functional_set:
                    available_partitions.remove(p)
                elif requested_location and p.name != requested_location:
                    available_partitions.remove(p)
        else:
        # dwang:
        #print "[dw_bgqsim] possible_location: ", self.possible_locations(nodes, queue) 
        # dwang: 
            for p in self.possible_locations(nodes, queue):
                skip = False
                for bad_name in forbidden:
                    if p.name == bad_name or bad_name in p.children or bad_name in p.parents:
                        skip = True
                        break

                if not skip:
                    if (not requested_location) or (p.name == requested_location):
                        available_partitions.add(p)

        available_partitions -= drain_partitions
        now = self.get_current_time()
        available_partitions = list(available_partitions)
        available_partitions.sort(key=lambda d: (d.name))
        best_partition_list = []
        best_partition_score = []

        # dwang: 
        ## print "[_find_job_location_resv] available_location: ", available_partitions
        print "[_find_job_location_resv] len.available_location: ", len(available_partitions)
        # dwang: 

        pcal = 0
        for partition in available_partitions: 
            #
            pcal = pcal + 1
            # if the job needs more time than the partition currently has available, look elsewhere
            if self.predict_backfill:
                runtime_estimate = float(walltime_p)   # *Adj_Est*
            else:
                runtime_estimate = float(walltime)
            '''
            if backfilling:
                if 60*runtime_estimate > (partition.backfill_time - now):
                    continue

            if self.reserve_ratio > 0:
                if self.reservation_violated(self.get_current_time_sec() + 60*runtime_estimate, partition.name):
                   continue
            '''
            
            # dwang:
            full_partitions.add(partition)
            #
            print "[_find_job_location_resv] _pcal: ", pcal 
            print "[_find_job_location_resv] partition: ", partition
            #print "[_find_job_location_resv] partition_2: ", available_partitions[partition]
            print "[_find_job_location_resv] partition.state: ", partition.state 
            #
            par_end_t, par_backfill_t = self.get_partition_expension(partition)
            print "[_find_job_location_resv] expension: ", par_backfill_t
            print "[_find_job_location_resv] now: ", now

            # partition_parent

            # partition_children

            # 
            ## score = (par_backfill_t - now)
            score = (par_end_t - now)
            print "[_find_job_location_resv] temp_score: ", score
       
            # the lower the score, the fewer new partitions will be blocked by this selection
            if score < best_score:
                best_score = score
                best_partition = partition

                best_partition_list[:] = []
                best_partition_list.append(partition)
                best_partition_score.append(score)
            #record equavalent partitions that have same best score
            elif score == best_score:
                best_partition_list.append(partition)  

        # # dwang:
        # print "[_find_job_location_resv] pcal: ", pcal
        # print "[_find_job_location_resv] full_partitions.SIZE: ", len(full_partitions)
        # print "[_find_job_location_resv] idle_partitions.SIZE: ", len(idle_partitions)
        # #
        # print "[_find_job_location_resv] best_partition_list.SIZE: ", len(best_partition_list)
        # print "[_find_job_location_resv] best_partition_list: ", best_partition_list
        # # dwang
        #
        # # dwang:
        # print "[dw_bgqsim] best_partition_score: ", best_partition_score
        # print "[dw_bgqsim] self.walltime_aware_cons: ", self.walltime_aware_cons
        # # dwang
        
        if self.walltime_aware_cons and len(best_partition_list) > 1:
            #print "best_partition_list=", [part.name for part in best_partition_list]
            #walltime aware job allocation (conservative)
            least_diff = MAXINT
            for partition in best_partition_list:
                nbpart = self.get_neighbor_by_partition(partition.name)
                if nbpart:
                    nbjob = self.get_running_job_by_partition(nbpart)
                    if nbjob:
                        nbjob_remain_length = nbjob.starttime + 60*float(nbjob.walltime) - self.get_current_time_sec()
                        diff = abs(60*float(walltime) - nbjob_remain_length)
                        if diff < least_diff:
                            least_diff = diff
                            best_partition = partition
                        msg = "jobid=%s, partition=%s, neighbor part=%s, neighbor job=%s, diff=%s" % (jobid, partition.name, nbpart, nbjob.jobid, diff)
                        #self.dbglog.LogMessage(msg)
            msg = "------------job %s allocated to best_partition %s-------------" % (jobid,  best_partition.name)
            #self.dbglog.LogMessage(msg)
        # 
        print "[_find_job_location_resv] best_partition: ", best_partition 
        # 
        if best_partition:
            ## print "[dw_bqsim] bpart_rest : ", best_partition.name
            ## print "[dw_bqsim] bpart_rest STATE: ", best_partition.state
            #
            running_job = self.get_running_job_by_partition(best_partition.name)
            ## if running_job:
                ## print "[dw_bqsim] bpart_rest RUNNING: ", running_job.jobid
            ## else:
                ## print "[dw_bqsim] bpart_rest RUNNING: NONE "
            #
            ## print "[dw_bqsim] bpart_rest SCHEDULED: ", self.cached_partitions[best_partition.name].scheduled
            #
            #
            for p_children_name in best_partition.children:
                p_children = self._partitions[p_children_name]
                ## print "[dw_bqsim] bpart_rest_ch : ", p_children.name
                ## print "[dw_bqsim] bpart_rest_ch STATE: ", p_children.state
                #
                ch_running_job = self.get_running_job_by_partition(best_partition.name)
                ## if ch_running_job:
                    ## print "[dw_bqsim] bpart_rest_ch RUNNING: ", ch_running_job.jobid
                ## else:
                    ## print "[dw_bqsim] bpart_rest_ch RUNNING: NONE "
                #
                ## print "[dw_bqsim] bpart_rest_ch SCHEDULED: ", self.cached_partitions[p_children_name].scheduled
            #
            ## print "[_find_job_location_resv] jobid: ", jobid
            ## print "[_find_job_location_resv] best_partition: ", best_partition  
            #
            return best_partition, {jobid: [best_partition.name]} 
        


    def get_partition_expension(self, partition): 
        print " [] get_partition_expension() ... "
        par_end_time = 0 
        par_backfill_time = 0 
        p_name = partition.name 
        print " [partExpen] p_name: ", p_name  
        #
        if partition.state == 'busy': 
            print " [partExpen] BUSY state ... " 
            print " [partExpen] BUSY state ... "
            nbjob = self.get_running_job_by_partition(p_name)
            print " [partExpen] BUSY jid: ", nbjob.get('jobid') 
            print " [partExpen] BUSY runtime: ", nbjob.get('runtime') 
            print " [partExpen] BUSY start_time: ", nbjob.get('start_time') 
            print " [partExpen] BUSY end_time: ", nbjob.get('end_time') 
            print " [partExpen] BUSY backfill_time: ", partition.backfill_time 
            par_end_time = nbjob.get('end_time') 
            par_backfill_time = partition.backfill_time 
            # 
        else: 
            print " [partExpen] ELSE state ... " 
            # 
            istop_1 = str(partition.state).find('(')
            istop_2 = str(partition.state).find(')')
            if istop_1 and istop_2: 
                print "istop_1: ", istop_1
                print "istop_2: ", istop_2 
                block_p_name = str(partition.state)[istop_1+1: istop_2]
                print " [partExpen] block_p_name, ", block_p_name
                #
                block_p = self._partitions[block_p_name] 
                print " [partExpen] ELSE state ... ", block_p.state 
                nbjob = self.get_running_job_by_partition(block_p_name)
                # 
                global rtj_resv_part_dict
                for rtj_resv_key in rtj_resv_part_dict:  
                    print " [partExpen] ELSE part_str_resv: ", rtj_resv_part_dict[rtj_resv_key][0]  
                    if rtj_resv_part_dict[rtj_resv_key][0] == block_p_name: 
                        print " [partExpen] ELSE part_resv_MATCH ... " 
                        print " [partExpen] ELSE match_Jobid: ", rtj_resv_key  
                        #
                        queuing_jobs = self._get_queuing_jobs() 
                        print " [partExpen] ELSE  len(queuing_jobs): ", len(queuing_jobs)   
                        #
			'''
                        live_jobs = self.get_live_job_by_id(rtj_resv_key) 
                        print " [partExpen] ELSE  live_jobs: ", live_jobs 
                        print " [partExpen] ELSE  live_jobs.jobid: ", live_jobs.get('jobid') 
                        print " [partExpen] ELSE  live_jobs.runtime: ", live_jobs.get('runtime') 
			'''
                        #
			gjob = self.queues.get_jobs([{'jobid':rtj_resv_key}])
                        print " [partExpen] ELSE  gjobs: ", gjob  
                        # print " [partExpen] ELSE  gjobs.runtime: ", gjob.get('runtime') 
			# 
                        #for id, spec in self.unsubmitted_job_spec_dict.iteritems():
                        ## rtj_resv_job = self.unsubmitted_job_spec_dict[rtj_resv_key] 
                        # 
                        print " [partExpen] ELSE job_runtime: ", job.runtime 
                # 
                print " [partExpen] ELSE jid: ", nbjob.get('jobid') 
                print " [partExpen] ELSE runtime: ", nbjob.get('runtime') 
                print " [partExpen] ELSE start_time: ", nbjob.get('start_time') 
                print " [partExpen] ELSE end_time: ", nbjob.get('end_time') 
                print " [partExpen] ELSE backfill_time: ", block_p.backfill_time 
                par_end_time = nbjob.get('end_time') 
                par_backfill_time = block_p.backfill_time 
                # 
                # dependent in resv_dict ... 
                # ... ...    
                # 
        # 
        return par_end_time, par_backfill_time  


    # dwang: 
    def _find_job_location_wcheckp(self, args, drain_partitions=set(), taken_partition=set(), backfilling=False):
        jobid = args['jobid']
        nodes = args['nodes']
        queue = args['queue']
        utility_score = args['utility_score']
        walltime = args['walltime']
        walltime_p = args['walltime_p']  #*AdjEst*
        forbidden = args.get("forbidden", [])
        required = args.get("required", [])

        best_score = sys.maxint
        best_partition = None

        available_partitions = set()

        requested_location = None
        if args['attrs'].has_key("location"):
            requested_location = args['attrs']['location']

        if required:
            # whittle down the list of required partitions to the ones of the proper size
            # this is a lot like the stuff in _build_locations_cache, but unfortunately,
            # reservation queues aren't assigned like real queues, so that code doesn't find
            # these
            for p_name in required:
                available_partitions.add(self.cached_partitions[p_name])
                available_partitions.update(self.cached_partitions[p_name]._children)

            possible = set()
            for p in available_partitions:
                possible.add(p.size)

            desired_size = 64
            job_nodes = int(nodes)
            for psize in sorted(possible):
                if psize >= job_nodes:
                    desired_size = psize
                    break

            for p in available_partitions.copy():
                if p.size != desired_size:
                    available_partitions.remove(p)
                elif p.name in self._not_functional_set:
                    available_partitions.remove(p)
                elif requested_location and p.name != requested_location:
                    available_partitions.remove(p)
        else:
        # dwang:
        #print "[dw_bgqsim] possible_location: ", self.possible_locations(nodes, queue) 
        # dwang: 
            for p in self.possible_locations(nodes, queue):
                skip = False
                for bad_name in forbidden:
                    if p.name == bad_name or bad_name in p.children or bad_name in p.parents:
                        skip = True
                        break

                if not skip:
                    if (not requested_location) or (p.name == requested_location):
                        available_partitions.add(p)

        available_partitions -= drain_partitions
        now = self.get_current_time()
        available_partitions = list(available_partitions)
        available_partitions.sort(key=lambda d: (d.name))
        best_partition_list = []
        best_partition_score = []
        partition_list = []
        partition_score = [] 

        # dwang:
        #print "[dw_bgqsim] available_location: ", available_partitions
        rtj_partitions = []
        rtj_list = []
        for partition in available_partitions:
            #print "av_partitions: ", partition.name
            #print "av_partitions_STATE: ", partition.state
            for p_children_name in partition.children:
                p_children = self._partitions[p_children_name]
                #print "ch_av_partitions: ", p_children.name
                #print "ch_av_partitions_STATE: ", p_children.state
                #
                if p_children.state == "rtj":
                    rtj_partitions.append(partition)
                    #print "ch_rtj_partitions: ", p_children.name
                    #print "ch_rtj_partitions_STATE: ", p_children.state
                    break;
        #available_partitions -= rtj_partitions
        #print "avail_partitions: ", available_partitions
        #print "len(avail_part): ", len(available_partitions)
        #print "rtj_partitions: ", rtj_partitions

        # dwang:
        best_score2 = 0
        rtj_conflict_count = 0
        rtj_depend_count = 0 
        for partition in available_partitions:
            flag_rtj = 0
            # if the job needs more time than the partition currently has available, look elsewhere
            if self.predict_backfill:
                runtime_estimate = float(walltime_p)   # *Adj_Est*
            else:
                runtime_estimate = float(walltime)

            if backfilling:
                if 60*runtime_estimate > (partition.backfill_time - now):
                    continue

            if self.reserve_ratio > 0:
                if self.reservation_violated(self.get_current_time_sec() + 60*runtime_estimate, partition.name):
                    continue

            # dwang:
            score = 0
            score2 = 0
            #if partition.state == "rtj":
            #    continue
 
            #print "[rtj_part] check_part ... "
            # let's check the impact on partitions that would become blocked
            #if (partition.state == "idle") or (partition.state == "busy"):
            # if partition.state != "rtj":
            job_temp = self.get_running_job_by_partition(partition.name)
            if job_temp and job_temp.user != 'realtime' or job_temp is None:
                #
                if partition.state == "busy":
                    score += 20 
                #
                for p_children_name in partition.children:
                    p_children = self._partitions[p_children_name]
                    job_temp = self.get_running_job_by_partition(p_children_name)
                    if job_temp and job_temp.user == 'realtime':
                    # if (p_children.state == "rtj"): #or (p_children.state == "temp_blocked"):
                        flag_rtj = 1
                        break
                        #score += 50
                if flag_rtj:
                    rtj_depend_count += 1
                    continue
                    ##break
                #
                #
                
                for p_parents_name in partition.parents:
                    p_parents = self._partitions[p_parents_name]
                    ppt_str = str(p_parents.state)
                    job_temp = self.get_running_job_by_partition(p_parents_name)
                    if job_temp and job_temp.user == 'realtime':
                    # if (p_parents.state == "rtj"): #or (p_parents.state == "temp_blocked") or ('blocked' in ppt_str):
                        flag_rtj = 1
                        break
                if flag_rtj:
                    rtj_depend_count += 1
                    continue
                    ##break
                ##
                for p in partition.parents:
                    pp = self._partitions[p]
                    if self.cached_partitions[p].state == "idle" and self.cached_partitions[p].scheduled:
                        score += 1
                    # dwang:
                    for pchild_name in pp.children:
                        pchild = self._partitions[pchild_name]
                        job_temp = self.get_running_job_by_partition(pchild_name)
                        if job_temp and job_temp.user == 'realtime':
                        # if pchild.state == "rtj":
                            score2 += 1
                # the lower the score, the fewer new partitions will be blocked by this selection
                if score < best_score:
                    best_score = score
                    best_partition = partition

                    best_partition_list[:] = []
                    best_partition_list.append(partition)
                    #
                    print "[rtj_none_conflict] p_name: ", partition
                    print "[rtj_none_conflict] p_state: ", partition.state
                    nbjob = self.get_running_job_by_partition(partition.name)
                    print "[rtj_none_conflict] p job: ", nbjob
                    #
                #record equavalent partitions that have same best score
                elif score == best_score:
                    best_partition_list.append(partition)
                    #
                    print "[rtj_none_conflict] p_name: ", partition
                    print "[rtj_none_conflict] p_state: ", partition.state
                    nbjob = self.get_running_job_by_partition(partition.name)
                    print "[rtj_none_conflict] p job: ", nbjob
                    #
            else:
                rtj_conflict_count += 1
                print "[rtj_conflict] p_name: ", partition
                print "[rtj_conflict] p_state: ", partition.state
                nbjob = self.get_running_job_by_partition(partition.name)
                print "[rtj_conflict] p job: ", nbjob
        
        #
        print "rtj_conflict_count: ", rtj_conflict_count
        print "rtj_depend_count: ", rtj_depend_count
        print "len(best_part_list): ", len(best_partition_list)
        #
        if self.walltime_aware_cons and len(best_partition_list) > 1:
            #print "best_partition_list=", [part.name for part in best_partition_list]
            #walltime aware job allocation (conservative)
            least_diff = MAXINT
            for partition in best_partition_list:
                nbpart = self.get_neighbor_by_partition(partition.name)
                if nbpart:
                    nbjob = self.get_running_job_by_partition(nbpart)
                    if nbjob:
                        nbjob_remain_length = nbjob.starttime + 60*float(nbjob.walltime) - self.get_current_time_sec()
                        diff = abs(60*float(walltime) - nbjob_remain_length)
                        if diff < least_diff:
                            least_diff = diff
                            best_partition = partition
                        msg = "jobid=%s, partition=%s, neighbor part=%s, neighbor job=%s, diff=%s" % (jobid, partition.name, nbpart, nbjob.jobid, diff)
                        #self.dbglog.LogMessage(msg)
            msg = "------------job %s allocated to best_partition %s-------------" % (jobid,  best_partition.name)
            #self.dbglog.LogMessage(msg)

        if best_partition:
            print "[dw_bqsim] bpart_wcheckp : ", best_partition.name 
            print "[dw_bqsim] bpart_wcheckp STATE: ", best_partition.state
            #
            for p_children_name in best_partition.children:
                p_children = self._partitions[p_children_name]
                print "[dw_bqsim] bpart_wcheckp_ch : ", p_children.name
                print "[dw_bqsim] bpart_wcheckp_ch STATE: ", p_children.state
            #partition.state = "rtj" #
            #
            for p_parents_name in best_partition.parents:
                p_parents = self._partitions[p_parents_name]
                print "[dw_bqsim] bpart_wcheckp_pt : ", p_parents.name
                print "[dw_bqsim] bpart_wcheckp_pt STATE: ", p_parents.state
            return {jobid: [best_partition.name]} 
    # dwang 


    def _find_job_location_wcheckp_sam_v1(self, args, drain_partitions=set(), taken_partition=set(),
                                   backfilling=False, job_length_type=None, checkp_t_internval=None, checkp_overhead_percent=None):
        jobid = args['jobid']
        nodes = args['nodes']
        queue = args['queue']
        utility_score = args['utility_score']
        walltime = args['walltime']
        walltime_p = args['walltime_p']  # *AdjEst*
        forbidden = args.get("forbidden", [])
        required = args.get("required", [])

        # best_score = sys.maxint
        best_score = (sys.maxint, sys.maxint, sys.maxint, sys.maxint, sys.maxint, sys.maxint)

        best_partition = None

        available_partitions = set()

        requested_location = None
        if args['attrs'].has_key("location"):
            requested_location = args['attrs']['location']

        if required:
            # whittle down the list of required partitions to the ones of the proper size
            # this is a lot like the stuff in _build_locations_cache, but unfortunately,
            # reservation queues aren't assigned like real queues, so that code doesn't find
            # these
            for p_name in required:
                available_partitions.add(self.cached_partitions[p_name])
                available_partitions.update(self.cached_partitions[p_name]._children)

            possible = set()
            for p in available_partitions:
                possible.add(p.size)

            desired_size = 64
            job_nodes = int(nodes)
            for psize in sorted(possible):
                if psize >= job_nodes:
                    desired_size = psize
                    break

            for p in available_partitions.copy():
                if p.size != desired_size:
                    available_partitions.remove(p)
                elif p.name in self._not_functional_set:
                    available_partitions.remove(p)
                elif requested_location and p.name != requested_location:
                    available_partitions.remove(p)
        else:
            # dwang:
            # print "[dw_bgqsim] possible_location: ", self.possible_locations(nodes, queue)
            # dwang:
            for p in self.possible_locations(nodes, queue):
                skip = False
                for bad_name in forbidden:
                    if p.name == bad_name or bad_name in p.children or bad_name in p.parents:
                        skip = True
                        break

                if not skip:
                    if (not requested_location) or (p.name == requested_location):
                        available_partitions.add(p)

        available_partitions -= drain_partitions
        now = self.get_current_time()
        available_partitions = list(available_partitions)
        available_partitions.sort(key=lambda d: (d.name))

        best_partition_list = []
        partition_list = []

        # valid_available_partitions = []
        # for available_partition in available_partitions:
        #     print available_partition
        #     if available_partition.size <= desired_size:
        #         valid_available_partitions.append(available_partition)

        # print('Removed ' + str(removed_partition_count) + ' partitions that had parents who were running jobs')
        #
        # available_partitions = valid_available_partitions

        removed_partition_count = 0

        # get the max partition size
        job_nodes = int(nodes)
        if self._defined_sizes.has_key(queue):
            # max partition size is the same size as the partition size requested
            max_partition_size = sorted([i for i in self._defined_sizes[queue] if i >= job_nodes])[0]

            # max partition size is one size bigger than the partition size requested
            # max_partition_size = sorted([i for i in self._defined_sizes[queue] if i >= job_nodes])[1]

        else:
            print('error getting desired partition size')
            exit(-1)

        # dwang:
        rtj_conflict_count = 0
        rtj_depend_count = 0
        for partition in available_partitions:

            # if partition.name == 'MIR-08000-3BFF1-8192':
            #     child_partition_names = [p_children_name for p_children_name in partition.children]
            #     print child_partition_names

            ###########################################################################
            # remove any partitions whose parent partitions are running jobs
            flag_parent_partition = 0
            for p_parents_name in partition.parents:
                p_parents = self._partitions[p_parents_name]
                job_temp = self.get_running_job_by_partition(p_parents_name)
                if job_temp and p_parents.size > max_partition_size:
                    removed_partition_count += 1
                    flag_parent_partition = 1
                    # print(
                    # 'Removing partition ' + partition.name + ' because parent ' + p_parents_name + ' has a job')
                    break
            if flag_parent_partition == 1:
                removed_partition_count += 1
                continue
            ###########################################################################

            flag_rtj = 0
            # if the job needs more time than the partition currently has available, look elsewhere
            if self.predict_backfill:
                runtime_estimate = float(walltime_p)  # *Adj_Est*
            else:
                runtime_estimate = float(walltime)

            if backfilling:
                if 60 * runtime_estimate > (partition.backfill_time - now):
                    continue

            if self.reserve_ratio > 0:
                if self.reservation_violated(self.get_current_time_sec() + 60 * runtime_estimate,
                                             partition.name):
                    continue


            job_temp = self.get_running_job_by_partition(partition.name)
            if job_temp and job_temp.user == 'realtime':
                rtj_conflict_count += 1
                # print "[rtj_conflict] p_name: ", partition
                # print "[rtj_conflict] p_state: ", partition.state
                nbjob = self.get_running_job_by_partition(partition.name)
                # print "[rtj_conflict] p job: ", nbjob
                continue

            # print "[rtj_part] check_part ... "
            # let's check the impact on partitions that would become blocked
            for p_children_name in partition.children:
                p_children = self._partitions[p_children_name]
                job_temp = self.get_running_job_by_partition(p_children_name)
                if job_temp and job_temp.user == 'realtime':
                    flag_rtj = 1
                    break
            if flag_rtj:
                rtj_depend_count += 1
                continue

            for p_parents_name in partition.parents:
                p_parents = self._partitions[p_parents_name]
                ppt_str = str(p_parents.state)
                job_temp = self.get_running_job_by_partition(p_parents_name)
                if job_temp and job_temp.user == 'realtime':
                    flag_rtj = 1
                    break
            if flag_rtj:
                rtj_depend_count += 1
                continue

            def compute_slowdown(job_temp):
                # runtime_org1 = float(job_temp.get('walltime')) * 60
                # job_end_time1 = walltime_runtime_differences[str(job_temp.get('jobid'))] + job_temp.get('end_time')
                # run_time1 = (job_end_time1 - job_temp.get('submittime'))
                #
                # runtime_org2 = float(job_temp.get('runtime'))
                # job_end_time2 = job_temp.get('end_time')
                # run_time2 = (job_end_time2 - job_temp.get('submittime'))
                #
                # runtime_org3 = predicted_run_times[str(job_temp.get('jobid'))]
                # job_end_time3 = job_temp.get('end_time')
                # run_time3 = (job_end_time3 - job_temp.get('submittime'))
                #
                # # this looks confusing but I'm just using the predicted_run_time to compute the estimated job_end_time
                # # actual end_time + walltime_runtime_difference - wall_time + predicted_run_time = estimated end_time
                # job_end_time4 = walltime_runtime_differences[str(job_temp.get('jobid'))] + job_temp.get('end_time') - \
                #                 float(job_temp.get('walltime')) * 60 + predicted_run_times[str(job_temp.get('jobid'))]
                # tmp_start_time = job_temp.get('starttime')
                # job_end_time5 = job_temp.get('starttime') + runtime_org3
                # run_time4 = (job_end_time4 - job_temp.get('submittime'))

                if job_length_type == 'walltime':
                    # THIS IS THE ORIGINAL CODE
                    runtime_org = float(job_temp.get('walltime')) * 60
                    job_end_time = walltime_runtime_differences[str(job_temp.get('jobid'))] + job_temp.get('end_time')
                elif job_length_type == 'actual':
                    # THIS IS A TEST - TO COMPUTE SLOWDOWN USING ACTUAL RUNTIME, REMOVE THIS WHEN DONE TESTING
                    runtime_org = float(job_temp.get('runtime'))
                    job_end_time = job_temp.get('end_time')
                elif job_length_type == 'predicted':
                    # THIS IS A TEST - TO COMPUTE SLOWDOWN USING PREDICTED RUNTIME, REMOVE THIS WHEN DONE TESTING
                    predicted_runtime = predicted_run_times[str(job_temp.get('jobid'))]
                    # job_end_time = job_temp.get('end_time')
                    # this looks confusing but I'm just using the predicted_run_time to compute the estimated job_end_time
                    # actual end_time + walltime_runtime_difference - wall_time + predicted_run_time = estimated end_time
                    job_end_time = walltime_runtime_differences[str(job_temp.get('jobid'))] + job_temp.get('end_time') - \
                                float(job_temp.get('walltime')) * 60 + predicted_run_times[str(job_temp.get('jobid'))]
                    runtime_org = predicted_runtime
                else:
                    print('Invalid argument for job_length_type: ', job_length_type)
                    exit(-1)

                # # # THIS IS A TEST - TO COMPUTE SLOWDOWN USING PREDICTED RUNTIME, REMOVE THIS WHEN DONE TESTING
                # # predicted_runtime = predicted_run_times[str(job_temp.get('jobid'))]
                # # job_end_time = job_temp.get('end_time')
                # # runtime_org = predicted_runtime
                #
                # # # THIS IS A TEST - TO COMPUTE SLOWDOWN USING ACTUAL RUNTIME, REMOVE THIS WHEN DONE TESTING
                # # runtime_org = float(job_temp.get('runtime'))
                # # job_end_time = job_temp.get('end_time')
                #
                # # THIS IS THE ORIGINAL CODE
                # runtime_org = float(job_temp.get('walltime')) * 60
                # job_end_time = walltime_runtime_differences[str(job_temp.get('jobid'))] + job_temp.get('end_time')

                slowdown = (job_end_time - job_temp.get('submittime')) / runtime_org

                return slowdown, job_end_time, runtime_org

            def test_slowdown_threshold(partition):
                job_temp = self.get_running_job_by_partition(partition.name)

                # if there is no running job then return true
                if not job_temp:
                    return True

                slowdown, job_end_time, runtime = compute_slowdown(job_temp)

                # # THIS IS A TEST - TO COMPUTE SLOWDOWN USING PREDICTED RUNTIME, REMOVE THIS WHEN DONE TESTING
                # predicted_runtime = predicted_run_times[str(job_temp.get('jobid'))]
                # # runtime_org1 = predicted_runtime
                # # runtime_org2 = float(job_temp.get('walltime')) * 60
                #
                # # job_end_time = job_temp.get('remain_time') + job_temp.get('start_time')
                # job_end_time = job_temp.get('end_time')
                # # runtime_org = max(runtime_org1, runtime_org2)
                # runtime_org = predicted_runtime
                #
                # # # THIS IS A TEST - TO COMPUTE SLOWDOWN USING ACTUAL RUNTIME, REMOVE THIS WHEN DONE TESTING
                # # runtime_org = float(job_temp.get('runtime'))
                # # job_end_time = job_temp.get('remain_time') + job_temp.get('start_time')
                #
                # # THIS IS THE ORIGINAL CODE
                # # runtime_org = float(job_temp.get('walltime')) * 60
                # # job_end_time = walltime_runtime_differences[str(job_temp.get('jobid'))] + job_temp.get('end_time')
                #
                # slowdown = (job_end_time - job_temp.get('submittime')) / runtime_org
                #
                # test1 = job_temp.get('end_time')
                # test2 = job_temp.get('remain_time') + job_temp.get('start_time')
                # test3 = walltime_runtime_differences[str(job_temp.get('jobid'))]
                #
                # if test1 != test2:
                #     print('what does this mean??')

                rtj_narrow_short_slowdown = 1.5
                rtj_narrow_long_slowdown = 1.5
                rtj_wide_short_slowdown = 1.5
                rtj_wide_long_slowdown = 1.5

                # batch_narrow_short_slowdown = 2.0 #1.5 #5.0
                # batch_narrow_long_slowdown = 1.3 #1.2 #1.5
                # batch_wide_short_slowdown = 3.0 #20
                # batch_wide_long_slowdown = 2.0 #1.5 #5.0

                # batch_narrow_short_slowdown = 3.0 #1.5 #5.0
                # batch_narrow_long_slowdown = 1.3 #1.2 #1.5
                # batch_wide_short_slowdown = 20.0 #20
                # batch_wide_long_slowdown = 4.0 #1.5 #5.0

                batch_narrow_short_slowdown = 3.0 #1.5 #5.0
                batch_narrow_long_slowdown = 1.3 #1.2 #1.5
                batch_wide_short_slowdown = 10.0 #20
                batch_wide_long_slowdown = 2.0 #1.5 #5.0


                if job_temp.get('jobid') in rtj_id: # if job is a realtime job
                    if float(job_temp.get('nodes')) <= 4096: # if job is narrow
                        if float(job_temp.get('walltime')) <= 120: # if job is short
                            slowdown_threshold = rtj_narrow_short_slowdown
                        else:  # if job is long
                            slowdown_threshold = rtj_narrow_long_slowdown
                    else:  # if job is wide
                        if float(job_temp.get('walltime')) <= 120:  # if job is short
                            slowdown_threshold = rtj_wide_short_slowdown
                        else:  # if job is long
                            slowdown_threshold = rtj_wide_long_slowdown
                else:  # if job is a batch job
                    if float(job_temp.get('nodes')) <= 4096:  # if job is narrow
                        if float(job_temp.get('walltime')) <= 120:  # if job is short
                            slowdown_threshold = batch_narrow_short_slowdown
                        else:  # if job is long
                            slowdown_threshold = batch_narrow_long_slowdown
                    else:  # if job is wide
                        if float(job_temp.get('walltime')) <= 120:  # if job is short
                            slowdown_threshold = batch_wide_short_slowdown
                        else:  # if job is long
                            slowdown_threshold = batch_wide_long_slowdown

                if slowdown > slowdown_threshold:
                    return False
                else:
                    return True

            slowdown_threshold_flag = False
            if test_slowdown_threshold(partition) == False:
                continue
            for p_children_name in partition.children:
                if test_slowdown_threshold(self._partitions[p_children_name]) == False:
                    slowdown_threshold_flag = True
                    break
            for p_parents_name in partition.parents:
                if test_slowdown_threshold(self._partitions[p_parents_name]) == False:
                    slowdown_threshold_flag = True
                    break
            if slowdown_threshold_flag:
                continue

            current_partition_score = 0.0
            # Don't preempt wide jobs - count number of parents busy
            # Don't preempt jobs that are almost done (less than 5 minutes or 5%)
            # Don't preempt jobs with high slowdown values

            weight_nearly_done = True
            weight_checkpoint_time = False
            weight_slowdown = True

            def partition_score(tmp_partition, src_partition):
                job_temp = self.get_running_job_by_partition(tmp_partition.name)
                score = float(job_temp.get('nodes'))

                # score = float(job_temp.get('nodes')) * float(job_temp.get('nodes'))
                # score = math.sqrt(float(job_temp.get('nodes')))
                # score = 1

                # # runtime_org = float(job_temp.get('walltime')) * 60
                # # job_end_time = walltime_runtime_differences[str(job_temp.get('jobid'))] + job_temp.get('end_time')
                #
                # # THIS IS A TEST - TO COMPUTE SLOWDOWN USING PREDICTED RUNTIME, REMOVE THIS WHEN DONE TESTING
                # predicted_runtime = predicted_run_times[str(job_temp.get('jobid'))]
                # runtime_org = predicted_runtime
                # # job_end_time = job_temp.get('remain_time') + job_temp.get('start_time')
                # job_end_time = job_temp.get('end_time')
                #
                # # THIS IS A TEST - TO COMPUTE SLOWDOWN USING ACTUAL RUNTIME, REMOVE THIS WHEN DONE TESTING
                # # runtime_org = float(job_temp.get('runtime'))
                # # job_end_time = job_temp.get('remain_time') + job_temp.get('start_time')
                # # THIS IS THE ORIGINAL CODE
                # # runtime_org = float(job_temp.get('walltime')) * 60
                # # job_end_time = walltime_runtime_differences[str(job_temp.get('jobid'))] + job_temp.get('end_time')
                slowdown, job_end_time, runtime_org = compute_slowdown(job_temp)

                if weight_nearly_done:
                    remaining_time = job_end_time - now
                    if remaining_time <= min(runtime_org * 0.25, 15 * 60.0):
                        score = score * 5.0

                if weight_checkpoint_time:
                    if checkp_t_internval is not None:
                        time_since_checkpoint = (now - float(job_temp.get('start_time'))) % checkp_t_internval
                        # checkpoint_weight = 1 + math.sqrt(time_since_checkpoint / checkp_t_internval)
                        checkpoint_weight = 1 + (time_since_checkpoint / (2.0 * checkp_t_internval))
                        score = score * checkpoint_weight

                if weight_slowdown:
                    # slowdown = (job_end_time - job_temp.get('submittime')) / runtime_org
                    score = score * math.log(2.0 + slowdown, 2)

                return score

            if partition.state == "busy":
                current_partition_score += partition_score(partition,partition)

            for p_parents_name in partition.parents:
                p_parents = self._partitions[p_parents_name]
                if (p_parents.state == "busy"):
                    current_partition_score += partition_score(p_parents,partition)

            for p_children_name in partition.children:
                p_children = self._partitions[p_children_name]
                if (p_children.state == "busy"):
                    current_partition_score += partition_score(p_children,partition)

            # don't start jobs that will block other partitions that are scheduled
            parents_scheduled_count = 0
            for p in partition.parents:
                pp = self._partitions[p]
                if self.cached_partitions[p].state == "idle" and self.cached_partitions[p].scheduled:
                    parents_scheduled_count += 1

            # score = (busy_count, almost_done_count, total_slowdown, parents_scheduled_count, wasted_time_since_checkpoint)
            tmp_score = (partition, current_partition_score)
            partition_list.append(tmp_score)

            # print "[rtj_none_conflict] p_name: ", partition
            print "[rtj_none_conflict] p_state: ", partition.state
            nbjob = self.get_running_job_by_partition(partition.name)
            # print "[rtj_none_conflict] p job: ", nbjob

        #
        # print "rtj_conflict_count: ", rtj_conflict_count
        # print "rtj_depend_count: ", rtj_depend_count
        # print "len(best_part_list): ", len(best_partition_list)
        #

        # print('Removed ' + str(removed_partition_count) + ' partitions that had parents who were running jobs')

        # score_margin = 0.1
        # tmp_partition_list = []
        # partition_list.sort(key=lambda x: x[1]) # sort partition list
        # best_score = partition_list[0][1]
        # # remove all partitions that are outside the threshold
        # for partition_item in partition_list:
        #     if partition_item[1] <= best_score * (1.0 + score_margin):
        #         tmp_partition_list.append(partition_item)
        #
        # best_partition_list = tmp_partition_list
        #
        # # if len(best_partition_list) == 1:
        # #     best_partition = best_partition_list[0][0]
        #
        # best_partition_list = [tmp_partition for tmp_partition, tmp_partition_score in best_partition_list]

        partition_list.sort(key=lambda x: x[1])  # sort partition list
        if len(partition_list) > 0:
            best_partition = partition_list[0][0]

        if self.walltime_aware_cons and len(best_partition_list) > 1:
            # print "best_partition_list=", [part.name for part in best_partition_list]
            # walltime aware job allocation (conservative)
            least_diff = MAXINT
            for partition, tmp_score in best_partition_list:
                nbpart = self.get_neighbor_by_partition(partition.name)
                if nbpart:
                    nbjob = self.get_running_job_by_partition(nbpart)
                    if nbjob:
                        nbjob_remain_length = nbjob.starttime + 60 * float(
                            nbjob.walltime) - self.get_current_time_sec()
                        diff = abs(60 * float(walltime) - nbjob_remain_length)
                        if diff < least_diff:
                            least_diff = diff
                            best_partition = partition
                        msg = "jobid=%s, partition=%s, neighbor part=%s, neighbor job=%s, diff=%s" % (
                        jobid, partition.name, nbpart, nbjob.jobid, diff)
                        # self.dbglog.LogMessage(msg)
            msg = "------------job %s allocated to best_partition %s-------------" % (
            jobid, best_partition.name)
            # self.dbglog.LogMessage(msg)

        if best_partition:
            # print "[dw_bqsim] bpart_wcheckp : ", best_partition.name
            # print "[dw_bqsim] bpart_wcheckp STATE: ", best_partition.state
            #
            for p_children_name in best_partition.children:
                p_children = self._partitions[p_children_name]
                # print "[dw_bqsim] bpart_wcheckp_ch : ", p_children.name
                # print "[dw_bqsim] bpart_wcheckp_ch STATE: ", p_children.state
            #
            for p_parents_name in best_partition.parents:
                p_parents = self._partitions[p_parents_name]
                # print "[dw_bqsim] bpart_wcheckp_pt : ", p_parents.name
                # print "[dw_bqsim] bpart_wcheckp_pt STATE: ", p_parents.state
            return {jobid: [best_partition.name]}
            # dwang

    # dwang:
    def _find_job_location_wcheckpH(self, args, ckptH_opt, drain_partitions=set(), taken_partition=set(), backfilling=False):
        # __dw_1222:
        # print "_find_job_location_wcheckpH() ... "
        # __dw_1222
        jobid = args['jobid']
        nodes = args['nodes']
        queue = args['queue']
        utility_score = args['utility_score']
        walltime = args['walltime']
        walltime_p = args['walltime_p']  #*AdjEst*
        forbidden = args.get("forbidden", [])
        required = args.get("required", [])

        best_score = sys.maxint
        best_partition = None

        available_partitions = set()

        requested_location = None
        if args['attrs'].has_key("location"):
            requested_location = args['attrs']['location']

        if required:
            # whittle down the list of required partitions to the ones of the proper size
            # this is a lot like the stuff in _build_locations_cache, but unfortunately,
            # reservation queues aren't assigned like real queues, so that code doesn't find
            # these
            for p_name in required:
                available_partitions.add(self.cached_partitions[p_name])
                available_partitions.update(self.cached_partitions[p_name]._children)

            possible = set()
            for p in available_partitions:
                possible.add(p.size)

            desired_size = 64
            job_nodes = int(nodes)
            for psize in sorted(possible):
                if psize >= job_nodes:
                    desired_size = psize
                    break

            for p in available_partitions.copy():
                if p.size != desired_size:
                    available_partitions.remove(p)
                elif p.name in self._not_functional_set:
                    available_partitions.remove(p)
                elif requested_location and p.name != requested_location:
                    available_partitions.remove(p)
        else:
        # dwang:
        #print "[dw_bgqsim] possible_location: ", self.possible_locations(nodes, queue)
        # dwang:
            for p in self.possible_locations(nodes, queue):
                skip = False
                for bad_name in forbidden:
                    if p.name == bad_name or bad_name in p.children or bad_name in p.parents:
                        skip = True
                        break

                if not skip:
                    if (not requested_location) or (p.name == requested_location):
                        available_partitions.add(p)

        available_partitions -= drain_partitions
        now = self.get_current_time()
        available_partitions = list(available_partitions)
        available_partitions.sort(key=lambda d: (d.name))
        best_partition_list = []
        best_partition_score = []
        partition_list = []
        partition_score = []
        #_0322:
        partition_preempt_list = []
        partition_preempt_score = []
        partition_preempt_best_score = sys.maxint
        #_0322

        # dwang:
        #print "[dw_bgqsim] available_location: ", available_partitions
        rtj_partitions = []
        rtj_list = []
        for partition in available_partitions:
            #print "av_partitions: ", partition.name
            #print "av_partitions_STATE: ", partition.state
            for p_children_name in partition.children:
                p_children = self._partitions[p_children_name]
                #print "ch_av_partitions: ", p_children.name
                #print "ch_av_partitions_STATE: ", p_children.state
                #
                if p_children.state == "rtj":
                    rtj_partitions.append(partition)
                    #print "ch_rtj_partitions: ", p_children.name
                    #print "ch_rtj_partitions_STATE: ", p_children.state
                    break;
        #available_partitions -= rtj_partitions
        #print "avail_partitions: ", available_partitions
        #print "len(avail_part): ", len(available_partitions)
        #print "rtj_partitions: ", rtj_partitions

        # dwang:
        best_score2 = 0
        rtj_conflict_count = 0
        rtj_depend_count = 0
        for partition in available_partitions:
            flag_rtj = 0
            # if the job needs more time than the partition currently has available, look elsewhere
            if self.predict_backfill:
                runtime_estimate = float(walltime_p)   # *Adj_Est*
            else:
                runtime_estimate = float(walltime)

            if backfilling:
                if 60*runtime_estimate > (partition.backfill_time - now):
                    continue

            if self.reserve_ratio > 0:
                if self.reservation_violated(self.get_current_time_sec() + 60*runtime_estimate, partition.name):
                    continue

            # dwang:
            score = 0
            score2 = 0
            #if partition.state == "rtj":
            #    continue

            #print "[rtj_part] check_part ... "
            # let's check the impact on partitions that would become blocked
            #if (partition.state == "idle") or (partition.state == "busy"):
            # if partition.state != "rtj":
            job_temp = self.get_running_job_by_partition(partition.name)
            if job_temp and job_temp.user != 'realtime' or job_temp is None:
                #
                if partition.state == "busy":
                    score += 20
                #
                for p_children_name in partition.children:
                    p_children = self._partitions[p_children_name]
                    job_temp = self.get_running_job_by_partition(p_children_name)
                    if job_temp and job_temp.user == 'realtime':
                    # if (p_children.state == "rtj"): #or (p_children.state == "temp_blocked"):
                        flag_rtj = 1
                        break
                        #score += 50
                if flag_rtj:
                    rtj_depend_count += 1
                    continue
                    ##break
                #
                #

                for p_parents_name in partition.parents:
                    p_parents = self._partitions[p_parents_name]
                    ppt_str = str(p_parents.state)
                    job_temp = self.get_running_job_by_partition(p_parents_name)
                    if job_temp and job_temp.user == 'realtime':
                    # if (p_parents.state == "rtj"): #or (p_parents.state == "temp_blocked") or ('blocked' in ppt_str):
                        flag_rtj = 1
                        break
                if flag_rtj:
                    rtj_depend_count += 1
                    continue
                    ##break
                ##
                for p in partition.parents:
                    pp = self._partitions[p]
                    if self.cached_partitions[p].state == "idle" and self.cached_partitions[p].scheduled:
                        score += 1
                    # dwang:
                    for pchild_name in pp.children:
                        pchild = self._partitions[pchild_name]
                        job_temp = self.get_running_job_by_partition(pchild_name)
                        if job_temp and job_temp.user == 'realtime':
                        # if pchild.state == "rtj":
                            score2 += 1
                # the lower the score, the fewer new partitions will be blocked by this selection
                if score < best_score:
                    best_score = score
                    best_partition = partition

                    best_partition_list[:] = []
                    best_partition_list.append(partition)
                    #
                    # print "[rtj_none_conflict] p_name: ", partition
                    # print "[rtj_none_conflict] p_state: ", partition.state
                    nbjob = self.get_running_job_by_partition(partition.name)
                    # print "[rtj_none_conflict] p job: ", nbjob
                    #
                #record equavalent partitions that have same best score
                elif score == best_score:
                    best_partition_list.append(partition)
                    #
                    # print "[rtj_none_conflict] p_name: ", partition
                    # print "[rtj_none_conflict] p_state: ", partition.state
            else:
                rtj_conflict_count += 1
                # print "[rtj_conflict] p_name: ", partition
                # print "[rtj_conflict] p_state: ", partition.state
                nbjob = self.get_running_job_by_partition(partition.name)
                # print "[rtj_conflict] p job: ", nbjob

        #
        # print "rtj_conflict_count: ", rtj_conflict_count
        # print "rtj_depend_count: ", rtj_depend_count
        # print "len(best_part_list): ", len(best_partition_list)
        #
        # dwang:
        # print "[_bgqsim_ckpH] best_score: ", best_score
        # print "[_bgqsim_ckpH] self.walltime_aware_cons: ", self.walltime_aware_cons
        # print "[_bgqsim_ckpH] best_partition: ", best_partition
        # _0322:
        # __ sort_tobe_batch_partition:
        # if len(best_partition_list)>1:
            # if best_partition.state == "busy":
                # print "_BUSY_partition_select ..."
        # _0322
        # dwang

        ''' 
        # __dw_1222: 
        ## if self.walltime_aware_cons and len(best_partition_list) > 1: 
        if len(best_partition_list) > 1:
        # __dw_1222
            print "best_partition_list=", [part.name for part in best_partition_list]
            print " ___ start best_partition_list[] ... "
            #walltime aware job allocation (conservative)
            least_diff = MAXINT
            for partition in best_partition_list:
                # __dw_1222: 
                print "[_bqsim_ckpH] _istop_1_1 ... "
                # __dw_1222 
                nbpart = self.get_neighbor_by_partition(partition.name)
		print "[_bqsim_ckpH] _istop_1_1_2 ... " 
                if nbpart:
                    # __dw_1222: 
                    print "[_bqsim_ckpH] _istop_1_2 ... "
                    #
                    # for pp in self.cached_partitions.itervalues(): 
                    for pp in self._partitions.itervalues(): 
                        print "  [_bqsim_ckpH] _pp: ", pp
                        print "  [_bqsim_ckpH] _pp_name: ", pp.name   
                    # 
		    print "[_bqsim_ckpH] _istop_1_2, nbpart: ", nbpart  
		    # __dw_1222 
                    # nbpart_name = str(nbpart) 
		    nbpart_name = "'%s'" % nbpart  
                    print "[_bqsim_ckpH] _istop_1_2, nbpart_name: ", nbpart_name 
                    # __dw_1222 
                    print "[_bqsim_ckpH] _istop_1_2_1 ... ... "
                    ## nb_partition = self._partitions[nbpart_name]
		    ## nb_partition = self.cached_partitions[nbpart_name]
		    ## 
                    print "[_bqsim_ckpH] _istop_1_2_2 ... ... "
                    ## nbjob = self.get_running_job_by_partition(nb_partition.name) 
		    nbjob = self.get_running_job_by_partition(nbpart_name)  
		    ## nbjob = self.get_running_job_by_partition(nbpart)  
		    print "[_bqsim_ckpH] _istop_1_2_3 ... ... " 
                    if nbjob:
                        # __dw_1222: 
                        print "[_bqsim_ckpH] _istop_1_3 ... "
                        # __dw_1222 
                        nbjob_remain_length = nbjob.starttime + 60*float(nbjob.walltime) - self.get_current_time_sec()
                        # __dw_1222: 
                        diff = abs(60*float(walltime) - nbjob_remain_length) # __system_utilization
                        diff_bcutoff = nbjob_remain_length                      # __batch_job_cutoff_time 
                                                                                # __batch_job_xfactor (incentive)
                                                                                # __realtime_job_xfactor 
                        #
                        print "[_bqsim_ckpH] diff : ", diff 
                        print "[_bqsim_ckpH] diff_bcutoff : ", diff_bcutoff
                        # __dw_1222 
                        if diff < least_diff:
                            least_diff = diff
                            best_partition = partition
                        msg = "jobid=%s, partition=%s, neighbor part=%s, neighbor job=%s, diff=%s" % (jobid, partition.name, nbpart, nbjob.jobid, diff)
                        #self.dbglog.LogMessage(msg) 
                        print(msg) 
                    else: 
                    	print "[_bqsim_ckpH] _istop_1_4 ... " 
                    	print "[_bqsim_ckpH] _istop_1_4, best_partition: ", best_partition 
            # 
            msg = "------------job %s allocated to best_partition %s-------------" % (jobid,  best_partition.name)
            #self.dbglog.LogMessage(msg)
            print(msg) 
        '''

        # __dw_1222:
        ## if self.walltime_aware_cons and len(best_partition_list) > 1:
        if len(best_partition_list) > 1:
        # __dw_1222
            # print " ___ start best_partition_list[] ... "
            # print " ___ start best_partition_list[] 222 ... "
            # print "best_partition_list=", [part.name for part in best_partition_list]
            #walltime aware job allocation (conservative)
            least_diff = MAXINT
            most_diff = 0
            for partition in best_partition_list:
                # __dw_1222:
                # print "[_bqsim_ckpH] _istop_1_1 ... "
                # __dw_1222
                ## nbpart = self.get_neighbor_by_partition(partition.name)
                nbpart = partition
                #
                # print "[_bqsim_ckpH] _istop_1_1_2 ... "
                if nbpart:
                    # __dw_1222:
                    # print "[_bqsim_ckpH] _istop_1_2 ... "
                    #
                    # for pp in self.cached_partitions.itervalues():
                    # for pp in self._partitions.itervalues():
                        # print "  [_bqsim_ckpH] _pp: ", pp
                        # print "  [_bqsim_ckpH] _pp_name: ", pp.name
                    #
                    # print "[_bqsim_ckpH] _istop_1_2, nbpart: ", nbpart
                    # __dw_1222
                    # nbpart_name = str(nbpart)
                    nbpart_name = "'%s'" % nbpart
                    # print "[_bqsim_ckpH] _istop_1_2, nbpart_name: ", nbpart_name
                    # print "[_bqsim_ckpH] _istop_1_3, nbpart.name: ", nbpart.name
                    # __dw_1222
                    # print "[_bqsim_ckpH] _istop_1_2_1 ... ... "
                    ## nb_partition = self._partitions[nbpart_name]
            ## nb_partition = self.cached_partitions[nbpart_name]
            ##
                    # print "[_bqsim_ckpH] _istop_1_2_2 ... ... "
                    ## nbjob = self.get_running_job_by_partition(nb_partition.name)
                    ## nbjob = self.get_running_job_by_partition(nbpart)
                    ### nbjob = self.get_running_job_by_partition(nbpart_name)
                    nbjob = self.get_running_job_by_partition(nbpart.name)
                    #
		    # print "[_bqsim_ckpH] _istop_1_2_3 ... ... "
                    if nbjob:
                        # __dw_1222:
                        # print "[_bqsim_ckpH] _istop_1_3 ... "
                        # __dw_1222
                        nbjob_remain_length = nbjob.starttime + 60*float(nbjob.walltime) - self.get_current_time_sec()
                        # __dw_1222:
                        diff = abs(60*float(walltime) - nbjob_remain_length) # __system_utilization
                        diff_bcutoff = nbjob_remain_length                      # __batch_job_cutoff_time
			diff_bcutoff_ratio = nbjob_remain_length / float(walltime)
										# __batch_job_cutoff_time_ratio
                                                                                # __batch_job_xfactor (incentive)
                                                                                # __realtime_job_xfactor
                        #
                        # print "[_bqsim_ckpH] diff : ", diff
                        # print "[_bqsim_ckpH] diff_bcutoff : ", diff_bcutoff
                        # print "[_bqsim_ckpH] diff_bcutoff_ratio : ", diff_bcutoff_ratio
                        #
                        diff = diff_bcutoff_ratio # diff / diff_bcutoff / diff_bcutoff_ratio
                        # __dw_1222
                        ### if diff < least_diff:
                        ###     least_diff = diff
                        if diff > most_diff:
                            most_diff = diff
                            best_partition = partition
                        msg = "jobid=%s, partition=%s, neighbor part=%s, neighbor job=%s, diff=%s" % (jobid, partition.name, nbpart, nbjob.jobid, diff)
                        #self.dbglog.LogMessage(msg)
                        # print(msg)
                    # else:
                    #     print "[_bqsim_ckpH] _istop_1_4 ... "
                    #     print "[_bqsim_ckpH] _istop_1_4, best_partition: ", best_partition
            #
            msg = "------------job %s allocated to best_partition %s-------------" % (jobid,  best_partition.name)
            #self.dbglog.LogMessage(msg)
            # print(msg)

        # __dw_1222:
        # print "[_bqsim_ckpH] _istop_2 ... ..."
        # __dw_1222
        if best_partition:
            # print "[_bqsim_ckpH] bpart_wcheckp : ", best_partition.name
            # print "[_bqsim_ckpH] bpart_wcheckp STATE: ", best_partition.state
            #
            for p_children_name in best_partition.children:
                p_children = self._partitions[p_children_name]
                # print "[_bqsim_ckpH] bpart_wcheckp_ch : ", p_children.name
                # print "[_bqsim_ckpH] bpart_wcheckp_ch STATE: ", p_children.state
            #partition.state = "rtj" #
            #
            for p_parents_name in best_partition.parents:
                p_parents = self._partitions[p_parents_name]
                # print "[_bqsim_ckpH] bpart_wcheckp_pt : ", p_parents.name
                # print "[_bqsim_ckpH] bpart_wcheckp_pt STATE: ", p_parents.state
            return {jobid: [best_partition.name]}
        # else:
        #     print "[_bqsim_ckpH] best_partition, ELSE ... "
    # dwang


    def _find_job_location_rtjmatch(self, args, drain_partitions=set(), taken_partition=set(), backfilling=False): 
        jobid = args['jobid']
        nodes = args['nodes']
        queue = args['queue']
        utility_score = args['utility_score']
        walltime = args['walltime']
        walltime_p = args['walltime_p']  #*AdjEst*
        forbidden = args.get("forbidden", [])
        required = args.get("required", [])

        best_score = sys.maxint
        best_partition = None

        available_partitions = set()

        requested_location = None
        if args['attrs'].has_key("location"):
            requested_location = args['attrs']['location']

        if required:
            # whittle down the list of required partitions to the ones of the proper size
            # this is a lot like the stuff in _build_locations_cache, but unfortunately,
            # reservation queues aren't assigned like real queues, so that code doesn't find
            # these
            for p_name in required:
                available_partitions.add(self.cached_partitions[p_name])
                available_partitions.update(self.cached_partitions[p_name]._children)

            possible = set()
            for p in available_partitions:
                possible.add(p.size)

            desired_size = 64
            job_nodes = int(nodes)
            for psize in sorted(possible):
                if psize >= job_nodes:
                    desired_size = psize
                    break

            for p in available_partitions.copy():
                if p.size != desired_size:
                    available_partitions.remove(p)
                elif p.name in self._not_functional_set:
                    available_partitions.remove(p)
                elif requested_location and p.name != requested_location:
                    available_partitions.remove(p)
        else:
        # dwang:
        #print "[dw_bgqsim] possible_location: ", self.possible_locations(nodes, queue) 
        # dwang: 
            for p in self.possible_locations(nodes, queue):
                skip = False
                for bad_name in forbidden:
                    if p.name == bad_name or bad_name in p.children or bad_name in p.parents:
                        skip = True
                        break

                if not skip:
                    if (not requested_location) or (p.name == requested_location):
                        available_partitions.add(p)

        available_partitions -= drain_partitions
        now = self.get_current_time()
        available_partitions = list(available_partitions)
        available_partitions.sort(key=lambda d: (d.name))
        best_partition_list = []
        best_partition_score = []
        partition_list = []
        partition_score = []

        # dwang:
        #print "[dw_bgqsim] available_location: ", available_partitions
        # dwang
        rtj_partitions = []
        rtj_list = []
        for partition in available_partitions:
            #print "av_partitions: ", partition.name
            #print "av_partitions_STATE: ", partition.state
            for p_children_name in partition.children:
                p_children = self._partitions[p_children_name]
                #print "ch_av_partitions: ", p_children.name
                #print "ch_av_partitions_STATE: ", p_children.state
                #
                if (p_children.state == "rtj") or (p_children.state == "temp_blocked"):
                    rtj_partitions.append(partition)
                    #print "ch_rtj_partitions: ", p_children.name
                    #print "ch_rtj_partitions_STATE: ", p_children.state
                    break;
            
            for p_parents_name in partition.parents:
                p_parents = self._partitions[p_parents_name]
                #print "ch_av_partitions: ", p_children.name
                #print "ch_av_partitions_STATE: ", p_children.state
                #
                if p_parents.state == "rtj":
                    rtj_partitions.append(partition)
                    #print "ch_rtj_partitions: ", p_children.name
                    #print "ch_rtj_partitions_STATE: ", p_children.state
                    break;
            
        #available_partitions -= rtj_partitions
        #print "avail_partitions: ", available_partitions
        #print "rtj_partitions: ", rtj_partitions
    
        # dwang:
        for partition in available_partitions:
            # if the job needs more time than the partition currently has available, look elsewhere
            #
            if self.predict_backfill:
                runtime_estimate = float(walltime_p)   # *Adj_Est*
            else:
                runtime_estimate = float(walltime)

            if backfilling:
                if 60*runtime_estimate > (partition.backfill_time - now):
                    continue

            if self.reserve_ratio > 0:
                if self.reservation_violated(self.get_current_time_sec() + 60*runtime_estimate, partition.name):
                    continue

            flag_rtj = 0;
            if partition.state == "idle":
                #if partition in rtj_partitions:
                #    break;
                # let's check the impact on partitions that would become blocked
                score = 0
                #
                for p_children_name in partition.children:
                    p_children = self._partitions[p_children_name]
                    job_temp = self.get_running_job_by_partition(p_children_name)
                    if job_temp and job_temp.user == 'realtime':
                    # if (p_children.state == "rtj"): #or (p_children.state == "temp_blocked"):
                        flag_rtj = 1
                        break
                if flag_rtj:
                    continue
                #
                '''
                for p_parents_name in partition.parents:
                    p_parents = self._partitions[p_parents_name]
                    ppt_str = str(p_parents.state)
                    if (p_parents.state == "rtj"): #or (p_parents.state == "temp_blocked") or ('blocked' in ppt_str):
                        flag_rtj = 1
                        break
                if flag_rtj:
                    break
                #
                ''' 
                #
                for p in partition.parents:
                    #if self.cached_partitions[p].state == "idle":
                    if self.cached_partitions[p].state == "idle" and self.cached_partitions[p].scheduled:
                        score += 1
                    '''
                    elif (self.cached_partitions[p].state != "idle") and (self.cached_partitions[p].state != "busy"):
                        score += 5 # possible blocked
                    '''
                # the lower the score, the fewer new partitions will be blocked by this selection
                if score < best_score:
                    best_score = score
                    best_partition = partition

                    best_partition_list[:] = []
                    best_partition_list.append(partition)
                    best_partition_score.append(score)
                #record equavalent partitions that have same best score
                elif score == best_score:
                    best_partition_list.append(partition)
                '''
                partition_list.append(partition)
                partition_score.append(score)

            if partition_list: 
                best_score = partition_score[0]
                for jcount in range(0,len(partition_list)): 
                    if partition_score[jcount] < best_score: 
                        best_score = partition_score[jcount]
                        best_partition = partition_list[jcount]
                '''
        # # dwang:
        # if best_partition_list:
        #     print "[dw_bgqsim] best_partition_list: ", best_partition_list
        #     print "[dw_bgqsim] best_partition_score: ", best_partition_score
        # else:
        #     print "[dw_bgqsim] [] "
        # # dwang
        '''
        if best_partition:
            return {jobid: [best_partition.name]}
        '''
            
        # dwang: ??? 
        if self.walltime_aware_cons and len(best_partition_list) > 1:
            #print "best_partition_list=", [part.name for part in best_partition_list]
            #walltime aware job allocation (conservative)
            least_diff = MAXINT
            for partition in best_partition_list:
                nbpart = self.get_neighbor_by_partition(partition.name)
                if nbpart:
                    nbjob = self.get_running_job_by_partition(nbpart)
                    if nbjob:
                        nbjob_remain_length = nbjob.starttime + 60*float(nbjob.walltime) - self.get_current_time_sec()
                        diff = abs(60*float(walltime) - nbjob_remain_length)
                        # print "[dw_bgqsim] best_partition_diff: ", diff
                        if diff < least_diff:
                            least_diff = diff
                            best_partition = partition
                        msg = "jobid=%s, partition=%s, neighbor part=%s, neighbor job=%s, diff=%s" % (jobid, partition.name, nbpart, nbjob.jobid, diff)
                        #self.dbglog.LogMessage(msg) 
            msg = "------------job %s allocated to best_partition %s-------------" % (jobid,  best_partition.name)
            #self.dbglog.LogMessage(msg) 
            # print "[dw_bgqsim] least_diff: ", least_diff

        if best_partition:
            # print "[dw_bqsim] bpart_rtjmatch : ", best_partition.name
            # print "[dw_bqsim] bpart_rtjmatch STATE: ", best_partition.state
            #
            running_job = self.get_running_job_by_partition(best_partition.name)
            # if running_job:
            #     print "[dw_bqsim] bpart_rtjmatch RUNNING: ", running_job.jobid
            # else:
            #     print "[dw_bqsim] bpart_rtjmatch RUNNING: NONE "
            # #
            # print "[dw_bqsim] bpart_rtjmatch SCHEDULED: ", self.cached_partitions[best_partition.name].scheduled
            #
            # if best_partition in rtj_partitions:
            #     print "[dw_bqsim] bpart_rtjmatch in RTJ_partition ... "
            #
            for p_children_name in best_partition.children:
                p_children = self._partitions[p_children_name]
                # print "[dw_bqsim] bpart_rtjmatch_ch : ", p_children.name
                # print "[dw_bqsim] bpart_rtjmatch_ch STATE: ", p_children.state
                #
                ch_running_job = self.get_running_job_by_partition(best_partition.name)
                # if ch_running_job:
                #     print "[dw_bqsim] bpart_rtjmatch_ch RUNNING: ", ch_running_job.jobid
                # else:
                #     print "[dw_bqsim] bpart_rtjmatch_ch RUNNING: NONE "
                # #
                # print "[dw_bqsim] bpart_rtjmatch_ch SCHEDULED: ", self.cached_partitions[p_children_name].scheduled
            #
            '''
            for p_parents_name in best_partition.parents:
                p_parents = self._partitions[p_parents_name]
                print "[dw_bqsim] bpart_rtjmatch_pt : ", p_parents.name
                print "[dw_bqsim] bpart_rtjmatch_pt STATE: ", p_parents.state
                print "[dw_bqsim] bpart_rtjmatch_pt len(iS): ", len(p_parents.state)
                ppt_str = str(p_parents.state)
                print "[dw_bqsim] bpart_rtjmatch_pt Bstr:", ppt_str
                print "[dw_bqsim] bpart_rtjmatch_pt iS_Bstr: ",'blocked' in ppt_str
                print "[dw_bqsim] bpart_rtjmatch_pt iS_Bstr: ", ppt_str.find("blocked")
                print "[dw_bqsim] bpart_rtjmatch_pt iS_B: ", (p_parents.state).find("blocked")
            '''
            return {jobid: [best_partition.name]}
        
        

    def find_job_location0(self, arg_list, end_times):
        best_partition_dict = {}
        if self.bridge_in_error:
            return {}
        # build the cached_partitions structure first  (in simulation conducted in init_part()
#        self._build_locations_cache()

        # first, figure out backfilling cutoffs per partition (which we'll also use for picking which partition to drain)
        job_end_times = {}
        for item in end_times:
            job_end_times[item[0][0]] = item[1]

        now = self.get_current_time()
        for p in self.cached_partitions.itervalues():
            if p.state == "idle":
                p.backfill_time = now
            else:
                p.backfill_time = now + 5*60
            p.draining = False

        for p in self.cached_partitions.itervalues():
            if p.name in job_end_times:
                if job_end_times[p.name] > p.backfill_time:
                    p.backfill_time = job_end_times[p.name]
                for parent_name in p.parents:
                    parent_partition = self.cached_partitions[parent_name]
                    if p.backfill_time > parent_partition.backfill_time:
                        parent_partition.backfill_time = p.backfill_time

        for p in self.cached_partitions.itervalues():
            if p.backfill_time == now:
                continue

            for child_name in p.children:
                child_partition = self.cached_partitions[child_name]
                if child_partition.backfill_time == now or child_partition.backfill_time > p.backfill_time:
                    child_partition.backfill_time = p.backfill_time

        # first time through, try for starting jobs based on utility scores
        drain_partitions = set()

        pos = 0
        for job in arg_list:
            pos += 1
            partition_name = self._find_job_location(job, drain_partitions)
            if partition_name:
                best_partition_dict.update(partition_name)
                #logging the scheduled job's postion in the queue, used for measuring fairness,
                #e.g. pos=1 means job scheduled from the head of the queue
                dbgmsg = "%s;S;%s;%s;%s;%s" % (self.get_current_time_date(), job['jobid'], pos, job.get('utility_score', -1), partition_name)
                self.dbglog.LogMessage(dbgmsg)
                break

            location = self._find_drain_partition(job)
            if location is not None:
                for p_name in location.parents:
                    drain_partitions.add(self.cached_partitions[p_name])
                for p_name in location.children:
                    drain_partitions.add(self.cached_partitions[p_name])
                    self.cached_partitions[p_name].draining = True
                drain_partitions.add(location)
                #self.logger.info("job %s is draining %s" % (winning_job['jobid'], location.name))
                location.draining = True

        # the next time through, try to backfill, but only if we couldn't find anything to start
        if not best_partition_dict:
            #
            # arg_list.sorlst(self._walltimecmp)
            #
            #for best-fit backfilling (large job first and then longer job first)
            if not self.backfill == "ff":
                if self.backfill == "bf":
                    arg_list = sorted(arg_list, key=lambda d: (-int(d['nodes'])*float(d['walltime'])))
                elif self.backfill == "sjfb":
                    print "+++[1136] ---------> sjfb +++"
                    arg_list = sorted(arg_list, key=lambda d:float(d['walltime']))

            for args in arg_list:
                partition_name = self._find_job_location(args, backfilling=True)
                if partition_name:
                    self.logger.info("backfilling job %s" % args['jobid'])
                    best_partition_dict.update(partition_name)
                    #logging the starting postion in the queue, 0 means backfilled
                    dbgmsg = "%s;S;%s;0;%s;%s" % (self.get_current_time_date(), args['jobid'], args.get('utility_score', -1), partition_name)
                    self.dbglog.LogMessage(dbgmsg)
                    break

#        print "best_partition_dict", best_partition_dict

        return best_partition_dict
    find_job_location0 = locking(exposed(find_job_location0))

    def permute(inputData, outputSoFar):
        for a in inputData:
            if a not in outputSoFar:
                if len(outputSoFar) == len(inputData) - 1:
                    yield outputSoFar + [a]
                else:
                    for b in permute(inputData, outputSoFar + [a]): # --- Recursion
                        yield b

    # dwang:
    #def find_job_location(self, arg_list, end_times):
    #def find_job_location(self, arg_list, end_times,simu_name,simu_tid):
    def find_job_location(self, arg_list, end_times, fp_backf):
    # dwang
        best_partition_dict = {}
        minimum_makespan = 100000

        if self.bridge_in_error:
            return {}

        # build the cached_partitions structure first  (in simulation conducted in init_part()
#        self._build_locations_cache()

        # first, figure out backfilling cutoffs per partition (which we'll also use for picking which partition to drain)
        job_end_times = {}
        for item in end_times:
            job_end_times[item[0][0]] = item[1]

        now = self.get_current_time()
        for p in self.cached_partitions.itervalues():
            if p.state == "idle":
                p.backfill_time = now
            else:
                p.backfill_time = now + 5*60
            p.draining = False

        for p in self.cached_partitions.itervalues():
            if p.name in job_end_times:
                if job_end_times[p.name] > p.backfill_time:
                    p.backfill_time = job_end_times[p.name]
                for parent_name in p.parents:
                    parent_partition = self.cached_partitions[parent_name]
                    if p.backfill_time > parent_partition.backfill_time:
                        parent_partition.backfill_time = p.backfill_time

        for p in self.cached_partitions.itervalues():
            if p.backfill_time == now:
                continue

            for child_name in p.children:
                child_partition = self.cached_partitions[child_name]
                if child_partition.backfill_time == now or child_partition.backfill_time > p.backfill_time:
                    child_partition.backfill_time = p.backfill_time

        def permute(inputData, outputSoFar):
            for a in inputData:
                if a not in outputSoFar:
                    if len(outputSoFar) == len(inputData) - 1:
                        yield outputSoFar + [a]
                    else:
                        for b in permute(inputData, outputSoFar + [a]): # --- Recursion
                            yield b

        def permute_first_N(inputData, window_size):
            if window_size == 1:
                yield inputData
            else:
                list1 = inputData[0:window_size]
                list2 = inputData[window_size:]
                for i in permute(list1, []):
                    list3 = i + list2
                    yield list3


        ###print self.get_current_time_date()
        #print [job.jobid for job in self.queuing_jobs]

        ###print "length of arg_list=", len(arg_list)
        #print arg_list
        permutes = []
        for i in permute_first_N(arg_list, self.window_size):
            permutes.append(i)

        ###for perm in permutes:
            ###print [item.get('jobid') for item in perm],
        ###print ""

        perm_count = 1
        for perm in permutes:
           # print "round=", perm_count
            #print "perm=", perm
            perm_count += 1
            # first time through, try for starting jobs based on utility scores
            drain_partitions = set()

            pos = 0
            last_end = 0
            
            jl_matches = {}
            for job in perm:
            ###    print "try jobid %s" % job.get('jobid')
                pos += 1
                job_partition_match = self._find_job_location(job, drain_partitions)
                if job_partition_match:  # partition found
               ###     print "found a match=", job_partition_match
                    jl_matches.update(job_partition_match)
                    #logging the scheduled job's postion in the queue, used for measuring fairness,
                    #e.g. pos=1 means job scheduled from the head of the queue
                    dbgmsg = "%s;S;%s;%s;%s;%s" % (self.get_current_time_date(), job['jobid'], pos, job.get('utility_score', -1), job_partition_match)
                    self.dbglog.LogMessage(dbgmsg)

                    #pre-allocate job
                    for partnames in job_partition_match.values():
                        for partname in partnames:
                            self.allocate_partition(partname)
                    #break

                    #calculate makespan this job contribute
                    if pos <= self.window_size:
                        expected_end = int(job.get("walltime"))*60
                        if expected_end > last_end:
                            last_end = expected_end
              ###          print "expected_end=", expected_end

                else:  # partition not found, start draining
                    location = self._find_drain_partition(job)
                    if location is not None:
                       # print "match not found, draing location %s for job %s " % (location, job.get("jobid"))
                        for p_name in location.parents:
                            drain_partitions.add(self.cached_partitions[p_name])
                        for p_name in location.children:
                            drain_partitions.add(self.cached_partitions[p_name])
                            self.cached_partitions[p_name].draining = True
                        drain_partitions.add(location)
                        #self.logger.info("job %s is draining %s" % (winning_job['jobid'], location.name))
                        location.draining = True

                        expected_start = location.backfill_time - self.get_current_time_sec()
                 ###       print "expected_start=", expected_start
                        expected_end = expected_start + int(job.get("walltime"))*60
                 ###       print "expected_end=", expected_end
                        if expected_end > last_end:
                            last_end = expected_end

            ###print "matches for round ", perm_count-1, jl_matches
            ###print "last_end=%s, min makespan=%s" % (last_end, minimum_makespan)

            #deallocate in order to reallocate for a next round
            for partnames in jl_matches.values():
                for partname in partnames:
                    self.deallocate_partition(partname)

            if last_end < minimum_makespan:
                minimum_makespan = last_end
                best_partition_dict = jl_matches

###            print "best_partition_dict=", best_partition_dict
   ###         if len(best_partition_dict.keys()) > 1:
      ###          print "****************"
      
        # the next time through, try to backfill, but only if we couldn't find anything to start
        if not best_partition_dict:
            # print "start backfill-----"
            # arg_list.sorlst(self._walltimecmp)

            #for best-fit backfilling (large job first and then longer job first)
            if not self.backfill == "ff":
                if self.backfill == "bf":
                    arg_list = sorted(arg_list, key=lambda d: (-int(d['nodes'])*float(d['walltime'])))
                elif self.backfill == "sjfb":
                    #print "+++[1329] ---------> sjfb +++"
                    arg_list = sorted(arg_list, key=lambda d:float(d['walltime']))

            # --> logRec
            #
            #backf_fname = './_rec/backf_' + simu_name + '_' + str(simu_tid) + '.txt'
            #fp_backf = open( backf_fname,'w+' )
            #
            #fp_backf.write('--> bf ... \n')
            #
            for args in arg_list:
                partition_name = self._find_job_location(args, backfilling=True)
                if partition_name:
                    self.logger.info("backfilling job %s" % args['jobid'])
                    #print "+++[bf] ---------> backfilling job_:", args['jobid']
                    #print "        ---------> backfilling score_:", args.get('utility_score', -1)
                    #print "        ---------> backfilling partation_:", partition_name
                    #
                    #fp_backf.write('%s: %d, %s \n' %( args['jobid'], ise, self.get_utilization_rate_SE(ise, 5*60) ))
                    #fp_backf.write('%s: %s, %s \n' %( args['jobid'], args.get('utility_score', -1), partition_name ))
                    # <-- logRec
                    best_partition_dict.update(partition_name)
                    #logging the starting postion in the queue, 0 means backfilled
           #         dbgmsg = "%s;S;%s;0;%s;%s" % (self.get_current_time_date(), args['jobid'], args.get('utility_score', -1), partition_name)
            #        self.dbglog.LogMessage(dbgmsg)
                    break
         

	# dwang: 
     #    print "[dw_bqsim] best_partition_dict", best_partition_dict
	# dwang  

        return best_partition_dict
    find_job_location = locking(exposed(find_job_location))



    def find_job_location_hpq_resv(self, arg_list, end_times, arg_list_rtj, fp_backf):
        #  
    # dwang
        best_partition_dict = {}
        minimum_makespan = 100000

        if self.bridge_in_error:
            return {}

        # build the cached_partitions structure first  (in simulation conducted in init_part()
#        self._build_locations_cache()

        # dwang: highpQ_resv 
        ## 
        # -> reserve partition for RTJ_resv
        # print "[find_job_location_hpq_resv] rtj_size: ", len(arg_list_rtj)
        # print "[find_job_location_hpq_resv] rest_size: ", len(arg_list)
        global rtj_resv_part_dict
        global hpResv_drain_partitions 
        # 
        for args in arg_list_rtj: 
            flag_rtj_exist = 0
            # print " [fjl_hpq_resv] args: ", args.get('jobid')
            if len(rtj_resv_part_dict) <1:
                #  
                # print " [fjl_hpq_resv] _find_job_location_resv() start ... "
                hpResvP_elem, hpResvP_dict_elem = self._find_job_location_resv(args, backfilling=True) 
                # print " [fjl_hpq_resv] avail_part: ", hpResvP_dict_elem
                # rtj_resv_part_dict = dict(); hpResv_drain_partitions = set()
                hpResv_drain_partitions.add(hpResvP_elem)
                rtj_resv_part_dict.update(hpResvP_dict_elem)
                # 
            else: 
                # print "[find_job_location_hpq_resv] ELSE ... "
                # print "[find_job_location_hpq_resv].keys: ", rtj_resv_part_dict.keys()
                if args.get('jobid') in rtj_resv_part_dict.keys(): 
                    flag_rtj_exist = 1
                    # print "[find_job_location_hpq_resv] RTJ_RESV exist ..."
                    #
                #  if flag_rtj_exist == 0: 
                else: 
                    # print " [fjl_hpq_resv] _find_job_location_resv() start ... "
                    hpResvP_elem, hpResvP_dict_elem = self._find_job_location_resv(args, backfilling=True)
                    # print " [fjl_hpq_resv] avail_part: ", hpResvP_dict_elem
                    # rtj_resv_part_dict = dict(); hpResv_drain_partitions = set()
                    hpResv_drain_partitions.add(hpResvP_elem)
                    rtj_resv_part_dict.update(hpResvP_dict_elem)
                # 
        # print " [fjl_hpq_resv] hpResv_drain_partitions: ", hpResv_drain_partitions
        # print " [fjl_hpq_resv] rtj_resv_part_dict: ", rtj_resv_part_dict
        # ... 
        # 
        # -> update cutoff times for partitions with RTJ_resv end_times 
        # ... 
        # dwang 

        # first, figure out backfilling cutoffs per partition (which we'll also use for picking which partition to drain)
        job_end_times = {}
        for item in end_times:
            job_end_times[item[0][0]] = item[1]

        ## 
        now = self.get_current_time()
        for p in self.cached_partitions.itervalues():
            if p.state == "idle":
                p.backfill_time = now
            else:
                p.backfill_time = now + 5*60
            p.draining = False

        for p in self.cached_partitions.itervalues():
            if p.name in job_end_times:
                if job_end_times[p.name] > p.backfill_time:
                    p.backfill_time = job_end_times[p.name]
                for parent_name in p.parents:
                    parent_partition = self.cached_partitions[parent_name]
                    if p.backfill_time > parent_partition.backfill_time:
                        parent_partition.backfill_time = p.backfill_time

        for p in self.cached_partitions.itervalues():
            if p.backfill_time == now:
                continue

            for child_name in p.children:
                child_partition = self.cached_partitions[child_name]
                if child_partition.backfill_time == now or child_partition.backfill_time > p.backfill_time:
                    child_partition.backfill_time = p.backfill_time 
        ## 

        def permute(inputData, outputSoFar):
            for a in inputData:
                if a not in outputSoFar:
                    if len(outputSoFar) == len(inputData) - 1:
                        yield outputSoFar + [a]
                    else:
                        for b in permute(inputData, outputSoFar + [a]): # --- Recursion
                            yield b

        def permute_first_N(inputData, window_size):
            if window_size == 1:
                yield inputData
            else:
                list1 = inputData[0:window_size]
                list2 = inputData[window_size:]
                for i in permute(list1, []):
                    list3 = i + list2
                    yield list3

        # 
        permutes = []
        for i in permute_first_N(arg_list, self.window_size):
            permutes.append(i) 

        perm_count = 1
        for perm in permutes: 
            perm_count += 1
            # first time through, try for starting jobs based on utility scores
            drain_partitions = set()

            pos = 0
            last_end = 0
            
            jl_matches = {}
            for job in perm:
            ###    print "try jobid %s" % job.get('jobid')
                pos += 1
                job_partition_match = self._find_job_location(job, drain_partitions)
                if job_partition_match:  # partition found
               ###     print "found a match=", job_partition_match
                    jl_matches.update(job_partition_match)
                    #logging the scheduled job's postion in the queue, used for measuring fairness,
                    #e.g. pos=1 means job scheduled from the head of the queue
                    dbgmsg = "%s;S;%s;%s;%s;%s" % (self.get_current_time_date(), job['jobid'], pos, job.get('utility_score', -1), job_partition_match)
                    self.dbglog.LogMessage(dbgmsg)

                    #pre-allocate job
                    for partnames in job_partition_match.values():
                        for partname in partnames:
                            self.allocate_partition(partname)
                    #break

                    #calculate makespan this job contribute
                    if pos <= self.window_size:
                        expected_end = int(job.get("walltime"))*60
                        if expected_end > last_end:
                            last_end = expected_end
              ###          print "expected_end=", expected_end

                else:  # partition not found, start draining
                    location = self._find_drain_partition(job)
                    if location is not None:
                       # print "match not found, draing location %s for job %s " % (location, job.get("jobid"))
                        for p_name in location.parents:
                            drain_partitions.add(self.cached_partitions[p_name])
                        for p_name in location.children:
                            drain_partitions.add(self.cached_partitions[p_name])
                            self.cached_partitions[p_name].draining = True
                        drain_partitions.add(location)
                        #self.logger.info("job %s is draining %s" % (winning_job['jobid'], location.name))
                        location.draining = True

                        expected_start = location.backfill_time - self.get_current_time_sec()
                 ###       print "expected_start=", expected_start
                        expected_end = expected_start + int(job.get("walltime"))*60
                 ###       print "expected_end=", expected_end
                        if expected_end > last_end:
                            last_end = expected_end

            ###print "matches for round ", perm_count-1, jl_matches
            ###print "last_end=%s, min makespan=%s" % (last_end, minimum_makespan)

            #deallocate in order to reallocate for a next round
            for partnames in jl_matches.values():
                for partname in partnames:
                    self.deallocate_partition(partname)

            if last_end < minimum_makespan:
                minimum_makespan = last_end
                best_partition_dict = jl_matches

###            print "best_partition_dict=", best_partition_dict
   ###         if len(best_partition_dict.keys()) > 1:
      ###          print "****************"
      
        # the next time through, try to backfill, but only if we couldn't find anything to start
        if not best_partition_dict:
            # print "start backfill-----"
            # arg_list.sorlst(self._walltimecmp)

            #for best-fit backfilling (large job first and then longer job first)
            if not self.backfill == "ff":
                if self.backfill == "bf":
                    arg_list = sorted(arg_list, key=lambda d: (-int(d['nodes'])*float(d['walltime'])))
                elif self.backfill == "sjfb":
                    #print "+++[1329] ---------> sjfb +++"
                    arg_list = sorted(arg_list, key=lambda d:float(d['walltime']))

            # --> logRec
            #
            #backf_fname = './_rec/backf_' + simu_name + '_' + str(simu_tid) + '.txt'
            #fp_backf = open( backf_fname,'w+' )
            #
            #fp_backf.write('--> bf ... \n')
            #

            #
            # print "[dw_bqsim_w_hpq_resv] len(arg_list): ", len(arg_list)
            #
            for args in arg_list:
                ### partition_name = self._find_job_location(args, backfilling=True)
                partition_name = self._find_job_location_wdrain(args, hpResv_drain_partitions, backfilling=True) 
                if partition_name:
                    self.logger.info("backfilling job %s" % args['jobid'])
                    #print "+++[bf] ---------> backfilling job_:", args['jobid']
                    #print "        ---------> backfilling score_:", args.get('utility_score', -1)
                    #print "        ---------> backfilling partation_:", partition_name
                    #
                    #fp_backf.write('%s: %d, %s \n' %( args['jobid'], ise, self.get_utilization_rate_SE(ise, 5*60) ))
                    #fp_backf.write('%s: %s, %s \n' %( args['jobid'], args.get('utility_score', -1), partition_name ))
                    # <-- logRec
                    best_partition_dict.update(partition_name)
                    #logging the starting postion in the queue, 0 means backfilled
           #         dbgmsg = "%s;S;%s;0;%s;%s" % (self.get_current_time_date(), args['jobid'], args.get('utility_score', -1), partition_name)
            #        self.dbglog.LogMessage(dbgmsg)
                    break 
    # dwang: 
    #     print "[dw_bqsim_w_hpq_resv] best_partition_dict: ", best_partition_dict
    # dwang  

        return best_partition_dict
    find_job_location_hpq_resv = locking(exposed(find_job_location_hpq_resv))



    # dwang:
    def find_job_location_wcheckp(self, arg_list, end_times, fp_backf, checkp_thresh): 
    # dwang
        best_partition_dict = {}
        minimum_makespan = 100000

        if self.bridge_in_error:
            return {}

        # build the cached_partitions structure first  (in simulation conducted in init_part()
#        self._build_locations_cache()

        # first, figure out backfilling cutoffs per partition (which we'll also use for picking which partition to drain)
        job_end_times = {}
        for item in end_times:
            job_end_times[item[0][0]] = item[1]

        now = self.get_current_time()

	# dwang: 
        #print "[dw_bqsim] time_now: ", now 
        #print "[dw_bqsim] len(cache_partation): ", len(self.cached_partitions)
        #for p in self.cached_partitions.itervalues():
	    #print("[] pName/size: %s, %d" %(p.name,p.size)) 
	    #print("   parent_size: %d" %len((p.parents)) ) 
	    #print("   children_size: %d" %len((p.children)) ) 
	# dwang  
        for p in self.cached_partitions.itervalues():
            if p.state == "idle":
                p.backfill_time = now
            else:
                p.backfill_time = now + 5*60
            p.draining = False

        for p in self.cached_partitions.itervalues():
            if p.name in job_end_times:
                if job_end_times[p.name] > p.backfill_time:
                    p.backfill_time = job_end_times[p.name]
                for parent_name in p.parents:
                    parent_partition = self.cached_partitions[parent_name]
                    if p.backfill_time > parent_partition.backfill_time:
                        parent_partition.backfill_time = p.backfill_time

        for p in self.cached_partitions.itervalues():
            if p.backfill_time == now:
                continue

            for child_name in p.children:
                child_partition = self.cached_partitions[child_name]
                if child_partition.backfill_time == now or child_partition.backfill_time > p.backfill_time:
                    child_partition.backfill_time = p.backfill_time

        def permute(inputData, outputSoFar):
            for a in inputData:
                if a not in outputSoFar:
                    if len(outputSoFar) == len(inputData) - 1:
                        yield outputSoFar + [a]
                    else:
                        for b in permute(inputData, outputSoFar + [a]): # --- Recursion
                            yield b

        def permute_first_N(inputData, window_size):
            if window_size == 1:
                yield inputData
            else:
                list1 = inputData[0:window_size]
                list2 = inputData[window_size:]
                for i in permute(list1, []):
                    list3 = i + list2
                    yield list3


        ###print self.get_current_time_date()
        #print [job.jobid for job in self.queuing_jobs]

        ###print "length of arg_list=", len(arg_list)
        #print arg_list
        permutes = []
        for i in permute_first_N(arg_list, self.window_size):
            permutes.append(i)

        ###for perm in permutes:
            ###print [item.get('jobid') for item in perm],
        ###print ""

        perm_count = 1
        for perm in permutes:
           # print "round=", perm_count
            #print "perm=", perm
            perm_count += 1
            # first time through, try for starting jobs based on utility scores
            drain_partitions = set()

            pos = 0
            last_end = 0
            jl_matches = {}
	    # dwang: 
            #print "[dw_bqsim] len(drain_partitions):", len(drain_partitions)
	    # dwang 
            for job in perm:
	        # dwang: 
                #print "try jobid %s" % job.get('jobid')
                #print "try nodes %s" % job.get('nodes')
	        # dwang 
                pos += 1
                # print "[dw_bqsim] len(drain_partitions):", len(drain_partitions)
                #job_partition_match = self._find_job_location(job, drain_partitions)
                job_partition_match = self._find_job_location_rtjmatch(job, drain_partitions)
                if job_partition_match:  # partition found
                    # print "[dw_bqsim] -- partition_rtjmatch "
               ###     print "found a match=", job_partition_match
                    jl_matches.update(job_partition_match)
                    #logging the scheduled job's postion in the queue, used for measuring fairness,
                    #e.g. pos=1 means job scheduled from the head of the queue
                    dbgmsg = "%s;S;%s;%s;%s;%s" % (self.get_current_time_date(), job['jobid'], pos, job.get('utility_score', -1), job_partition_match)
                    self.dbglog.LogMessage(dbgmsg)
                    
                    #pre-allocate job
                    for partnames in job_partition_match.values():
                        for partname in partnames:
                            self.allocate_partition(partname)
                    #break
                    
                    #calculate makespan this job contribute
                    if pos <= self.window_size:
                        expected_end = int(job.get("walltime"))*60
                        if expected_end > last_end:
                            last_end = expected_end
              ###          print "expected_end=", expected_end
              
                else:  # partition not found, start draining
                    location = self._find_drain_partition(job)
                    # print "[dw_bqsim] -- partition_drain "
                    if location is not None:
                       # print "match not found, draing location %s for job %s " % (location, job.get("jobid"))
                        for p_name in location.parents:
                            drain_partitions.add(self.cached_partitions[p_name])
                        for p_name in location.children:
                            drain_partitions.add(self.cached_partitions[p_name])
                            self.cached_partitions[p_name].draining = True
                        drain_partitions.add(location)
                        #self.logger.info("job %s is draining %s" % (winning_job['jobid'], location.name))
                        location.draining = True

                        expected_start = location.backfill_time - self.get_current_time_sec()
                 ###       print "expected_start=", expected_start
                        expected_end = expected_start + int(job.get("walltime"))*60
                 ###       print "expected_end=", expected_end
                        if expected_end > last_end:
                            last_end = expected_end
                 
            ###print "matches for round ", perm_count-1, jl_matches
            ###print "last_end=%s, min makespan=%s" % (last_end, minimum_makespan)


            #deallocate in order to reallocate for a next round
            for partnames in jl_matches.values():
                for partname in partnames:
                    self.deallocate_partition(partname)
            
            if last_end < minimum_makespan:
                minimum_makespan = last_end
                best_partition_dict = jl_matches

###            print "best_partition_dict=", best_partition_dict
   ###         if len(best_partition_dict.keys()) > 1:
      ###          print "****************"


        # the next time through, try to backfill, but only if we couldn't find anything to start
        if not best_partition_dict:
            #print "start backfill-----"
            # arg_list.sorlst(self._walltimecmp)

            #for best-fit backfilling (large job first and then longer job first)
            if not self.backfill == "ff":
                if self.backfill == "bf":
                    arg_list = sorted(arg_list, key=lambda d: (-int(d['nodes'])*float(d['walltime'])))
                elif self.backfill == "sjfb":
                    #print "+++[1329] ---------> sjfb +++"
                    arg_list = sorted(arg_list, key=lambda d:float(d['walltime']))

            # --> logRec
            #
            #backf_fname = './_rec/backf_' + simu_name + '_' + str(simu_tid) + '.txt'
            #fp_backf = open( backf_fname,'w+' )
            #
            #fp_backf.write('--> bf ... \n')
            #
            # print "[dw_bqsim] -- partition_checkpt "
            for args in arg_list:
                #partition_name = self._find_job_location(args, backfilling=True)
                partition_name = self._find_job_location_wcheckp(args)
                if partition_name:
                    self.logger.info("backfilling job %s" % args['jobid'])
                    #print "+++[bf] ---------> backfilling job_:", args['jobid']
                    #print "        ---------> backfilling score_:", args.get('utility_score', -1)
                    #print "        ---------> backfilling partation_:", partition_name
                    #
                    #fp_backf.write('%s: %d, %s \n' %( args['jobid'], ise, self.get_utilization_rate_SE(ise, 5*60) ))
                    #fp_backf.write('%s: %s, %s \n' %( args['jobid'], args.get('utility_score', -1), partition_name ))
                    # <-- logRec
                    best_partition_dict.update(partition_name)
                    #logging the starting postion in the queue, 0 means backfilled
           #         dbgmsg = "%s;S;%s;0;%s;%s" % (self.get_current_time_date(), args['jobid'], args.get('utility_score', -1), partition_name)
            #        self.dbglog.LogMessage(dbgmsg)
                    break

	# dwang: 
        #print "[dw_bqsim] best_partition_dict_wcheckpt", best_partition_dict
	# dwang  

        return best_partition_dict
    find_job_location_wcheckp = locking(exposed(find_job_location_wcheckp))

    # samnickolay:
    def find_job_location_wcheckp_sam_v1(self, arg_list, end_times, fp_backf, checkp_thresh, job_length_type,
                                         checkp_t_internval=None, checkp_overhead_percent=None):
        # dwang
        best_partition_dict = {}
        minimum_makespan = 100000

        if self.bridge_in_error:
            return {}

            # build the cached_partitions structure first  (in simulation conducted in init_part()
            #        self._build_locations_cache()

        # first, figure out backfilling cutoffs per partition (which we'll also use for picking which partition to drain)
        job_end_times = {}
        for item in end_times:
            job_end_times[item[0][0]] = item[1]

        now = self.get_current_time()

        # dwang:
        # print "[dw_bqsim] time_now: ", now
        # print "[dw_bqsim] len(cache_partation): ", len(self.cached_partitions)
        # for p in self.cached_partitions.itervalues():
        # print("[] pName/size: %s, %d" %(p.name,p.size))
        # print("   parent_size: %d" %len((p.parents)) )
        # print("   children_size: %d" %len((p.children)) )
        # dwang
        for p in self.cached_partitions.itervalues():
            if p.state == "idle":
                p.backfill_time = now
            else:
                p.backfill_time = now + 5 * 60
            p.draining = False

        for p in self.cached_partitions.itervalues():
            if p.name in job_end_times:
                if job_end_times[p.name] > p.backfill_time:
                    p.backfill_time = job_end_times[p.name]
                for parent_name in p.parents:
                    parent_partition = self.cached_partitions[parent_name]
                    if p.backfill_time > parent_partition.backfill_time:
                        parent_partition.backfill_time = p.backfill_time

        for p in self.cached_partitions.itervalues():
            if p.backfill_time == now:
                continue

            for child_name in p.children:
                child_partition = self.cached_partitions[child_name]
                if child_partition.backfill_time == now or child_partition.backfill_time > p.backfill_time:
                    child_partition.backfill_time = p.backfill_time

        def permute(inputData, outputSoFar):
            for a in inputData:
                if a not in outputSoFar:
                    if len(outputSoFar) == len(inputData) - 1:
                        yield outputSoFar + [a]
                    else:
                        for b in permute(inputData, outputSoFar + [a]):  # --- Recursion
                            yield b

        def permute_first_N(inputData, window_size):
            if window_size == 1:
                yield inputData
            else:
                list1 = inputData[0:window_size]
                list2 = inputData[window_size:]
                for i in permute(list1, []):
                    list3 = i + list2
                    yield list3

        ###print self.get_current_time_date()
        # print [job.jobid for job in self.queuing_jobs]

        ###print "length of arg_list=", len(arg_list)
        # print arg_list
        permutes = []
        for i in permute_first_N(arg_list, self.window_size):
            permutes.append(i)

            ###for perm in permutes:
            ###print [item.get('jobid') for item in perm],
        ###print ""

        perm_count = 1
        for perm in permutes:
            # print "round=", perm_count
            # print "perm=", perm
            perm_count += 1
            # first time through, try for starting jobs based on utility scores
            drain_partitions = set()

            pos = 0
            last_end = 0
            jl_matches = {}
            # dwang:
            # print "[dw_bqsim] len(drain_partitions):", len(drain_partitions)
            # dwang
            for job in perm:
                # dwang:
                # print "try jobid %s" % job.get('jobid')
                # print "try nodes %s" % job.get('nodes')
                # dwang
                pos += 1
                # print "[dw_bqsim] len(drain_partitions):", len(drain_partitions)
                # job_partition_match = self._find_job_location(job, drain_partitions)
                job_partition_match = self._find_job_location_rtjmatch(job, drain_partitions)
                if job_partition_match:  # partition found
                    # print "[dw_bqsim] -- partition_rtjmatch "
                    ###     print "found a match=", job_partition_match
                    jl_matches.update(job_partition_match)
                    # logging the scheduled job's postion in the queue, used for measuring fairness,
                    # e.g. pos=1 means job scheduled from the head of the queue
                    dbgmsg = "%s;S;%s;%s;%s;%s" % (
                    self.get_current_time_date(), job['jobid'], pos, job.get('utility_score', -1), job_partition_match)
                    self.dbglog.LogMessage(dbgmsg)

                    # pre-allocate job
                    for partnames in job_partition_match.values():
                        for partname in partnames:
                            self.allocate_partition(partname)
                    # break

                    # calculate makespan this job contribute
                    if pos <= self.window_size:
                        expected_end = int(job.get("walltime")) * 60
                        if expected_end > last_end:
                            last_end = expected_end
                            ###          print "expected_end=", expected_end

                else:  # partition not found, start draining
                    location = self._find_drain_partition(job)
                    # print "[dw_bqsim] -- partition_drain "
                    if location is not None:
                        # print "match not found, draing location %s for job %s " % (location, job.get("jobid"))
                        for p_name in location.parents:
                            drain_partitions.add(self.cached_partitions[p_name])
                        for p_name in location.children:
                            drain_partitions.add(self.cached_partitions[p_name])
                            self.cached_partitions[p_name].draining = True
                        drain_partitions.add(location)
                        # self.logger.info("job %s is draining %s" % (winning_job['jobid'], location.name))
                        location.draining = True

                        expected_start = location.backfill_time - self.get_current_time_sec()
                        ###       print "expected_start=", expected_start
                        expected_end = expected_start + int(job.get("walltime")) * 60
                        ###       print "expected_end=", expected_end
                        if expected_end > last_end:
                            last_end = expected_end

            ###print "matches for round ", perm_count-1, jl_matches
            ###print "last_end=%s, min makespan=%s" % (last_end, minimum_makespan)


            # deallocate in order to reallocate for a next round
            for partnames in jl_matches.values():
                for partname in partnames:
                    self.deallocate_partition(partname)

            if last_end < minimum_makespan:
                minimum_makespan = last_end
                best_partition_dict = jl_matches

                ###            print "best_partition_dict=", best_partition_dict
                ###         if len(best_partition_dict.keys()) > 1:
                ###          print "****************"

        # the next time through, try to backfill, but only if we couldn't find anything to start
        if not best_partition_dict:
            # print "start backfill-----"
            # arg_list.sorlst(self._walltimecmp)

            # for best-fit backfilling (large job first and then longer job first)
            if not self.backfill == "ff":
                if self.backfill == "bf":
                    arg_list = sorted(arg_list, key=lambda d: (-int(d['nodes']) * float(d['walltime'])))
                elif self.backfill == "sjfb":
                    # print "+++[1329] ---------> sjfb +++"
                    arg_list = sorted(arg_list, key=lambda d: float(d['walltime']))

            # --> logRec
            #
            # backf_fname = './_rec/backf_' + simu_name + '_' + str(simu_tid) + '.txt'
            # fp_backf = open( backf_fname,'w+' )
            #
            # fp_backf.write('--> bf ... \n')
            #
            # print "[dw_bqsim] -- partition_checkpt "
            for args in arg_list:
                # partition_name = self._find_job_location(args, backfilling=True)
                partition_name = self._find_job_location_wcheckp_sam_v1(args, job_length_type=job_length_type,
                                                                        checkp_t_internval=checkp_t_internval,
                                                                        checkp_overhead_percent=checkp_overhead_percent)
                if partition_name:
                    self.logger.info("backfilling job %s" % args['jobid'])
                    # print "+++[bf] ---------> backfilling job_:", args['jobid']
                    # print "        ---------> backfilling score_:", args.get('utility_score', -1)
                    # print "        ---------> backfilling partation_:", partition_name
                    #
                    # fp_backf.write('%s: %d, %s \n' %( args['jobid'], ise, self.get_utilization_rate_SE(ise, 5*60) ))
                    # fp_backf.write('%s: %s, %s \n' %( args['jobid'], args.get('utility_score', -1), partition_name ))
                    # <-- logRec
                    best_partition_dict.update(partition_name)
                    # logging the starting postion in the queue, 0 means backfilled
                    #         dbgmsg = "%s;S;%s;0;%s;%s" % (self.get_current_time_date(), args['jobid'], args.get('utility_score', -1), partition_name)
                    #        self.dbglog.LogMessage(dbgmsg)
                    break

                    # dwang:
                    # print "[dw_bqsim] best_partition_dict_wcheckpt", best_partition_dict
                    # dwang

        return best_partition_dict

    find_job_location_wcheckp_sam_v1 = locking(exposed(find_job_location_wcheckp_sam_v1))



    # dwang:
    def find_job_location_wcheckpH( self, arg_list, end_times, fp_backf, checkp_thresh, checkp_heur_opt ): 
    # dwang
        # __dw_1222:
        # print "find_job_location_wcheckpH() ... "
        # __dw_1222
        best_partition_dict = {}
        minimum_makespan = 100000

        if self.bridge_in_error:
            return {}

        # build the cached_partitions structure first  (in simulation conducted in init_part()
#        self._build_locations_cache()

        # first, figure out backfilling cutoffs per partition (which we'll also use for picking which partition to drain)
        job_end_times = {}
        for item in end_times:
            job_end_times[item[0][0]] = item[1]

        now = self.get_current_time()

    # dwang: 
        #print "[dw_bqsim] time_now: ", now 
        #print "[dw_bqsim] len(cache_partation): ", len(self.cached_partitions)
        #for p in self.cached_partitions.itervalues():
        #print("[] pName/size: %s, %d" %(p.name,p.size)) 
        #print("   parent_size: %d" %len((p.parents)) ) 
        #print("   children_size: %d" %len((p.children)) ) 
    # dwang  
        for p in self.cached_partitions.itervalues():
            if p.state == "idle":
                p.backfill_time = now
            else:
                p.backfill_time = now + 5*60
            p.draining = False

        for p in self.cached_partitions.itervalues():
            if p.name in job_end_times:
                if job_end_times[p.name] > p.backfill_time:
                    p.backfill_time = job_end_times[p.name]
                for parent_name in p.parents:
                    parent_partition = self.cached_partitions[parent_name]
                    if p.backfill_time > parent_partition.backfill_time:
                        parent_partition.backfill_time = p.backfill_time

        for p in self.cached_partitions.itervalues():
            if p.backfill_time == now:
                continue

            for child_name in p.children:
                child_partition = self.cached_partitions[child_name]
                if child_partition.backfill_time == now or child_partition.backfill_time > p.backfill_time:
                    child_partition.backfill_time = p.backfill_time

        def permute(inputData, outputSoFar):
            for a in inputData:
                if a not in outputSoFar:
                    if len(outputSoFar) == len(inputData) - 1:
                        yield outputSoFar + [a]
                    else:
                        for b in permute(inputData, outputSoFar + [a]): # --- Recursion
                            yield b

        def permute_first_N(inputData, window_size):
            if window_size == 1:
                yield inputData
            else:
                list1 = inputData[0:window_size]
                list2 = inputData[window_size:]
                for i in permute(list1, []):
                    list3 = i + list2
                    yield list3


        ###print self.get_current_time_date()
        #print [job.jobid for job in self.queuing_jobs]

        ###print "length of arg_list=", len(arg_list)
        #print arg_list
        permutes = []
        for i in permute_first_N(arg_list, self.window_size):
            permutes.append(i)

        ###for perm in permutes:
            ###print [item.get('jobid') for item in perm],
        ###print ""

        perm_count = 1
        for perm in permutes:
           # print "round=", perm_count
            #print "perm=", perm
            perm_count += 1
            # first time through, try for starting jobs based on utility scores
            drain_partitions = set()

            pos = 0
            last_end = 0
            jl_matches = {}
        # dwang: 
            #print "[dw_bqsim] len(drain_partitions):", len(drain_partitions)
        # dwang 
            for job in perm:
            # dwang: 
                #print "try jobid %s" % job.get('jobid')
                #print "try nodes %s" % job.get('nodes')
            # dwang 
                pos += 1
                # print "[dw_bqsim] len(drain_partitions):", len(drain_partitions)
                #job_partition_match = self._find_job_location(job, drain_partitions)
                job_partition_match = self._find_job_location_rtjmatch(job, drain_partitions)
                if job_partition_match:  # partition found
                    # print "[dw_bqsim] -- partition_rtjmatch "
               ###     print "found a match=", job_partition_match
                    jl_matches.update(job_partition_match)
                    #logging the scheduled job's postion in the queue, used for measuring fairness,
                    #e.g. pos=1 means job scheduled from the head of the queue
                    dbgmsg = "%s;S;%s;%s;%s;%s" % (self.get_current_time_date(), job['jobid'], pos, job.get('utility_score', -1), job_partition_match)
                    self.dbglog.LogMessage(dbgmsg)
                    
                    #pre-allocate job
                    for partnames in job_partition_match.values():
                        for partname in partnames:
                            self.allocate_partition(partname)
                    #break
                    
                    #calculate makespan this job contribute
                    if pos <= self.window_size:
                        expected_end = int(job.get("walltime"))*60
                        if expected_end > last_end:
                            last_end = expected_end
              ###          print "expected_end=", expected_end
              
                else:  # partition not found, start draining
                    location = self._find_drain_partition(job)
                    # print "[dw_bqsim] -- partition_drain "
                    if location is not None:
                       # print "match not found, draing location %s for job %s " % (location, job.get("jobid"))
                        for p_name in location.parents:
                            drain_partitions.add(self.cached_partitions[p_name])
                        for p_name in location.children:
                            drain_partitions.add(self.cached_partitions[p_name])
                            self.cached_partitions[p_name].draining = True
                        drain_partitions.add(location)
                        #self.logger.info("job %s is draining %s" % (winning_job['jobid'], location.name))
                        location.draining = True

                        expected_start = location.backfill_time - self.get_current_time_sec()
                 ###       print "expected_start=", expected_start
                        expected_end = expected_start + int(job.get("walltime"))*60
                 ###       print "expected_end=", expected_end
                        if expected_end > last_end:
                            last_end = expected_end
                 
            ###print "matches for round ", perm_count-1, jl_matches
            ###print "last_end=%s, min makespan=%s" % (last_end, minimum_makespan)


            #deallocate in order to reallocate for a next round
            for partnames in jl_matches.values():
                for partname in partnames:
                    self.deallocate_partition(partname)
            
            if last_end < minimum_makespan:
                minimum_makespan = last_end
                best_partition_dict = jl_matches

###            print "best_partition_dict=", best_partition_dict
   ###         if len(best_partition_dict.keys()) > 1:
      ###          print "****************"


        # the next time through, try to backfill, but only if we couldn't find anything to start
        if not best_partition_dict:
            #print "start backfill-----"
            # arg_list.sorlst(self._walltimecmp)

            #for best-fit backfilling (large job first and then longer job first)
            if not self.backfill == "ff":
                if self.backfill == "bf":
                    arg_list = sorted(arg_list, key=lambda d: (-int(d['nodes'])*float(d['walltime'])))
                elif self.backfill == "sjfb":
                    #print "+++[1329] ---------> sjfb +++"
                    arg_list = sorted(arg_list, key=lambda d:float(d['walltime']))

            # --> logRec
            #
            #backf_fname = './_rec/backf_' + simu_name + '_' + str(simu_tid) + '.txt'
            #fp_backf = open( backf_fname,'w+' )
            #
            #fp_backf.write('--> bf ... \n')
            #
            # print "[dw_bqsim] -- partition_checkpt "
            for args in arg_list:
                #partition_name = self._find_job_location(args, backfilling=True)
                partition_name = self._find_job_location_wcheckpH( args, checkp_heur_opt )
                # __dw_1222:
                # print "[dw_bqsim_ckpH] partition_name: ", partition_name
                # __dw_1222 
                if partition_name:
                    self.logger.info("backfilling job %s" % args['jobid'])
                    #print "+++[bf] ---------> backfilling job_:", args['jobid']
                    #print "        ---------> backfilling score_:", args.get('utility_score', -1)
                    #print "        ---------> backfilling partation_:", partition_name
                    #
                    #fp_backf.write('%s: %d, %s \n' %( args['jobid'], ise, self.get_utilization_rate_SE(ise, 5*60) ))
                    #fp_backf.write('%s: %s, %s \n' %( args['jobid'], args.get('utility_score', -1), partition_name ))
                    # <-- logRec
                    best_partition_dict.update(partition_name)
                    #logging the starting postion in the queue, 0 means backfilled
           #         dbgmsg = "%s;S;%s;0;%s;%s" % (self.get_current_time_date(), args['jobid'], args.get('utility_score', -1), partition_name)
            #        self.dbglog.LogMessage(dbgmsg)
                    break

        # __dw_1222: 
        # print "[dw_bqsim_ckpH] best_partition_dict: ", best_partition_dict
        # __dw_1222  

        return best_partition_dict
    find_job_location_wcheckpH = locking(exposed(find_job_location_wcheckpH)) 




    # dwang:
    def get_preempt_list(self, partition_dict_wcheckp):
        # print "[] get_preempt_list() ... "
        # 
        preempt_list = []
        
        # print "[] preemp_dict: ", partition_dict_wcheckp
        for jobid in partition_dict_wcheckp:
            pname = partition_dict_wcheckp[jobid]
            # print "[]   sub_pname: ", pname
            p = self.cached_partitions[pname[0]]
            # main_partition
            # print "main_p: ", pname[0]
            nbjob = self.get_running_job_by_partition(pname[0])
            if nbjob:
                # print "++--> main_Preempt ... "
                preempt_job = {};
                preempt_job['jobid'] = nbjob.jobid;
                preempt_job['pname'] = pname[0];
                preempt_list.append(preempt_job)
            ## parent_partition
            for parent_name in p.parents:
                # print "parent_p: ", parent_name
                nbjob = self.get_running_job_by_partition(parent_name)
                if nbjob:
                    # print "++--> parent_Preempt ... "
                    preempt_job = {};
                    preempt_job['jobid'] = nbjob.jobid;
                    preempt_job['pname'] = parent_name;
                    preempt_list.append(preempt_job)
            # ...
            ## children_partition
            for children_name in p.children:
                p_children = self._partitions[children_name]
                # print "children_p: ", children_name
                nbjob = self.get_running_job_by_partition(children_name)
                if nbjob:
                    # print "++--> children_Preempt ... "
                    preempt_job = {};
                    preempt_job['jobid'] = nbjob.jobid;
                    preempt_job['pname'] = children_name;
                    preempt_list.append(preempt_job)
            # ... 
        # print "[dw_bqsim] preempt_len: ", len(preempt_list)
        return preempt_list
    get_preempt_list = locking(exposed(get_preempt_list))
    # dwang


    def allocate_partition(self, name):
        """temperarly allocate a partition avoiding being allocated by other job"""
        #print "in allocate_partition, name=", name
        try:
            part = self.partitions[name]
        except KeyError:
            self.logger.error("reserve_partition(%r, %r) [does not exist]" % (name, size))
            return False
        if part.state == "idle":
            part.state = "allocated"
            for p in part._parents:
                if p.state == "idle":
                    p.state = "temp_blocked"
            for p in part._children:
                if p.state == "idle":
                    p.state = "temp_blocked"

    def deallocate_partition(self, name):
        """the reverse process of allocate_partition"""
        part = self.partitions[name]

        if part.state == "allocated":
            part.state = "idle"

            for p in part._parents:
                if p.state == "temp_blocked":
                    p.state = "idle"
            for p in part._children:
                if p.state != "temp_blocked":
                    p.state = "idle"

    def release_partition (self, name):
        """Release a reserved partition.
        Arguments:
        name -- name of the partition to release
        """
        try:
            partition = self.partitions[name]
        except KeyError:
            self.logger.error("release_partition(%r) [already free]" % (name))
            return False
        #if not partition.state == "busy":
        if (partition.state != "busy") and (partition.state != "rtj"):
            self.logger.info("release_partition(%r) [not busy]" % (name))
            return False

        self._partitions_lock.acquire()
        try:
            partition.state = "idle"
        except:
            self.logger.error("error in release_partition", exc_info=True)
        self._partitions_lock.release()

        # explicitly unblock the blocked partitions
        self.update_partition_state()

        self.logger.info("release_partition(%r)" % (name))
        return True
    release_partition = exposed(release_partition)


    def reserve_partition (self, name, size=None):
        """Reserve a partition and block all related partitions.

        Arguments:
        name -- name of the partition to reserve
        size -- size of the process group reserving the partition (optional)
        """

        try:
            partition = self.partitions[name]
        except KeyError:
            self.logger.error("reserve_partition(%r, %r) [does not exist]" % (name, size))
            return False
#        if partition.state != "allocated":
#            self.logger.error("reserve_partition(%r, %r) [%s]" % (name, size, partition.state))
#            return False
        if not partition.functional:
            self.logger.error("reserve_partition(%r, %r) [not functional]" % (name, size))
        if size is not None and size > partition.size:
            self.logger.error("reserve_partition(%r, %r) [size mismatch]" % (name, size))
            return False

        if partition.state == "busy":
            # print "try to reserve a busy partition: %s!!!" % name
            return False

        #self._partitions_lock.acquire()
        try:
            partition.state = "busy"
            partition.reserved_until = False
        except:
            self.logger.error("error in reserve_partition", exc_info=True)
            # print "try to reserve a busy partition!!"
        #self._partitions_lock.release()
        # explicitly call this, since the above "busy" is instantaneously available
        self.update_partition_state()
        #
        #check_pname = 'MIR-08800-3BBF1-2048'
        #check_p = self._partitions[check_pname]
        #print "[dw start_Rev] check_pname: ", check_p.name
        #print "[dw start_Rev] check_state: ", check_p.state
        #
        self.logger.info("reserve_partition(%r, %r)" % (name, size))
        return True
    reserve_partition = exposed(reserve_partition)

    # dwang : 
    def reserve_rtj_partition (self, name, size=None):
        """Reserve a partition and block all related partitions.

        Arguments:
        name -- name of the partition to reserve
        size -- size of the process group reserving the partition (optional)
        """

        try:
            partition = self.partitions[name]
        except KeyError:
            self.logger.error("reserve_rtj_partition(%r, %r) [does not exist]" % (name, size))
            return False
#        if partition.state != "allocated":
#            self.logger.error("reserve_partition(%r, %r) [%s]" % (name, size, partition.state))
#            return False
        if not partition.functional:
            self.logger.error("reserve_rtj_partition(%r, %r) [not functional]" % (name, size))
        if size is not None and size > partition.size:
            self.logger.error("reserve_rtj_partition(%r, %r) [size mismatch]" % (name, size))
            return False

        if partition.state == "busy":
            # print "[rtj] try to reserve a busy partition: %s!!!" % name
            return False
        if partition.state == "rtj":
            # print "[rtj] try to reserve a RTJ partition: %s!!!" % name
            return False

        #self._partitions_lock.acquire()
        try:
            # partition.state = "rtj"
            # samnickolay
            partition.state = "busy"
            # print "[rtj] set_partition_RTJ !!"
            partition.reserved_until = False
        except:
            self.logger.error("error in reserve_partition", exc_info=True)
            # print "error in reserve_rtj_partition !!"
        #self._partitions_lock.release()
        # explicitly call this, since the above "busy" is instantaneously available
        self.update_partition_state()

        #
        #check_pname = 'MIR-08800-3BBF1-2048'
        #check_p = self._partitions[check_pname]
        #print "[dw start_rtRev] check_pname: ", check_p.name
        #print "[dw start_rtRev] check_state: ", check_p.state
        #

        self.logger.info("reserve_rtj_partition(%r, %r)" % (name, size))
        return True
    reserve_rtj_partition = exposed(reserve_rtj_partition)
    # dwang 


#####--------utility functions
    # order the jobs with biggest utility first
    def utilitycmp(self, job1, job2):
        return -cmp(job1.score, job2.score)

    def compute_utility_scores (self):
        utility_scores = []
        current_time = self.get_current_time_sec()

        #for balanced utility computing
        if self.metric_aware:
            max_wait, avg_wait = self.get_current_max_avg_queue_time()
            max_walltime, min_walltime = self.get_current_max_min_walltime()

        for job in self.queues.get_jobs([{'is_runnable':True}]):
            utility_name = self.queues[job.queue].policy

            ###
            # samnickolay
            if self.predict_backfill:
                runtime_estimate = float(job.walltime_p)  # *Adj_Est*
            else:
                runtime_estimate = float(job.walltime)
            slowdown = (current_time + runtime_estimate * 60 - job.submittime) / (runtime_estimate * 60)

            # # THIS IS A TEST - TO COMPUTE SLOWDOWN USING ACTUAL RUNTIME, REMOVE THIS WHEN DONE TESTING
            # runtime_org = float(job.get('runtime'))
            # slowdown = (current_time - job.get('submittime') + job.get('remain_time')) / runtime_org

            # # THIS IS A TEST - TO COMPUTE SLOWDOWN USING PREDICTED RUNTIME, REMOVE THIS WHEN DONE TESTING
            # from bqsim import predicted_run_times
            # predicted_runtime = predicted_run_times[str(job.get('jobid'))]
            # slowdown = (current_time - job.get('submittime') + predicted_runtime) / predicted_runtime

            # samnickolay
            ###

            args = {'queued_time':current_time - float(job.submittime),
                    'wall_time': 60*float(job.walltime),
                    'wall_time_p':  60*float(job.walltime_p), ##  *AdjEst*
                    'slowdown': slowdown,   # samnickolay
                    'size': float(job.nodes),
                    'user_name': job.user,
                    'project': job.project,
                    'queue_priority': int(self.queues[job.queue].priority),
                    #'machine_size': max_nodes,
                    'jobid': int(job.jobid),
                    'score': job.score,
                    'recovering': job.recovering,
                    'state': job.state,
                    }
            try:
                if utility_name in self.builtin_utility_functions:
                    utility_func = self.builtin_utility_functions[utility_name]
                else:
                    utility_func = self.user_utility_functions[utility_name]

                if self.metric_aware:
                    utility_func = self.comput_utility_score_balanced

                utility_func.func_globals.update(args)

                if self.metric_aware:
                    score = utility_func(self.balance_factor, max_wait, max_walltime, min_walltime)
                else:
                    score = utility_func()
            except KeyError:
                # do something sensible when the requested utility function doesn't exist
                # probably go back to the "default" one

                # and if we get here, try to fix it and throw away this scheduling iteration
                self.logger.error("cannot find utility function '%s' named by queue '%s'" % (utility_name, job.queue))
                self.user_utility_functions[utility_name] = self.builtin_utility_functions["default"]
                self.logger.error("falling back to 'default' policy to replace '%s'" % utility_name)
                return
            except:
                # do something sensible when the requested utility function explodes
                # probably go back to the "default" one
                # and if we get here, try to fix it and throw away this scheduling iteration
                self.logger.error("error while executing utility function '%s' named by queue '%s'" % (utility_name, job.queue), \
                    exc_info=True)
                self.user_utility_functions[utility_name] = self.builtin_utility_functions["default"]
                self.logger.error("falling back to 'default' policy to replace '%s'" % utility_name)
                return

            try:
                job.score = score #in trunk it is job.score += score, (coscheduling need to temperally change score)
                #print "job id=%s, score=%s" % (job.jobid, job.score)
            except:
                self.logger.error("utility function '%s' named by queue '%s' returned a non-number" % (utility_name, job.queue), \
                    exc_info=True)
                self.user_utility_functions[utility_name] = self.builtin_utility_functions["default"]
                self.logger.error("falling back to 'default' policy to replace '%s'" % utility_name)
                return

    def define_user_utility_functions(self):
        self.logger.info("building user utility functions")
        self.user_utility_functions.clear()
        filename = os.path.expandvars(get_bgsched_config("utility_file", ""))
        try:
            f = open(filename)
        except:
            #self.logger.error("Can't read utility function definitions from file %s" % get_bgsched_config("utility_file", ""))
            return

        str = f.read()

        try:
            code = compile(str, filename, 'exec')
        except:
            self.logger.error("Problem compiling utility function definitions.", exc_info=True)
            return

        globals = {'math':math, 'time':time}
        locals = {}
        try:
            exec code in globals, locals
        except:
            self.logger.error("Problem executing utility function definitions.", exc_info=True)

        for thing in locals.values():
            #            if type(thing) is types.FunctionType:
                if thing.func_name in self.builtin_utility_functions:
                    self.logger.error("Attempting to overwrite builtin utility function '%s'.  User version discarded." % \
                        thing.func_name)
                else:
                    self.user_utility_functions[thing.func_name] = thing
    define_user_utility_functions = exposed(define_user_utility_functions)

    def define_builtin_utility_functions(self):
        self.logger.info("building builtin utility functions")
        self.builtin_utility_functions.clear()

        # I think this duplicates cobalt's old scheduling policy
        # higher queue priorities win, with jobid being the tie breaker
        def default0():
            val = queue_priority + 0.1
            return val

        def default():
            '''FCFS'''
            val = queued_time
            return val

        def default1():
            '''WFP'''
            if self.predict_queue:
                wall_time_sched = wall_time_p
            else:
                wall_time_sched = wall_time

            val = ( queued_time / wall_time_sched)**3 * size

            return val

        def high_prio():
            val = 1.0
            return val

        self.builtin_utility_functions["default"] = default
        self.builtin_utility_functions["high_prio"] = high_prio

        # samnickolay - custom utility function -
        # technically I should have followed the proper procedure for adding a custom user utility function
        # (see define_user_utility_functions defined above). I added the function here for quick testing purposes
        # sort the jobs in decreasing order of slowdown (hence why we take the reciprocal of the slowdown
        def custom_utility_function_v1():
            val = slowdown
            # val = 1.0 / slowdown
            return val

        self.builtin_utility_functions["custom_v1"] = custom_utility_function_v1


#####----waltime prediction stuff
    def get_walltime_Ap(self, spec):  #*AdjEst*
        '''get walltime adjusting parameter from history manager component'''

        projectname = spec.get('project')
        username = spec.get('user')
        if prediction_scheme == "paired":
            return self.history_manager.get_Ap_by_keypair(username, projectname)

        Ap_proj = self.history_manager.get_Ap('project', projectname)

        Ap_user = self.history_manager.get_Ap('user', username)

        if prediction_scheme == "project":
            return Ap_proj
        elif prediction_scheme == "user":
            # print "Ap_user==========", Ap_user
            return Ap_user
        elif prediction_scheme == "combined":
            return (Ap_proj + Ap_user) / 2
        else:
            return self.history_manager.get_Ap_by_keypair(username, projectname)


#####---- Walltime-aware Spatial Scheduling part

    def calc_loss_of_capacity(self):
        '''calculate loss of capacity for one iteration'''

        if self.num_waiting > 0:
            idle_nodes = TOTAL_NODES - self.num_busy
            has_loss = False
            for job in self.queuing_jobs:
                if (int(job.nodes)) < idle_nodes:
                    has_loss = True
                    break
            if has_loss:
                loss = self.current_cycle_capacity_loss()
                self.capacity_loss += loss
    calc_loss_of_capacity = exposed(calc_loss_of_capacity)

    def current_cycle_capacity_loss(self):
        loss  = 0
        current_time = self.get_current_time_sec()
        next_time = self.event_manager.get_next_event_time_sec()

        if next_time > current_time:
            time_length = next_time - current_time
            idle_midplanes = len(self.get_midplanes_by_state('idle'))
            idle_node = idle_midplanes * MIDPLANE_SIZE
            loss = time_length * idle_node
        return loss

    def total_capacity_loss_rate(self):
        timespan_sec = self.event_manager.get_time_span()

        total_NH = TOTAL_NODES *  (timespan_sec / 3600)

        #print "total_nodehours=", total_NH
        #print "total loss capcity (node*hour)=", self.capacity_loss / 3600

        loss_rate = self.capacity_loss /  (total_NH * 3600)

        # print "capacity loss rate=", loss_rate
        return loss_rate

    def equal_partition(self, nodeno1, nodeno2):
        proper_partsize1 = 0
        proper_partsize2 = 1
        for psize in self.part_size_list:
            if psize >= nodeno1:
                proper_partsize1 = psize
                break
        for psize in self.part_size_list:
            if psize >= nodeno2:
                proper_partsize2 = psize
                break
        if proper_partsize1 == proper_partsize2:
            return True
        else:
            return False

    def run_matched_job(self, jobid, partition):
        '''implementation of aggresive scheme in sc10 submission'''

        #get neighbor partition (list) for running
        partlist = []
        nbpart = self.get_neighbor_by_partition(partition)
        if nbpart:
            nb_partition = self._partitions[nbpart]
            if nb_partition.state != "idle":
                #self.dbglog.LogMessage("return point 1")
                return None
        else:
            #self.dbglog.LogMessage("return point 2")
            return None
        partlist.append(nbpart)

        #find a job in the queue whose length matches the top-queue job
        topjob = self.get_live_job_by_id(jobid)

        base_length = float(topjob.walltime)
        #print "job %d base_length=%s" % (jobid, base_length)
        base_nodes = int(topjob.nodes)

        min_diff = MAXINT
        matched_job = None
        msg = "queueing jobs=%s" % ([job.jobid for job in self.queuing_jobs])
        #self.dbglog.LogMessage(msg)

        for job in self.queuing_jobs:
            #self.dbglog.LogMessage("job.nodes=%s, base_nodes=%s" % (job.nodes, base_nodes))

            if self.equal_partition(int(job.nodes), base_nodes):
                length = float(job.walltime)
                #self.dbglog.LogMessage("length=%s, base_length=%s" % (length, base_length))
                if length > base_length:
                    continue
                diff = abs(base_length - length)
                #print "diff=", diff
                #self.dbglog.LogMessage("diff=%s" % (diff))
                if diff < min_diff:
                    min_diff = diff
                    matched_job = job

        if matched_job == None:
            pass
            #self.dbglog.LogMessage("return point 3")
        else:
            #self.dbglog.LogMessage(matched_job.jobid)
            pass

        #run the matched job on the neiborbor partition
        if matched_job and partlist:
            self.start_job([{'tag':'job', 'jobid':matched_job.jobid}], {'location':partlist})
            msg = "job=%s, partition=%s, mached_job=%s, matched_partitions=%s" % (jobid, partition, matched_job.jobid, partlist)
            self.dbglog.LogMessage(msg)

        return 1

    def get_neighbor_by_partition(self, partname):
        '''get the neighbor partition by given partition name.
          note: this functionality is specific to intrepid partition naming and for partition size smaller than 4k'''
        nbpart = ""
        partition = self._partitions[partname]
        partsize = partition.size
        if partsize == 512:  #e.g. ANL-R12-M0-512  --> ANL-R12-M1-512
            nbpart = "%s%s%s" % (partname[0:9], 1-int(partname[9]), partname[10:])  #reverse the midplane
        elif partsize == 1024:  #e.g.  ANL-R12-1024 --> ANL-R13-1024
            rackno = int(partname[6])
            if rackno % 2 == 0:  #even
                nbrackno = rackno + 1
            else:
                nbrackno = rackno - 1
            nbpart = "%s%s%s" % (partname[0:6], nbrackno, partname[7:])    #find the neighbor rack
        elif partsize == 2048:  #e.g. ANL-R12-R13-2048 --> ANL-R14-R15-2048
            rackno1 = int(partname[6])
            rackno2 = int(partname[10])
            if rackno1 % 4 == 0:  #0, 4 ...
                nbrackno1 = rackno1 + 2
                nbrackno2 = rackno2 + 2
            else:  #2, 6
                nbrackno1 = rackno1 - 2
                nbrackno2 = rackno2 - 2
            nbpart = "%s%s%s%s%s" % (partname[0:6], nbrackno1, partname[7:10], nbrackno2, partname[11:])
        elif partsize == 4096:  #e.g. ANL-R10-R13-4096 --> ANL-R14-R17-4096
            rackno1 = int(partname[6])
            rackno2 = int(partname[10])
            if rackno1 == 0:
                nbrackno1 = rackno1 + 4
                nbrackno2 = rackno2 + 4
            elif rackno1 == 4:
                nbrackno1 = rackno1 - 4
                nbrackno2 = rackno2 - 4
            nbpart = "%s%s%s%s%s" % (partname[0:6], nbrackno1, partname[7:10], nbrackno2, partname[11:])
        return nbpart

    def get_running_job_by_partition(self, partname):
        '''return a running job given the partition name'''
        #
        # print "[] get_running_job_by_partition() ... ... "
        # print "[] partname: ", partname
        #
        partition = self._partitions[partname]
        '''
        ## for pp in self.cached_partitions.itervalues(): 
	for pp in self._partitions.itervalues():  
            print "  [] _pp: ", pp 
            print "  [] _pp.name: ", pp.name  
            if pp.name == partname: 
                print "  [] partition_equal, pp: ", pp 
                partition = pp 
                break 
        ''' 
	# 
        # print "[] get_running_job_by_partition()_istop_1 ... "
        #
        if partition.state == "idle":
	    # print "[]  return, None ... "
            return None
        for rjob in self.running_jobs:
            partitions = rjob.location
            if partname in partitions: 
		# print "[]  return, rjob ... "
                return rjob
        # print "[]  return, None_full ... "
        return None

#####--begin--CoScheduling stuff
    def init_jobid_qtime_pairs(self):
        '''initialize mate job dict'''
        self.jobid_qtime_pairs = []

        for id, spec in self.unsubmitted_job_spec_dict.iteritems():
            qtime = spec['submittime']
            self.jobid_qtime_pairs.append((qtime, int(id)))

        def _qtimecmp(tup1, tup2):
            return cmp(tup1[0], tup2[0])

        self.jobid_qtime_pairs.sort(_qtimecmp)

    def get_jobid_qtime_pairs(self):
        '''get jobid_qtime_pairs list, remote function'''
        return self.jobid_qtime_pairs
    get_jobid_qtime_pairs = exposed(get_jobid_qtime_pairs)

    def set_mate_job_dict(self, remote_mate_job_dict):
        '''set self.mate_job_dict, remote function'''
        self.mate_job_dict = remote_mate_job_dict
        matejobs = len(self.mate_job_dict.keys())
        proportion = float(matejobs) / self.total_job

        self.coscheduling = True

        # print "Co-scheduling enabled, blue gene scheme=%s, cluster scheme=%s" % (self.cosched_scheme, self.cosched_scheme_remote)

        # print "Number of mate job pairs: %s, proportion in blue gene jobs: %s%%"\
        #      % (len(self.mate_job_dict.keys()), round(proportion *100, 1))
        self.generate_mate_job_log()

    set_mate_job_dict = exposed(set_mate_job_dict)

    def try_to_run_mate_job(self, _jobid):
        '''try to run mate job, start all the jobs that can run. If the started
        jobs include the given mate job, return True else return False.  _jobid : int
        '''
        #if the job is not yielding, do not continue; no other job is possibly to be scheduled
        if _jobid not in self.yielding_job_list:
            return False

        mate_job_started = False

        #start all the jobs that can run
        while True:
            running_jobs = [job for job in self.queues.get_jobs([{'has_resources':True}])]

            end_times = []

            now = self.get_current_time_sec()

            for job in running_jobs:
                end_time = max(float(job.starttime) + 60 * float(job.walltime), now + 5*60)
                end_times.append([job.location, end_time])

            active_jobs = [job for job in self.queues.get_jobs([{'is_runnable':True}])] #waiting jobs
            active_jobs.sort(self.utilitycmp)

            job_location_args = []
            for job in active_jobs:
                if not job.jobid == _jobid and self.mate_job_dict.get(job.jobid, 0) > 0:
                    #if a job other than given job (_jobid) has mate, skip it.
                    continue

                job_location_args.append({'jobid': str(job.jobid),
                                          'nodes': job.nodes,
                                          'queue': job.queue,
                                          'forbidden': [],
                                          'utility_score': job.score,
                                          'walltime': job.walltime,
                                          'walltime_p': job.walltime_p,  #*AdjEst*
                                          'attrs': job.attrs,
                 } )

            if len(job_location_args) == 0:
                break

            #print "queue order=", [item['jobid'] for item in job_location_args]

            best_partition_dict = self.find_job_location(job_location_args, end_times)

            if best_partition_dict:
                #print "best_partition_dict=", best_partition_dict

                for canrun_jobid in best_partition_dict:
                    nodelist = best_partition_dict[canrun_jobid]

                    if str(_jobid) == canrun_jobid:
                        mate_job_started = True

                    self.start_job([{'tag':"job", 'jobid':int(canrun_jobid)}], {'location':nodelist})
                    #print "bqsim.try_to_run_mate, start job jobid ", canrun_jobid
            else:
                break

        return mate_job_started
    try_to_run_mate_job = exposed(try_to_run_mate_job)

    def run_holding_job(self, specs):
        '''start holding job'''
        for spec in specs:
            jobid = spec.get('jobid')
            nodelist = self.job_hold_dict.get(jobid, None)
            if nodelist == None:
                #print "cannot find holding resources"
                return
            #print "start holding job %s on location %s" % (spec['jobid'], nodelist)
            self.start_job([spec], {'location':nodelist})
            del self.job_hold_dict[jobid]

    run_holding_job = exposed(run_holding_job)

    def hold_job(self, spec, updates):
        '''hold a job. a holding job is not started but hold some resources that can run itself in the future
        once its mate job in a remote system can be started immediatly. Note, one time hold only one job'''

        def _hold_job(job, newattr):
            '''callback function to update job start/end time'''
            temp = job.to_rx()
            newattr = self.hold_job_updates(temp, newattr)
            temp.update(newattr)
            job.update(newattr)
            self.log_job_event("H", self.get_current_time_date(), temp)

        current_holden_nodes = 0
        for partlist in self.job_hold_dict.values():
            host = partlist[0]
            nodes = int(host.split("-")[-1])
            current_holden_nodes += nodes

        nodelist = updates['location']

        partsize = 0
        for partname in nodelist:
            partsize += int(partname.split("-")[-1])

        job_id = spec['jobid']
        if current_holden_nodes + partsize < self.max_holding_sys_util * self.total_nodes:
            self.job_hold_dict[spec['jobid']] = nodelist

            if not self.first_hold_time_dict.has_key(job_id):
                self.first_hold_time_dict[job_id] = self.get_current_time_sec()

            for partname in nodelist:
                self.reserve_partition(partname)

            if not self.first_yield_hold_time_dict.has_key(job_id):
                self.first_yield_hold_time_dict[job_id] = self.get_current_time_sec()

            return self.queues.get_jobs([spec], _hold_job, updates)
        else:
            #if execeeding the maximum limite of holding nodes, the job will not hold but yield
            self.yielding_job_list.append(job_id)  #int
            #record the first time this job yields
            if not self.first_yield_hold_time_dict.has_key(job_id):
                self.first_yield_hold_time_dict[job_id] = self.get_current_time_sec()
                self.dbglog.LogMessage("%s: job %s first yield" % (self.get_current_time_date(), job_id))
            return 0

    def hold_job_updates(self, jobspec, newattr):
        '''Return the state updates (including state queued -> running,
        setting the start_time, end_time)'''
        updates = {}

        updates['is_runnable'] = False
        updates['has_resources'] = False
        updates['state'] = "holding"
        updates['last_hold'] = self.get_current_time_sec()

        updates.update(newattr)

        if SELF_UNHOLD_INTERVAL > 0:
            release_time = self.get_current_time_sec() + SELF_UNHOLD_INTERVAL
            self.insert_time_stamp(release_time, "U", {'jobid':jobspec['jobid'], 'location':newattr['location']})

        return updates

    def unhold_job(self, jobid):
        '''if a job holds a partition longer than MAX_HOLD threshold, the job will release the partition and starts yielding'''
        nodelist = self.job_hold_dict.get(jobid)

        #release holden partitions
        if nodelist:
            for partname in nodelist:
                self.release_partition(partname)
        else:
            # print "holding job %s not found in job_hold_dict: " % jobid
            return 0

        def _unholding_job(job, newattr):
            '''callback function'''
            temp = job.to_rx()
            newattr = self.unholding_job_updates(temp, newattr)
            temp.update(newattr)
            job.update(newattr)
            self.log_job_event("U", self.get_current_time_date(), temp)

            del self.job_hold_dict[jobid]

        return self.queues.get_jobs([{'jobid':jobid}], _unholding_job, {'location':self.job_hold_dict.get(jobid, ["N"])})

    def unholding_job_updates(self, jobspec, newattr):
        '''unhold job once the job has consumed SELF_UNHOLD_INTERVAL or system-wide unhold_all'''
        updates = {}

        updates['is_runnable'] = True
        updates['has_resources'] = False
        updates['state'] = "queued"
        #set the job to lowest priority at this scheduling point.
        #if no other job gets the nodes it released, the unholden job can hold those nodes again
        updates['score'] = 0
        #accumulate hold_time, adding last hold time to total hold_time
        updates['hold_time'] = jobspec['hold_time'] + self.get_current_time_sec() - jobspec['last_hold']
        updates['last_hold'] = 0

        updates.update(newattr)

        return updates

    def unhold_all(self):
        '''unhold all jobs. periodically invoked to prevent deadlock'''
        for jobid in self.job_hold_dict.keys():
            job_hold_time = self.get_current_time_sec() - self.first_hold_time_dict[jobid]
            #if a job has holden at least 10 minutes, then periodically unhold it
            if job_hold_time >  AT_LEAST_HOLD:
                self.unhold_job(jobid)

    def get_mate_job_status(self, jobid):
        '''return mate job status, remote function, invoked by remote component'''
        #local_job = self.get_live_job_by_id(jobid)
        ret_dict = {'jobid':jobid}
        ret_dict['status'] = self.get_coschedule_status(jobid)
        return ret_dict
    get_mate_job_status = exposed(get_mate_job_status)

    def get_mate_jobs_status_local(self, remote_jobid):
        '''return mate job status, invoked by local functions'''
        status_dict = {}
        try:
            status_dict = ComponentProxy(REMOTE_QUEUE_MANAGER).get_mate_job_status(remote_jobid)
        except:
            self.logger.error("failed to connect to remote cluster queue-manager component!")
            self.dbglog.LogMessage("failed to connect to remote cluster queue-manager component!")
        return status_dict

    def get_coschedule_status(self, jobid):
        '''return job status regarding coscheduling,
           input: jobid
           output: listed as follows:
            1. "queuing"
            2. "holding"
            3. "unsubmitted"
            4. "running"
            5. "ended"
        '''
        ret_status = "unknown"
        job = self.get_live_job_by_id(jobid)
        if job:  #queuing or running
            has_resources = job.has_resources
            is_runnable = job.is_runnable
            if is_runnable and not has_resources:
                ret_status = "queuing"
            if not is_runnable and has_resources:
                ret_status = "running"
            if not is_runnable and not has_resources:
                ret_status = "holding"
        else:  #unsubmitted or ended
            if self.unsubmitted_job_spec_dict.has_key(str(jobid)):
                ret_status = "unsubmitted"
            else:
                ret_status = "unknown"  #ended or no such job
                del self.mate_job_dict[jobid]
        return ret_status

    def generate_mate_job_log(self):
        '''output a file with mate jobs one pair per line'''

        #initialize debug logger
        if self.output_log:
            matelog = PBSlogger(self.output_log+"-mates")
        else:
            matelog = PBSlogger(".mates")

        for k, v in self.mate_job_dict.iteritems():
            msg = "%s:%s" % (k, v)
            matelog.LogMessage(msg)
        matelog.closeLog()

#####--end--CoScheduling stuff


#####----------display stuff

    def get_midplanes_by_state(self, status):
        idle_midplane_list = []

        for partition in self._partitions.itervalues():
            if partition.size == MIDPLANE_SIZE:
                if partition.state == status:
                    idle_midplane_list.append(partition.name)

        return idle_midplane_list

    def show_resource(self):
        '''print rack_matrix'''

        self.mark_matrix()

        for row in self.rack_matrix:
            for rack in row:
                if rack[0] == 1:
                    print "*",
                elif rack[0] == 0:
                    print GREENS + 'X' + ENDC,
                elif rack[0] == 2:
                    print YELLOWS + '+' + ENDC,
                else:
                    print rack[0],
            print '\r'
            for rack in row:
                if rack[1] == 1:
                    print "*",
                elif rack[1] == 0:
                    print GREENS + 'X' + ENDC,
                elif rack[1] == 2:
                    print YELLOWS + '+' + ENDC,
                else:
                    print rack[1],
            print '\r'

    def get_holden_midplanes(self):
        '''return a list of name of 512-size partitions that are in the job_hold_list'''
        midplanes = []
        for partlist in self.job_hold_dict.values():
            partname = partlist[0]
            midplanes.extend(self.get_midplanes(partname))
        return midplanes

    def get_midplanes(self, partname):
        '''return a list of sub-partitions each contains 512-nodes(midplane)'''
        midplane_list = []
        partition = self._partitions[partname]

        if partition.size == MIDPLANE_SIZE:
            midplane_list.append(partname)
        elif partition.size > MIDPLANE_SIZE:
            children = partition.children
            for part in children:
                if self._partitions[part].size == MIDPLANE_SIZE:
                    midplane_list.append(part)
        else:
            parents = partition.parents
            for part in parents:
                if self._partitions[part].size == MIDPLANE_SIZE:
                    midplane_list.append(part)

        return midplane_list

    def mark_matrix(self):
        idle_midplanes = self.get_midplanes_by_state('idle')
        self.reset_rack_matrix()
        for name in idle_midplanes:  #sample name for a midplane:  ANL-R15-M0-512
            print name
            row = int(name[5])
            col = int(name[6])
            M = int(name[9])
            self.rack_matrix[row][col][M] = 1
        holden_midplanes = self.get_holden_midplanes()
        if self.coscheduling and self.cosched_scheme == "hold":
            for name in holden_midplanes:
                row = int(name[5])
                col = int(name[6])
                M = int(name[9])
                self.rack_matrix[row][col][M] = 2

    def reset_rack_matrix(self):
        self.rack_matrix = [
                [[0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0]],
                [[0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0]],
                [[0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0]],
                [[0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0]],
                [[0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0]],
            ]
        #self.rack_matrix = [[[0,0] for i in range(8)] for j in range(5)]

    def print_screen(self, cur_event=""):
        '''print screen, show number of waiting jobs, running jobs, busy_nodes%'''

        #os.system('clear')

        print "Blue Gene"

        if PRINT_SCREEN == False:
            print "simulation in progress, please wait"
            return

        current_datetime = self.event_manager.get_current_date_time()
        print "%s %s" % (current_datetime, cur_event)

        self.show_resource()

#        print "number of waiting jobs: ", self.num_waiting


#        max_wait, avg_wait = self.get_current_max_avg_queue_time()

        #print "maxium waiting time (min): ", int(max_wait / 60.0)
        #print "average waiting time (min): ", int(avg_wait / 60.0)

        waiting_job_bar = REDS
        for i in range(self.num_waiting):
            waiting_job_bar += "*"
        waiting_job_bar += ENDC

        print waiting_job_bar

        holding_jobs = len(self.job_hold_dict.keys())
        holding_midplanes = 0
        hold_partitions = []
        for partlist in self.job_hold_dict.values():
            host = partlist[0]
            hold_partitions.append(host)
            nodes = int(host.split("-")[-1])
            holding_midplanes += nodes / MIDPLANE_SIZE

        print "number of running jobs: ", self.num_running
        running_job_bar = BLUES
        for i in range(self.num_running):
            running_job_bar += "+"
        running_job_bar += ENDC
        print running_job_bar

        print "number of holding jobs: ", holding_jobs

        print "number of holden midplanes: ", holding_midplanes
        #print "holden partitions: ", hold_partitions

        midplanes = self.num_busy / MIDPLANE_SIZE
        print "number of busy midplanes: ", midplanes
        print "system utilization: ", float(self.num_busy) / self.total_nodes

        busy_midplane_bar = GREENS

        i = 0
        while i < midplanes:
            busy_midplane_bar += "x"
            i += 1
        j = 0
        busy_midplane_bar += ENDC
        busy_midplane_bar += YELLOWS
        while j < holding_midplanes:
            busy_midplane_bar += "+"
            j += 1
            i += 1
        busy_midplane_bar += ENDC
        for k in range(i, self.total_midplane):
            busy_midplane_bar += "-"
        busy_midplane_bar += REDS
        busy_midplane_bar += "|"
        busy_midplane_bar += ENDC
        print busy_midplane_bar
        print "completed jobs/total jobs:  %s/%s" % (self.num_end, self.total_job)

        progress = 100 * self.num_end / self.total_job

        progress_bar = ""
        i = 0
        while i < progress:
            progress_bar += "="
            i += 1
        for j in range(i, 100):
            progress_bar += "-"
        progress_bar += "|"
        print progress_bar

        #if self.get_current_time_sec() > 1275393600:
         #   time.sleep(1)

        if self.sleep_interval:
            time.sleep(self.sleep_interval)

        print "waiting jobs:", [(job.jobid, job.nodes) for job in self.queuing_jobs]

#        wait_jobs = [job for job in self.queues.get_jobs([{'is_runnable':True}])]
#
#        if wait_jobs:
#            wait_jobs.sort(self.utilitycmp)
#            top_jobs = wait_jobs[0:5]
#        else:
#            top_jobs = []
#
#        if top_jobs:
#            print "high priority waiting jobs: ", [(job.jobid, job.nodes) for job in top_jobs]
#        else:
#            print "hig priority waiting jobs:"

        #print "holding jobs: ", [(k,v[0].split("-")[-1]) for k, v in self.job_hold_dict.iteritems()]
        print "\n\n"

    # dwang:
    #def post_simulation_handling(self):
    def post_simulation_handling(self,simu_name, simu_tid, checkp_t_internval, dsize_pnode, bw_temp_write):
    # # dwang
    #     '''post screen after simulation completes'''
    #     #print self.first_yield_hold_time_dict
    #     capacity_loss_rate = self.total_capacity_loss_rate()
    #     msg  = "capacity_loss:%f" % capacity_loss_rate
    #     self.dbglog.LogMessage(msg)
    #

        import collections
        experiment_metrics = collections.OrderedDict()

        min_queue_time = None
        min_start_time = None
        max_end_time = None
        for job in self.started_job_dict.itervalues():
            if min_queue_time is None:
                min_queue_time = job.get('submittime')
                min_start_time = job.get('start_time')
                max_end_time = job.get('end_time')
                continue
            if min_queue_time > job.get('submittime'):
                min_queue_time = job.get('submittime')
            if min_start_time > job.get('start_time'):
                min_start_time = job.get('start_time')
            if max_end_time < job.get('end_time'):
                max_end_time = job.get('end_time')

        min_queue_date = datetime.fromtimestamp(min_queue_time)
        # min_queue_date = sec_to_date(min_queue_time)
        utilization_window_start_date = datetime.combine(min_queue_date.date(), datetime.min.time())
        from datetime import timedelta
        utilization_window_start_date += timedelta(days=1)
        utilization_window_end_date = utilization_window_start_date + timedelta(days=5)
        print('utilization_window_start_date: ' + str(utilization_window_start_date))
        print('utilization_window_end_date' + str(utilization_window_end_date))

        t = utilization_records

        trimmed_utilization_records = []

        epoch = datetime.utcfromtimestamp(0)
        utilization_window_start_time = (utilization_window_start_date - epoch).total_seconds()

        utilization_window_end_time = (utilization_window_end_date - epoch).total_seconds()

        for utilization_record in utilization_records:
            if utilization_record[0] >= utilization_window_start_time and utilization_record[1] <= utilization_window_end_time:
                trimmed_utilization_records.append(utilization_record)
            elif utilization_record[0] < utilization_window_start_time and \
                            utilization_record[1] > utilization_window_start_time and \
                            utilization_record[1] <= utilization_window_end_time:
                modified_utilization_record = (utilization_window_start_time, utilization_record[1], utilization_record[2])
                trimmed_utilization_records.append(modified_utilization_record)
            elif utilization_record[0] >= utilization_window_start_time and \
                            utilization_record[0] < utilization_window_end_time and \
                            utilization_record[1] > utilization_window_end_time:
                modified_utilization_record = (utilization_record[0], utilization_window_end_time, utilization_record[2])
                trimmed_utilization_records.append(modified_utilization_record)

        trimmed_utilization = sum([(u_record[1] - u_record[0]) * float(u_record[2]) for u_record in trimmed_utilization_records])
        trimmed_utilization_time = float(trimmed_utilization_records[-1][1] - trimmed_utilization_records[0][0])
        system_utilization_trimmed = trimmed_utilization / trimmed_utilization_time

        utilization = sum([(u_record[1] - u_record[0]) * float(u_record[2]) for u_record in utilization_records])
        utilization_time = float(utilization_records[-1][1] - utilization_records[0][0])
        system_utilization = utilization / utilization_time

        experiment_metrics['makespan_trimmed'] = trimmed_utilization_time
        experiment_metrics['system_utilization_trimmed'] = system_utilization_trimmed

        experiment_metrics['makespan'] = utilization_time
        experiment_metrics['system_utilization'] = system_utilization

        trimmed_started_job_dict = {}
        for jobid, job in self.started_job_dict.iteritems():
            job_start_time = jobs_start_times[int(jobid)][0][0]
            job_end_time = job.get('end_time')

            if job_start_time >= utilization_window_start_time and job_end_time <= utilization_window_end_time:
                trimmed_started_job_dict[jobid] = job

            # if job.get('jobid') == '373200' or job.get('jobid') == 373200:
            #     temp_dict = jobs_start_times[int(job.get('jobid'))]

        job_categories = ['', 'narrow_short_', 'narrow_long_', 'wide_short_', 'wide_long_']
        job_types = ['batch', 'rt']
        job_metrics = ['count_', 'slowdown_', 'turnaround_time_', 'run_time_']

        trimmed_jobs = ['', '_trimmed']

        for job_category in job_categories:
            for job_type in job_types:
                for job_metric in job_metrics:
                    for trimmed_job in trimmed_jobs:
                        if 'count' in job_metric:
                            experiment_metrics[job_category + job_metric + job_type + trimmed_job] = 0
                        else:
                            experiment_metrics[job_category + job_metric + job_type + trimmed_job] = []

        experiment_metrics['run_time'] = []

        def compute_job_metrics(experiment_metrics, job_dict, trimmed_jobs=False):

            for job in job_dict.itervalues():
                # for job in self.started_job_dict.itervalues():
                temp_wait_time = (job.get('end_time') - job.get('submittime') - float(job.get('runtime_org')))

                temp_turnaround_time = job.get('end_time') - job.get('submittime')
                # temp_slowdown = (job.get('end_time') - job.get('submittime')) / float(job.get('runtime_org'))

                # compute bounded slowdown
                bounded_slowdown_threshold = 10.0 * 60.0  # 10 minutes in seconds
                temp_bounded_runtime_sim = max(float(job.get('runtime_org')), bounded_slowdown_threshold)
                temp_bounded_runtime_orig = max(float(job.get('runtime_org')), bounded_slowdown_threshold)
                temp_slowdown = (temp_wait_time + temp_bounded_runtime_sim) / temp_bounded_runtime_orig

                temp_run_time = float(job.get('runtime_org'))

                experiment_metrics['run_time'].append(temp_run_time)

                if float(job.get('partsize')) <= 4096:  # if job is narrow
                    if float(job.get('runtime_org')) <= 120 * 60.0:  # if job is short
                        job_category = 'narrow_short'
                    else:  # if job is long
                        job_category = 'narrow_long'
                else:  # if job is wide
                    if float(job.get('runtime_org')) <= 120 * 60.0:  # if job is short
                        job_category = 'wide_short'
                    else:  # if job is long
                        job_category = 'wide_long'

                if job.get('jobid') in rtj_id:  # if job is a realtime job
                    job_type = 'rt'
                else:
                    job_type = 'batch'

                if trimmed_jobs is True:
                    job_type += '_trimmed'

                experiment_metrics['slowdown_' + job_type].append(temp_slowdown)
                experiment_metrics['turnaround_time_' + job_type].append(temp_turnaround_time)
                experiment_metrics['run_time_' + job_type].append(temp_run_time)
                experiment_metrics['count_' + job_type] += 1

                experiment_metrics[job_category + '_turnaround_time_' + job_type].append(temp_turnaround_time)
                experiment_metrics[job_category + '_slowdown_' + job_type].append(temp_slowdown)
                experiment_metrics[job_category + '_run_time_' + job_type].append(temp_run_time)
                experiment_metrics[job_category + '_count_' + job_type] += 1

        compute_job_metrics(experiment_metrics, self.started_job_dict, trimmed_jobs=False)
        compute_job_metrics(experiment_metrics, trimmed_started_job_dict, trimmed_jobs=True)

        job_metrics_to_average = ['slowdown', 'turnaround_time', 'run_time']

        # average all of the values in the metrics lists
        for key, value in experiment_metrics.iteritems():
            test_metric_in_key_list = [job_metric_to_average in key for job_metric_to_average in job_metrics_to_average]
            if True in test_metric_in_key_list:
                if len(value) > 0:
                    experiment_metrics[key] = float(sum(value)) / len(value)
                    if 'time' in key:
                        experiment_metrics[key] = experiment_metrics[key] / 60.0
                else:
                    experiment_metrics[key] = 0

        # from bqsim import utilization_records, jobs_start_times
        global utilization_records
        global jobs_start_times
        utilization_records = []
        jobs_start_times = {}

        return experiment_metrics
    post_simulation_handling = exposed(post_simulation_handling)

#############metric-aware###

    def get_current_max_avg_queue_time(self):
        '''return the average waiting time of jobs in the current queue'''
        current_time = self.get_current_time_sec()
        queued_times =[current_time - float(job.submittime) for job in self.queuing_jobs]
        if len(queued_times) > 0:
            max_wait = max(queued_times)
            avg_wait = sum(queued_times) / len(queued_times)
        else:
            max_wait = 0
            avg_wait = 0
        return max_wait, avg_wait

    def get_current_max_min_walltime(self):
        '''return the max and min walltime in the current queue (in seconds)'''
        current_time = self.get_current_time_sec()
        wall_times =[60*float(job.walltime) for job in self.queuing_jobs]
        if len(wall_times) > 0:
            max_walltime = max(wall_times)
            min_walltime = min(wall_times)
        else:
            max_walltime = 0
            min_walltime = 0
        return max_walltime, min_walltime

    def comput_utility_score_balanced(self, balance_factor, max_wait, max_walltime, min_walltime):
        '''compute utility score balancing FCFS and SJF using a balance factor [0, 1]'''
        if max_wait == 0:
            wait_score = 0
        else:
            wait_score = 100.0 * queued_time / max_wait

        if max_walltime == min_walltime:
            length_score = 0
        else:
            length_score = 100.0 * (max_walltime - wall_time)/(max_walltime - min_walltime)

        balanced_score = wait_score * balance_factor + length_score * (1.0 - balance_factor)

        #print "wait=%s, max_wait=%s" % (queued_time, max_wait)
        #print "walltime=%s, MAX_WALLTIME=%s, MIN_WALLTIME=%s" % (wall_time, max_walltime, min_walltime)
        #print "wait_score=%s, length_score=%s, balanced_score=%s" % (wait_score, length_score, balanced_score)

        return balanced_score

    def monitor_metrics(self):
        '''main function of metrics monitoring'''

        self.monitor_metrics_util()
        self.monitor_metrics_wait()

    monitor_metrics = exposed(monitor_metrics)


    def monitor_metrics_wait(self):
        '''main function of metrics monitoring activities for wait'''
    #    print self.get_current_time_date(), " metrics monitor invoked"
        #self.get_utilization_rate(3600*24)
        #current_avg_wait = self.get_avg_wait_last_period(0)
        aggr_wait = self.get_aggr_wait_last_period(0)
        
        # print "--- aggr_wait: "
        # print aggr_wait

        if self.adaptive in ["10", "11"]:
            if aggr_wait > 1000:
                self.balance_factor = 0.5
            else:
                self.balance_factor = 1
                # print aggr_wait / 60

        self.queue_depth_data.append(aggr_wait / 60)
#        if self.balance_factor != before:
#            print "balance_factor changed to:", self.balance_factor

    def monitor_metrics_util(self):
        '''main function of metrics monitoring actitivies for utilization'''
        util_instant = self.get_utilization_rate(0)
        util_1h = self.get_utilization_rate(3600)
        util_10h = self.get_utilization_rate(3600*10)
        util_24h = self.get_utilization_rate(3600*24)

        print "--- util_instant/1h/10h/24h: "
        print util_instant, util_1h, util_10h, util_24h

        if self.adaptive in ["01", "11"]:
            if util_10h > util_24h:
                self.window_size = 1
            else:
                self.window_size = 4

    def get_utilization_rate(self, period):
        '''get the average utilization rate in the last 'period' of time'''

        now = self.get_current_time_sec()

        utilization = 0
        if period==0:
            utilization = float(self.num_busy) / TOTAL_NODES
            return utilization
        elif period > 0:
            start_point = now - period
            total_busy_node_sec = 0

            for k, v in self.started_job_dict.iteritems():
                jobid = k
                # if jobid != v.get('jobid'):
                #     # print "jobid=", jobid, "valueid=", v.get('jobid')
                jobstart = float(v.get("start_time"))
                jobend = float(v.get("end_time"))
                partitions = v.get("location")
                partsize = int(partitions[0].split('-')[-1])

                #jobs totally within the period
                if jobstart > start_point and jobend < now:
                    node_sec =  (jobend - jobstart) * partsize
                    total_busy_node_sec += node_sec
                    self.delivered_node_hour += node_sec / 3600
                    #print "1 now=%s, jobid=%s, start=%s, end=%s, partsize=%s, nodehour=%s" % (sec_to_date(now), jobid, sec_to_date(jobstart), sec_to_date(jobend), partsize, node_sec /(40960*3600))

                #jobs starting in the period but not ended yet
                if jobstart > start_point and jobstart < now and jobend >= now:
                    node_sec = (now - jobstart) * partsize
                    total_busy_node_sec += node_sec
                    self.delivered_node_hour += node_sec / 3600
                    #print "2 now=%s, jobid=%s, start=%s, end=%s, partsize=%s, nodehour=%s" % (sec_to_date(now), jobid, sec_to_date(jobstart), sec_to_date(jobend), partsize, node_sec /(40960*3600))

                #jobs started before the period start but ended in the period
                if jobstart <= start_point and jobend > start_point and jobend < now:
                    node_sec = (jobend - start_point) * partsize
                    total_busy_node_sec += node_sec
                    self.delivered_node_hour += node_sec / 3600
                    #print "3 now=%s, jobid=%s, start=%s, end=%s, partsize=%s, nodehour=%s" % (sec_to_date(now), jobid, sec_to_date(jobstart), sec_to_date(jobend), partsize, node_sec /(40960*3600))

                #jobs started before the period start but ended after the period end
                if jobstart <= start_point and jobend >= now:
                    node_sec = period * partsize
                    total_busy_node_sec += node_sec
                    self.delivered_node_hour += node_sec / 3600
                    #print "4 now=%s, jobid=%s, start=%s, end=%s, partsize=%s, nodehour=%s" % (sec_to_date(now), jobid, sec_to_date(jobstart), sec_to_date(jobend), partsize, node_sec /(40960.0*3600))

            avg_utilization = float(total_busy_node_sec) / (period*TOTAL_NODES)
            return avg_utilization
    get_utilization_rate = locking(exposed(get_utilization_rate))


    def get_utilization_rate_SE(self, start_point, period):
        '''get the average utilization rate in the last 'period' of time'''
        now = start_point + period
        
        utilization = 0
        if period==0:
            utilization = float(self.num_busy) / TOTAL_NODES
            return utilization
        elif period > 0:
            total_busy_node_sec = 0
            
            for k, v in self.started_job_dict.iteritems():
                jobid = k
                # if jobid != v.get('jobid'):
                #     print "jobid=", jobid, "valueid=", v.get('jobid')
                jobstart = float(v.get("start_time"))
                jobend = float(v.get("end_time"))
                partitions = v.get("location")
                partsize = int(partitions[0].split('-')[-1])
                
                #jobs totally within the period
                if jobstart > start_point and jobend < now:
                    node_sec =  (jobend - jobstart) * partsize
                    total_busy_node_sec += node_sec
                    self.delivered_node_hour += node_sec / 3600
                #print "1 now=%s, jobid=%s, start=%s, end=%s, partsize=%s, nodehour=%s" % (sec_to_date(now), jobid, sec_to_date(jobstart), sec_to_date(jobend), partsize, node_sec /(40960*3600))
        
                #jobs starting in the period but not ended yet
                if jobstart > start_point and jobstart < now and jobend >= now:
                    node_sec = (now - jobstart) * partsize
                    total_busy_node_sec += node_sec
                    self.delivered_node_hour += node_sec / 3600
                    #print "2 now=%s, jobid=%s, start=%s, end=%s, partsize=%s, nodehour=%s" % (sec_to_date(now), jobid, sec_to_date(jobstart), sec_to_date(jobend), partsize, node_sec /(40960*3600))
                
                #jobs started before the period start but ended in the period
                if jobstart <= start_point and jobend > start_point and jobend < now:
                    node_sec = (jobend - start_point) * partsize
                    total_busy_node_sec += node_sec
                    self.delivered_node_hour += node_sec / 3600
                    #print "3 now=%s, jobid=%s, start=%s, end=%s, partsize=%s, nodehour=%s" % (sec_to_date(now), jobid, sec_to_date(jobstart), sec_to_date(jobend), partsize, node_sec /(40960*3600))
            
                #jobs started before the period start but ended after the period end
                if jobstart <= start_point and jobend >= now:
                    node_sec = period * partsize
                    total_busy_node_sec += node_sec
                    self.delivered_node_hour += node_sec / 3600
                    #print "4 now=%s, jobid=%s, start=%s, end=%s, partsize=%s, nodehour=%s" % (sec_to_date(now), jobid, sec_to_date(jobstart), sec_to_date(jobend), partsize, node_sec /(40960.0*3600))

            avg_utilization_SE = float(total_busy_node_sec) / (period*TOTAL_NODES)
            return avg_utilization_SE


    def get_utilization_rate_SE_diag(self, start_point, period):
        '''get the average utilization rate in the last 'period' of time'''
        now = start_point + period
        
        utilization = 0
        if period==0:
            utilization = float(self.num_busy) / TOTAL_NODES
            return utilization
        elif period > 0:
            total_busy_node_sec = 0
            
            for k, v in self.started_job_dict.iteritems():
                jobid = k
                # if jobid != v.get('jobid'):
                #     print "jobid=", jobid, "valueid=", v.get('jobid')
                jobstart = float(v.get("start_time"))
                jobend = float(v.get("end_time"))
                partitions = v.get("location")
                partsize = int(partitions[0].split('-')[-1])
                
                #jobs totally within the period
                if jobstart > start_point and jobend < now:
                    node_sec =  (jobend - jobstart) * partsize
                    total_busy_node_sec += node_sec
                    self.delivered_node_hour += node_sec / 3600
                #print "1 now=%s, jobid=%s, start=%s, end=%s, partsize=%s, nodehour=%s" % (sec_to_date(now), jobid, sec_to_date(jobstart), sec_to_date(jobend), partsize, node_sec /(40960*3600))
        
                #jobs starting in the period but not ended yet
                if jobstart > start_point and jobstart < now and jobend >= now:
                    node_sec = (now - jobstart) * partsize
                    total_busy_node_sec += node_sec
                    self.delivered_node_hour += node_sec / 3600
                    #print "2 now=%s, jobid=%s, start=%s, end=%s, partsize=%s, nodehour=%s" % (sec_to_date(now), jobid, sec_to_date(jobstart), sec_to_date(jobend), partsize, node_sec /(40960*3600))
                
                #jobs started before the period start but ended in the period
                if jobstart <= start_point and jobend > start_point and jobend < now:
                    node_sec = (jobend - start_point) * partsize
                    total_busy_node_sec += node_sec
                    self.delivered_node_hour += node_sec / 3600
                    #print "3 now=%s, jobid=%s, start=%s, end=%s, partsize=%s, nodehour=%s" % (sec_to_date(now), jobid, sec_to_date(jobstart), sec_to_date(jobend), partsize, node_sec /(40960*3600))
            
                #jobs started before the period start but ended after the period end
                if jobstart <= start_point and jobend >= now:
                    node_sec = period * partsize
                    total_busy_node_sec += node_sec
                    self.delivered_node_hour += node_sec / 3600
                    #print "4 now=%s, jobid=%s, start=%s, end=%s, partsize=%s, nodehour=%s" % (sec_to_date(now), jobid, sec_to_date(jobstart), sec_to_date(jobend), partsize, node_sec /(40960.0*3600))

            avg_utilization_SE = float(total_busy_node_sec) / (period*TOTAL_NODES)
            return avg_utilization_SE, total_busy_node_sec, period, TOTAL_NODES  


    def get_avg_wait_last_period(self, period):
        '''get the average waiting in the last 'period' of time'''

        total_wait = 0
        now = self.get_current_time_sec()

        if period==0: #calculate the average waiting of current queuing jobs
            count = 0
            for job in self.queuing_jobs:
                submittime = job.submittime
                wait = now - submittime
                total_wait += wait
                count += 1

            if count > 0:
                avg_wait = total_wait / count
            else:
                avg_wait = 0

        elif period > 0:  #calculate the average waiting of jobs *started* within last period winodw
            start_point = now - period
            count = 0

            for k, v in self.started_job_dict.iteritems():
                jobid = k
                jobsubmit = float(v.get("submittime"))
                jobstart = float(v.get("start_time"))

                #jobs started within the period
                if jobstart > start_point and jobstart < now:
                    jobwait = jobstart - jobsubmit
                    total_wait += jobwait
                    count += 1

            if count > 0:
                avg_wait = total_wait / count
            else:
                avg_wait = 0

        # print avg_wait
        return avg_wait

    def get_aggr_wait_last_period(self, period=0):
        '''get the queue depth (aggregate waiting) in the last 'period' of time (in minutes)'''

        total_wait = 0
        now = self.get_current_time_sec()

        if period==0: #calculate the aggr waiting of current queuing jobs
            count = 0
            for job in self.queuing_jobs:
                submittime = job.submittime
                wait = now - submittime
                total_wait += wait
                count += 1

            #agg_wait = total_wait

        elif period > 0:  #calculate the aggr waiting of jobs *started* within last period winodw
            start_point = now - period
            count = 0

            for k, v in self.started_job_dict.iteritems():
                jobid = k
                jobsubmit = float(v.get("submittime"))
                jobstart = float(v.get("start_time"))

                #jobs started within the period
                if jobstart > start_point and jobstart < now:
                    jobwait = jobstart - jobsubmit
                    total_wait += jobwait
                    count += 1

            if count > 0:
                avg_wait = total_wait / count
            else:
                avg_wait = 0

        #print total_wait / 60
        return total_wait / 60

