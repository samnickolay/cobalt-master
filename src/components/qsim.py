#!/usr/bin/env python

"""Qsim executable."""

import inspect
import optparse
import os
import sys

import Cobalt.Util
from Cobalt.Components.evsim import EventSimulator
from Cobalt.Components.bqsim import BGQsim
#from Cobalt.Components.cqsim import ClusterQsim
from Cobalt.Components.histm import HistoryManager
from Cobalt.Components.base import run_component
from Cobalt.Components.slp import TimingServiceLocator
from Cobalt.Components.bgsched import BGSched
from Cobalt.Components.qsim import Qsimulator
from Cobalt.Proxy import ComponentProxy, local_components
from datetime import datetime
import time

arg_list = ['bgjob', 'cjob', 'config_file', 'outputlog', 'sleep_interval',
            'predict', 'coscheduling', 'wass', 'BG_Fraction', 'cluster_fraction',
            'bg_trace_start', 'bg_trace_end', 'c_trace_start', 'c_trace_end',
            'Anchor', 'anchor', 'vicinity', 'mate_ratio', 'batch', 'backfill', 'reserve_ratio',
            'metrica', 'balance_factor', 'window_size', 'adaptive',
            'realtime', 'preempt_overhead', # esjung: added realtime and preempt_overhead
            'times', 'name', 'checkpoint', 'rt_percent', # dwang: added 'times', 'name', 'checkp_sched_version#', 'rt_percent'
            'checkp_dsize', 'checkp_w_bandwidth', 'checkp_r_bandwidth', # dwang: added 'checkp_dsize', 'checkp_w_bandwidth',
            'checkp_t_internval', 'intv_pcent', # dwang: added 'checkp_r_bandwidth', 'checkp_t_internval'
            'checkpH_opt', # dwang: added 'checkpH_opt'
            'utility_function', 'job_length_type', 'rt_job_categories', 'checkp_overhead_percent'] # samnickolay: added 'utility_function'


def datetime_strptime (value, format):
    """Parse a datetime like datetime.strptime in Python >= 2.5"""
    return datetime(*time.strptime(value, format)[0:6])

class Option (optparse.Option):

    """An extended optparse option with cbank-specific types.

    Types:
    date -- parse a datetime from a variety of string formats
    """

    DATE_FORMATS = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%y-%m-%d",
        "%y-%m-%d %H:%M:%S",
        "%y-%m-%d %H:%M",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%y",
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%y %H:%M",
        "%Y%m%d",
    ]

    def check_date (self, opt, value):
        """Parse a datetime from a variety of string formats."""
        for format in self.DATE_FORMATS:
            try:
                dt = datetime_strptime(value, format)
            except ValueError:
                continue
            else:
                # Python can't translate dates before 1900 to a string,
                # causing crashes when trying to build sql with them.
                if dt < datetime(1900, 1, 1):
                    raise optparse.OptionValueError(
                        "option %s: date must be after 1900: %s" % (opt, value))
                else:
                    return dt
        raise optparse.OptionValueError(
            "option %s: invalid date: %s" % (opt, value))

    TYPES = optparse.Option.TYPES + ( "date", )


    TYPE_CHECKER = optparse.Option.TYPE_CHECKER.copy()
    TYPE_CHECKER['date'] = check_date


def profile_main(opts):
    '''profile integrated qsim'''
    import hotshot, hotshot.stats
    prof = hotshot.Profile("qsim.profile")
    prof.runcall(integrated_main, opts)

def integrated_main(options):
    TimingServiceLocator()

    if opts.predict:
        histm = HistoryManager(**options)

    evsim = EventSimulator(**options)

    if opts.bgjob:
        bqsim = BGQsim(**options)
    if opts.cjob:
        cqsim = ClusterQsim(**options)

    if opts.bgjob and opts.cjob and opts.coscheduling:
        print "inserting 'unhold' events into event list..."
        if opts.coscheduling[0] == "hold":
            evsim.init_unhold_events(0)
        if opts.coscheduling[1] == "hold":
            evsim.init_unhold_events(1)

    if opts.adaptive:
        print "inserting metrics monitor events into event list..."
        evsim.init_mmon_events()

    if opts.batch:
        print "simulation started"
    else:
        raw_input("Press Enter to start simulation...")

    starttime_sec = time.time()

    # dwang: 
    if opts.batch:
        # --> logRec
        #
        backf_fname = './_rec_' + options.get("name") + '/backf_' + options.get("name") + '_' + str(options.get("times")) + '.txt'
        fp_backf = open( backf_fname,'w+' )
        #
        pre_bj_fname = './_rec_' + options.get("name") + '/preBj_' + options.get("name") + '_' + str(options.get("times")) + '.txt'
        fp_pre_bj = open( pre_bj_fname,'w+' )
        while not evsim.is_finished():
            # dwang:
            #evsim.event_driver(opts.preempt_overhead)
            #evsim.event_driver(opts.preempt_overhead,options.get("name"), options.get("times"))
            evsim.event_driver(opts.preempt_overhead,fp_backf,fp_pre_bj, options.get("checkpoint"),
                               options.get("checkp_dsize"), options.get("checkp_w_bandwidth"), options.get("checkp_r_bandwidth"), 
                               options.get("checkp_t_internval"), options.get("intv_pcent"), options.get("checkpH_opt"),
                               options.get("job_length_type"), options.get("checkp_overhead_percentage") )
            # dwang
    else:
        while not evsim.is_finished():
            evsim.event_driver()
            os.system('clear')
            if opts.bgjob:
                bqsim.print_screen()
                pass
            if opts.cjob:
                cqsim.print_screen()
                pass

    if opts.bgjob:
	print "--- test dW-1"
        bqsim.monitor_metrics()
	print "--- test dW-2"

    if opts.bgjob:
        # dwang:
        #bqsim.post_simulation_handling()
        experiment_metrics = bqsim.post_simulation_handling(options.get("name"), options.get("times"), options.get("checkp_t_internval"), options.get("checkp_dsize"), options.get("checkp_w_bandwidth"))
        # dwang
    if opts.cjob:
        cqsim.post_simulation_handling()
    # dwang 

    endtime_sec = time.time()
    print "----Simulation is finished, please check output log for further analysis.----"
    return experiment_metrics
#    print "the simulation lasts %s seconds (~%s minutes)" % (int(endtime_sec - starttime_sec), int((endtime_sec - starttime_sec)/60))



if __name__ == "__main__":
    
    print 'current trace function', sys.gettrace()
    p = optparse.OptionParser()

    p.add_option("-j", "--job", dest="bgjob", type="string",
        help="file name of the job trace (when scheduling for bg system only)")
    p.add_option("-c", "--cjob", dest="cjob", type="string",
        help="file name of the job trace from the cluster system")
    p.add_option("-p", "--partition", dest="config_file", type="string",
        help="file name of the partition configuration of the Blue Gene system")
    p.add_option("-o", "--output", dest="outputlog", type="string",
        help="featuring string for output log")
    p.add_option("-i", "--interval", dest="sleep_interval", type="float",
        help="seconds to wait at each event when printing screens")
    p.add_option("-F", "--bg_frac", dest="BG_Fraction", type="float", default=False,
        help="parameter to adjust bg workload. All the interval between job arrivals will be multiplied with the parameter")
    p.add_option("-f", "--cluster_frac", dest="cluster_fraction", type="float", default=False,
        help="parameter to adjust cluster workload. All the interval between job arrivals will be multiplied with the parameter")
    p.add_option(Option("-S", "--Start",
        dest="bg_trace_start", type="date",
        help="bg job submission times (in job trace) should be after 12.01am on this date.\
        By default it equals to the first job submission time in job trace 'bgjob'"))
    p.add_option(Option("-E", "--End",
        dest="bg_trace_end", type="date",
        help="bg job submission time (in job trace) should be prior to 12.01am on this date \
        By default it equals to the last job submission time in job trace 'bgjob'"))
    p.add_option(Option("-s", "--start",
        dest="c_trace_start", type="date",
        help="cluster job submission times (in job trace) should be after 12.01am on this date. \
        By default it equals to the first job submission time in job trace 'cjob'"))
    p.add_option(Option("-e", "--end",
        dest="c_trace_end", type="date",
        help="cluster job submission time (in job trace) should be prior to 12.01am on this date \
        By default it equals to the last job submission time in job trace 'cjob'"))
    p.add_option(Option("-A", "--Anchor",
        dest="Anchor", type="date",
        help="the virtual start date of simulation for bqsim. If not specified, it is same as bg_trace_start"))
    p.add_option(Option("-a", "--anchor",
        dest="anchor", type="date",
        help="the virtual start date of simulation for bqsim. If not specified, it is same as c_trace_start"))
    p.add_option("-P", "--prediction", dest="predict", type="string", default=False,
        help="[xyz] x,y,z=0|1. x,y,z==1 means to use walltime prediction for (x:queuing / y:backfilling / z:running) jobs")
    p.add_option("-W", "--walltimeaware", dest="wass", type="string", default=False,
        help="[cons | aggr | both] specify the walltime aware spatial scheduling scheme: cons=conservative scheme, aggr=aggressive scheme, both=cons+aggr")
    p.add_option("-C", "--coscheduling", dest="coscheduling", nargs=2, type="string", default=False,
        help="[x y] (x,y=hold | yield). specify the coscheduling scheme: 'hold' or 'yield' resource if mate job can not run. x for bqsim, y for cqsim.")
    p.add_option("-v", "--vicinity", dest="vicinity", type="float", default=0.0,
        help="Threshold to determine mate jobs in coscheduling. \
        Two jobs can be considered mated only if their submission time difference is smaller than 'vicinity'")
    p.add_option("-r", "--ratio", dest="mate_ratio", type="float", default=0.0,
        help="Specifies the ratio of number mate jobs to number total jobs. Used in the case two job traces have the same number of total jobs.")
    p.add_option("-b", "--batch", dest="batch", action = "store_true", default = False,
        help="enable batch execution model, do not print screen")
    p.add_option(Option("-l", "--backfill",
        dest="backfill", type="string",
        help="specify backfilling scheme [ff|bf|sjfb] ff=first-fit, bf=best-fit, sjfb=short-job-first backfill"))
    p.add_option(Option("-R", "--reservation",
        dest="reserve_ratio", type="float", default=0.0,
        help="float (0--1), specify the proportion of reserved jobs in the job trace, by default it is 0."))
    p.add_option("-m", "--metrica", dest="metrica", action = "store_true", default = False,
        help="enable metric aware job scheduling")
    p.add_option("--bf", dest="balance_factor", type = "string", default = "1.0",
        help="balance factor used for metric aware job scheduling.")
    p.add_option("-w","--win_size", dest="window_size", type = "int", default = "1",
        help="window size used in window based job allocation. default is 1.")
    p.add_option("--adaptive", dest="adaptive", type = "string", default = False,
        help="enable adaptive policy tuning and specify scheme.  [00 | 10 | 01 | 11] ")
    
    # esjung: add options for realtime jobs and preemption
    p.add_option("-t", "--realtime", dest="realtime", nargs=3, type="string", default=False,
        help="[frequency duration nodes]")
    p.add_option("--preemption", dest="preempt_overhead", type="float",
        help="mins to preempt existing jobs")
    # esjung
    
    # dwang: add options for simulation time input
    p.add_option("-X", "--times", dest="times", type="int", default=1,
        help="Specifies the total times of simulation.")
    p.add_option("-N", "--name", dest="name", type="string",
        help="Specifies the name of simulation.")
    p.add_option("-H", "--checkpoint", dest="checkpoint", type="string",
        help="Specifies checkpointing-based scheduler version.")
    p.add_option("-Y", "--precent", dest="rt_percent", type="int",
        help="Specifies percentage of jobs random selected to be real-time job.")
    ## 'checkp_dsize', 'checkp_w_bandwidth', 'checkp_r_bandwidth', 'checkp_t_internval'
    p.add_option("--checkp_dsize", dest="checkp_dsize", type="int",
        help="Specifies checkpointing data size (MB) per node.")
    p.add_option("--checkp_w_bandwidth", dest="checkp_w_bandwidth", type="int",
        help="Specifies single-level checkpointing PFS write bandwidth (GB/s).")
    p.add_option("--checkp_r_bandwidth", dest="checkp_r_bandwidth", type="int",
        help="Specifies single-level checkpointing PFS read bandwidth (GB/s).")
    p.add_option("--checkp_t_internval", dest="checkp_t_internval", type="float",
        help="Specifies checkpoint internval (s) for periodical checkpointing.")
    p.add_option("--intv_pcent", dest="intv_pcent", type="float",
        help="Specifies checkpoint internval percentage of application walltime for checkpointing.")
    ## 'checkpH_opt',
    p.add_option("--checkpH_opt", dest="checkpH_opt", type="string",
        help="Specifies advanced-heuristics used for batch job checkpointing.")
    # dwang

    ###
    # samnickolay
    p.add_option("-U", "--utility_function", dest="utility_function", type="string", default='default',
                 help="Specifies utility function to use when computing jobs' utility scores. Default is 'default'")
    # samnickolay
    ###

    ###
    # samnickolay
    p.add_option("-L", "--job_length_type", dest="job_length_type", type="string", default='default',
                 help="Specifies which job length type to use for slowdown calculations ('walltime', 'actual', 'predicted')."
                      + " Only needed when using schedule_jobs_wcheckp_v2p_sam_v1.'")
    p.add_option("-J", "--rt_job_categories", dest="rt_job_categories", type="string", default='default',
                 help="Specifies which job categories can be realtime jobs."
                      + " Only needed when using schedule_jobs_wcheckp_v2p_sam_v1.'")
    p.add_option("-O", "--overhead_checkpoint_percent", dest="checkp_overhead_percent", type="float", default=-1,
                 help="Specifies which the overhead (percentage of walltime) for checkpointing jobs"
                      + " Only needed when using schedule_jobs_wcheckp_v2p_app_sam_v1.'")
    # samnickolay
    ###


    start_sec = time.time()

    coscheduling_schemes = ["hold", "yield"]
    wass_schemes = ["cons", "aggr", "both"]

    opts, args = p.parse_args()

    if not opts.bgjob and not opts.cjob:
        print "Error: Please specify at least one job trace!"
        p.print_help()
        sys.exit()

    if opts.bgjob and not opts.config_file:
        print "Error: Please specify partition configuration file for the Blue Gene system"
        p.print_help()
        sys.exit()

    if opts.coscheduling:
        print opts.coscheduling
        scheme1 = opts.coscheduling[0]
        if len(opts.coscheduling) == 2:
            scheme2 = opts.coscheduling[1]

        if not (scheme1 in coscheduling_schemes and scheme2 in coscheduling_schemes):
            print "Error: invalid coscheduling scheme '%s'. Valid schemes are: %s" % (opts.coscheduling,  coscheduling_schemes)
            p.print_help()
            sys.exit()

    if opts.wass:
        if not opts.wass in wass_schemes:
            print "Error: invalid walltime-aware spatial scheduling scheme '%s'. Valid schemes are: %s" % (opts.wass,  wass_schemes)
            p.print_help()
            sys.exit()

    if opts.predict:
        invalid = False
        scheme = opts.predict
        if not len(scheme) == 3:
            invalid = True
        else:
            for s in scheme:
                if s not in ['0', '1']:
                    invalid = True
        if invalid:
            print "Error: invalid prediction scheme %s. Valid schemes are: xyz, x,y,z=0|1" % (scheme)
            p.print_help()
            sys.exit()

    if opts.bg_trace_start:
        print "bg trace start date=", opts.bg_trace_start
        t_tuple = time.strptime(str(opts.bg_trace_start), "%Y-%m-%d %H:%M:%S")
        opts.bg_trace_start = time.mktime(t_tuple)
    if opts.bg_trace_end:
        print "bg trace end date=", opts.bg_trace_end
        t_tuple = time.strptime(str(opts.bg_trace_end), "%Y-%m-%d %H:%M:%S")
        opts.bg_trace_end = time.mktime(t_tuple)

    if opts.c_trace_start:
        print "cluster trace start date=", opts.c_trace_start
        t_tuple = time.strptime(str(opts.c_trace_start), "%Y-%m-%d %H:%M:%S")
        opts.c_trace_start = time.mktime(t_tuple)
    if opts.c_trace_end:
        print "cluster trace end date=", opts.c_trace_end
        t_tuple = time.strptime(str(opts.c_trace_end), "%Y-%m-%d %H:%M:%S")
        opts.c_trace_end = time.mktime(t_tuple)

    if opts.Anchor:
        print "bg simulation start date=", opts.Anchor
        t_tuple = time.strptime(str(opts.Anchor), "%Y-%m-%d %H:%M:%S")
        opts.Anchor = time.mktime(t_tuple)
    if opts.anchor:
        print "cluster simulation start date=", opts.anchor
        t_tuple = time.strptime(str(opts.anchor), "%Y-%m-%d %H:%M:%S")
        opts.anchor = time.mktime(t_tuple)

    options = {}
    for argname in arg_list:
	# print("[dw] argname: %s. " %argname)
        if getattr(opts, argname):
            options[argname] = getattr(opts, argname)
	    ### print(" opt_arg: " %options.get(argname))

    # dwang:
    if opts.times:
        print("[dw] the simulation will run %d times. " %opts.times)
    # dwang

    # dwang:
    ### print("[dw_checkp_dsize] --> %d. " %options.get("checkp_dsize")) 
    print("[dw_ckp_intv_pcent] --> %f " %options.get("intv_pcent")) 
    print("[dw_ckp_backfill] --> %s " %options.get("backfill"))
    print("[dw_ckpH_opt] --> %s " %options.get("checkpH_opt"))
    # dwang

    # samnickolay
    print("[samnickolay] utility function = %s" % options.get("utility_function"))
    # samnickolay

    # dwang:
    # integrated_main(options)
    experiment_metrics_list = []
    for itimes in range(0,opts.times):
        options["times"] = itimes
        print("[dw_key] -----> Simu %d times ------------------------------  " %options.get("times"))
        experiment_metrics = integrated_main(options)
        experiment_metrics_list.append(experiment_metrics)
    # dwang

    #profile_main(options)

    end_sec = time.time()

    print "the simulation totally lasts %s seconds (~%s minutes)" % (int(end_sec - start_sec), int((end_sec - start_sec)/60))

    print experiment_metrics_list

    print "Trials: %s" %opts.times

    output_values_set = ()

    experiment_metrics = {}
    for key, value in experiment_metrics_list[0].iteritems():
        temp_metric_values = [tmp_experiment_metrics[key] for tmp_experiment_metrics in experiment_metrics_list if tmp_experiment_metrics[key] != 0]
        if len(temp_metric_values) > 0:
            avg_value = sum(temp_metric_values) / float(len(temp_metric_values))
        else:
            avg_value = -1.0
        experiment_metrics[key] = avg_value
        print('%s: %s' % (key, avg_value ))
        print([tmp_experiment_metrics[key] for tmp_experiment_metrics in experiment_metrics_list])

        # avg_value = sum([tmp_experiment_metrics[key] for tmp_experiment_metrics in experiment_metrics_list])
        # experiment_metrics[key] = avg_value / float(len(experiment_metrics_list))
        # print('%s: %s' % (key, avg_value / float(len(experiment_metrics_list))))

    output_string = "-----\n"
    output_string += "metrics:\n"
    output_string += str(experiment_metrics["makespan"]) + "\n"
    output_string += str(experiment_metrics["system_utilization"]) + "\n"
    output_string += "\n"
    output_string += str(experiment_metrics["makespan_trimmed"]) + "\n"
    output_string += str(experiment_metrics["system_utilization_trimmed"]) + "\n"

    job_categories = ['', 'narrow_short_', 'narrow_long_', 'wide_short_', 'wide_long_']
    job_types = ['rt', 'batch']
    job_metrics = ['slowdown_', 'turnaround_time_', 'count_']

    trimmed_jobs = ['', '_trimmed']

    output_string += "+++++\n"
    output_string += "All Jobs\n"
    for job_category in job_categories:
        for job_metric in job_metrics:
            for job_type in job_types:
                output_string += "\n" + str(experiment_metrics[job_category + job_metric + job_type])
        output_string += "\n"

    output_string += "=====\n"
    output_string += "Trimmed Jobs\n"
    for job_category in job_categories:
        for job_metric in job_metrics:
            for job_type in job_types:
                output_string += "\n" + str(experiment_metrics[job_category + job_metric + job_type + '_trimmed'])
        output_string += "\n"

    print output_string

    dir_name = './slowdown_results/'
    import os
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)

    outputfile_name = dir_name + options.get("name")

    print '\nSaving experiment results to file - ', outputfile_name, '\n'

    with open(outputfile_name, "a") as outputfile:

        outputfile.write("\n\n")
        outputfile.write(str(options.get("name")) + "\n")
        outputfile.write("-----\n")

        outputfile.write(str(options.get("rt_percent")) + "\n")
        outputfile.write(str(options.get("rt_job_categories")) + "\n")
        try:
            val = options.get("job_length_type")
        except:
            val = ""
        outputfile.write(str(options.get("checkpoint")) + "-" + str(val) + "\n")

        outputfile.write(output_string)