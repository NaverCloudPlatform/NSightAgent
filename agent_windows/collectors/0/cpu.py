import json
import sys
import win32pdh

from apscheduler.schedulers.blocking import BlockingScheduler

sys.path.append(sys.argv[1])

from collectors.libs.time_util import get_ntp_time

global counter_dict
counter_dict = {}

global counter_metric
counter_metric = {'processor': 'used_rto',
                  'idle': 'idle_rto',
                  'user': 'user_rto',
                  'interrupt': 'interrupt_tm_rto',
                  'privileged': 'prv_mde_exec_tm_rto',
                  'dpc': 'dly_pcd_call_tm_rto',
                  'proc': 'proc_tm_rto'}

global counter_type
counter_type = {'processor': win32pdh.PDH_FMT_DOUBLE,
                'idle': win32pdh.PDH_FMT_DOUBLE,
                'user': win32pdh.PDH_FMT_DOUBLE,
                'interrupt': win32pdh.PDH_FMT_DOUBLE,
                'privileged': win32pdh.PDH_FMT_DOUBLE,
                'dpc': win32pdh.PDH_FMT_DOUBLE,
                'proc': win32pdh.PDH_FMT_DOUBLE}


def main(argv):
    scheduler = BlockingScheduler()

    global hq
    hq = win32pdh.OpenQuery()

    counters, instances = win32pdh.EnumObjectItems(None, None, 'Processor', win32pdh.PERF_DETAIL_WIZARD)

    for instance in instances:

        if instance == '_Total':
            continue

        processor_path = "\\Processor(%s)\\%% Processor Time" % instance
        idle_path = "\\Processor(%s)\\%% Idle Time" % instance
        user_path = "\\Processor(%s)\\%% User Time" % instance
        interrupt_path = "\\Processor(%s)\\%% Interrupt Time" % instance
        privileged_path = "\\Processor(%s)\\%% Privileged Time" % instance
        dpc_path = "\\Processor(%s)\\%% DPC Time" % instance
        proc_path = "\\Processor(%s)\\%% Processor Time" % instance

        counter_dict[instance] = {}
        counter_dict[instance]['processor'] = win32pdh.AddCounter(hq, processor_path)
        counter_dict[instance]['idle'] = win32pdh.AddCounter(hq, idle_path)
        counter_dict[instance]['user'] = win32pdh.AddCounter(hq, user_path)
        counter_dict[instance]['interrupt'] = win32pdh.AddCounter(hq, interrupt_path)
        counter_dict[instance]['privileged'] = win32pdh.AddCounter(hq, privileged_path)
        counter_dict[instance]['dpc'] = win32pdh.AddCounter(hq, dpc_path)
        counter_dict[instance]['proc'] = win32pdh.AddCounter(hq, proc_path)

    scheduler.add_job(query, 'cron', minute='*/1', second='0')
    scheduler.start()

    win32pdh.CloseQuery(hq)


def query():
    global hq
    global counter_dict

    win32pdh.CollectQueryData(hq)

    avg_used = 0
    max_used = 0.0
    avg_idle = 0
    avg_user = 0
    avg_interrupt = 0
    avg_prv = 0
    avg_dpc = 0
    avg_proc = 0

    # cpu_count = len(counter_dict)

    count_used = 0
    count_idle = 0
    count_user = 0
    count_interrupt = 0
    count_prv = 0
    count_dpc = 0
    count_proc = 0

    out_list = []

    ntp_checked, timestamp = get_ntp_time()

    for instance in counter_dict:

        dimensions = {'cpu_idx': instance}
        metrics = {}

        try:
            _, val = win32pdh.GetFormattedCounterValue(counter_dict[instance]['processor'], counter_type['processor'])
            metrics[counter_metric['processor']] = val
            avg_used += val
            if val > max_used:
                max_used = val
            count_used += 1
        except Exception as e:
            pass

        try:
            _, val = win32pdh.GetFormattedCounterValue(counter_dict[instance]['idle'], counter_type['idle'])
            metrics[counter_metric['idle']] = val
            avg_idle += val
            count_idle += 1
        except Exception as e:
            pass

        try:
            _, val = win32pdh.GetFormattedCounterValue(counter_dict[instance]['user'], counter_type['user'])
            metrics[counter_metric['user']] = val
            avg_user += val
            count_user += 1
        except Exception as e:
            pass

        try:
            _, val = win32pdh.GetFormattedCounterValue(counter_dict[instance]['interrupt'], counter_type['interrupt'])
            metrics[counter_metric['interrupt']] = val
            avg_interrupt += val
            count_interrupt += 1
        except Exception as e:
            pass

        try:
            _, val = win32pdh.GetFormattedCounterValue(counter_dict[instance]['privileged'], counter_type['privileged'])
            metrics[counter_metric['privileged']] = val
            avg_prv += val
            count_prv += 1
        except Exception as e:
            pass

        try:
            _, val = win32pdh.GetFormattedCounterValue(counter_dict[instance]['dpc'], counter_type['dpc'])
            metrics[counter_metric['dpc']] = val
            avg_dpc += val
            count_dpc += 1
        except Exception as e:
            pass

        try:
            _, val = win32pdh.GetFormattedCounterValue(counter_dict[instance]['proc'], counter_type['proc'])
            metrics[counter_metric['proc']] = val
            avg_proc += val
            count_proc += 1
        except Exception as e:
            pass

        if metrics:
            out = {'dimensions': dimensions,
                   'metrics': metrics,
                   'timestamp': timestamp,
                   'ntp_checked': ntp_checked}
            out_list.append(out)

    metrics = {}
    if count_used > 0:
        metrics['avg_cpu_used_rto'] = avg_used / count_used
        metrics['max_cpu_used_rto'] = max_used
    if count_idle > 0:
        metrics['avg_cpu_idle_rto'] = avg_idle / count_idle
    if count_user > 0:
        metrics['avg_cpu_user_rto'] = avg_user / count_user
    if count_interrupt > 0:
        metrics['avg_interrupt_tm_rto'] = avg_interrupt / count_interrupt
    if count_prv > 0:
        metrics['avg_prv_mde_exec_tm_rto'] = avg_prv / count_prv
    if count_dpc > 0:
        metrics['avg_dly_pcd_call_tm_rto'] = avg_dpc / count_dpc
    if count_proc > 0:
        metrics['avg_proc_tm_rto'] = avg_proc / count_proc

    if metrics:
        out = {'dimensions': {'schema_type': 'svr'},
               'metrics': metrics,
               'timestamp': timestamp,
               'ntp_checked': ntp_checked}
        out_list.append(out)

    # if cpu_count > 0:
    #     metrics = {'avg_cpu_used_rto': avg_used / cpu_count, 'max_cpu_used_rto': max_used,
    #                'avg_cpu_idle_rto': avg_idle / cpu_count, 'avg_cpu_user_rto': avg_user / cpu_count,
    #                'avg_interrupt_tm_rto': avg_interrupt / cpu_count, 'avg_prv_mde_exec_tm_rto': avg_prv / cpu_count,
    #                'avg_dly_pcd_call_tm_rto': avg_dpc / cpu_count, 'avg_proc_tm_rto': avg_proc / cpu_count}
    #     out = {'dimensions': {'schema_type': 'svr'},
    #            'metrics': metrics,
    #            'timestamp': timestamp,
    #            'ntp_checked': ntp_checked}
    #     out_list.append(out)

    if out_list:
        print(json.dumps(out_list))
        sys.stdout.flush()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
