import json
import sys
import time
import win32pdh

from apscheduler.schedulers.blocking import BlockingScheduler

global counter_dict
counter_dict = {}

global counter_metric
counter_metric = {'processor': 'used_rto',
                  'idle': 'idle_rto',
                  'user': 'user_rto',
                  'interrupt': 'interrupt_tm_rto',
                  'privileged': 'prv_mde_exec_tm_rto',
                  'dpc': 'dly_pcd_call_tm_rto'}

global counter_type
counter_type = {'processor': win32pdh.PDH_FMT_DOUBLE,
                'idle': win32pdh.PDH_FMT_DOUBLE,
                'user': win32pdh.PDH_FMT_DOUBLE,
                'interrupt': win32pdh.PDH_FMT_DOUBLE,
                'privileged': win32pdh.PDH_FMT_DOUBLE,
                'dpc': win32pdh.PDH_FMT_DOUBLE}


def main(argv):
    scheduler = BlockingScheduler()

    # print_counter_info('Processor')

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

        counter_dict[instance] = {}
        counter_dict[instance]['processor'] = win32pdh.AddCounter(hq, processor_path)
        counter_dict[instance]['idle'] = win32pdh.AddCounter(hq, idle_path)
        counter_dict[instance]['user'] = win32pdh.AddCounter(hq, user_path)
        counter_dict[instance]['interrupt'] = win32pdh.AddCounter(hq, interrupt_path)
        counter_dict[instance]['privileged'] = win32pdh.AddCounter(hq, privileged_path)
        counter_dict[instance]['dpc'] = win32pdh.AddCounter(hq, dpc_path)

    scheduler.add_job(query, 'cron', minute='*/1', second='0')
    scheduler.start()

    win32pdh.CloseQuery(hq)


def query():
    global hq
    # global counter_handle
    global counter_dict

    win32pdh.CollectQueryData(hq)

    avg_used = 0
    max_used = 0.0
    avg_idle = 0
    avg_user = 0
    avg_interrupt = 0
    avg_prv = 0
    avg_dpc = 0
    cpu_count = len(counter_dict)

    out_list = []
    for instance in counter_dict:

        dimensions = {'cpu_idx': instance}
        metrics = {}
        timestamp = int(time.time() * 1000)

        # for counter in counter_dict[instance]:
        #     try:
        #         _, val = win32pdh.GetFormattedCounterValue(counter_dict[instance][counter], counter_type[counter])
        #         metrics[counter_metric[counter]] = val
        #     except Exception as e:
        #         pass

        try:
            _, val = win32pdh.GetFormattedCounterValue(counter_dict[instance]['processor'], counter_type['processor'])
            metrics[counter_metric['processor']] = val
            avg_used += val
            if val > max_used:
                max_used = val
        except Exception as e:
            pass

        try:
            _, val = win32pdh.GetFormattedCounterValue(counter_dict[instance]['idle'], counter_type['idle'])
            metrics[counter_metric['idle']] = val
            avg_idle += val
        except Exception as e:
            pass

        try:
            _, val = win32pdh.GetFormattedCounterValue(counter_dict[instance]['user'], counter_type['user'])
            metrics[counter_metric['user']] = val
            avg_user += val
        except Exception as e:
            pass

        try:
            _, val = win32pdh.GetFormattedCounterValue(counter_dict[instance]['interrupt'], counter_type['interrupt'])
            metrics[counter_metric['interrupt']] = val
            avg_interrupt += val
        except Exception as e:
            pass

        try:
            _, val = win32pdh.GetFormattedCounterValue(counter_dict[instance]['privileged'], counter_type['privileged'])
            metrics[counter_metric['privileged']] = val
            avg_prv += val
        except Exception as e:
            pass

        try:
            _, val = win32pdh.GetFormattedCounterValue(counter_dict[instance]['dpc'], counter_type['dpc'])
            metrics[counter_metric['dpc']] = val
            avg_dpc += val
        except Exception as e:
            pass

        if metrics:
            out = {'dimensions': dimensions,
                   'metrics': metrics,
                   'timestamp': timestamp}
            out_list.append(out)

    if cpu_count > 0:
        metrics = {'avg_cpu_used_rto': avg_used / cpu_count, 'max_cpu_used_rto': max_used,
                   'avg_cpu_idle_rto': avg_idle / cpu_count, 'avg_cpu_user_rto': avg_user / cpu_count,
                   'avg_interrupt_tm_rto': avg_interrupt / cpu_count, 'avg_prv_mde_exec_tm_rto': avg_prv / cpu_count,
                   'avg_dly_pcd_call_tm_rto': avg_dpc / cpu_count}
        out = {'dimensions': {'schema_type': 'svr'},
               'metrics': metrics,
               'timestamp': int(time.time() * 1000)}
        out_list.append(out)

    print(json.dumps(out_list))
    sys.stdout.flush()


def print_counter_info(counter_object):
    counters, instances = win32pdh.EnumObjectItems(None, None, counter_object, win32pdh.PERF_DETAIL_WIZARD)
    print counters
    print instances


if __name__ == '__main__':
    sys.exit(main(sys.argv))
