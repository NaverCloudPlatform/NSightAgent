import json

import psutil
import sys
import time
import win32pdh

from apscheduler.schedulers.blocking import BlockingScheduler


# global counter_metric
# counter_metric = {'pgin': 'pgin_mb',
#                   'pgout': 'pgout_mb',
#                   'pgfault': 'pgfault_rto'}
#
# global counter_type
# counter_type = {'pgin': win32pdh.PDH_FMT_LONG,
#                 'pgout': win32pdh.PDH_FMT_LONG,
#                 'pgfault': win32pdh.PDH_FMT_LONG}

def main(argv):
    scheduler = BlockingScheduler()

    # print_counter_info('Memory')

    global hq
    hq = win32pdh.OpenQuery()

    global chs
    chs = {'pgin': win32pdh.AddCounter(hq, "\\Memory\\Pages Input/sec"),
           'pgout': win32pdh.AddCounter(hq, "\\Memory\\Pages Output/sec"),
           'pgfault': win32pdh.AddCounter(hq, "\\Memory\\Page Faults/sec")}

    scheduler.add_job(query, 'cron', minute='*/1', second='0')
    scheduler.start()

    win32pdh.CloseQuery(hq)


def query():
    virtual_mem = psutil.virtual_memory()

    dimensions = {}
    metrics = {}
    timestamp = int(time.time() * 1000)

    mb = 1024 * 1024.0

    metrics['mem_mb'] = virtual_mem.total / mb
    metrics['used_mem_mb'] = virtual_mem.used / mb
    metrics['free_mem_mb'] = virtual_mem.free / mb
    metrics['mem_usert'] = virtual_mem.used * 100.0 / virtual_mem.total

    win32pdh.CollectQueryData(hq)

    try:
        _, val = win32pdh.GetFormattedCounterValue(chs['pgin'], win32pdh.PDH_FMT_LONG)
        metrics['pgin_mb'] = val / 1024.0
    except Exception as e:
        pass

    try:
        _, val = win32pdh.GetFormattedCounterValue(chs['pgout'], win32pdh.PDH_FMT_LONG)
        metrics['pgout_mb'] = val / 1024.0
    except Exception as e:
        pass

    try:
        _, val = win32pdh.GetFormattedCounterValue(chs['pgfault'], win32pdh.PDH_FMT_LONG)
        metrics['pgfault_tcnt'] = val
    except Exception as e:
        pass

    if metrics:
        out = {'dimensions': dimensions,
               'metrics': metrics,
               'timestamp': timestamp}
        out_list = [out]
        print(json.dumps(out_list))
        sys.stdout.flush()


def print_counter_info(counter_object):
    counters, instances = win32pdh.EnumObjectItems(None, None, counter_object, win32pdh.PERF_DETAIL_WIZARD)
    print counters
    print instances


if __name__ == '__main__':
    sys.exit(main(sys.argv))
