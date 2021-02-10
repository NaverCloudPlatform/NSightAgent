import json
import sys
import win32pdh

import psutil
import pythoncom
import wmi
from apscheduler.schedulers.blocking import BlockingScheduler

sys.path.append(sys.argv[1])

from collectors.libs.time_util import get_ntp_time


def main(argv):
    scheduler = BlockingScheduler()

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
    ntp_checked, timestamp = get_ntp_time()

    mb = 1024 * 1024.0

    metrics['mem_mb'] = virtual_mem.total / mb
    metrics['used_mem_mb'] = virtual_mem.used / mb
    metrics['free_mem_mb'] = virtual_mem.free / mb
    metrics['mem_usert'] = virtual_mem.used * 100.0 / virtual_mem.total

    pythoncom.CoInitialize()
    os = wmi.WMI().Win32_OperatingSystem()[0]
    pythoncom.CoUninitialize()

    swap_total = long(os.SizeStoredInPagingFiles)
    swap_free = long(os.FreeSpaceInPagingFiles)
    swap_used = swap_total - swap_free
    swap_usert = swap_used * 100.0 / swap_total

    metrics['swap_mb'] = swap_total / 1024.0
    metrics['swap_free_mb'] = swap_free / 1024.0
    metrics['swap_used_mb'] = swap_used / 1024.0
    metrics['swap_usert'] = swap_usert

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
               'timestamp': timestamp,
               'ntp_checked': ntp_checked}
        out_list = [out]
        print(json.dumps(out_list))
        sys.stdout.flush()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
