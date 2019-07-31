import ctypes
import json
import psutil
import sys
import time
import win32pdh

from apscheduler.schedulers.blocking import BlockingScheduler


global counter_dict
counter_dict = {}


def main(argv):
    scheduler = BlockingScheduler()

    # print_counter_info('Process')

    global hq
    hq = win32pdh.OpenQuery()

    counters, instances = win32pdh.EnumObjectItems(None, None, 'Process', win32pdh.PERF_DETAIL_WIZARD)

    for instance in instances:

        if instance == '_Total' or instance == 'Idle' or instance == 'System':
            continue

        counter_dict[instance] = {}

        counter_dict[instance]['proc_id'] = win32pdh.AddCounter(hq, "\\Process(%s)\\ID Process" % instance)
        counter_dict[instance]['cpu_usert'] = win32pdh.AddCounter(hq, "\\Process(%s)\\%% Processor Time" % instance)
        counter_dict[instance]['cpu_tm_ss'] = win32pdh.AddCounter(hq, "\\Process(%s)\\Elapsed Time" % instance)
        counter_dict[instance]['pf'] = win32pdh.AddCounter(hq, "\\Process(%s)\\Page Faults/sec" % instance)

        counter_dict[instance]['prit_rnk'] = win32pdh.AddCounter(hq, "\\Process(%s)\\Priority Base" % instance)
        counter_dict[instance]['thd_cnt'] = win32pdh.AddCounter(hq, "\\Process(%s)\\Thread Count" % instance)
        counter_dict[instance]['vir_mem_byt_cnt'] = win32pdh.AddCounter(hq, "\\Process(%s)\\Private Bytes" % instance)
        counter_dict[instance]['parent_pid'] = win32pdh.AddCounter(hq, "\\Process(%s)\\Creating Process ID" % instance)
        counter_dict[instance]['svc_tm'] = win32pdh.AddCounter(hq, "\\Process(%s)\\Handle Count" % instance)

    scheduler.add_job(query, 'cron', minute='*/1', second='0')
    scheduler.start()

    win32pdh.CloseQuery(hq)


def query():
    global hq
    global counter_dict

    win32pdh.CollectQueryData(hq)

    proc_cnt = 0
    cpu_cnt = get_cpu_core_count()

    out_list = []

    proc_mem_usert = 0.0

    for instance in counter_dict:

        dimensions = {'proc_nm': instance}
        metrics = {}
        timestamp = int(time.time() * 1000)

        try:
            _, proc_id = win32pdh.GetFormattedCounterValue(counter_dict[instance]['proc_id'], win32pdh.PDH_FMT_LONG)
            metrics['proc_id'] = proc_id
            p = psutil.Process(pid=proc_id)
            dimensions['proc_nm'] = p.name()
            metrics['proc_stat_cd'] = p.status()
        except Exception:
            pass

        try:
            _, cpu_usert = win32pdh.GetFormattedCounterValue(counter_dict[instance]['cpu_usert'], win32pdh.PDH_FMT_LONG)
            if cpu_cnt is not None and cpu_cnt > 0:
                metrics['cpu_usert'] = cpu_usert / cpu_cnt
            else:
                metrics['cpu_usert'] = cpu_usert
        except Exception:
            pass

        try:
            _, cpu_tm_ss = win32pdh.GetFormattedCounterValue(counter_dict[instance]['cpu_tm_ss'], win32pdh.PDH_FMT_LONG)
            metrics['cpu_tm_ss'] = cpu_tm_ss
        except Exception:
            pass

        try:
            _, pf = win32pdh.GetFormattedCounterValue(counter_dict[instance]['pf'], win32pdh.PDH_FMT_LONG)
            metrics['pf'] = pf
        except Exception:
            pass

        try:
            _, prit_rnk = win32pdh.GetFormattedCounterValue(counter_dict[instance]['prit_rnk'], win32pdh.PDH_FMT_DOUBLE)
            metrics['prit_rnk'] = prit_rnk
        except Exception:
            pass

        try:
            _, thd_cnt = win32pdh.GetFormattedCounterValue(counter_dict[instance]['thd_cnt'], win32pdh.PDH_FMT_DOUBLE)
            metrics['thd_cnt'] = thd_cnt
        except Exception:
            pass

        try:
            _, vir_mem_byt_cnt = win32pdh.GetFormattedCounterValue(counter_dict[instance]['vir_mem_byt_cnt'], win32pdh.PDH_FMT_DOUBLE)
            metrics['vir_mem_byt_cnt'] = vir_mem_byt_cnt
            metrics['mem_usert'] = vir_mem_byt_cnt * 100.0 / psutil.virtual_memory().total
            proc_mem_usert += metrics['mem_usert']
        except Exception:
            pass

        try:
            _, parent_pid = win32pdh.GetFormattedCounterValue(counter_dict[instance]['parent_pid'], win32pdh.PDH_FMT_LONG)
            metrics['parent_pid'] = parent_pid
        except Exception:
            pass

        try:
            _, svc_tm = win32pdh.GetFormattedCounterValue(counter_dict[instance]['svc_tm'], win32pdh.PDH_FMT_DOUBLE)
            metrics['svc_tm'] = svc_tm * 1000
        except Exception:
            pass

        # ctypes.windll.user32.IsHungAppWindow
        proc_cnt += 1
        if metrics:
            metrics['proc_cpu_cnt'] = cpu_cnt
            out = {'dimensions': dimensions,
                   'metrics': metrics,
                   'timestamp': timestamp}
            out_list.append(out)

    out = {'dimensions': {'schema_type': 'svr'},
           'metrics': {'proc_cnt': proc_cnt, 'proc_mem_usert': proc_mem_usert},
           'timestamp': int(time.time() * 1000)
           }
    out_list.append(out)
    print(json.dumps(out_list))
    sys.stdout.flush()


def get_cpu_core_count():
    return psutil.cpu_count(logical=False)


def print_counter_info(counter_object):
    counters, instances = win32pdh.EnumObjectItems(None, None, counter_object, win32pdh.PERF_DETAIL_WIZARD)
    print counters
    print instances


if __name__ == '__main__':
    sys.exit(main(sys.argv))
