import json
import sys
import win32pdh
from fnmatch import fnmatch

import psutil
from apscheduler.schedulers.blocking import BlockingScheduler

sys.path.append(sys.argv[1])

from collectors.libs.cache import CacheProxy
from collectors.libs.time_util import get_ntp_time

global counter_dict
counter_dict = {}


def main(argv):
    scheduler = BlockingScheduler()

    global hq
    hq = win32pdh.OpenQuery()

    counters, instances = win32pdh.EnumObjectItems(None, None, 'Process', win32pdh.PERF_DETAIL_WIZARD)

    proc_num = {}
    for instance in instances:

        if instance == '_Total' or instance == 'Idle' or instance == 'System':
            continue

        if instance in proc_num:
            proc_num[instance] = proc_num[instance] + 1
        else:
            proc_num[instance] = 1

        for instance in proc_num:
            num = proc_num[instance]
            for id in xrange(num):
                instance_with_id = '%s#%d' % (instance, id)
                counter_dict[instance_with_id] = {}

                path = win32pdh.MakeCounterPath((None, 'Process', instance, None, id, 'ID Process'))
                counter_dict[instance_with_id]['proc_id'] = win32pdh.AddCounter(hq, path)

                path = win32pdh.MakeCounterPath((None, 'Process', instance, None, id, '% Processor Time'))
                counter_dict[instance_with_id]['cpu_usert'] = win32pdh.AddCounter(hq, path)

                path = win32pdh.MakeCounterPath((None, 'Process', instance, None, id, 'Elapsed Time'))
                counter_dict[instance_with_id]['cpu_tm_ss'] = win32pdh.AddCounter(hq, path)

                path = win32pdh.MakeCounterPath((None, 'Process', instance, None, id, 'Page Faults/sec'))
                counter_dict[instance_with_id]['pf'] = win32pdh.AddCounter(hq, path)

                path = win32pdh.MakeCounterPath((None, 'Process', instance, None, id, 'Priority Base'))
                counter_dict[instance_with_id]['prit_rnk'] = win32pdh.AddCounter(hq, path)

                path = win32pdh.MakeCounterPath((None, 'Process', instance, None, id, 'Thread Count'))
                counter_dict[instance_with_id]['thd_cnt'] = win32pdh.AddCounter(hq, path)

                path = win32pdh.MakeCounterPath((None, 'Process', instance, None, id, 'Private Bytes'))
                counter_dict[instance_with_id]['vir_mem_byt_cnt'] = win32pdh.AddCounter(hq, path)

                path = win32pdh.MakeCounterPath((None, 'Process', instance, None, id, 'Creating Process ID'))
                counter_dict[instance_with_id]['parent_pid'] = win32pdh.AddCounter(hq, path)

                path = win32pdh.MakeCounterPath((None, 'Process', instance, None, id, 'Handle Count'))
                counter_dict[instance_with_id]['svc_tm'] = win32pdh.AddCounter(hq, path)

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

    top10 = []

    proc_mem_usert = 0.0

    cache = CacheProxy('plugin_process')
    config_process_list = cache.get('process_list')
    process_info_list = []

    ntp_checked, timestamp = get_ntp_time()

    for instance_with_id in counter_dict:

        instance = instance_with_id.split('#')[0]
        dimensions = {'proc_nm': instance}
        metrics = {}

        try:
            _, proc_id = win32pdh.GetFormattedCounterValue(counter_dict[instance_with_id]['proc_id'], win32pdh.PDH_FMT_LONG)
            metrics['proc_id'] = proc_id
            p = psutil.Process(pid=proc_id)
            dimensions['proc_nm'] = p.name()
            metrics['proc_stat_cd'] = p.status()
        except Exception:
            pass

        process_info = None
        if config_process_list:
            for p in config_process_list:
                if fnmatch(dimensions['proc_nm'], p):
                    process_info = {'pid': metrics['proc_id'],
                                    'name': dimensions['proc_nm'],
                                    'match': p}

        try:
            _, cpu_usert = win32pdh.GetFormattedCounterValue(counter_dict[instance_with_id]['cpu_usert'], win32pdh.PDH_FMT_DOUBLE)
            if cpu_cnt is not None and cpu_cnt > 0:
                metrics['cpu_usert'] = cpu_usert / cpu_cnt
            else:
                metrics['cpu_usert'] = cpu_usert
        except Exception:
            pass

        try:
            _, cpu_tm_ss = win32pdh.GetFormattedCounterValue(counter_dict[instance_with_id]['cpu_tm_ss'], win32pdh.PDH_FMT_LONG)
            metrics['cpu_tm_ss'] = cpu_tm_ss
        except Exception:
            pass

        try:
            _, pf = win32pdh.GetFormattedCounterValue(counter_dict[instance_with_id]['pf'], win32pdh.PDH_FMT_DOUBLE)
            metrics['pf'] = pf
        except Exception:
            pass

        try:
            _, prit_rnk = win32pdh.GetFormattedCounterValue(counter_dict[instance_with_id]['prit_rnk'], win32pdh.PDH_FMT_LONG)
            metrics['prit_rnk'] = prit_rnk
        except Exception:
            pass

        try:
            _, thd_cnt = win32pdh.GetFormattedCounterValue(counter_dict[instance_with_id]['thd_cnt'], win32pdh.PDH_FMT_LONG)
            metrics['thd_cnt'] = thd_cnt
        except Exception:
            pass

        try:
            _, vir_mem_byt_cnt = win32pdh.GetFormattedCounterValue(counter_dict[instance_with_id]['vir_mem_byt_cnt'], win32pdh.PDH_FMT_LONG)
            metrics['vir_mem_byt_cnt'] = vir_mem_byt_cnt
            metrics['p_proc_mem_usert'] = vir_mem_byt_cnt * 100.0 / psutil.virtual_memory().total
            proc_mem_usert += metrics['p_proc_mem_usert']
        except Exception:
            pass

        try:
            _, parent_pid = win32pdh.GetFormattedCounterValue(counter_dict[instance_with_id]['parent_pid'], win32pdh.PDH_FMT_LONG)
            metrics['parent_pid'] = parent_pid
        except Exception:
            pass

        try:
            _, svc_tm = win32pdh.GetFormattedCounterValue(counter_dict[instance_with_id]['svc_tm'], win32pdh.PDH_FMT_DOUBLE)
            metrics['svc_tm'] = svc_tm * 1000
        except Exception:
            pass

        proc_cnt += 1
        if metrics:
            metrics['proc_cpu_cnt'] = cpu_cnt
            out = {'dimensions': dimensions,
                   'metrics': metrics,
                   'timestamp': timestamp,
                   'ntp_checked': ntp_checked}
            top10_check_insert(top10, out)

            if process_info is not None:
                if 'cpu_usert' in metrics:
                    process_info['cpu_usert'] = metrics['cpu_usert']
                else:
                    process_info['cpu_usert'] = 0.0

                if 'p_proc_mem_usert' in metrics:
                    process_info['mem_usert'] = metrics['p_proc_mem_usert']
                else:
                    process_info['mem_usert'] = 0.0

                if 'vir_mem_byt_cnt' in metrics:
                    process_info['mem_byt_cnt'] = metrics['vir_mem_byt_cnt']
                else:
                    process_info['mem_byt_cnt'] = 0.0

                if 'thd_cnt' in metrics:
                    process_info['thd_cnt'] = metrics['thd_cnt']
                else:
                    process_info['thd_cnt'] = 0

                process_info_list.append(process_info)

    for item in top10:
        out_list.append(item)

    out = {'dimensions': {'schema_type': 'svr'},
           'metrics': {'proc_cnt': proc_cnt, 'proc_mem_usert': proc_mem_usert},
           'timestamp': timestamp,
           'ntp_checked': ntp_checked
           }
    out_list.append(out)

    if config_process_list:
        for p in config_process_list:
            pid_list = []

            process_count = 0
            tot_cpu_usert = 0.0
            tot_mem_usert = 0.0
            tot_mem_byt_cnt = 0.0
            tot_thd_cnt = 0

            for process_info in process_info_list:
                if process_info['match'] == p:
                    process_count += 1
                    tot_cpu_usert += process_info['cpu_usert']
                    tot_mem_usert += process_info['mem_usert']
                    tot_mem_byt_cnt += process_info['mem_byt_cnt']
                    tot_thd_cnt += process_info['thd_cnt']
                    pid_list.append(process_info['pid'])

            dimensions = {
                'proc_name': p,
                'schema_type': 'plugin_process'
            }

            pid_list_record = cache.get('pip_list_record_' + p)
            cache.set('pip_list_record_' + p, pid_list)
            if pid_list_record is None or len(pid_list_record) == 0:
                if len(pid_list) > 0:
                    is_process_up = 1
                else:
                    is_process_up = 0
            else:
                is_process_up = 1
                for pid in pid_list_record:
                    if pid not in pid_list:
                        is_process_up = 0
                        break

            if process_count == 0:
                metrics = {
                    'is_process_up': is_process_up,
                    'process_count': process_count,
                    'avg_cpu_usert': 0.0,
                    'avg_mem_usert': 0.0,
                    'avg_mem_byt_cnt': 0.0,
                    'avg_thd_cnt': 0,
                    'tot_cpu_usert': tot_cpu_usert,
                    'tot_mem_usert': tot_mem_usert,
                    'tot_mem_byt_cnt': tot_mem_byt_cnt,
                    'tot_thd_cnt': tot_thd_cnt
                }
            else:
                metrics = {
                    'is_process_up': is_process_up,
                    'process_count': process_count,
                    'avg_cpu_usert': tot_cpu_usert / process_count,
                    'avg_mem_usert': tot_mem_usert / process_count,
                    'avg_mem_byt_cnt': tot_mem_byt_cnt / process_count,
                    'avg_thd_cnt': tot_thd_cnt / float(process_count),
                    'tot_cpu_usert': tot_cpu_usert,
                    'tot_mem_usert': tot_mem_usert,
                    'tot_mem_byt_cnt': tot_mem_byt_cnt,
                    'tot_thd_cnt': tot_thd_cnt
                }

            plugin_process_out = {
                'dimensions': dimensions,
                'metrics': metrics,
                'timestamp': timestamp,
                'ntp_checked': ntp_checked
            }

            out_list.append(plugin_process_out)

    print(json.dumps(out_list))
    sys.stdout.flush()

    cache.close()


def top10_check_insert(arr, item):
    if 'cpu_usert' not in item['metrics']:
        return

    limit = 10
    for i in range(0, len(arr) - 1):
        if item['metrics']['cpu_usert'] > arr[i]['metrics']['cpu_usert']:
            arr.insert(i, item)
            if len(arr) > limit:
                arr.pop()
            return
    if len(arr) < limit:
        arr.append(item)
    elif len(arr) > limit:
        arr.pop()


def get_cpu_core_count():
    return psutil.cpu_count(logical=False)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
