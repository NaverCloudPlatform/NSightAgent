import commands
import json
import os
import resource
import sys
import time

sys.path.append(sys.argv[1])

from collectors.libs import time_util
from collectors.libs.cache import CacheProxy


def main(argv):
    HZ = get_HZ()
    # print HZ
    total_mem = get_total_mem()
    # print total_mem

    cache = CacheProxy('process')

    cpu_total_jiffies = cache.counter_to_gauge('cpu_total_jiffies', get_cpu_total_jiffies())

    pids = get_pids()

    out_list = []

    mem_usert_total = 0.0
    count = 0

    top10 = []

    ntp_checked, timestamp = time_util.get_ntp_time()

    page_size = resource.getpagesize()

    for pid in pids:

        stat_path = '/proc/%d/stat' % pid

        if not os.path.isfile(stat_path):
            continue

        try:
            with open(stat_path, 'r') as f_stat:

                line = f_stat.readline()
                values = line.split(None)
                if len(values) < 24:
                    continue

                name = values[1][1: len(values[1]) - 1]
                status = values[2]
                ppid = values[3]

                dimensions = {'proc_id': pid,
                              'parent_pid': ppid,
                              'proc_nm': name,
                              'proc_stat_cd': status}
                metrics = {}

                used_cpu_jiff = cache.counter_to_gauge('used_cpu_jiff_%d' % pid, long(values[13]) + long(values[14]))

                if used_cpu_jiff is None or cpu_total_jiffies is None:
                    cpu_usert = None
                    t_cpu_usert = None
                else:
                    cpu_usert = used_cpu_jiff * 100.0 / cpu_total_jiffies
                    t_cpu_usert = cpu_usert

                mem = long(values[23]) * page_size
                if total_mem is None:
                    mem_usert = 0.0
                else:
                    mem_usert = mem * 100.0 / total_mem / 1024

                vir_mem = float(values[22]) / 1024.0

                cpu_time = (float(values[13]) + float(values[14])) / HZ

                priority = int(values[17])

                nice = int(values[18])

                thread_num = int(values[19])

                cpu_core_cnt = get_cpu_core_count()

                time_now = time.time()
                start_time = time_now - get_uptime() + float(values[21]) / HZ
                # start_time_local = time.localtime(start_time)
                # start_time_str = time.strftime("%Y-%m-%d %H:%M:%S", start_time_local)

                dimensions['strt_ymdt'] = long(start_time * 1000)
                dimensions['proc_cpu_cnt'] = cpu_core_cnt

                if cpu_usert is not None:
                    metrics['cpu_usert'] = cpu_usert

                if t_cpu_usert is not None:
                    metrics['proc_t_cpu_usert'] = t_cpu_usert

                if mem_usert is not None:
                    metrics['p_proc_mem_usert'] = mem_usert
                    mem_usert_total += mem_usert

                metrics['vir_mem_byt_cnt'] = vir_mem
                metrics['cpu_tm_ss'] = cpu_time
                metrics['prit_rnk'] = priority
                metrics['nice_val'] = nice
                metrics['thd_cnt'] = thread_num

                if metrics:
                    out = {'dimensions': dimensions,
                           'metrics': metrics,
                           'timestamp': timestamp,
                           'ntp_checked': ntp_checked}
                    top10_check_insert(top10, out)
                    # out_list.append(out)
                    count += 1
        except Exception:
            pass

    for item in top10:
        out_list.append(item)

    out = {'dimensions': {'schema_type': 'svr'},
           'metrics': {'proc_cnt': count, 'proc_mem_usert': mem_usert_total, 'run_que_len': get_run_que_len()},
           'timestamp': timestamp,
           'ntp_checked': ntp_checked
           }
    out_list.append(out)

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


def get_pids():
    return [int(x) for x in os.listdir('/proc') if x.isdigit()]


def get_HZ():
    (status, output) = commands.getstatusoutput('cat /boot/config-`uname -r` | grep CONFIG_HZ=')
    if status != 0:
        return None
    else:
        value = output.split('=')
        return long(value[1])


def get_total_mem():
    (status, output) = commands.getstatusoutput('cat /proc/meminfo | grep MemTotal')
    if status != 0:
        return None
    else:
        kv = output.split(None)
        return int(kv[1])


def get_uptime():
    f = open('/proc/uptime')
    values = f.readline().split(None)
    f.close()
    return float(values[0])


def get_run_que_len():
    f = open('/proc/stat')
    for line in f:
        if line.startswith('procs_running'):
            run_que_len = int(line.split()[1])
        pass
    f.close()
    return run_que_len


def get_cpu_total_jiffies():
    f = open('/proc/stat')
    values = f.readline().split(None)
    f.close()
    user = long(values[1])
    nice = long(values[2])
    system = long(values[3])
    idle = long(values[4])
    iowait = long(values[5])
    irq = long(values[6])
    softirq = long(values[7])
    total = user + nice + system + idle + iowait + irq + softirq
    return total


def get_cpu_core_count():
    count = 0
    f = open('/proc/stat')
    for line in f:
        values = line.split(None)
        if len(values[0]) >= 3 and values[0][0:3] == 'cpu':
            count += 1
    f.close()
    return count


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main(sys.argv))
