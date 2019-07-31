import commands
import json
import os
import sys
import time

from diskcache import Cache


def main(argv):
    HZ = get_HZ()
    # print HZ
    total_mem = get_total_mem()
    # print total_mem

    collectors_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    cache = Cache(os.path.join(collectors_dir, 'cache/script/process'))

    cpu_total_jiffies = counter_calc_delta(cache, 'cpu_total_jiffies', get_cpu_total_jiffies())

    pids = get_pids()

    out_list = []

    mem_usert_total = 0.0

    for pid in pids:
        try:
            f_stat = open('/proc/%d/stat' % pid)
        except IOError:
            continue

        line = f_stat.readline()

        values = line.split(None)
        name = values[1][1: len(values[1]) - 1]
        status = values[2]
        ppid = values[3]

        dimensions = {'proc_id': pid,
                      'parent_pid': ppid,
                      'proc_nm': name,
                      'proc_stat_cd': status}
        metrics = {}

        used_cpu_jiff = counter_calc_delta(cache, 'used_cpu_jiff_%d' % pid, long(values[13] + values[14]))

        if used_cpu_jiff is None or cpu_total_jiffies is None:
            cpu_usert = None
            t_cpu_usert = None
        else:
            cpu_usert = used_cpu_jiff * 100.0 / cpu_total_jiffies
            t_cpu_usert = cpu_usert

        mem = long(values[23])
        if total_mem is None:
            mem_usert = 0.0
        else:
            mem_usert = mem * 100.0 / total_mem / 1024

        vir_mem = float(values[22]) / 1024.0

        cpu_time = float(values[13] + values[14]) / HZ

        priority = int(values[17])

        nice = int(values[18])

        thread_num = int(values[19])

        cpu_core_cnt = get_cpu_core_count()

        timestamp = time.time()
        start_time = timestamp - get_uptime() + float(values[21]) / HZ
        start_time_local = time.localtime(start_time)
        start_time_str = time.strftime("%Y-%m-%d %H:%M:%S", start_time_local)

        dimensions['strt_ymdt'] = start_time_str
        dimensions['proc_cpu_cnt'] = cpu_core_cnt

        if cpu_usert is not None:
            metrics['cpu_usert'] = cpu_usert

        if t_cpu_usert is not None:
            metrics['proc_t_cpu_usert'] = t_cpu_usert

        if mem_usert is not None:
            metrics['mem_usert'] = mem_usert
            mem_usert_total += mem_usert

        metrics['vir_mem_byt_cnt'] = vir_mem
        metrics['cpu_tm_ss'] = cpu_time
        metrics['prit_rnk'] = priority
        metrics['nice_val'] = nice
        metrics['thd_cnt'] = thread_num

        if metrics:
            out = {'dimensions': dimensions,
                   'metrics': metrics,
                   'timestamp': int(time.time() * 1000)}
            out_list.append(out)

        f_stat.close()

    out = {'dimensions': {'schema_type': 'svr'},
           'metrics': {'proc_cnt': len(pids), 'proc_mem_usert': mem_usert_total, 'run_que_len': get_run_que_len()},
           'timestamp': int(time.time() * 1000)
           }
    out_list.append(out)

    print(json.dumps(out_list))
    sys.stdout.flush()

    cache.close()


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


def counter_calc_delta(cache, key, value):
    last_value = cache.get(key)
    cache.set(key, value)
    if last_value is None:
        return None
    delta = value - last_value
    if delta < 0 or delta > last_value:
        return None
    return delta


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main(sys.argv))
