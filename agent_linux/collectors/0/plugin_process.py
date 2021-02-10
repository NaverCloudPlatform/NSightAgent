import commands
import json
import os
import resource
import sys
from fnmatch import fnmatch

sys.path.append(sys.argv[1])

from collectors.libs import time_util
from collectors.libs.cache import CacheProxy
from collectors.libs.config_client import ConfigClient


def main(argv):
    if len(argv) < 4:
        print("error: parameters missing")
        return

    cache = CacheProxy('plugin_process')

    last_config_version = cache.get('version')
    config_version = int(argv[2])
    host_id = argv[3]
    if last_config_version is None or config_version != last_config_version:
        config_client = ConfigClient()
        current_version, config_process_list = config_client.get_user_config('plugin_process', host_id)
        if config_process_list is not None:
            cache.set('process_list', config_process_list)
            cache.set('version', current_version)
    else:
        config_process_list = cache.get('process_list')

    if not config_process_list:
        cache.close()
        return

    ntp_checked, timestamp = time_util.get_ntp_time()

    cpu_total_jiffies = cache.counter_to_gauge('cpu_total_jiffies', get_cpu_total_jiffies())
    total_mem = get_total_mem()
    pids = get_pids()

    process_info_list = []

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

                cmdline_path = '/proc/%d/cmdline' % pid
                if os.path.isfile(cmdline_path) and os.access(cmdline_path, os.R_OK):
                    with open(cmdline_path, 'r') as f_cmd:
                        cmdline = f_cmd.readline().replace('\0', ' ').strip()
                        if cmdline:
                            name = cmdline

                for p in config_process_list:
                    if fnmatch(name, p):
                        process_info = {'pid': pid,
                                        'name': name,
                                        'match': p}
                        status = values[2]
                        ppid = values[3]

                        process_info['parent_pid'] = ppid
                        process_info['proc_stat_cd'] = status

                        used_cpu_jiff = cache.counter_to_gauge('used_cpu_jiff_%d' % pid,
                                                               long(values[13]) + long(values[14]))

                        if used_cpu_jiff is None or cpu_total_jiffies is None:
                            cpu_usert = 0.0
                        else:
                            cpu_usert = used_cpu_jiff * 100.0 / cpu_total_jiffies

                        mem = long(values[23]) * page_size
                        if total_mem is None:
                            mem_usert = 0.0
                        else:
                            mem_usert = mem * 100.0 / total_mem / 1024

                        vir_mem = float(values[22]) / 1024.0

                        thread_num = int(values[19])

                        process_info['cpu_usert'] = cpu_usert
                        process_info['mem_usert'] = mem_usert
                        process_info['mem_byt_cnt'] = vir_mem
                        process_info['thd_cnt'] = thread_num

                        process_info_list.append(process_info)

        except Exception:
            pass

    out_list = []
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
            'proc_name': p
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

        out = {
            'dimensions': dimensions,
            'metrics': metrics,
            'timestamp': timestamp,
            'ntp_checked': ntp_checked
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


def get_cpu_total_jiffies():
    with open('/proc/stat') as f:
        values = f.readline().split(None)
        user = long(values[1])
        nice = long(values[2])
        system = long(values[3])
        idle = long(values[4])
        iowait = long(values[5])
        irq = long(values[6])
        softirq = long(values[7])
        total = user + nice + system + idle + iowait + irq + softirq
    return total


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main(sys.argv))
