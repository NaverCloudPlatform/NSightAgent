import json
import sys

sys.path.append(sys.argv[1])

from collectors.libs import time_util
from collectors.libs.cache import CacheProxy


def main(argv):

    cache = CacheProxy('cpu')

    f = open('/proc/stat')

    out_list = []

    avg_used = 0
    max_used = 0.0
    avg_idle = 0
    avg_user = 0
    avg_sys = 0
    avg_nice = 0
    avg_irq = 0
    avg_softirq = 0
    avg_io_wait = 0

    count_used = 0
    count_idle = 0
    count_user = 0
    count_sys = 0
    count_nice = 0
    count_irq = 0
    count_softirq = 0
    count_io_wait = 0

    ntp_checked, timestamp = time_util.get_ntp_time()

    for line in f:
        values = line.split(None)
        name_len = len(values[0])
        if name_len > 3 and values[0][0:3] == 'cpu':
            cpu_id = int(values[0][3:name_len])

            dimensions = {'cpu_idx': cpu_id}
            metrics = {}

            user_cnt = long(values[1])
            nice_cnt = long(values[2])
            system_cnt = long(values[3])
            idle_cnt = long(values[4])
            iowait_cnt = long(values[5])
            irq_cnt = long(values[6])
            softirq_cnt = long(values[7])
            total_cnt = user_cnt + nice_cnt + system_cnt + idle_cnt + iowait_cnt + irq_cnt + softirq_cnt

            user = cache.counter_to_gauge('user_%d' % cpu_id, user_cnt)
            nice = cache.counter_to_gauge('nice_%d' % cpu_id, nice_cnt)
            system = cache.counter_to_gauge('system_%d' % cpu_id, system_cnt)
            idle = cache.counter_to_gauge('idle_%d' % cpu_id, idle_cnt)
            iowait = cache.counter_to_gauge('iowait_%d' % cpu_id, iowait_cnt)
            irq = cache.counter_to_gauge('irq_%d' % cpu_id, irq_cnt)
            softirq = cache.counter_to_gauge('softirq_%d' % cpu_id, softirq_cnt)
            total = cache.counter_to_gauge('total_%d' % cpu_id, total_cnt)

            if total is None or total == 0:
                continue

            if user is not None:
                metrics['user_rto'] = user * 100.0 / total
                avg_user += metrics['user_rto']
                count_user += 1

            if system is not None:
                metrics['sys_rto'] = system * 100.0 / total
                avg_sys += metrics['sys_rto']
                count_sys += 1

            if nice is not None:
                metrics['nice_rto'] = nice * 100.0 / total
                avg_nice += metrics['nice_rto']
                count_nice += 1

            if idle is not None:
                metrics['idle_rto'] = idle * 100.0 / total
                avg_idle += metrics['idle_rto']
                count_idle += 1

            if irq is not None:
                metrics['irq_rto'] = irq * 100.0 / total
                avg_irq += metrics['irq_rto']
                count_irq += 1

            if softirq is not None:
                metrics['softirq_rto'] = softirq * 100.0 / total
                avg_softirq += metrics['softirq_rto']
                count_softirq += 1

            if iowait is not None:
                metrics['io_wait_rto'] = iowait * 100.0 / total
                avg_io_wait += metrics['io_wait_rto']
                count_io_wait += 1

            if user is not None and nice is not None \
                    and system is not None and iowait is not None \
                    and irq is not None and softirq is not None:
                used = user + nice + system + iowait + irq + softirq
                metrics['used_rto'] = used * 100.0 / total
                avg_used += metrics['used_rto']
                if metrics['used_rto'] > max_used:
                    max_used = metrics['used_rto']
                count_used += 1

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
    if count_sys > 0:
        metrics['avg_cpu_sys_rto'] = avg_sys / count_sys
    if count_nice > 0:
        metrics['avg_nice_rto'] = avg_nice / count_nice
    if count_irq > 0:
        metrics['avg_irq_rto'] = avg_irq / count_irq
    if count_softirq > 0:
        metrics['avg_softirq_rto'] = avg_softirq / count_softirq
    if count_io_wait > 0:
        metrics['avg_io_wait_rto'] = avg_io_wait / count_io_wait

    # if cpu_count > 0:
    #     metrics = {'avg_cpu_used_rto': avg_used / cpu_count, 'max_cpu_used_rto': max_used,
    #                'avg_cpu_idle_rto': avg_idle / cpu_count, 'avg_cpu_user_rto': avg_user / cpu_count,
    #                'avg_cpu_sys_rto': avg_sys / cpu_count, 'avg_nice_rto': avg_nice / cpu_count,
    #                'avg_irq_rto': avg_irq / cpu_count, 'avg_softirq_rto': avg_softirq / cpu_count,
    #                'avg_io_wait_rto': avg_io_wait / cpu_count}

    if metrics:
        out = {'dimensions': {'schema_type': 'svr'},
               'metrics': metrics,
               'timestamp': timestamp,
               'ntp_checked': ntp_checked}
        out_list.append(out)

    if out_list:
        print(json.dumps(out_list))
        sys.stdout.flush()

    f.close()
    cache.close()


# def counter_calc_delta(cache, key, value):
#     last_value = cache.get(key)
#     cache.set(key, value)
#     if last_value is None:
#         return None
#     delta = value - last_value
#     if delta < 0 or delta > last_value:
#         return None
#     return delta


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main(sys.argv))
