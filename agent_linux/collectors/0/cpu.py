#v0
import json
import os
import sys
import time

from diskcache import Cache


def main(argv):

    collectors_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    cache = Cache(os.path.join(collectors_dir, 'cache/script/cpu'))

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

    cpu_count = 0

    for line in f:
        values = line.split(None)
        name_len = len(values[0])
        if name_len > 3 and values[0][0:3] == 'cpu':
            cpu_id = int(values[0][3:name_len])
            cpu_count += 1

            dimensions = {'cpu_idx': cpu_id}
            metrics = {}
            timestamp = int(time.time() * 1000)

            user_cnt = long(values[1])
            nice_cnt = long(values[2])
            system_cnt = long(values[3])
            idle_cnt = long(values[4])
            iowait_cnt = long(values[5])
            irq_cnt = long(values[6])
            softirq_cnt = long(values[7])
            total_cnt = user_cnt + nice_cnt + system_cnt + idle_cnt + iowait_cnt + irq_cnt + softirq_cnt

            user = counter_calc_delta(cache, 'user_%d' % cpu_id, user_cnt)
            nice = counter_calc_delta(cache, 'nice_%d' % cpu_id, nice_cnt)
            system = counter_calc_delta(cache, 'system_%d' % cpu_id, system_cnt)
            idle = counter_calc_delta(cache, 'idle_%d' % cpu_id, idle_cnt)
            iowait = counter_calc_delta(cache, 'iowait_%d' % cpu_id, iowait_cnt)
            irq = counter_calc_delta(cache, 'irq_%d' % cpu_id, irq_cnt)
            softirq = counter_calc_delta(cache, 'softirq_%d' % cpu_id, softirq_cnt)
            total = counter_calc_delta(cache, 'total_%d' % cpu_id, total_cnt)

            if total is None or total == 0:
                continue

            if user is not None:
                metrics['user_rto'] = user * 100.0 / total
                avg_user += metrics['user_rto']

            if system is not None:
                metrics['sys_rto'] = system * 100.0 / total
                avg_sys += metrics['sys_rto']

            if nice is not None:
                metrics['nice_rto'] = nice * 100.0 / total
                avg_nice += metrics['nice_rto']

            if idle is not None:
                metrics['idle_rto'] = idle * 100.0 / total
                avg_idle += metrics['idle_rto']

            if irq is not None:
                metrics['irq_rto'] = irq * 100.0 / total
                avg_irq += metrics['irq_rto']

            if softirq is not None:
                metrics['softirq_rto'] = softirq * 100.0 / total
                avg_softirq += metrics['softirq_rto']

            if iowait is not None:
                metrics['io_wait_rto'] = iowait * 100.0 / total
                avg_io_wait += metrics['io_wait_rto']

            if user is not None and nice is not None \
                    and system is not None and iowait is not None \
                    and irq is not None and softirq is not None:
                used = user + nice + system + iowait + irq + softirq
                metrics['used_rto'] = used * 100.0 / total
                avg_used += metrics['used_rto']
                if metrics['used_rto'] > max_used:
                    max_used = metrics['used_rto']

            if metrics:
                out = {'dimensions': dimensions,
                       'metrics': metrics,
                       'timestamp': timestamp}
                out_list.append(out)

    if cpu_count > 0:
        metrics = {'avg_cpu_used_rto': avg_used / cpu_count, 'max_cpu_used_rto': max_used,
                   'avg_cpu_idle_rto': avg_idle / cpu_count, 'avg_cpu_user_rto': avg_user / cpu_count,
                   'avg_cpu_sys_rto': avg_sys / cpu_count, 'avg_nice_rto': avg_nice / cpu_count,
                   'avg_irq_rto': avg_irq / cpu_count, 'avg_softirq_rto': avg_softirq / cpu_count,
                   'avg_io_wait_rto': avg_io_wait / cpu_count}

        out = {'dimensions': {'schema_type': 'svr'},
               'metrics': metrics,
               'timestamp': int(time.time() * 1000)}
        out_list.append(out)

    print(json.dumps(out_list))
    sys.stdout.flush()

    f.close()
    cache.close()


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
