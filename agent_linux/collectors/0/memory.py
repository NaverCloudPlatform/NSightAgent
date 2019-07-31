#v0
import json
import os
import sys
import time

from diskcache import Cache


def main(argv):

    collectors_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    cache = Cache(os.path.join(collectors_dir, 'cache/script/memory'))

    f_mem_info = open('/proc/meminfo')

    for line in f_mem_info:
        values = line.split(None)
        if values[0] == 'MemTotal:':
            mem_mb = long(values[1]) / 1024.0
        if values[0] == 'MemFree:':
            free_mem_mb = long(values[1]) / 1024.0
            # used_mem_mb = mem_mb - free_mem_mb
            # mem_usert = used_mem_mb * 100 / mem_mb
        if values[0] == 'Shmem:':
            shared_mem_mb = long(values[1]) / 1024.0
        if values[0] == 'Buffers:':
            bffr_mb = long(values[1]) / 1024.0
        if values[0] == 'Cached:':
            cache_mb = long(values[1]) / 1024.0
        if values[0] == 'SwapTotal:':
            swap_mb = long(values[1]) / 1024.0
        if values[0] == 'SwapFree:':
            if swap_mb > 0.0:
                swap_free_mb = long(values[1]) / 1024.0
                swap_used_mb = swap_mb - swap_free_mb
                swap_usert = swap_used_mb * 100 / swap_mb
            else:
                swap_free_mb = 0.0
                swap_used_mb = 0.0
                swap_usert = 0.0

    used_mem_mb = mem_mb - free_mem_mb - bffr_mb - cache_mb
    mem_usert = used_mem_mb * 100 / mem_mb

    f_mem_info.close()

    f_vmstat = open('/proc/vmstat')

    for line in f_vmstat:
        values = line.split(None)
        if values[0] == 'pgpgin':
            pgin_mb = counter_calc_delta(cache, 'pgin', long(values[1]))
        if values[0] == 'pgpgout':
            pgout_mb = counter_calc_delta(cache, 'pgout', long(values[1]))
        if values[0] == 'pgfault':
            pgfault = counter_calc_delta(cache, 'pgfault', long(values[1]))

    f_vmstat.close()
    cache.close()

    dimensions = {}
    metrics = {}
    timestamp = int(time.time() * 1000)

    metrics['mem_mb'] = mem_mb
    metrics['free_mem_mb'] = free_mem_mb
    metrics['used_mem_mb'] = used_mem_mb
    metrics['mem_usert'] = mem_usert
    metrics['shared_mem_mb'] = shared_mem_mb
    metrics['bffr_mb'] = bffr_mb
    metrics['cache_mb'] = cache_mb
    metrics['swap_mb'] = swap_mb
    metrics['swap_free_mb'] = swap_free_mb
    metrics['swap_used_mb'] = swap_used_mb
    metrics['swap_usert'] = swap_usert

    if pgin_mb is not None:
        metrics['pgin_mb'] = pgin_mb / 1024.0 / 60
    if pgout_mb is not None:
        metrics['pgout_mb'] = pgout_mb / 1024.0 / 60
    if pgfault is not None:
        metrics['pgfault_tcnt'] = pgfault / 60

    out = {'dimensions': dimensions,
           'metrics': metrics,
           'timestamp': timestamp}
    out_list = [out]
    print(json.dumps(out_list))
    sys.stdout.flush()


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
