import json
import os
import sys
import time

import ntplib
import psutil

sys.path.append(sys.argv[1])

from collectors.configs.script_config import get_configs
from collectors.libs.cache import CacheProxy
from collectors.libs.average_recoder import AverageRecorder


def main(argv):
    config = get_configs()

    cache = CacheProxy('svr')

    ntp_host = config.get('ntp', 'ntp_host')
    agent_version = config.get('agent', 'agent_version')

    dimensions = {}
    metrics = {}

    load_average_1m = int(os.popen('wmic cpu get LoadPercentage').read().split()[1])
    avg_recoder = AverageRecorder(cache, "load_avg_")
    avg_recoder.record(load_average_1m, int(time.time() / 60))
    load_average_5m, load_average_15m = avg_recoder.get_avg()
    metrics['load_average_1m'] = load_average_1m / 100.0
    if load_average_5m is not None:
        metrics['load_average_5m'] = load_average_5m / 100.0
    if load_average_15m is not None:
        metrics['load_average_15m'] = load_average_15m / 100.0

    cache.close()

    metrics['boot_time'] = int(psutil.boot_time())

    timestamp = int(time.time() * 1000)
    ntp_checked = True

    try:
        client = ntplib.NTPClient()
        response = client.request(ntp_host)
        timestamp = int(response.tx_time * 1000)
        deviation = int(time.time() * 1000) - timestamp
        metrics['time_deviation'] = deviation
    except Exception:
        ntp_checked = False

    metrics['user_cnt'] = len(psutil.users())

    metrics['agent_version'] = agent_version

    out = {'dimensions': dimensions,
           'metrics': metrics,
           'timestamp': timestamp,
           'ntp_checked': ntp_checked}
    out_list = [out]
    print(json.dumps(out_list))
    sys.stdout.flush()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
