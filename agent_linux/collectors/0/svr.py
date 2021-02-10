import ConfigParser
import json
import os
import sys
import time

import ntplib

sys.path.append(sys.argv[1])

from collectors.configs.script_config import get_configs


def main(argv):
    config = get_configs()

    ntp_host = config.get('ntp', 'ntp_host')
    agent_version = config.get('agent', 'agent_version')

    dimensions = {}
    metrics = {}

    metrics['boot_time'] = long(boot_time())

    timestamp = long(time.time() * 1000)
    ntp_checked = True

    try:
        client = ntplib.NTPClient()
        response = client.request(ntp_host)
        timestamp = long(response.tx_time * 1000)
        deviation = long(time.time() * 1000) - timestamp
        metrics['time_deviation'] = deviation
    except Exception:
        ntp_checked = False

    metrics['user_cnt'] = user_cnt()

    load_array = load_averages()
    load_average_1m = float(load_array[0])
    load_average_5m = float(load_array[1])
    load_average_15m = float(load_array[2])
    metrics['load_average_1m'] = load_average_1m
    metrics['load_average_5m'] = load_average_5m
    metrics['load_average_15m'] = load_average_15m

    metrics['agent_version'] = agent_version

    out = {'dimensions': dimensions,
           'metrics': metrics,
           'timestamp': timestamp,
           'ntp_checked': ntp_checked}
    out_list = [out]
    print(json.dumps(out_list))
    sys.stdout.flush()


def boot_time():
    with open('/proc/stat') as f:
        for line in f:
            if line.startswith('btime'):
                ret = float(line.strip().split()[1])
                return ret


def load_averages():
    result = os.popen("uptime").read()
    key = 'load average:'
    index = result.find(key) + len(key)
    array = result[index:].strip().split(',')
    return array


def user_cnt():
    return int(os.popen("who | awk '{print $1}' | sort | uniq | wc -l").read().strip())


if __name__ == '__main__':
    sys.exit(main(sys.argv))
