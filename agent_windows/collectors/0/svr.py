import ConfigParser
import json
import os

import ntplib

import time

import psutil
import sys


def main(argv):
    config_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'script_configs'))
    config_file_path = os.path.join(config_dir, 'svr_config.cfg')

    cp = ConfigParser.ConfigParser()
    cp.read(config_file_path)

    ntp_host = cp.get('ntp', 'ntp_host')
    agent_version = cp.get('agent', 'agent_version')

    dimensions = {}
    metrics = {}
    timestamp = int(time.time() * 1000)

    metrics['boot_time'] = int(psutil.boot_time())

    client = ntplib.NTPClient()
    response = client.request(ntp_host)
    deviation = int(time.time() * 1000) - int(response.tx_time * 1000)
    metrics['time_deviation'] = deviation

    metrics['user_cnt'] = len(psutil.users())

    metrics['agent_version'] = agent_version

    out = {'dimensions': dimensions,
           'metrics': metrics,
           'timestamp': timestamp}
    out_list = [out]
    print(json.dumps(out_list))
    sys.stdout.flush()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
