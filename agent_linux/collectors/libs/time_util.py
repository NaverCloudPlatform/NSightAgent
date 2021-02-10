import time

import ntplib

from collectors.configs.script_config import get_configs


def get_ntp_time():
    try:
        config = get_configs()

        ntp_host = config.get('ntp', 'ntp_host')

        client = ntplib.NTPClient()
        response = client.request(ntp_host)
        return True, long(response.tx_time * 1000)
    except Exception as e:
        return False, long(time.time() * 1000)
