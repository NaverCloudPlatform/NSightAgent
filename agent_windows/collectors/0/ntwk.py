import json
import os
import time

import chardet
import psutil
import sys

from diskcache import Cache


def main(argv):
    collectors_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    cache = Cache(os.path.join(collectors_dir, 'cache/script/network'))

    dic = psutil.net_if_addrs()
    counters = psutil.net_io_counters(pernic=True)

    out_list = []

    avg_snd_bps = 0.0
    max_snd_bps = 0.0
    avg_rcv_bps = 0.0
    max_rcv_bps = 0.0
    avg_snd_pps = 0.0
    max_snd_pps = 0.0
    avg_rcv_pps = 0.0
    max_rcv_pps = 0.0
    avg_snd_fail_packt_cnt = 0.0
    avg_rcv_fail_packt_cnt = 0.0

    nic_count = 0

    for name in dic:
        snic_list = dic[name]
        mac = None
        ipv4 = None
        for snic in snic_list:
            if snic.family == -1:
                mac = snic.address.replace('-', ':')
            elif snic.family == 2:
                ipv4 = snic.address

        if mac is None or ipv4 is None or mac == '00:00:00:00:00:00':
            continue

        nic_count += 1

        dimensions = {'nic_desc': name.decode(chardet.detect(name)['encoding']).encode('utf-8'),
                      'nic_ip': ipv4,
                      'macaddr': mac}
        metrics = {}
        timestamp = int(time.time() * 1000)

        snetio = counters[name]

        snd_bps = counter_calc_delta(cache, '%s_bytes_sent' % mac, snetio.bytes_sent) * 8
        rcv_bps = counter_calc_delta(cache, '%s_bytes_recv' % mac, snetio.bytes_recv) * 8
        snd_pps = counter_calc_delta(cache, '%s_packets_sent' % mac, snetio.packets_sent)
        rcv_pps = counter_calc_delta(cache, '%s_packets_recv' % mac, snetio.packets_recv)
        snd_fail_packt_cnt = counter_calc_delta(cache, '%s_errout' % mac, snetio.errout)
        rcv_fail_packt_cnt = counter_calc_delta(cache, '%s_errin' % mac, snetio.errin)

        if snd_bps is not None:
            metrics['snd_bps'] = snd_bps / 60.0
            avg_snd_bps += metrics['snd_bps']
            if metrics['snd_bps'] > max_snd_bps:
                max_snd_bps = metrics['snd_bps']
        if rcv_bps is not None:
            metrics['rcv_bps'] = rcv_bps / 60.0
            avg_rcv_bps += metrics['rcv_bps']
            if metrics['rcv_bps'] > max_rcv_bps:
                max_rcv_bps = metrics['rcv_bps']
        if snd_pps is not None:
            metrics['snd_pps'] = snd_pps / 60.0
            avg_snd_pps += metrics['snd_pps']
            if metrics['snd_pps'] > max_snd_pps:
                max_snd_pps = metrics['snd_pps']
        if rcv_pps is not None:
            metrics['rcv_pps'] = rcv_pps / 60.0
            avg_rcv_pps += metrics['rcv_pps']
            if metrics['rcv_pps'] > max_rcv_pps:
                max_rcv_pps = metrics['rcv_pps']
        if snd_fail_packt_cnt is not None:
            metrics['snd_fail_packt_cnt'] = snd_fail_packt_cnt
            avg_snd_fail_packt_cnt += snd_fail_packt_cnt
        if rcv_fail_packt_cnt is not None:
            metrics['rcv_fail_packt_cnt'] = rcv_fail_packt_cnt
            avg_rcv_fail_packt_cnt += rcv_fail_packt_cnt

        if metrics:
            out = {'dimensions': dimensions,
                   'metrics': metrics,
                   'timestamp': timestamp}
            out_list.append(out)

    metrics = {}
    metrics['avg_snd_bps'] = avg_snd_bps / nic_count
    metrics['max_snd_bps'] = max_snd_bps
    metrics['avg_rcv_bps'] = avg_rcv_bps / nic_count
    metrics['max_rcv_bps'] = max_rcv_bps
    metrics['avg_snd_pps'] = avg_snd_pps / nic_count
    metrics['max_snd_pps'] = max_snd_pps
    metrics['avg_rcv_pps'] = avg_rcv_pps / nic_count
    metrics['max_rcv_pps'] = max_rcv_pps
    metrics['avg_snd_fail_packt_cnt'] = avg_snd_fail_packt_cnt / nic_count
    metrics['avg_rcv_fail_packt_cnt'] = avg_rcv_fail_packt_cnt / nic_count

    out = {'dimensions': {'schema_type': 'svr'},
           'metrics': metrics,
           'timestamp': int(time.time() * 1000)}
    out_list.append(out)

    print(json.dumps(out_list))
    sys.stdout.flush()

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
