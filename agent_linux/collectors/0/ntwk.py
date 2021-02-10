import json
import os
import re
import sys

sys.path.append(sys.argv[1])

from collectors.libs import time_util
from collectors.libs.cache import CacheProxy


def main(argv):

    cache = CacheProxy('ntwk')

    nics = net_if_addrs()
    # nic_count = len(nics)

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
    avg_clsn_packt_cnt = 0.0

    count_snd_bps = 0
    count_rcv_bps = 0
    count_snd_pps = 0
    count_rcv_pps = 0
    count_snd_fail = 0
    count_rcv_fail = 0
    count_clsn_packt = 0

    ntp_checked, timestamp = time_util.get_ntp_time()

    for nic in nics:
        name = nic['name']
        mac = nic['maddr']
        ip = nic['ip']

        dimensions = {'nic_desc': name,
                      'nic_ip': ip,
                      'macaddr': mac}
        metrics = {}

        try:
            f_bytes_sent = open('/sys/class/net/%s/statistics/tx_bytes' % name)
            f_bytes_recv = open('/sys/class/net/%s/statistics/rx_bytes' % name)
            f_packets_sent = open('/sys/class/net/%s/statistics/tx_packets' % name)
            f_packets_recv = open('/sys/class/net/%s/statistics/rx_packets' % name)
            f_errout = open('/sys/class/net/%s/statistics/tx_errors' % name)
            f_errin = open('/sys/class/net/%s/statistics/rx_errors' % name)
            f_collisions = open('/sys/class/net/%s/statistics/collisions' % name)
        except Exception:
            continue

        bytes_sent = long(f_bytes_sent.read())
        bytes_recv = long(f_bytes_recv.read())
        packets_sent = long(f_packets_sent.read())
        packets_recv = long(f_packets_recv.read())
        errout = long(f_errout.read())
        errin = long(f_errin.read())
        collisions = long(f_collisions.read())

        f_bytes_sent.close()
        f_bytes_recv.close()
        f_packets_sent.close()
        f_packets_recv.close()
        f_errout.close()
        f_errin.close()
        f_collisions.close()

        snd_bps = cache.counter_to_gauge('%s_bytes_sent' % mac, bytes_sent)
        rcv_bps = cache.counter_to_gauge('%s_bytes_recv' % mac, bytes_recv)
        snd_pps = cache.counter_to_gauge('%s_packets_sent' % mac, packets_sent)
        rcv_pps = cache.counter_to_gauge('%s_packets_recv' % mac, packets_recv)
        snd_fail_packt_cnt = cache.counter_to_gauge('%s_errout' % mac, errout)
        rcv_fail_packt_cnt = cache.counter_to_gauge('%s_errin' % mac, errin)
        clsn_packt_cnt = cache.counter_to_gauge('%s_collisions' % mac, collisions)

        if snd_bps is not None:
            metrics['snd_bps'] = snd_bps * 8 / 60.0
            avg_snd_bps += metrics['snd_bps']
            if metrics['snd_bps'] > max_snd_bps:
                max_snd_bps = metrics['snd_bps']
            count_snd_bps += 1
        if rcv_bps is not None:
            metrics['rcv_bps'] = rcv_bps * 8 / 60.0
            avg_rcv_bps += metrics['rcv_bps']
            if metrics['rcv_bps'] > max_rcv_bps:
                max_rcv_bps = metrics['rcv_bps']
            count_rcv_bps += 1
        if snd_pps is not None:
            metrics['snd_pps'] = snd_pps / 60.0
            avg_snd_pps += metrics['snd_pps']
            if metrics['snd_pps'] > max_snd_pps:
                max_snd_pps = metrics['snd_pps']
            count_snd_pps += 1
        if rcv_pps is not None:
            metrics['rcv_pps'] = rcv_pps / 60.0
            avg_rcv_pps += metrics['rcv_pps']
            if metrics['rcv_pps'] > max_rcv_pps:
                max_rcv_pps = metrics['rcv_pps']
            count_rcv_pps += 1
        if snd_fail_packt_cnt is not None:
            metrics['snd_fail_packt_cnt'] = snd_fail_packt_cnt
            avg_snd_fail_packt_cnt += snd_fail_packt_cnt
            count_snd_fail += 1
        if rcv_fail_packt_cnt is not None:
            metrics['rcv_fail_packt_cnt'] = rcv_fail_packt_cnt
            avg_rcv_fail_packt_cnt += rcv_fail_packt_cnt
            count_rcv_fail += 1
        if clsn_packt_cnt is not None:
            metrics['clsn_packt_cnt'] = clsn_packt_cnt
            avg_clsn_packt_cnt += clsn_packt_cnt
            count_clsn_packt += 1

        if metrics:
            out = {'dimensions': dimensions,
                   'metrics': metrics,
                   'timestamp': timestamp,
                   'ntp_checked': ntp_checked}
            out_list.append(out)

    metrics = {}

    if count_snd_bps > 0:
        metrics['avg_snd_bps'] = avg_snd_bps / count_snd_bps
        metrics['max_snd_bps'] = max_snd_bps
    if count_rcv_bps > 0:
        metrics['avg_rcv_bps'] = avg_rcv_bps / count_rcv_bps
        metrics['max_rcv_bps'] = max_rcv_bps
    if count_snd_pps > 0:
        metrics['avg_snd_pps'] = avg_snd_pps / count_snd_pps
        metrics['max_snd_pps'] = max_snd_pps
    if count_rcv_pps > 0:
        metrics['avg_rcv_pps'] = avg_rcv_pps / count_rcv_pps
        metrics['max_rcv_pps'] = max_rcv_pps
    if count_snd_fail > 0:
        metrics['avg_snd_fail_packt_cnt'] = avg_snd_fail_packt_cnt / count_snd_fail
    if count_rcv_fail > 0:
        metrics['avg_rcv_fail_packt_cnt'] = avg_rcv_fail_packt_cnt / count_rcv_fail
    if count_clsn_packt > 0:
        metrics['avg_clsn_packt_cnt'] = avg_clsn_packt_cnt / count_clsn_packt

    if metrics:
        out = {'dimensions': {'schema_type': 'svr'},
               'metrics': metrics,
               'timestamp': timestamp,
               'ntp_checked': ntp_checked}
        out_list.append(out)

    if out_list:
        print(json.dumps(out_list))
        sys.stdout.flush()

    cache.close()


def net_if_addrs():
    nic_info_str = os.popen('ip addr').read().strip()

    infos = re.split('\d+:\s+(.*?):', nic_info_str)
    index = 0
    nic_array = []
    infos = infos[1:]

    while (index * 2) < len(infos):
        nic = {'name': infos[index * 2]}
        info_lines = infos[index * 2 + 1].split('\n')

        keep = True
        for line in info_lines:
            line = line.strip()

            if line.find('docker') > -1:
                keep = False
                break

            if line.find('cilium') > -1:
                keep = False
                break

            if line.find('nodelocaldns') > -1:
                keep = False
                break

            if line.startswith('link/'):
                vals = line.split()
                if vals[0].split('/')[1] == 'loopback' or vals[0].split('/')[1] == 'sit':
                    keep = False
                    break
                else:
                    nic['maddr'] = vals[1]

            if line.startswith('inet '):
                vals = line.split()
                nic['ip'] = vals[1].split('/')[0]

        if keep and 'maddr' in nic and 'ip' in nic:
            nic_array.append(nic)
        index += 1

    return nic_array


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
