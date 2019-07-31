import json
import os
import re
import sys
import time

from diskcache import Cache


def main(argv):
    collectors_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    cache = Cache(os.path.join(collectors_dir, 'cache/script/network'))

    nics = net_if_addrs()
    nic_count = len(nics)

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

    for nic in nics:
        name = nic['name']
        mac = nic['maddr']
        ip = nic['ip']

        dimensions = {'nic_desc': name,
                      'nic_ip': ip,
                      'macaddr': mac}
        metrics = {}

        f_bytes_sent = open('/sys/class/net/%s/statistics/tx_bytes' % name)
        f_bytes_recv = open('/sys/class/net/%s/statistics/rx_bytes' % name)
        f_packets_sent = open('/sys/class/net/%s/statistics/tx_packets' % name)
        f_packets_recv = open('/sys/class/net/%s/statistics/rx_packets' % name)
        f_errout = open('/sys/class/net/%s/statistics/tx_errors' % name)
        f_errin = open('/sys/class/net/%s/statistics/rx_errors' % name)
        f_collisions = open('/sys/class/net/%s/statistics/collisions' % name)

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

        snd_bps = counter_calc_delta(cache, '%s_bytes_sent' % mac, bytes_sent) * 8
        rcv_bps = counter_calc_delta(cache, '%s_bytes_recv' % mac, bytes_recv) * 8
        snd_pps = counter_calc_delta(cache, '%s_packets_sent' % mac, packets_sent)
        rcv_pps = counter_calc_delta(cache, '%s_packets_recv' % mac, packets_recv)
        snd_fail_packt_cnt = counter_calc_delta(cache, '%s_errout' % mac, errout)
        rcv_fail_packt_cnt = counter_calc_delta(cache, '%s_errin' % mac, errin)
        clsn_packt_cnt = counter_calc_delta(cache, '%s_collisions' % mac, collisions)

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
        if clsn_packt_cnt is not None:
            metrics['clsn_packt_cnt'] = clsn_packt_cnt
            avg_clsn_packt_cnt += clsn_packt_cnt

        if metrics:
            out = {'dimensions': dimensions,
                   'metrics': metrics,
                   'timestamp': int(time.time() * 1000)}
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
    metrics['avg_clsn_packt_cnt'] = avg_clsn_packt_cnt / nic_count

    out = {'dimensions': {'schema_type': 'svr'},
           'metrics': metrics,
           'timestamp': int(time.time() * 1000)}
    out_list.append(out)

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

        if keep:
            nic_array.append(nic)
        index += 1

    return nic_array


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
