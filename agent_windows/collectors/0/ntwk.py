# coding=utf-8
import json
import sys
import win32pdh

from apscheduler.schedulers.blocking import BlockingScheduler

sys.path.append(sys.argv[1])

from collectors.libs.time_util import get_ntp_time

global counter_dict
counter_dict = {}


def main(argv):
    scheduler = BlockingScheduler()

    global hq
    hq = win32pdh.OpenQuery()

    counters, instances = win32pdh.EnumObjectItems(None, None, 'Network Interface', win32pdh.PERF_DETAIL_WIZARD)

    instances = filter_instance(instances)

    print instances

    for instance in instances:

        counter_dict[instance] = {}

        counter_dict[instance]['snd_bps'] = win32pdh.AddCounter(hq, "\\Network Interface(%s)\\Bytes Sent/sec" % instance)
        counter_dict[instance]['rcv_bps'] = win32pdh.AddCounter(hq, "\\Network Interface(%s)\\Bytes Received/sec" % instance)
        counter_dict[instance]['snd_pps'] = win32pdh.AddCounter(hq, "\\Network Interface(%s)\\Packets Sent/sec" % instance)
        counter_dict[instance]['rcv_pps'] = win32pdh.AddCounter(hq, "\\Network Interface(%s)\\Packets Received/sec" % instance)

        counter_dict[instance]['snd_fail_packt_cnt'] = win32pdh.AddCounter(
            hq, "\\Network Interface(%s)\\Packets Outbound Errors" % instance)
        counter_dict[instance]['rcv_fail_packt_cnt'] = win32pdh.AddCounter(
            hq, "\\Network Interface(%s)\\Packets Received Errors" % instance)

    scheduler.add_job(query, 'cron', minute='*/1', second='0')
    scheduler.start()

    win32pdh.CloseQuery(hq)


def query():
    global hq
    # global counter_handle
    global counter_dict

    win32pdh.CollectQueryData(hq)

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

    # nic_count = 0

    count_snd_bps = 0
    count_rcv_bps = 0
    count_snd_pps = 0
    count_rcv_pps = 0
    count_snd_fail = 0
    count_rcv_fail = 0

    ntp_checked, timestamp = get_ntp_time()

    for instance in counter_dict:

        dimensions = {'nic_desc': instance}
        metrics = {}

        # nic_count += 1

        try:
            _, snd_bps = win32pdh.GetFormattedCounterValue(counter_dict[instance]['snd_bps'],
                                                                win32pdh.PDH_FMT_LONG)
            metrics['snd_bps'] = snd_bps * 8
            avg_snd_bps += metrics['snd_bps']
            if metrics['snd_bps'] > max_snd_bps:
                max_snd_bps = metrics['snd_bps']
            count_snd_bps += 1
        except Exception as e:
            print('error: %s' % e)
            pass

        try:
            _, rcv_bps = win32pdh.GetFormattedCounterValue(counter_dict[instance]['rcv_bps'],
                                                                 win32pdh.PDH_FMT_LONG)
            metrics['rcv_bps'] = rcv_bps * 8
            avg_rcv_bps += metrics['rcv_bps']
            if metrics['rcv_bps'] > max_rcv_bps:
                max_rcv_bps = metrics['rcv_bps']
            count_rcv_bps += 1
        except Exception as e:
            pass

        try:
            _, snd_pps = win32pdh.GetFormattedCounterValue(counter_dict[instance]['snd_pps'], win32pdh.PDH_FMT_LONG)
            metrics['snd_pps'] = snd_pps
            avg_snd_pps += metrics['snd_pps']
            if metrics['snd_pps'] > max_snd_pps:
                max_snd_pps = metrics['snd_pps']
            count_snd_pps += 1
        except Exception as e:
            pass

        try:
            _, rcv_pps = win32pdh.GetFormattedCounterValue(counter_dict[instance]['rcv_pps'], win32pdh.PDH_FMT_LONG)
            metrics['rcv_pps'] = rcv_pps
            avg_rcv_pps += metrics['rcv_pps']
            if metrics['rcv_pps'] > max_rcv_pps:
                max_rcv_pps = metrics['rcv_pps']
            count_rcv_pps += 1
        except Exception as e:
            pass

        try:
            _, snd_fail_packt_cnt = win32pdh.GetFormattedCounterValue(counter_dict[instance]['snd_fail_packt_cnt'], win32pdh.PDH_FMT_LONG)
            metrics['snd_fail_packt_cnt'] = snd_fail_packt_cnt
            avg_snd_fail_packt_cnt += snd_fail_packt_cnt
            count_snd_fail += 1
        except Exception as e:
            pass

        try:
            _, rcv_fail_packt_cnt = win32pdh.GetFormattedCounterValue(counter_dict[instance]['rcv_fail_packt_cnt'], win32pdh.PDH_FMT_LONG)
            metrics['rcv_fail_packt_cnt'] = rcv_fail_packt_cnt
            avg_rcv_fail_packt_cnt += rcv_fail_packt_cnt
            count_rcv_fail += 1
        except Exception as e:
            pass

        if metrics:
            out = {'dimensions': dimensions,
                   'metrics': metrics,
                   'timestamp': timestamp,
                   'ntp_checked': ntp_checked}
            out_list.append(out)

    metrics = {}
    if count_snd_bps > 0:
        metrics['avg_snd_bps'] = avg_snd_bps
        metrics['max_snd_bps'] = max_snd_bps
    if count_rcv_bps > 0:
        metrics['avg_rcv_bps'] = avg_rcv_bps
        metrics['max_rcv_bps'] = max_rcv_bps
    if count_snd_pps > 0:
        metrics['avg_snd_pps'] = avg_snd_pps
        metrics['max_snd_pps'] = max_snd_pps
    if count_rcv_pps > 0:
        metrics['avg_rcv_pps'] = avg_rcv_pps
        metrics['max_rcv_pps'] = max_rcv_pps
    if count_snd_fail:
        metrics['avg_snd_fail_packt_cnt'] = avg_snd_fail_packt_cnt
    if count_rcv_fail:
        metrics['avg_rcv_fail_packt_cnt'] = avg_rcv_fail_packt_cnt

    if metrics:
        out = {'dimensions': {'schema_type': 'svr'},
               'metrics': metrics,
               'timestamp': timestamp,
               'ntp_checked': ntp_checked}
        out_list.append(out)

    # if nic_count > 0:
    #     metrics = {}
    #
    #     metrics['avg_snd_bps'] = avg_snd_bps
    #     metrics['max_snd_bps'] = max_snd_bps
    #     metrics['avg_rcv_bps'] = avg_rcv_bps
    #     metrics['max_rcv_bps'] = max_rcv_bps
    #     metrics['avg_snd_pps'] = avg_snd_pps
    #     metrics['max_snd_pps'] = max_snd_pps
    #     metrics['avg_rcv_pps'] = avg_rcv_pps
    #     metrics['max_rcv_pps'] = max_rcv_pps
    #     metrics['avg_snd_fail_packt_cnt'] = avg_snd_fail_packt_cnt
    #     metrics['avg_rcv_fail_packt_cnt'] = avg_rcv_fail_packt_cnt

    if out_list:
        print(json.dumps(out_list))
        sys.stdout.flush()


def filter_instance(instances):
    target = []

    for name in instances:
        nic_desc = name.encode(encoding='utf-8')

        if nic_desc.find('isatap') > -1 \
                or nic_desc.find('Tunneling') > -1 \
                or nic_desc.find('Miniport') > -1 \
                or nic_desc.find('本地连接') > -1 \
                or nic_desc.find('로컬 영역 연결') > -1:
            continue

        target.append(nic_desc.decode(encoding='utf-8'))

    return target


if __name__ == '__main__':
    sys.exit(main(sys.argv))
