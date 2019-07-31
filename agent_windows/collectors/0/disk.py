import json
import sys
import time
import win32pdh

from apscheduler.schedulers.blocking import BlockingScheduler

global counter_dict
counter_dict = {}


def main(argv):
    scheduler = BlockingScheduler()

    # print_counter_info('PhysicalDisk')

    global hq
    hq = win32pdh.OpenQuery()

    counters, instances = win32pdh.EnumObjectItems(None, None, 'PhysicalDisk', win32pdh.PERF_DETAIL_WIZARD)

    for instance in instances:

        if instance == '_Total':
            continue

        counter_dict[instance] = {}

        counter_dict[instance]['read_byt_cnt'] = win32pdh.AddCounter(hq,
                                                                     "\\PhysicalDisk(%s)\\Disk Read Bytes/sec" % instance)
        counter_dict[instance]['write_byt_cnt'] = win32pdh.AddCounter(hq,
                                                                      "\\PhysicalDisk(%s)\\Disk Write Bytes/sec" % instance)
        counter_dict[instance]['read_cnt'] = win32pdh.AddCounter(hq, "\\PhysicalDisk(%s)\\Disk Reads/sec" % instance)
        counter_dict[instance]['write_cnt'] = win32pdh.AddCounter(hq, "\\PhysicalDisk(%s)\\Disk Writes/sec" % instance)

        counter_dict[instance]['avgrq_sz'] = win32pdh.AddCounter(hq,
                                                                 "\\PhysicalDisk(%s)\\Avg. Disk Bytes/Transfer" % instance)
        counter_dict[instance]['avgqu_sz'] = win32pdh.AddCounter(hq,
                                                                 "\\PhysicalDisk(%s)\\Avg. Disk Queue Length" % instance)
        counter_dict[instance]['used_rto'] = win32pdh.AddCounter(hq, "\\PhysicalDisk(%s)\\%% Disk Time" % instance)
        counter_dict[instance]['await_tm'] = win32pdh.AddCounter(hq,
                                                                 "\\PhysicalDisk(%s)\\Disk Transfers/sec" % instance)
        counter_dict[instance]['svc_tm'] = win32pdh.AddCounter(hq,
                                                               "\\PhysicalDisk(%s)\\Avg. Disk sec/Transfer" % instance)

    scheduler.add_job(query, 'cron', minute='*/1', second='0')
    scheduler.start()

    win32pdh.CloseQuery(hq)


def query():
    global hq
    # global counter_handle
    global counter_dict

    win32pdh.CollectQueryData(hq)

    out_list = []

    avg_read_byt = 0.0
    avg_write_byt = 0.0
    avg_read = 0.0
    avg_write = 0.0
    max_read_byt = 0
    max_write_byt = 0
    max_read = 0
    max_write = 0

    disk_count = len(counter_dict)

    for instance in counter_dict:

        dimensions = {'disk_idx': instance}
        metrics = {}
        timestamp = int(time.time() * 1000)

        try:
            _, read_byt_cnt = win32pdh.GetFormattedCounterValue(counter_dict[instance]['read_byt_cnt'],
                                                                win32pdh.PDH_FMT_LONG)
            metrics['read_byt_cnt'] = read_byt_cnt
            avg_read_byt += read_byt_cnt
            if read_byt_cnt > max_read_byt:
                max_read_byt = read_byt_cnt
        except Exception as e:
            pass

        try:
            _, write_byt_cnt = win32pdh.GetFormattedCounterValue(counter_dict[instance]['write_byt_cnt'],
                                                                 win32pdh.PDH_FMT_LONG)
            metrics['write_byt_cnt'] = write_byt_cnt
            avg_write_byt += write_byt_cnt
            if write_byt_cnt > max_write_byt:
                max_write_byt = write_byt_cnt
        except Exception as e:
            pass

        try:
            _, read_cnt = win32pdh.GetFormattedCounterValue(counter_dict[instance]['read_cnt'], win32pdh.PDH_FMT_LONG)
            metrics['read_cnt'] = read_cnt
            avg_read += read_cnt
            if read_cnt > max_read:
                max_read = read_cnt
        except Exception as e:
            pass

        try:
            _, write_cnt = win32pdh.GetFormattedCounterValue(counter_dict[instance]['write_cnt'], win32pdh.PDH_FMT_LONG)
            metrics['write_cnt'] = write_cnt
            avg_write += write_cnt
            if write_cnt > max_write:
                max_write = write_cnt
        except Exception as e:
            pass

        try:
            _, avgrq_sz = win32pdh.GetFormattedCounterValue(counter_dict[instance]['avgrq_sz'], win32pdh.PDH_FMT_DOUBLE)
            metrics['avgrq_sz'] = avgrq_sz / 512
        except Exception as e:
            pass

        try:
            _, avgqu_sz = win32pdh.GetFormattedCounterValue(counter_dict[instance]['avgqu_sz'], win32pdh.PDH_FMT_DOUBLE)
            metrics['avgqu_sz'] = avgqu_sz
        except Exception as e:
            pass

        try:
            _, used_rto = win32pdh.GetFormattedCounterValue(counter_dict[instance]['used_rto'], win32pdh.PDH_FMT_DOUBLE)
            metrics['used_rto'] = used_rto
        except Exception as e:
            pass

        try:
            _, await = win32pdh.GetFormattedCounterValue(counter_dict[instance]['await_tm'], win32pdh.PDH_FMT_DOUBLE)
            if await == 0:
                await_tm = 0.0
            else:
                await_tm = used_rto / await * 1000
            metrics['await_tm'] = await_tm
        except Exception as e:
            pass

        try:
            _, svc_tm = win32pdh.GetFormattedCounterValue(counter_dict[instance]['svc_tm'], win32pdh.PDH_FMT_DOUBLE)
            metrics['svc_tm'] = svc_tm * 1000
        except Exception as e:
            pass

        if metrics:
            out = {'dimensions': dimensions,
                   'metrics': metrics,
                   'timestamp': timestamp}
            out_list.append(out)

    if disk_count > 0:
        metrics = {}

        metrics['avg_read_byt_cnt'] = avg_read_byt / disk_count
        metrics['avg_write_byt_cnt'] = avg_write_byt / disk_count
        metrics['avg_read_cnt'] = avg_read / disk_count
        metrics['avg_write_cnt'] = avg_write / disk_count
        metrics['max_read_byt_cnt'] = max_read_byt
        metrics['max_write_byt_cnt'] = max_write_byt
        metrics['max_read_cnt'] = max_read
        metrics['max_write_cnt'] = max_write

        out = {'dimensions': {'schema_type': 'svr'},
               'metrics': metrics,
               'timestamp': int(time.time() * 1000)}
        out_list.append(out)

    print(json.dumps(out_list))
    sys.stdout.flush()


def print_counter_info(counter_object):
    counters, instances = win32pdh.EnumObjectItems(None, None, counter_object, win32pdh.PERF_DETAIL_WIZARD)
    print counters
    print instances


if __name__ == '__main__':
    sys.exit(main(sys.argv))
