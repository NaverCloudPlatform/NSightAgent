import json
import sys

sys.path.append(sys.argv[1])

from collectors.libs import time_util
from collectors.libs.cache import CacheProxy


def main(argv):

    cache = CacheProxy('disk')

    f_diskstats = open("/proc/diskstats")

    out_list = []

    avg_read_byt = 0.0
    avg_write_byt = 0.0
    avg_read = 0.0
    avg_write = 0.0
    max_read_byt = 0
    max_write_byt = 0
    max_read = 0
    max_write = 0

    # disk_count = 0
    count_read_byt = 0
    count_write_byt = 0
    count_read = 0
    count_write = 0

    last_disk_name = None

    ntp_checked, timestamp = time_util.get_ntp_time()

    for line in f_diskstats:
        values = line.split(None)
        if values[3] == "0":
            continue

        # disk_count += 1
        disk_idx = values[2]

        dimensions = {'disk_idx': disk_idx}
        metrics = {}

        rd_ios = cache.counter_to_gauge('rd_ios_%s' % disk_idx, long(values[3]))
        rd_sectors = cache.counter_to_gauge('rd_sectors_%s' % disk_idx, long(values[5]))
        # rd_ticks = cache.counter_to_gauge('rd_ticks_%s' % disk_idx, long(values[6]))
        wr_ios = cache.counter_to_gauge('wr_ios_%s' % disk_idx, long(values[7]))
        wr_sectors = cache.counter_to_gauge('wr_sectors_%s' % disk_idx, long(values[9]))
        # wr_ticks = cache.counter_to_gauge('wr_ticks_%s' % disk_idx, long(values[10]))
        io_ticks = cache.counter_to_gauge('io_ticks_%s' % disk_idx, long(values[12]))
        time_in_queue = cache.counter_to_gauge('time_in_queue_%s' % disk_idx, long(values[13]))

        if rd_ios is None or wr_ios is None:
            continue

        if last_disk_name and disk_idx.find(last_disk_name) > -1:
            continue
        else:
            last_disk_name = disk_idx

        read_byt_cnt = None
        write_byt_cnt = None
        await_tm = None
        avgrq_sz = None
        svc_tm = None

        if rd_sectors is not None:
            read_byt_cnt = rd_sectors * 512
        if wr_sectors is not None:
            write_byt_cnt = wr_sectors * 512

        if rd_sectors is not None and wr_sectors is not None:
            if (rd_ios + wr_ios) > 0:
                avgrq_sz = (rd_sectors + wr_sectors) / float(rd_ios + wr_ios)
            else:
                avgrq_sz = 0.0
        if time_in_queue is not None:
            avgqu_sz = time_in_queue / 1000.0 / 60.0
        if time_in_queue is not None:
            if (rd_ios + wr_ios) > 0:
                await_tm = time_in_queue / float(rd_ios + wr_ios)
            else:
                await_tm = 0.0
        if io_ticks is not None:
            if (rd_ios + wr_ios) > 0:
                svc_tm = io_ticks / float(rd_ios + wr_ios)
            else:
                svc_tm = 0.0
        if io_ticks is not None:
            used_rto = io_ticks * 100 / 1000.0 / 60.0

        if read_byt_cnt is not None:
            metrics['read_byt_cnt'] = read_byt_cnt / 60.0
            avg_read_byt += read_byt_cnt
            if read_byt_cnt > max_read_byt:
                max_read_byt = read_byt_cnt
            count_read_byt += 1
        if write_byt_cnt is not None:
            metrics['write_byt_cnt'] = write_byt_cnt / 60.0
            avg_write_byt += write_byt_cnt
            if write_byt_cnt > max_write_byt:
                max_write_byt = write_byt_cnt
            count_write_byt += 1
        if rd_ios is not None:
            metrics['read_cnt'] = rd_ios / 60.0
            avg_read += rd_ios
            if rd_ios > max_read:
                max_read = rd_ios
            count_read += 1
        if wr_ios is not None:
            metrics['write_cnt'] = wr_ios / 60.0
            avg_write += wr_ios
            if wr_ios > max_write:
                max_write = wr_ios
            count_write += 1
        if avgrq_sz is not None:
            metrics['avgrq_sz'] = avgrq_sz
        if avgqu_sz is not None:
            metrics['avgqu_sz'] = avgqu_sz
        if await_tm is not None:
            metrics['await_tm'] = await_tm
        if svc_tm is not None:
            metrics['svc_tm'] = svc_tm
        if used_rto is not None:
            metrics['used_rto'] = used_rto

        if metrics:
            out = {'dimensions': dimensions,
                   'metrics': metrics,
                   'timestamp': timestamp,
                   'ntp_checked': ntp_checked}
            out_list.append(out)

    # if disk_count > 0:
    #     metrics = {}
    #
    #     metrics['avg_read_byt_cnt'] = avg_read_byt / 60.0 / disk_count
    #     metrics['avg_write_byt_cnt'] = avg_write_byt / 60.0 / disk_count
    #     metrics['avg_read_cnt'] = avg_read / 60.0 / disk_count
    #     metrics['avg_write_cnt'] = avg_write / 60.0 / disk_count
    #     metrics['max_read_byt_cnt'] = max_read_byt
    #     metrics['max_write_byt_cnt'] = max_write_byt
    #     metrics['max_read_cnt'] = max_read
    #     metrics['max_write_cnt'] = max_write
    #
    #     out = {'dimensions': {'schema_type': 'svr'},
    #            'metrics': metrics,
    #            'timestamp': timestamp,
    #            'ntp_checked': ntp_checked}
    #     out_list.append(out)

    metrics = {}

    if count_read_byt > 0:
        metrics['avg_read_byt_cnt'] = avg_read_byt / 60.0 / count_read_byt
        metrics['max_read_byt_cnt'] = max_read_byt
    if count_write_byt > 0:
        metrics['avg_write_byt_cnt'] = avg_write_byt / 60.0 / count_write_byt
        metrics['max_write_byt_cnt'] = max_write_byt
    if count_read > 0:
        metrics['avg_read_cnt'] = avg_read / 60.0 / count_read
        metrics['max_read_cnt'] = max_read
    if count_write > 0:
        metrics['avg_write_cnt'] = avg_write / 60.0 / count_write
        metrics['max_write_cnt'] = max_write

    if metrics:
        out = {'dimensions': {'schema_type': 'svr'},
               'metrics': metrics,
               'timestamp': timestamp,
               'ntp_checked': ntp_checked}
        out_list.append(out)

    if out_list:
        print(json.dumps(out_list))
        sys.stdout.flush()

    f_diskstats.close()
    cache.close()


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main(sys.argv))
