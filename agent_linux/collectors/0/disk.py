#v0
import json
import os
import sys
import time

from diskcache import Cache


def main(argv):

    collectors_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    cache = Cache(os.path.join(collectors_dir, 'cache/script/disk'))

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

    disk_count = 0

    for line in f_diskstats:
        values = line.split(None)
        if values[3] == "0":
            continue

        disk_count += 1
        disk_idx = values[2]

        dimensions = {'disk_idx': disk_idx}
        metrics = {}
        timestamp = int(time.time() * 1000)

        rd_ios = counter_calc_delta(cache, 'rd_ios_%s' % disk_idx, long(values[3]))
        rd_sectors = counter_calc_delta(cache, 'rd_sectors_%s' % disk_idx, long(values[5]))
        # rd_ticks = counter_calc_delta(cache, 'rd_ticks_%s' % disk_idx, long(values[6]))
        wr_ios = counter_calc_delta(cache, 'wr_ios_%s' % disk_idx, long(values[7]))
        wr_sectors = counter_calc_delta(cache, 'wr_sectors_%s' % disk_idx, long(values[9]))
        # wr_ticks = counter_calc_delta(cache, 'wr_ticks_%s' % disk_idx, long(values[10]))
        io_ticks = counter_calc_delta(cache, 'io_ticks_%s' % disk_idx, long(values[12]))
        time_in_queue = counter_calc_delta(cache, 'time_in_queue_%s' % disk_idx, long(values[13]))

        if rd_ios is None or wr_ios is None:
            continue

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
        if write_byt_cnt is not None:
            metrics['write_byt_cnt'] = write_byt_cnt / 60.0
            avg_write_byt += write_byt_cnt
            if write_byt_cnt > max_write_byt:
                max_write_byt = write_byt_cnt
        if rd_ios is not None:
            metrics['read_cnt'] = rd_ios / 60.0
            avg_read += rd_ios
            if rd_ios > max_read:
                max_read = rd_ios
        if wr_ios is not None:
            metrics['write_cnt'] = wr_ios / 60.0
            avg_write += wr_ios
            if wr_ios > max_write:
                max_write = wr_ios
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
                   'timestamp': timestamp}
            out_list.append(out)

    if disk_count > 0:
        metrics = {}

        metrics['avg_read_byt_cnt'] = avg_read_byt / 60.0 / disk_count
        metrics['avg_write_byt_cnt'] = avg_write_byt / 60.0 / disk_count
        metrics['avg_read_cnt'] = avg_read / 60.0 / disk_count
        metrics['avg_write_cnt'] = avg_write / 60.0 / disk_count
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

    f_diskstats.close()
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
