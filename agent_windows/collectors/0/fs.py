import json
import time
import win32api

import psutil
import sys


def main(argv):
    partitions = psutil.disk_partitions()
    # print(partitions)

    out_list = []

    fs_all_mb = 0.0
    fs_used_mb = 0.0
    fs_free_mb = 0.0
    max_fs_usert = 0.0

    for sdiskpart in partitions:

        if not sdiskpart.fstype or sdiskpart.fstype == 'CDFS':
            continue

        dimensions = {'dev_nm': sdiskpart.device,
                      'mnt_nm': sdiskpart.mountpoint,
                      'fs_type_nm': sdiskpart.fstype,
                      'mnt_stat_cd': 'MONTGTSTAT_RUN'}

        sys_value = win32api.GetDiskFreeSpaceEx(sdiskpart.mountpoint)

        all_byt_cnt = sys_value[1]
        free_byt_cnt = sys_value[2]
        used_byt_cnt = all_byt_cnt - free_byt_cnt
        fs_usert = used_byt_cnt * 100.0 / all_byt_cnt

        timestamp = int(time.time() * 1000)
        metrics = {'all_byt_cnt': all_byt_cnt / 1024 / 1024.0,
                   'used_byt_cnt': used_byt_cnt / 1024 / 1024.0,
                   'free_byt_cnt': free_byt_cnt / 1024 / 1024.0,
                   'fs_usert': fs_usert}

        fs_all_mb += all_byt_cnt
        fs_used_mb += used_byt_cnt
        fs_free_mb += free_byt_cnt
        if fs_usert > max_fs_usert:
            max_fs_usert = fs_usert

        out = {'dimensions': dimensions,
               'metrics': metrics,
               'timestamp': timestamp}
        out_list.append(out)

    metrics = {}
    metrics['fs_all_mb'] = fs_all_mb / 1024 / 1024.0
    metrics['fs_used_mb'] = fs_used_mb / 1024 / 1024.0
    metrics['fs_free_mb'] = fs_free_mb / 1024 / 1024.0
    metrics['avg_fs_usert'] = fs_used_mb * 100 / fs_all_mb
    metrics['max_fs_usert'] = max_fs_usert

    out = {'dimensions': {'schema_type': 'svr'},
           'metrics': metrics,
           'timestamp': int(time.time() * 1000)}
    out_list.append(out)

    print(json.dumps(out_list))
    sys.stdout.flush()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
