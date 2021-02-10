import json
import sys
import win32api
import win32pdh

import psutil

sys.path.append(sys.argv[1])

from collectors.libs.time_util import get_ntp_time


def main(argv):
    lpmap = {}
    counters, instances = win32pdh.EnumObjectItems(None, None, 'PhysicalDisk', win32pdh.PERF_DETAIL_WIZARD)
    for instance in instances:
        instance = instance.encode('utf-8')
        if instance != '_Total':
            plist = instance.split()
            if len(plist) > 1:
                p_id = plist[0]
                llist = plist[1:]
                for l in llist:
                    lpmap[l + '\\'] = 'Disk %s :' % p_id

    partitions = psutil.disk_partitions()

    out_list = []

    fs_all_mb = 0.0
    fs_used_mb = 0.0
    fs_free_mb = 0.0
    max_fs_usert = 0.0

    ntp_checked, timestamp = get_ntp_time()

    for sdiskpart in partitions:

        if not sdiskpart.fstype or sdiskpart.fstype == 'CDFS':
            continue

        dimensions = {'dev_nm': lpmap[sdiskpart.device],
                      'mnt_nm': sdiskpart.mountpoint,
                      'fs_type_nm': sdiskpart.fstype,
                      'mnt_stat_cd': 1}

        sys_value = win32api.GetDiskFreeSpaceEx(sdiskpart.mountpoint)

        all_byt_cnt = sys_value[1]
        free_byt_cnt = sys_value[2]
        used_byt_cnt = all_byt_cnt - free_byt_cnt
        fs_usert = used_byt_cnt * 100.0 / all_byt_cnt

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
               'timestamp': timestamp,
               'ntp_checked': ntp_checked}
        out_list.append(out)

    metrics = {}
    metrics['fs_all_mb'] = fs_all_mb / 1024 / 1024.0
    metrics['fs_used_mb'] = fs_used_mb / 1024 / 1024.0
    metrics['fs_free_mb'] = fs_free_mb / 1024 / 1024.0
    metrics['avg_fs_usert'] = fs_used_mb * 100 / fs_all_mb
    metrics['max_fs_usert'] = max_fs_usert

    out = {'dimensions': {'schema_type': 'svr'},
           'metrics': metrics,
           'timestamp': timestamp,
           'ntp_checked': ntp_checked}
    out_list.append(out)

    print(json.dumps(out_list))
    sys.stdout.flush()


# def get_ntp_time():
#     try:
#         config_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'script_configs'))
#         config_file_path = os.path.join(config_dir, 'svr_config.cfg')
#
#         cp = ConfigParser.ConfigParser()
#         cp.read(config_file_path)
#
#         ntp_host = cp.get('ntp', 'ntp_host')
#
#         client = ntplib.NTPClient()
#         response = client.request(ntp_host)
#         return True, int(response.tx_time * 1000)
#     except Exception as e:
#         return False, int(time.time() * 1000)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
