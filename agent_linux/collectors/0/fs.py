#0
import json
import os
import sys
import time


def main(argv):

    partitions = disk_partitions()

    out_list = []

    fs_all = 0.0
    fs_used_all = 0.0
    fs_free_all = 0.0
    fs_valid_all = 0.0
    max_fs_usert = 0.0
    f_files_all = 0.0
    f_free_all = 0.0

    for sdiskpart in partitions:

        vfs = os.statvfs(sdiskpart['device'])

        dimensions = {'dev_nm': sdiskpart['device'],
                      'mnt_nm': sdiskpart['mountpoint'],
                      'fs_type_nm': sdiskpart['fstype'],
                      'mnt_stat_cd': 'MONTGTSTAT_RUN'}
        metrics = {}
        timestamp = int(time.time() * 1000)

        all_byt_cnt = vfs.f_blocks * vfs.f_bsize / 1024 / 1024.0
        used_byt_cnt = (vfs.f_blocks - vfs.f_bfree) * vfs.f_bsize / 1024 / 1024.0
        free_byt_cnt = (vfs.f_bavail * vfs.f_bsize) / 1024 / 1024.0

        reserved_block = vfs.f_bfree - vfs.f_bavail

        fs_usert = (vfs.f_blocks - vfs.f_bfree) * 100.0 / (vfs.f_blocks - reserved_block)
        ind_usert = (vfs.f_files - vfs.f_ffree) * 100.0 / vfs.f_files

        metrics['all_byt_cnt'] = all_byt_cnt
        metrics['used_byt_cnt'] = used_byt_cnt
        metrics['free_byt_cnt'] = free_byt_cnt
        metrics['fs_usert'] = fs_usert
        metrics['ind_usert'] = ind_usert

        # total
        fs_all += all_byt_cnt
        fs_used_all += used_byt_cnt
        fs_free_all += free_byt_cnt
        fs_valid_all += (vfs.f_blocks - reserved_block) * vfs.f_bsize / 1024 / 1024.0
        f_files_all += vfs.f_files
        f_free_all += vfs.f_ffree
        if fs_usert > max_fs_usert:
            max_fs_usert = fs_usert

        if metrics:
            out = {'dimensions': dimensions,
                   'metrics': metrics,
                   'timestamp': timestamp}
            out_list.append(out)

    metrics = {}
    metrics['fs_all_mb'] = fs_all
    metrics['fs_used_mb'] = fs_used_all
    metrics['fs_free_mb'] = fs_free_all
    metrics['avg_fs_usert'] = fs_used_all * 100 / fs_valid_all
    metrics['max_fs_usert'] = max_fs_usert
    metrics['avg_ind_usert'] = (f_files_all - f_free_all) * 100.0 / f_files_all

    out = {'dimensions': {'schema_type': 'svr'},
           'metrics': metrics,
           'timestamp': int(time.time() * 1000)}
    out_list.append(out)

    print(json.dumps(out_list))
    sys.stdout.flush()


def disk_partitions():
    fstypes = set()
    with open('/proc/filesystems', 'r') as f_fs:
        for line in f_fs:
            line = line.strip()
            if not line.startswith("nodev"):
                fstypes.add(line.strip())
            else:
                fstype = line.split("\t")[1]
                if fstype == "zfs":
                    fstypes.add("zfs")

    retlist = []
    with open('/etc/mtab', 'r') as f_mt:
        for line in f_mt:
            vals = line.strip().split()
            if vals[2] in fstypes:
                retlist.append({'device': vals[0], 'mountpoint': eval(repr(vals[1]).replace('\\\\', '\\')), 'fstype': vals[2]})

    return retlist


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main(sys.argv))
