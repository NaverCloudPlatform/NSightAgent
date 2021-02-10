import json
import os
import sys

sys.path.append(sys.argv[1])

from collectors.libs import time_util


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

    ntp_checked, timestamp = time_util.get_ntp_time()

    for sdiskpart in partitions:

        if sdiskpart['state'] == 0:
            dimensions = {'dev_nm': sdiskpart['device'],
                          'mnt_nm': sdiskpart['mountpoint'],
                          'fs_type_nm': sdiskpart['fstype'],
                          'mnt_stat_cd': sdiskpart['state']}

            out = {'dimensions': dimensions,
                   'metrics': {},
                   'timestamp': timestamp,
                   'ntp_checked': ntp_checked}
            out_list.append(out)

        else:
            vfs = os.statvfs(sdiskpart['mountpoint'])

            # dimensions = {'dev_nm': sdiskpart['device'],
            #               'mnt_nm': sdiskpart['mountpoint'],
            #               'fs_type_nm': sdiskpart['fstype'],
            #               'mnt_stat_cd': 'MONTGTSTAT_RUN'}

            dimensions = {'dev_nm': sdiskpart['device'],
                          'mnt_nm': sdiskpart['mountpoint'],
                          'fs_type_nm': sdiskpart['fstype'],
                          'mnt_stat_cd': sdiskpart['state']}

            metrics = {}

            all_byt_cnt = vfs.f_blocks * vfs.f_bsize / 1024 / 1024.0
            used_byt_cnt = (vfs.f_blocks - vfs.f_bfree) * vfs.f_bsize / 1024 / 1024.0
            free_byt_cnt = (vfs.f_bavail * vfs.f_bsize) / 1024 / 1024.0

            reserved_block = vfs.f_bfree - vfs.f_bavail

            fs_usert = (vfs.f_blocks - vfs.f_bfree) * 100.0 / (vfs.f_blocks - reserved_block)
            if vfs.f_files == 0:
                ind_usert = 0.0
            else:
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
                       'timestamp': timestamp,
                       'ntp_checked': ntp_checked}
                out_list.append(out)

    metrics = {}
    if fs_valid_all > 0:
        metrics['fs_all_mb'] = fs_all
        metrics['fs_used_mb'] = fs_used_all
        metrics['fs_free_mb'] = fs_free_all
        metrics['avg_fs_usert'] = fs_used_all * 100 / fs_valid_all
        metrics['max_fs_usert'] = max_fs_usert
        metrics['avg_ind_usert'] = (f_files_all - f_free_all) * 100.0 / f_files_all

    if metrics:
        out = {'dimensions': {'schema_type': 'svr'},
               'metrics': metrics,
               'timestamp': timestamp,
               'ntp_checked': ntp_checked}
        out_list.append(out)

    if out_list:
        print(json.dumps(out_list))
        sys.stdout.flush()


# def disk_partitions():
#
#     fstypes = set()
#     with open('/proc/filesystems', 'r') as f_fs:
#         for line in f_fs:
#             line = line.strip()
#             if not line.startswith("nodev"):
#                 fstypes.add(line.strip())
#             else:
#                 fstype = line.split("\t")[1]
#                 if fstype == "zfs":
#                     fstypes.add("zfs")
#
#     retlist = []
#     with open('/etc/mtab', 'r') as f_mt:
#         for line in f_mt:
#             vals = line.strip().split()
#             if vals[2] in fstypes:
#                 retlist.append({'device': vals[0], 'mountpoint': eval(repr(vals[1]).replace('\\\\', '\\')), 'fstype': vals[2]})
#
#     return retlist


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

    fslist = []
    with open('/etc/fstab', 'r') as fstab:
        for line in fstab:
            if not line.strip() or line.startswith('#'):
                continue
            vals = line.strip().split()
            device = vals[0]
            if device.startswith('UUID'):
                uuid = device[5:]
                device = get_dev_name(uuid)
            if vals[2] in fstypes:
                fslist.append({'device': device, 'mountpoint': vals[1], 'fstype': vals[2], 'state': 0})

    with open('/etc/mtab', 'r') as f_mt:
        for fs in fslist:
            for line in f_mt:
                tab_vals = line.strip().split()
                if fs['mountpoint'] == tab_vals[1] and fs['fstype'] == tab_vals[2]:
                    fs['device'] = tab_vals[0]
                    fs['state'] = 1
                    fs['mountpoint'] = eval(repr(tab_vals[1]).replace('\\\\', '\\'))

    return fslist


def get_dev_name(uuid):
    dev_list = os.popen("blkid").read().strip().splitlines()
    for dev in dev_list:
        parts = dev.split()
        label_part = parts[0]
        uuid_part = parts[1]
        if uuid_part[6:len(uuid_part) - 1] == uuid:
            return label_part[0:len(label_part) - 1]


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main(sys.argv))
