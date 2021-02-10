import json
import os
import sys

sys.path.append(sys.argv[1])

from collectors.libs import time_util
from collectors.libs.cache import CacheProxy
from collectors.libs.config_client import ConfigClient


def main(argv):

    if len(argv) < 4:
        print("error: parameters missing")
        return

    cache = CacheProxy('plugin_file')

    last_config_version = cache.get('version')
    config_version = int(argv[2])
    host_id = argv[3]
    # config_version = 0
    # host_id = 'test'
    if last_config_version is None or config_version != last_config_version:
        config_client = ConfigClient()
        current_version, file_list = config_client.get_user_config('plugin_file', host_id)
        if file_list is not None:
            cache.set('file_list', file_list)
            cache.set('version', current_version)
    else:
        file_list = cache.get('file_list')

    if file_list:
        ntp_checked, timestamp = time_util.get_ntp_time()
        out_list = []
        for path in file_list:
            dimensions = {'path': path}
            metrics = {}
            if os.path.isfile(path):
                metrics['file_exist'] = 1
                last_modify_time = long(os.stat(path).st_mtime * 1000)
                metrics['last_modify_time'] = last_modify_time
                last_modify_time_record = cache.get('lmt_' + path)
                if last_modify_time_record is None:
                    metrics['file_modified'] = 0
                    cache.set('lmt_' + path, last_modify_time)
                elif last_modify_time != last_modify_time_record:
                    metrics['file_modified'] = 1
                    cache.set('lmt_' + path, last_modify_time)
                else:
                    metrics['file_modified'] = 0
                metrics['file_size'] = os.path.getsize(path)
            else:
                metrics['file_exist'] = 0
                # metrics['last_modify_time'] = 0
                # metrics['size'] = 0

            out = {'dimensions': dimensions,
                   'metrics': metrics,
                   'timestamp': timestamp,
                   'ntp_checked': ntp_checked}
            out_list.append(out)

        print(json.dumps(out_list))
        sys.stdout.flush()

    cache.close()


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main(sys.argv))
