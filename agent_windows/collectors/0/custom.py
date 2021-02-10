import json
import os
import sys
import time

sys.path.append(sys.argv[1])

from collectors.configs.script_config import get_configs
from collectors.libs import time_util
from collectors.libs.cache import CacheProxy


def main(argv):
    config = get_configs()

    section = 'custom_path'
    if not config.has_section(section):
        return

    cache = CacheProxy('custom')

    options = config.options(section)

    delay_limit = config.getint('custom_config', 'dely_limit')

    out_list = []
    ntp_checked, timestamp = time_util.get_ntp_time()

    for key in options:
        dir_path = config.get(section, key)
        if check_valid(dir_path):
            key_out = {'data': [],
                       'source_key': key}

            log_list = get_log_list(dir_path)
            log_record = cache.get(key)
            for log in log_list:
                log_path = '%s/%s' % (dir_path, log)

                if log_record and log < log_record:
                    os.remove(log_path)
                    continue
                else:
                    cache.set(key, log)

                if os.path.isfile(log_path) and os.access(log_path, os.R_OK):
                    delete = False
                    with open(log_path) as f:
                        offset_key = '%s-%s' % (key, log)

                        offset = cache.get(offset_key)
                        if offset:
                            f.seek(offset)
                        else:
                            offset = 0

                        while True:
                            line = f.readline()
                            if line:
                                offset += len(line.decode('ascii'))
                                cache.set(offset_key, offset)

                                line_dict = parse_line(line)
                                if line_dict:
                                    if ('timestamp' in line_dict) and (line_dict['timestamp'] < long(time.time() * 1000) - delay_limit):
                                        pass
                                    else:
                                        data = {'dimensions': {},
                                               'metrics': line_dict,
                                               'timestamp': timestamp,
                                               'ntp_checked': ntp_checked}
                                        key_out['data'].append(data)
                            else:
                                if log_path != get_latest_log(dir_path):
                                    cache.delete(offset_key)
                                    delete = True
                                break
                    if delete:
                        os.remove(log_path)

            if key_out['data']:
                out_list.append(key_out)

    cache.close()
    if out_list:
        print(json.dumps(out_list))
        sys.stdout.flush()


def check_valid(dir_path):
    return os.path.isdir(dir_path)


def get_latest_log(dir_path):
    log_list = get_log_list(dir_path)
    return log_list[-1]


def get_log_list(dir_path):
    log_list = os.listdir(dir_path)
    log_list.sort()
    return log_list


def parse_line(line):
    try:
        line_json = json.loads(line)
        if 'data' in line_json:
            data_obj = line_json['data']
            if 'metrics' in data_obj:
                data = data_obj['metrics']
            else:
                data = {}
            if 'dimensions' in data_obj:
                for key in data_obj['dimensions']:
                    data[key] = data_obj['dimensions'][key]
            if 'time' in line_json:
                data['timestamp'] = line_json['time'] * 1000
            return data
        else:
            return line_json
    except ValueError:
        return None


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main(sys.argv))
