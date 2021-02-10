import sys

sys.path.append(sys.argv[1])

from collectors.libs.cache import CacheProxy
from collectors.libs.config_client import ConfigClient


def main(argv):
    if len(argv) < 4:
        print("error: parameters missing")
        return

    cache = CacheProxy('plugin_process')

    last_config_version = cache.get('version')
    config_version = int(argv[2])
    host_id = argv[3]
    if last_config_version is None or config_version != last_config_version:
        config_client = ConfigClient()
        current_version, config_process_list = config_client.get_user_config('plugin_process', host_id)
        if config_process_list is not None:
            cache.set('process_list', config_process_list)
            cache.set('version', current_version)
    cache.close()


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main(sys.argv))
