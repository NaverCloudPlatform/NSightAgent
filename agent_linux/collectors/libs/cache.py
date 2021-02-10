import os

from diskcache import Cache

from collectors.configs.script_config import get_configs


class CacheProxy:

    def __init__(self, script):
        self.config = get_configs()
        collectors_dir = self.config.get('base', 'collectors_dir')
        self.cache = Cache(os.path.join(collectors_dir, 'cache/script/', script))

    def get(self, key):
        return self.cache.get(key)

    def set(self, key, value):
        self.cache.set(key, value)

    def delete(self, key):
        self.cache.delete(key)

    def close(self):
        self.cache.close()

    def counter_to_gauge(self, key, value):
        last_value = self.get(key)
        self.set(key, value)
        if last_value is None:
            return None
        gauge = value - last_value
        if gauge < 0 or gauge > last_value:
            return None
        return gauge
