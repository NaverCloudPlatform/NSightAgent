import json
import time
import uuid

from collectors.libs.cache import CacheProxy

READER_ERR_KEY = 'reader_err'
SENDER_ERR_KEY = 'sender_err'
OTHER_ERR_KEY = 'other_err'


class ErrorReport(object):

    def __init__(self):
        self.cache = CacheProxy('err_cache')

    def __del__(self):
        self.cache.close()

    def get_report_id(self):
        report_id = self.cache.get('report_id')
        if report_id is None:
            report_id = uuid.uuid1().__str__()
            self.cache.set('report_id', report_id)
        return report_id

    def record_err_info(self, err_name, err):
        err_info = {err_name: err,
                    'timestamp': int(time.time())}
        self.cache.set(err_name, json.dumps(err_info))

    def pop_err_info(self, err_name):
        err_info_str = self.cache.get(err_name)
        self.cache.delete(err_name)
        if err_info_str is None:
            return None
        else:
            return json.loads(err_info_str)

    def set_report_enabled(self, enable):
        enabled = 0
        if enable:
            enabled = 1
        self.cache.set('report_enabled', enabled)

    def get_report_enabled(self):
        enabled = self.cache.get('report_enabled')
        if enabled is None:
            return False
        else:
            return bool(enabled)

    def record_reader_err(self, err):
        self.record_err_info(READER_ERR_KEY, err)

    def record_sender_err(self, err):
        self.record_err_info(SENDER_ERR_KEY, err)

    def record_other_err(self, err):
        self.record_err_info(OTHER_ERR_KEY, err)

    def pop_reader_err(self):
        return self.pop_err_info(READER_ERR_KEY)

    def pop_sender_err(self):
        return self.pop_err_info(SENDER_ERR_KEY)

    def pop_other_err(self):
        return self.pop_err_info(OTHER_ERR_KEY)
