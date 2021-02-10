import json


class AverageRecorder(object):

    def __init__(self, cache, key):
        self.cache = cache
        self.key = key

        record5_str = self.cache.get(key + '5')
        record15_str = self.cache.get(key + '15')
        if record5_str is None:
            self.record5 = {}
        else:
            self.record5 = json.loads(record5_str)

        if record15_str is None:
            self.record15 = {}
        else:
            self.record15 = json.loads(record15_str)

    def record(self, value, index):
        self.record5[str(index % 5)] = value
        self.record15[str(index % 15)] = value
        self.cache.set(self.key + '5', json.dumps(self.record5))
        self.cache.set(self.key + '15', json.dumps(self.record15))

    def get_avg(self):
        avg5 = None
        avg15 = None
        if len(self.record5) == 5:
            avg5 = record_avg(self.record5)
        if len(self.record15) == 15:
            avg15 = record_avg(self.record15)
        return avg5, avg15


def record_avg(record):
    sum = 0.0
    for k in record:
        sum += record[k]
    return sum / len(record)
