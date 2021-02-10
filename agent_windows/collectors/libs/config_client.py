import json
import urllib2

from collectors.configs.script_config import get_configs


class ConfigClient:

    def __init__(self):
        self.script_config = get_configs()

    def get_user_config(self, script, hostid):
        url = self.script_config.get('config_update', 'url')

        try:
            headers = {}
            req = urllib2.Request(url, headers=headers)

            body = {'hostid': hostid, 'type': script}
            response = urllib2.urlopen(req, json.dumps(body), timeout=3)
            if response.getcode() == 200:
                content = json.loads(response.read().rstrip('\n'))
                if script in content:
                    return content[script]['updateTime'], content[script]['configList']
                else:
                    return 0, []
        except urllib2.URLError:
            pass

        return None, None
