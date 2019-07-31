import json
import os
import shutil
import threading
import time
from urllib import request

import agent_globals


class ScriptsUpdater(threading.Thread):

    def __init__(self, options, collector_holder, update_queue, response_queue):
        super(ScriptsUpdater, self).__init__()

        self.options = options
        self.collector_holder = collector_holder
        self.update_queue = update_queue
        self.response_queue = response_queue
        self.update_dir = os.path.join(options.basedir, 'updates')

    def run(self):

        if not os.path.exists(self.update_dir):
            os.mkdir(self.update_dir)
        else:
            clean_dir(self.update_dir)

        while agent_globals.RUN:
            update_list, remove_list = self.check_script_list()
            if not self.response_queue.empty():
                response_msg = self.response_queue.get()

            time.sleep(60)

            self.download(update_list)

    def check_script_list(self):
        pass

    def download_script(self, script_type, script_version, os_type, os_version):
        url = self.options.fea_addr + '/nsight/meta/script/' + script_type
        headers = {}
        data = {'script.version': script_version, 'os.type': os_type, 'os.version': os_version}
        datajson = json.dumps(data).encode(encoding='utf-8')
        req = request.Request(url=url, data=datajson, headers=headers)
        res = request.urlopen(req)
        content = res.decode(encoding='utf-8')

        script_path = os.path.join(self.update_dir, script_type + '.py')
        script_file = open(script_path, 'w')
        script_file.write(content)
        script_file.close()
        return script_path

    def download_config(self, script_type, script_version, os_type, os_version):
        url = self.options.fea_addr + '/nsight/meta/config/' + script_type
        headers = {}
        data = {'script.version': script_version, 'os.type': os_type, 'os.version': os_version}
        datajson = json.dumps(data).encode(encoding='utf-8')
        req = request.Request(url=url, data=datajson, headers=headers)
        res = request.urlopen(req)
        content = res.decode(encoding='utf-8')

        config_path = os.path.join(self.update_dir, script_type, 'config.py')
        config_file = open(config_path, 'w')
        config_file.write(content)
        config_file.close()
        return config_path


def clean_dir(path):
    if not os.path.isdir(path):
        return
    for i in os.listdir(path):
        c_path = os.path.join(path, i)
        if os.path.isdir(c_path):
            clean_dir(c_path)
            os.rmdir(c_path)
        else:
            os.remove(c_path)


def move_script(coldir, update_msg):
    if update_msg.delete:
        dst = os.path.join(coldir, str(update_msg.interval), update_msg.colname)
        if os.path.isfile(dst):
            os.remove(dst)
        dst_dir = os.path.join(coldir, 'script_configs', update_msg.colname[0:update_msg.colname.find('.')])
        dst = os.path.join(dst_dir, 'config.py')
        if os.path.isfile(dst):
            os.remove(dst)
        if os.path.isdir(dst_dir):
            os.rmdir(dst_dir)
    else:
        if update_msg.script_path is not None:
            dst_dir = os.path.join(coldir, str(update_msg.interval))
            if not os.path.exists(dst_dir):
                os.makedirs(dst_dir)
            dst = os.path.join(dst_dir, update_msg.colname)
            shutil.move(update_msg.script_path, dst)
        if update_msg.config_path is not None:
            dst_dir = os.path.join(coldir, 'script_configs', update_msg.colname[0:update_msg.colname.find('.')])
            if not os.path.exists(dst_dir):
                os.makedirs(dst_dir)
            dst = os.path.join(dst_dir, 'config.py')
            shutil.move(update_msg.config_path, dst)


class UpdateMessage:

    def __init__(self, colname, script_path, script_version, config_path, config_version, interval=0, delete=False):
        self.colname = colname
        self.script_path = script_path
        self.script_version = script_version
        self.config_path = config_path
        self.config_version = config_version
        self.interval = interval
        self.delete = delete


