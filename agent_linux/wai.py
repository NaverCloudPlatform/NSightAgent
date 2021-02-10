import json
import os
import re
import socket
import time
import urllib2

import logger

LOG = logger.get_logger('wai')

HOST_ID = ''
REFRESH_TIME = 0
HOST_NAME = ''
MAC_IP_LIST = []


def get_host_id(options=None):
    global HOST_ID
    global REFRESH_TIME

    global HOST_NAME
    if not HOST_NAME:
        HOST_NAME = getHostName()

    global MAC_IP_LIST
    if not MAC_IP_LIST:
        MAC_IP_LIST = getMacIpList()

    now = time.time()
    if now - REFRESH_TIME > 10 * 60 or not HOST_ID:
        headers = {'Content-Type': 'application/json',
                   'registerToken': options.wai_token}
        req = urllib2.Request(options.wai_addr + '/wai/v1/hostIds/register', headers=headers)

        data = {'hostname': getHostName(),
                'macIPAddressPairs': getMacIpList()}
        json_object = json.dumps(data)

        try:
            response = urllib2.urlopen(req, json_object, timeout=3)
            response_data = response.read().rstrip('\n')
            # print response_data
            HOST_ID = json.loads(response_data)['hostId']
            REFRESH_TIME = now
        except urllib2.URLError as e:
            LOG.error("Got URLError error %s", e)
    return HOST_ID


def getHostName():
    # return 'liuji-virtual-machine-b'
    return socket.gethostname()


def getMacIpList():
    # return [{'macAddress': '00:0c:29:4c:cb:cc',
    #          'ipAddress': '192.168.40.131'}]

    macip_list = []

    nics = net_if_addrs()
    for nic in nics:
        macip_list.append({'macAddress': nic['maddr'].replace('-', ':'),
                           'ipAddress': nic['ip']})

    return macip_list


def net_if_addrs():
    nic_info_str = os.popen('ip addr').read().strip()

    infos = re.split('\d+:\s+(.*?):', nic_info_str)
    index = 0
    nic_array = []
    infos = infos[1:]

    while (index * 2) < len(infos):
        nic = {'name': infos[index * 2]}
        info_lines = infos[index * 2 + 1].split('\n')

        keep = True
        for line in info_lines:

            line = line.strip()

            if line.find('docker') > -1:
                keep = False
                break

            if line.find('cilium') > -1:
                keep = False
                break

            if line.find('nodelocaldns') > -1:
                keep = False
                break

            if line.startswith('link/'):
                vals = line.split()
                if vals[0].split('/')[1] == 'loopback' or vals[0].split('/')[1] == 'sit':
                    keep = False
                    break
                else:
                    nic['maddr'] = vals[1]

            if line.startswith('inet '):
                vals = line.split()
                nic['ip'] = vals[1].split('/')[0]

        if keep and 'maddr' in nic and 'ip' in nic:
            nic_array.append(nic)
        index += 1

    return nic_array
