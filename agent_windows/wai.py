import json
import socket

import psutil
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
        # req = urllib2.Request('http://10.113.103.203:10280' + '/wai/v1/hostIds', headers=headers)

        # data = {'hostname': getHostName(),
        #         'macIPAddressPairs': getMacIpList()}
        data = {'macIPAddressPairs': getMacIpList()}
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
    macip_list = []
    dic = psutil.net_if_addrs()
    # print dic
    for adapter in dic:
        snicList = dic[adapter]
        mac = ''
        ipv4 = ''
        for snic in snicList:
            if snic.family == -1:
                mac = snic.address.replace('-', ':')
            elif snic.family == 2:
                ipv4 = snic.address
        if mac and ipv4:
            macip_list.append({'macAddress': mac,
                               'ipAddress': ipv4})
    return macip_list
