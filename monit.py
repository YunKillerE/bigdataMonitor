#!/usr/bin/env python2
#****************************************************************#
# ScriptName: monit.py
# Author: liujmsunits@hotmail.com
# Author Github: https://github.com/jimmy-src
# Create Date: 2018-06-15 15:18
# Modify Author: liujmsunits@hotmail.com
# Modify Date: 2018-06-15 15:18
# Function:
#***************************************************************#

# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import json
from cm_api.api_client import ApiResource

CM_HOST="bf_cm01"
# CM_HOST="10.114.25.153"
ROLE_IGNORE_LIST={"IMPALA"}
DISK_WATCH_LIST = {"/", "/data1", "/data2", "/data3", "/data4",
    "/data5", "/data6", "/data7", "/data8", "/data9", "/data10",
    "/ssd1", "/ssd2", "/ssd"}
DISK_ALARM_THRESHOLD = 0.8
# DISK_ALARM_THRESHOLD = 0.0

def host_summary(api):
    ts = str(datetime.now())
    hosts = api.get_all_hosts()
    for h in hosts:
        host = api.get_host(h.hostId)
        d = host.to_json_dict()
        d["ts"] = ts
        d["healthSummary"] = host.healthSummary
        print(json.dumps(d))

def service_summary(api, cluster_id=0):
    ts = str(datetime.now())
    c = api.get_all_clusters()[cluster_id]
    services = c.get_all_services()
    for s in services:
        if s.type in ROLE_IGNORE_LIST:
            continue
        d = s.to_json_dict()
        d["ts"] = ts
        d["healthSummary"] = s.healthSummary
        print(json.dumps(d))

def service_role_summary(api, cluster_id=0):
    ts = str(datetime.now())
    c = api.get_all_clusters()[cluster_id]
    services = c.get_all_services()
    for s in services:
        if s.type in ROLE_IGNORE_LIST:
            continue
        d = s.to_json_dict()
        d["ts"] = ts
        d["healthSummary"] = s.healthSummary
        print(json.dumps(d))

def safe_list_get(l, idx, default):
    try:
        return l[idx]
    except IndexError:
        return default

def extract_filesystem(qr):
    ret = {}
    for o in qr.objects:
        for ts in o.timeSeries:
            attrs = ts.metadata.attributes
            value = safe_list_get(ts.data, 0, None)
            if attrs['category'] == 'FILESYSTEM':
                attrs['metricName'] = ts.metadata.metricName
                if value:
                    attrs['value'] = value.value
                yield attrs

def group_by_filesystem(ls):
    d = {}
    for i in ls:
        mountpoint = i['mountpoint']
        if mountpoint not in d:
            cur_d = {}
        else:
            cur_d = d[mountpoint]

        cur_d['category'] = i['category']
        cur_d['hostId'] = i['hostId']
        cur_d['filesystemType'] = i['filesystemType']
        cur_d['entityName'] = i['entityName']
        cur_d['rackId'] = i['rackId']
        cur_d['hostname'] = i['hostname']
        cur_d['mountpoint'] = i['mountpoint']
        cur_d['partition'] = i['partition']
        if 'metricName' in i and 'value' in i:
            cur_d[i['metricName']] = i['value']
        d[mountpoint] = cur_d

    for k,v in d.items():
        if 'capacity_used' in v and 'capacity' in v:
            capacity_usage_percentage = float(v['capacity_used']) / float(v['capacity'])
            v['capacity_usage_percentage'] = capacity_usage_percentage
            if capacity_usage_percentage > DISK_ALARM_THRESHOLD:
                v['healthSummary'] = "BAD"
            else:
                v['healthSummary'] = "GOOD"
            v['disk_alarm_threshold'] = DISK_ALARM_THRESHOLD
    return d

def query_metrics(api):
    ts = str(datetime.now())
    from_time = datetime.now() - timedelta(minutes=5)
    hostId = "dedacb69-ccf1-433a-95d1-3f682a8d9c70"
    query = 'select * where hostId = "%s" and category="DISK"' % hostId
    query = 'select * where hostId = "%s"' % hostId
    query = 'select capacity, capacity_used where category = FILESYSTEM and hostId = "%s"' % hostId

    # all_hostIds = " or ".join(['hostId="%s"' % h.hostId for h in api.get_all_hosts()])
    # print(all_hostIds)
    # # query = 'select * where ' + all_hostIds
    # query = 'select capacity, capacity_used where category = FILESYSTEM and %s' % hostId
    # r = api.query_timeseries(query, from_time=from_time)
    # for k,v in group_by_filesystem(extract_filesystem(r)).items():
    #     if k in DISK_WATCH_LIST:
    #         print(v)
    # return

    for h in api.get_all_hosts():
        query = 'select capacity, capacity_used where category = FILESYSTEM and hostId = "%s"' % h.hostId
        r = api.query_timeseries(query, from_time=from_time)
        for k,v in group_by_filesystem(extract_filesystem(r)).items():
            if k in DISK_WATCH_LIST:
                v['ts'] = ts
                print(json.dumps(v))
    return

def main():
    host = CM_HOST
    port = 7180
    user = "vlog"
    passwd = "yunchen1234"
    api = ApiResource(host, port, user, passwd, version=17)
    host_summary(api)
    service_summary(api)
    service_role_summary(api)
    query_metrics(api)

if __name__ == "__main__":
    main()

