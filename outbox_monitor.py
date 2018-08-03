#!/usr/bin/env python2
# -*- coding: UTF-8 -*-
#****************************************************************#
# ScriptName: outbox_monitor.py
# Author: liujmsunits@hotmail.com
# Author Github: https://github.com/jimmy-src
# Create Date: 2018-07-02 10:46
# Modify Author: liujmsunits@hotmail.com
# Modify Date: 2018-07-02 10:46
# Function: 
#***************************************************************#

# 需求：
    # 1,查询oarcle中所有 IS_SEND=1 的所有场景名称
    # 2,根据上面的场景名称查询mysql中相应表的sendtime大于预定义的时间的所有数据，如果结果为0,则预警
    #   场景        监控时间  工作日时长      非工作日
    #   1001-1007   9-24      30min           6hous
    #   2001        11:30     2hours          无
    #               15:30     2hours          无
    #   default     9-24      2hours          6hours 
    
# 思路：
    # 1,定义获取上面定义的各种不同时间的函数
    # 2,确定四条查询sql，oracle查询sql，还有mysql三个场景的预警量的查询sql
    # 3,然后根据流程来做判断
    # 4,定义全局参数，场景表list（场景，工作日时间，节假日时间）


import json
import os
import sys
import commands
from pyhive import hive
import time


# 全局变量

cjList = ["1001","1002","1003","1004","1005","1006","2001"]
timeList = {
        "1001_workday": "30",
        "1001_holiday": "360",
        "1002_workday": "30",
        "1002_holiday": "360",
        "1003_workday": "30",
        "1003_holiday": "360",
        "1004_workday": "30",
        "1004_holiday": "360",
        "1005_workday": "30",
        "1005_holiday": "360",
        "1006_workday": "30",
        "1006_holiday": "360",
        "2001_workday": "420",
        "2001_holiday": "0"
        }

errorJson = {
    "scenariaoName": "alarmHandler Warning",
    'healthSummary': "BAD"
}


hiveHost = "10.202.144.122"



def getTime(xMins):
    timeago = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()-xMins*60))
    timearray = time.strptime(timeago,'%Y-%m-%d %H:%M:%S')
    return int(time.mktime(timearray)*1000)

def getCurrDay(xMins):
    timeago = time.strftime('%Y%m%d',time.localtime(time.time()-xMins*60))
    return int(timeago)

def getFormatDay(xMins):
    timeago = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()-xMins*60))
    return timeago

def getHours():
    timeago = time.strftime('%H',time.localtime(time.time()))
    return timeago

def get0ClockTime():
    t = time.localtime(time.time())
    time1 = time.mktime(time.strptime(time.strftime('%Y-%m-%d 00:00:00',t),'%Y-%m-%d %H:%M:%S'))
    return long(time1*1000)

def getsqlResult(pyName, sqlStr):
    (status,result) = commands.getstatusoutput("python "+pyName+" '"+sqlStr+"' ")
    if status == 0:
        return result
    else:
        return "0"
def hiveQuery(SQL):
    os.system("kinit bigf_admin -kt /etc/bigf.keytab")
    cursor = hive.connect(host=hiveHost,auth="KERBEROS",kerberos_service_name="hive").cursor()
    cursor.execute("add jar /opt/cloudera/parcels/CDH/jars/hive-contrib-1.1.0-cdh5.12.0.jar")
    cursor.execute(SQL)
    bb = cursor.fetchall()
    return bb

def isHoliday():
    currDay = int(getCurrDay(0))
    tenDayAgo = str(getCurrDay(14400))
    tenDayFuture = str(getCurrDay(-14400))
    hiveSql = 'select vacation_begin_date,vacation_end_date from dskx_tbvacation_a where vacation_begin_date > '+tenDayAgo+' and vacation_begin_date < '+tenDayFuture
    #print(hiveSql)
    timeList = hiveQuery(hiveSql)
    for i in timeList:
        if currDay >= i[0] and currDay <= i[1]:
            return True
        else:
            return False

def main():
    # sql
    oracleSql = 'select scenario_id from ALARMHANDLER_SENDCONFIG where is_send=1'

    # collect all scenariao id from query oracle 
    allScenariao = list(set(getsqlResult("/bigf/admin/monitor/outbox_monitor/oracle.py",oracleSql).replace("[","").replace("]","").replace("\'","").split(", ")))
    #print(allScenariao)

    # 然后通过循环场景来查询mysql中指定区间的预警量
    if allScenariao != "0":
        #allScenariaoFormat = getsqlResult("oracle.py",oracleSql).replace("[","").replace("]","").replace("\'","").split(",")
	#print("allScenariaoFormat:"+allScenariaoFormat)
        #print("first")
	ish = isHoliday()
	hours = getHours()
	#print("hours:"+hours)
        for i in allScenariao:
            #print(i)
            if i in cjList:
                #print("second")
                # 执行hive查询当天日期是否是节假日，然后输出一个过去多长时间的时间，供sql查询
                num = ""
                sql = ""
                numMessage = ""
                #print("1num=:"+num)
                if ish:
                    num = int(timeList.get(i+"_holday"))
                    #print("2num=:"+str(num))
                    queryTime = str(getFormatDay(num))
                    sql = 'select count(*) from AlarmRecord_'+i+' where Alarm_SendTime > "'+queryTime+'"'
                    #print(sql)
                    numMessage = int(getsqlResult("/bigf/admin/monitor/outbox_monitor/mysql.py",sql, "m"))
                else:
                    num = int(timeList.get(i+"_workday"))
                    #print("3num=:"+str(num))
                    queryTime = str(getFormatDay(num))
                    sql = 'select count(*) from AlarmRecord_'+i+' where Alarm_SendTime > "'+queryTime+'"'
                    #print(sql)
                    numMessage = int(getsqlResult("mysql.py",sql))
    
                #print("numMessage:"+str(numMessage))
		if num != "0" and i != '2001':
                    if numMessage == 0:
                        errorJson.update({"info": i+" scenariao "+str(num)+" mins no message send"})
                        print(json.dumps(errorJson))
		if num != "0" and hours == '16' and i == '2001':
                    if numMessage == 0:
                        errorJson.update({"info": i+" scenariao "+str(num)+" mins no message send"})
                        print(json.dumps(errorJson))

if __name__ == "__main__":
        main()
