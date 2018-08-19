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
    # 1,查询mysql中的所有表，形成list
    # 2,循环表增加分区
    # 3,先定义好删除策略，然后循环表进行删除分区
    
# 思路：
    # 1,定义获取上面定义的各种不同时间的函数
    # 2,增加分区命令函数
    # 3,删除策略定义，并定义默认策略


import json
import os
import sys
import commands
from pyhive import hive
import time


# 全局变量

#tableName:days
deleteList = {
        "t2": "1440",
        "default": "14400"
        }

def getTime(xMins):
    timeago = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()-xMins*60))
    timearray = time.strptime(timeago,'%Y-%m-%d %H:%M:%S')
    return int(time.mktime(timearray)*1000)

def getCurrDay(xMins):
    timeago = time.strftime('%Y%m%d',time.localtime(time.time()-xMins*60))
    return str(timeago)

def getDay(xMins):
    timeago = time.strftime('%Y-%m-%d',time.localtime(time.time()-xMins*60))
    return str(timeago)

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

def getsqlResult(pyName, sqlStr, queryType):
    (status,result) = commands.getstatusoutput("python "+pyName+" \""+sqlStr+"\" "+" "+queryType)
    if status == 0:
        return result
    else:
        return "0"

def main():
    # all table list
    tableList = getsqlResult("mysql.py",'show tables',"s")
    print(type(tableList))
    print("all table :"+tableList)

    #get curr day
    currDay = getCurrDay(0)
    cDay = getDay(0)

    # add partition
    for i in tableList.split("\n"):
        sql = "alter table "+i+" add partition (partition p"+currDay+" values less than (TO_DAYS('"+cDay+"')));"
        print("sql:"+sql)
        print("add partition for ........."+i)
        getsqlResult("mysql4partition.py",sql,"o")

    # delete partition
    for i in tableList.split("\n"):
        tmp = deleteList.get(i)
        if tmp is None:
            print("use default delete policy")
            intDay = int(deleteList.get("default"))
            sql = "alter table "+i+" drop partition p"+getCurrDay(intDay)
            print("default sql:"+sql)
            getsqlResult("mysql4partition.py",sql,"o")
        else:
            intDay = int(deleteList.get(i))
            sql = "alter table "+i+" drop partition p"+getCurrDay(intDay)
            print("alter sql:"+sql)
            getsqlResult("mysql4partition.py",sql,"o")


if __name__ == "__main__":
        main()
