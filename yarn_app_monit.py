#!/usr/bin/env python2
#****************************************************************#
# ScriptName: yarn_app_monit.py
# Author: liujmsunits@hotmail.com
# Author Github: https://github.com/jimmy-src
# Create Date: 2018-06-15 15:18
# Modify Author: liujmsunits@hotmail.com
# Modify Date: 2018-06-15 15:18
# Function:
#***************************************************************#

# -*- coding: utf-8 -*-

from yarn_api_client import ResourceManager
import time
import json
import sys
import os
import datetime

#需求

#1)	每10分钟获取一次过去10分钟提交的所有任务
#2)	如果为空情况：
#	a)	如果连续三次获取到的任务都为空，则直接预警，意味着5mins任务有异常
#3)	如果不为空情况：
#	a)	如果所有任务中有任务的状态为FAILD，则直接预警，意味着有任务失败
#	b)	如果任务为5mins任务，且运行时间超过30分钟，则直接预警
#	c)	如果任务为24小时任务，且运行时间超过8小时，则直接预警


#global variable

xMins = 10
rmPort = 8088
countPath = "/tmp/apps_monitor_count.txt"
#yarnMasterDict = {'vmsit-master2':'yarn-RESOURCEMANAGER-bbdf5ad98cc34e76d3c924177eb8f11f','vmsit-node01':'yarn-RESOURCEMANAGER-bdf23488b2343be6550dadb465f014a8'}
#cmApiAddress = "curl -u admin:admin http://10.114.25.134:7180/api/v17/clusters/cluster/services/yarn/roles/"

yarnMasterDict = {'bfmaster02':'yarn-RESOURCEMANAGER-4ae6f096c469729e7f48a4d2b5ce5e7f','bfmaster01':'yarn-RESOURCEMANAGER-d00b7f79b4b845c282ecc4f58edc8c66'}
cmApiAddress = "curl -u admin:admin http://bf_cm01:7180/api/v17/clusters/cluster/services/yarn/roles/"


allAppsName = 'com.batch.alarmrecord.AlarmRecordFilter \
com.yunchen.batch.BatchOneDay \
com.batch.acctingest.AcctCustomerIngest \
main.ExchangeRateEntranceMain \
com.batch.export.DynamicDetail \
com.batch.export.AccountCustDetail \
com.batch.alarmrecord.AlarmRecordFilter \
com.batch.acctingest.AcctCustomerHive2Ignite \
com.batch.alarmrecord.AlarmRecordFilter \
com.yunchen.footprint.main.MainEntranceDriver \
com.yunchen.batch.wyj.secondwarning.SecondEarlyWarning3 \
com.yunchen.batch.wyj.costomeringest.CustIDNumIngest \
com.yunchen.batch.wyj.custr.CustrToHive \
com.yunchen.batch.wyj.firstwarning.FirstWarning \
com.yunchen.batch.wyj.tbvacation.VacationTable \
com.batch.alarmrecord.AlarmRecordFilter'

#1,get x mins time age

def getTime():
	timeago = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()-xMins*60))
	timearray = time.strptime(timeago,'%Y-%m-%d %H:%M:%S')
	return int(time.mktime(timearray)*1000)

def get0ClockTime():
	t = time.localtime(time.time())
	time1 = time.mktime(time.strptime(time.strftime('%Y-%m-%d 00:00:00',t),'%Y-%m-%d %H:%M:%S'))
	return long(time1*1000)

def getDeltaTime(CURRTIME,APPTIME):
	time1_tmp = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime((CURRTIME/1000)))
	time2_tmp = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime((APPTIME/1000)))
	d1 = datetime.datetime.strptime(time1_tmp,'%Y-%m-%d %H:%M:%S')
	d2 = datetime.datetime.strptime(time2_tmp,'%Y-%m-%d %H:%M:%S')
	d3 = time.strftime('%Y-%m-%d',time.localtime((CURRTIME/1000))) 
	d4 = time.strftime('%Y-%m-%d',time.localtime((APPTIME/1000)))
	if d1 > d2 and d3 == d4:
		return int((d1-d2).seconds)
	elif d1 > d2 and d3 > d4:
		return 24
	else:
		return 0
	
#2,get all apps in x mins

def getRmAddress():
	for i in yarnMasterDict.keys():
		res = os.popen(cmApiAddress+yarnMasterDict.get(i))
		text = res.read()
		x = text.rfind("ACTIVE")
		if x != -1:
			return i


def getAllApps(rmAddress):
	xTime = getTime()
	rm=ResourceManager(address=rmAddress,port=rmPort,timeout=30)
	res=rm.cluster_applications(started_time_begin=str(xTime)).data
	runningApps = rm.cluster_applications(state="RUNNING").data
	if res.get("apps") is None:
		return "null"
	else:
		if runningApps.get("apps") is None:
			return res.get("apps").get("app")
		else:
			return res.get("apps").get("app")+runningApps.get("apps").get("app")
			print(runningApps.get("apps").get("app")+res.get("apps").get("app"))

def get24MinsApp(rmAddress):
	t = get0ClockTime()
	rm=ResourceManager(address=rmAddress,port=rmPort,timeout=30)
	res=rm.cluster_applications(started_time_begin=str(t)).data
	if res.get("apps") is None:
		return "null"
	else:
		tmpList = []
		ll = res.get("apps").get("app")
		for i in ll:
			if i.get("name") == "com.yunchen.batch.BatchOneDay":
				tmpList.append(i)
		return tmpList


def readTmpCount():
	if os.path.exists(countPath):
		fo = open(countPath,"r+")
		return fo.read()
		fo.close
	else:
		return '0'


def writeTmpCount(STR):
	if os.path.exists(countPath):
		os.remove(countPath)
		fo = open(countPath,"w")
		fo.write(STR)
		fo.close
	else:
                fo = open(countPath,"w")
                fo.write(STR)
                fo.close

errorJson = {
	"appName": "com.yunchen.batch.Batch5mins",
	"info": "30mins no submit jobs",
	'healthSummary': "BAD"
}

def emptyApps():
	if readTmpCount() == '0':
		writeTmpCount("1")
	elif readTmpCount() == '1':
		writeTmpCount("2")
	elif readTmpCount() == '2':
		print(errorJson)
		os.remove(countPath)
	else:
		os.remove(countPath)
		sys.exit(0)


def appsProcess(ALLAPPS,stateApp):
	for i in ALLAPPS:
#		if i.get("finalStatus") == "FAILED" or i.get("state") == "FAILED":
#			i.update(healtySummary="BAD")
#			print(json.dumps(i))
#		else:
#			if i.get("state") == "RUNNING":
#				
#				#curr_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
#				#app_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime((float(i.get("startedTime")))/1000))
#				curr_time = int(time.time()*1000)
#				app_time = int(i.get("startedTime"))
#
#				deltTime = getDeltaTime(curr_time, app_time) 
#
#				if i.get("name") == "com.yunchen.batch.Batch5mins":
#					t = int(deltTime/60)
#					if t > 30:
#						i.update(healtySummary="BAD")
#						print(json.dumps(i))
#
#				if i.get("name") == "ExchangeRateDo" or i.get("name") == "com.batch.alarmrecord.AlarmRecordFilter" or i.get("name") == "com.yunchen.batch.Batch24mins":
#					t = int(deltTime/60/60)
#					if t > 8:
#						i.update(healtySummary="BAD")
#						print(json.dumps(i))
		
		curr_time = int(time.time()*1000)
		app_time = int(i.get("startedTime"))
	
		deltTime = getDeltaTime(curr_time, app_time)
		
		if stateApp != "null":
			for s in stateApp:
				if s.get("finalStatus") == "SUCCESS":
					if i.get("name") == "com.yunchen.batch.Batch5mins":
						if i.get("finalStatus") == "FAILED" or i.get("state") == "FAILED":
							i.update(healtySummary="BAD")
							print(json.dumps(i))

					t = int(deltTime/60)
					if t > 30:
						i.update(healtySummary="BAD")
						print(json.dumps(i))

		if allAppsName.rfind(i.get("name")) != -1 : 
			if i.get("finalStatus") == "FAILED" or i.get("state") == "FAILED":
                                i.update(healtySummary="BAD")
                                print(json.dumps(i))

			t = int(deltTime/60/60)
			if t > 8:
				i.update(healtySummary="BAD")
				print(json.dumps(i))							

def main():
	rmAddress = getRmAddress()
	stateApp = get24MinsApp(rmAddress)
	allApps = getAllApps(rmAddress)
	if allApps == "null":
		emptyApps()
	else:
		appsProcess(allApps,stateApp)



if __name__ == "__main__":
        main()
