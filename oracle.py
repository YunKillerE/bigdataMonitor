#!/usr/bin/python
# -*- coding: UTF-8 -*-
#****************************************************************#
# ScriptName: oracle.py
# Author: 云尘(jimmy)
# Create Date: 2016-03-16 17:48
# Modify Author: liujmsunits@hotmail.com
# Modify Date: 2016-03-22 00:24
# Function:
#***************************************************************#
import cx_Oracle
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class ORACLE:
    """
    对pymssql的简单封装
    pymssql库，该库到这里下载：http://www.lfd.uci.edu/~gohlke/pythonlibs/#pymssql
    使用该库时，需要在Sql Server Configuration Manager里面将TCP/IP协议开启
    用法：
    """
    def __init__(self,host,user,pwd,db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db

    def __GetConnect(self):
        """
        得到连接信息
        返回: conn.cursor()
        """
        if not self.db:
            raise(NameError,"没有设置数据库信息")
        self.conn = cx_Oracle.connect(self.user+'/'+self.pwd+'@'+self.host+'/'+self.db)
        #self.conn = cx_Oracle.connect(self.user,self.pwd,self.host/self.db)
        #self.conn = cx_Oracle.connect('medauto/tjgl@192.168.3.236/xe')
        cur = self.conn.cursor()
        if not cur:
            raise(NameError,"连接数据库失败")
        else:
            return cur

    def ExecQuery(self,sql):
        """
        执行查询语句
        返回的是一个包含tuple的list，list的元素是记录行，tuple的元素是每行记录的字段

        调用示例：
                ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
                resList = ms.ExecQuery("SELECT id,NickName FROM WeiBoUser")
                for (id,NickName) in resList:
                    print str(id),NickName
        """
        cur = self.__GetConnect()
        cur.execute(sql)
        resList = cur.fetchall()

        #查询完毕后必须关闭连接
        self.conn.close()
        return resList

    def ExecNonQuery(self,sql):
        """
        执行非查询语句

        调用示例：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        cur = self.__GetConnect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()

def main():

## ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
## #返回的是一个包含tuple的list，list的元素是记录行，tuple的元素是每行记录的字段
## ms.ExecNonQuery("insert into WeiBoUser values('2','3')")

    ms = ORACLE(host="192.168.3.236",user="medauto",pwd="tjgl",db="xe")
    #f = open(sys.argv[1],'r')
    #print sys.argv[1]
    mssql = sys.argv[1]
    resList = []
    resList = ms.ExecQuery(mssql)
    for (cc,) in resList:
        resList.apped(cc)
    if len(resList) != 0:
        print(resList)

if __name__ == '__main__':
    main()
