#!/usr/bin/env python3
#****************************************************************#
# ScriptName: flink_monit.py
# Author: liujmsunits@hotmail.com
# Author Github: https://github.com/jimmy-src
# Create Date: 2018-06-15 15:18
# Modify Author: liujmsunits@hotmail.com
# Modify Date: 2018-06-15 15:18
# Function:
#***************************************************************#

# -*- coding: utf-8 -*-

import json
import requests
from datetime import datetime, timedelta
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base

DELETE_TS_RANGE=timedelta(hours=0, minutes=60, seconds=0)
HEALTY_TS_RANGE=timedelta(hours=0, minutes=10, seconds=0)
HEALTY_TS_MIN_RANGE=timedelta(hours=0, minutes=9, seconds=50)
PERSISTENT_STORE="sqlite:////bigf/admin/flink_monitor.db"
JOBMANAGER_HOST = "bfmaster01"
JOBMANAGER_PORT = 8081

Base = declarative_base()

class FlinkMetric(Base):
    __tablename__ = 'flink_metrics'

    id = Column(Integer, primary_key = True)
    ts = Column(DateTime)
    jobname = Column(String)
    vertice_name = Column(String)
    read_bytes = Column(BigInteger)
    write_bytes = Column(BigInteger)
    read_records = Column(BigInteger)
    write_records = Column(BigInteger)

    def __repr__(self):
        return "<FlinkMetric(id='%d', ts='%s', '%s'.'%s', read_bytes='%d', write_bytes='%d', read_records='%d', write_records='%d')>" % (
                self.id, self.ts, self.jobname, self.vertice_name, self.read_bytes, self.write_bytes, self.read_records, self.write_records)

class FlinkApi(object):

    def __init__(self, jobmanager_host, jobmanager_port, persistent_store=None):
        self.jm_host = jobmanager_host
        self.jm_port = jobmanager_port
        self.root_url = "http://%s:%d/" % (jobmanager_host, jobmanager_port)
        self.persistent_store = persistent_store

        if persistent_store:
            self.persistent_get_or_create(persistent_store)

    def persistent_get_or_create(self, persistent_store):
        self.engine = create_engine(persistent_store)
        metadata = MetaData(self.engine)
        if not self.engine.dialect.has_table(self.engine, FlinkMetric.__tablename__):
            Base.metadata.create_all(self.engine)

        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def get_all_running_jobs(self):
        url = self.root_url + "/joboverview"
        r = requests.get(url).json()
        return r

    def get_job(self, job_id):
        url = self.root_url + "/jobs/" + job_id
        r = requests.get(url).json()
        # print(json.dumps(r, indent=2))
        return r

    def monit_running_jobs(self):
        url = self.root_url + "/joboverview"
        r = requests.get(url).json()
        print([j['name'] for j in r['running']])
        return r

    def monit_running_jobs(self, job_name='RecordPreprocessing', vertice_name="Source: KafkaSource"):
        url = self.root_url + "/joboverview"
        try:
          r = requests.get(url).json()

          for j in r['running']:
              if j['name'] == job_name:
                  for v in self.get_job(j['jid'])["vertices"]:
                      if v['name'] == vertice_name:
                          # print(json.dumps(v["metrics"], indent=2))
                          fm = FlinkMetric(ts=datetime.now(), jobname=job_name,
                                  vertice_name=v['name'],
                                  read_bytes=int(v['metrics']['read-bytes']),
                                  write_bytes=int(v['metrics']['write-bytes']),
                                  read_records=int(v['metrics']['read-records']),
                                  write_records=int(v['metrics']['write-records']))
                          self.session.add(fm)
                          self.session.commit()
                          return v["metrics"]
        except Exception as ex:
            print("Catch a exception: '%s'" % str(ex))
            return None

        return None

    def delete_old_persistent_records(self):
        ts_lower_mark = datetime.now() - DELETE_TS_RANGE
        self.session.query(FlinkMetric).filter(FlinkMetric.ts < ts_lower_mark).delete()
        self.session.commit()

    def show_all_persistent_records(self):
        for fm in self.session.query(FlinkMetric).all():
            print(fm)

    def check_healty(self, job_name='RecordPreprocessing', vertice_name="Source: KafkaSource"):
        ts = str(datetime.now())
        ts_mark = datetime.now() - HEALTY_TS_RANGE

        q = self.session.query(
                func.min(FlinkMetric.ts).label("min_ts"),
                func.max(FlinkMetric.ts).label("max_ts"),
                func.min(FlinkMetric.write_bytes).label("min_write_bytes"),
                func.max(FlinkMetric.write_bytes).label("max_write_bytes"),
                func.min(FlinkMetric.write_records).label("min_write_records"),
                func.max(FlinkMetric.write_records).label("max_write_records")
                ).group_by(FlinkMetric.jobname, FlinkMetric.vertice_name).filter(FlinkMetric.ts >= ts_mark)

        for result in q.all():
            ts_delta = result.max_ts - result.min_ts
            if ts_delta > HEALTY_TS_MIN_RANGE:
                healtySummary = "BAD" if result.min_write_records >= result.max_write_records else "GOOD"
            else:
                healtySummary = "GOOD"

            ret = {
                    'ts' : ts,
                    'job_name': job_name,
                    'vertice_name': vertice_name,
                    'ts_delta': str(ts_delta),
                    'min_write_bytes': result.min_write_bytes,
                    'max_write_bytes': result.max_write_bytes,
                    'min_write_records': result.min_write_records,
                    'max_write_records': result.max_write_records,
                    'healthSummary': healtySummary
            }
            print(json.dumps(ret))

def main():
    fapi = FlinkApi(jobmanager_host = JOBMANAGER_HOST,
            jobmanager_port = JOBMANAGER_PORT,
            persistent_store=PERSISTENT_STORE)
    # fapi.get_all_running_jobs()
    ret = fapi.monit_running_jobs()
    if ret:
        fapi.check_healty()
        fapi.delete_old_persistent_records()
    # fapi.show_all_persistent_records()


if __name__ == "__main__":
    main()
