#!/bin/bash

set -x
ROOT=/bigf/admin/monitor/

mkdir -p $ROOT && cd $ROOT
rm -f $ROOT/Anaconda2-4.4.0-Linux-x86_64.sh && wget -P $ROOT http://bfes05/soft/python/Anaconda2-4.4.0-Linux-x86_64.sh
rm -f $ROOT/Anaconda3-4.4.0-Linux-x86_64.sh && wget -P $ROOT http://bfes05/soft/python/Anaconda3-4.4.0-Linux-x86_64.sh

rm -rf /bigf/admin/monitor/anaconda2/ && sh $ROOT/Anaconda2-4.4.0-Linux-x86_64.sh -b -p /bigf/admin/monitor/anaconda2/
rm -rf /bigf/admin/monitor/anaconda3/ && sh $ROOT/Anaconda3-4.4.0-Linux-x86_64.sh -b -p /bigf/admin/monitor/anaconda3/

/bigf/admin/monitor/anaconda2/bin/pip install /bigf/admin/monitor/cm_api-16.0.0.tar.gz

