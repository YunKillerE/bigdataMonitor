#!/bin/bash
#****************************************************************#
# ScriptName: run_monit.sh
# Author: liujmsunits@hotmail.com
# Author Github: https://github.com/jimmy-src
# Create Date: 2018-06-15 15:19
# Modify Author: liujmsunits@hotmail.com
# Modify Date: 2018-06-15 15:19
# Function:
#***************************************************************#


/bigf/admin/monitor/anaconda2/bin/python /bigf/admin/monitor/monit.py >> /bigf/log/tmp3/monit.log
/bigf/admin/monitor/anaconda3/bin/python /bigf/admin/monitor/flink_monitor.py >> /bigf/log/tmp3/monit.log
/bigf/admin/monitor/anaconda2/bin/python /bigf/admin/monitor/yarn_app_monit.py 1>> /bigf/log/tmp3/monit.log 2> /dev/null

