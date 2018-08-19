#!/bin/bash
#****************************************************************#
# ScriptName: first_add_partition.sh
# Author: liujmsunits@hotmail.com
# Author Github: https://github.com/jimmy-src
# Create Date: 2018-08-15 16:23
# Modify Author: liujmsunits@hotmail.com
# Modify Date: 2018-08-15 16:23
# Function: 
#***************************************************************#
currDay=`date +%Y%m%d`

USER="outbox"

PASSWD="outbox"

HOST="10.114.25.16"

 

for i in `seq 1 8`

do

        sql1="alter table AlarmRecord_100$i change id id int(11)"

        sql2="alter table AlarmRecord_100$i drop primary key;"

        sql3="ALTER TABLE AlarmRecord_100$i ADD PRIMARY KEY(id,Trade_GeneTime);"

        sql4="alter table AlarmRecord_100$i change id id int(11) NOT NULL AUTO_INCREMENT;"

        sql5="alter table AlarmRecord_100$i partition by range (to_days(Trade_GeneTime)) (partition p$currDay values less than (TO_DAYS(\"$currDay\")));"

        sql6="alter table AlarmRecord_100$i add partition (partition p2 values less than (TO_DAYS(\"$currDay\")));"

        sql7="show create table AlarmRecord_100$i"

 

        echo "$sql1 \n $sql2 \n $sql3 \n $sql4 \n $sql5 \n $sql6 \n"

        echo "====================================================="

 

        mysql -u$USER -p$PASSWD -h $HOST -D outbox -e "$sql1"

        mysql -u$USER -p$PASSWD -h $HOST -D outbox -e "$sql2"

        mysql -u$USER -p$PASSWD -h $HOST -D outbox -e "$sql3"

        mysql -u$USER -p$PASSWD -h $HOST -D outbox -e "$sql4"

        mysql -u$USER -p$PASSWD -h $HOST -D outbox -e "$sql5"

        mysql -u$USER -p$PASSWD -h $HOST -D outbox -e "$sql7"

done
