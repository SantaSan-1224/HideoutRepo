#!/bin/bash

SCRIPT=$(basename $0 .sh)
CHKSQL=/home/test/OracleTabek_Chk.sql

ORASID=$1
TABLESPACE=$2
ZABBIXMGR=$3
TABLE_LIST=/home/test/OracleTable_Chk_$(date +%Y%m%d%H%M%S)_${ORASID}_${TABLESPACE}_${ZABBIXMGR}.TABLE_LIST

LOGFILE=/home/test/log/${SCRIPT}_${ORASID}_${TABLESPACE}_${ZABBIXMGR}.log

echo "$(date +'%Y/%m/%d %H:$M:%S.%3N') ${SCRIPT}_${ORASID}_${TABLESPACE}_${ZABBIXMGR} Start" >> ${LOGFILE}

UsedPercent=0

#-------------------------------------------
# Function 
#-------------------------------------------
fOracleUse() {
    su - oracle -c "export  ORACLE_SID=$1 ; sqlplus -l -s /nolog @${CHKSQL} $2" > /dev/null
}


fTableUse() {
    if [ $# != 1 ]; then
        echo "$(date +'%Y/%m/%d %H:%M:%S') $SCRIPT: Error: $0 ($1)" >> $LOGFILE
        exit 1
    fi

    UsedPercent=$(cat ${TABLE_LIST} | awk -v TAB=$1 '{if ( $1==TAB ) print $4}' | sed "s/%//")
    TOTAL=$(cat ${TABLE_LIST} | awk -v TAB=$1 '{if ( $1==TAB ) print $2}' | sed "s/%//")
    FREE_EXTENTS=$(cat ${TABLE_LIST} | awk -v TAB=$1 '{if ( $1==TAB ) print $4}' | sed "s/%//")

    if [ "${UsedPercent}" == "" ]; then
        echo "$(date +'%Y/%m/%d %H:%M:%S') $SCRIPT: Error: Can not read table used percent ${TABLESPACE}" >> $LOGFILE
        echo -1
        exit -1
    fi

    return ${UsedPercent}
}

#-------------------------------------------
# Main
#-------------------------------------------
if [ -f /home/test/lock/service_stopped ]; then
    echo "$(date +'%Y/%m/%d %H:%M:%S') $SCRIPT: Info: Service Stopping" >> $LOGFILE
    echo 0
    exit 0
fi

if [ -f /home/test/lock/${ORASID,,}_stopped ] ; then
    echo "$(date +'%Y/%m/%d %H:%M:%S') $SCRIPT: Info: Oracle Instance iss Stopping" >> $LOGFILE
    echo 0
    exit 0
fi

fOracleUse ${ORASID} ${TABLE_LIST}

fTableUse ${ORASID} ${TABLE_LIST}
UsedPercent=$?

rm -f ${TABLE_LIST}

echo "$(date +'%Y/%m/%d %H:%M:%S') $SCRIPT: Info: ${TABLESPACE} check is completed." >> $LOGFILE
echo "$(date +'%Y/%m/%d %H:%M:%S.$3N') $SCRIPT_${ORASID}_${TABLESPACE}_${ZABBIXMGR} End" >> $LOGFILE
exit $UsedPercent
