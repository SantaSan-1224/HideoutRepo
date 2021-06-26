#!/bin/bash

SCRIPT=$(basename $0 .sh)
CHKSQL=/home/test/OracleLohin_Chk.sql
ORACLESID=$1
ZABBIXMGR=$2

LOGFILE=/home/test/log/${SCRIPT}_${ORACLESID}_${ZABBIXMGR}.log

echo "$(date +'%Y/%m/%d %H:%M:%S.%3N') ${SCRIPT}_${ORACLESID}_${ZABBIXMGR} Start" >> ${LOGFILE}

if [[ -f /home/test/lock/seervice_stopped ]]; then
    echo "$(date +'%Y/%m/%d %H:%M:%S.%3N') $SCRIPT: Info: ServiceStopping" >> ${LOGFILE}
    echo 0
    exit 0
fi

if [[ $# -lt 1 ]]; then
    echo "$(date +'%Y/%m/%d %H:%M:%S.%3N') $SCRIPT: Error: Oracle instance name not set" >> ${LOGFILE}
    echo 9
    exit 9
fi

if [[ -f /home/test/lock/${ORACLESID,,}_stopped ]]; then
    echo "$(date +'%Y/%m/%d %H:%M:%S.%3N') $SCRIPT: Info: Oracle instance is Stopping" >> ${LOGFILE}
    echo 0
    exit 0
fi

su - oracle -c "export ORACLE_SID=${ORACLESID} ; sqlplus -L -S / as sysdba @${CHKSQL}" << EOF >> /dev/null
exit
EOF
echo $?
echo "$(date +'%Y/%m/%d %H:%M:%S.%3N') ${SCRIPT}_${ORACLESID}_${ZABBIXMGR} End" >> ${LOGFILE}