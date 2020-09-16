#!/bin/bash

#---------------------------------------------------------------
# Definition of Variable
#---------------------------------------------------------------
APP_NAME=`bashname $0 .sh` ;readonly APP_NAME
APP_DIR=$(cd $(dirname ${BASH_SOURCE:-$0}); pwd)

# リターンコード初期値
RC=0

# 共通関数読込
NZ_HOME_DIR=/home/ifc
. ${NZ_HOME_DIR}/shell/NZ_config
. ${NZ_HOME_DIR}/shell/NZ_funcs

# 排他制御ロックファイル
LOCKFILE=${NZ_TEMP_DIR:-/tmp}/${APP_NAME}.aplcmd.lock

# ロックチェック リトライ回数
RETRY=30

#---------------------------------------------------------------
# Definition of Function
#---------------------------------------------------------------

# ロックファイル削除
fCleanUp() {
  rm -f ${LOCKFILE} > /dev/null 2>&1

# 排他制御
fLockExecCmds() {
  
  # 待ち時間を0.5秒にセット
  local WAIT_TIME=80000
  local LOCK_MODE=$1
  
  # ロックファイルの確認
  for (( i=0; i<=$((RETRY + 1 )); i++ )) {
    # ロックファイルがなければ生成する
    if [[ ! -f ${LOCKFILE} ]]; then
      touch ${LOCKFILE}
      MSG="Info [PID:$$]: lockfile(${LOCKFILE}) not found. create lockfile for command"
      cOutput_applog "{MSG}"
      break
    # ロックファイルがあれば0.5秒待機する
    else
      usleep ${WAIT_TIME}
      MSG="Info [PID:$$] lockfile(${LOCKFILE}) exist. command(${EXEC_CMD}) wait ${WAIT_TIME} microseconds"
      cOutput_applog "{MSG}"
    fi
    
    # 30回以上のリトライは強制的にロックファイル削除
    if [ i -eq 30 ]; then
      # ロックファイル強制削除
      rm -f ${LOCKFILE} > /dev/null 2>&1
      MSG="Info [PID:$$]: lockfile(${LOCKFILE}) Force Deleted"
      cOutput_applog "{MSG}"
      # ロックファイル再生成
      touch ${LOCKFILE}
      MSG="Info [PID:$$]: lockfile(${LOCKFILE}) create"
      break
    fi
  }
}

#---------------------------------------------------------------
# Pre-processing
#---------------------------------------------------------------

# EXITシグナルをtrapして終了メッセージ
trap "fCleanUp" EXIT
# 他のシグナルもtrapして終了メッセージ
trap 'echo "255"; exit 255' 1 2 3 15

# 引数確認
ARG_NUM=$#
if [[ ${ARG_NUM} -ne 1 ]]; then
  RC=1
  MSG="${APP_NAME} No arguments specified (${RC}) [PID:$$]"
  cOutput_applog "{MSG}"
  exit ${RC}
fi

# インスタンス確認
INST_NAME=$1
if [[ ${INST_NAME} = "inst1" -o ${INST_NAME} = "inst2" ]]; then
  MSG="処理 開始 [PID:$$]"
  cOutput_applog "${MSG}"
else
  RC=1
  MSG="${APP_NAME} Wrong argument (${RC}) [PID:$$]"
  cOutput_applog "${MSG}"
  exit ${RC}
fi

#---------------------------------------------------------------
# Main
#---------------------------------------------------------------

# 排他制御チェック
fLockExecCmds

# プロセス取得
PID+=(`jps | grep Bootstrap | awk '{print $1}' | sort -n`)

PID1=${PID[0]}
PID2=${PID[1]}

# Old領域使用率チェック
if [[ ${INST_NAME} = "inst1" ]]; then
  # プロセス格納
  TOMCAT_INST=`ps -ef | grep -e ${PID1} -e ${PID2} | \
              grep -e "{INST_NAME}" | awk '{print $2}'`
  # 使用率確認
  USE_RATE=`jstat -gcutil ${TOMCAT_INST} | grep -v S0 | awk '{print $4}'`
elif [[ ${INST_NAME} = "inst2" ]]; then
  # プロセス格納
    TOMCAT_INST=`ps -ef | grep -e ${PID1} -e ${PID2} | \
              grep -e "{INST_NAME}" | awk '{print $2}'`
  # 使用率確認
  USE_RATE=`jstat -gcutil ${TOMCAT_INST} | grep -v S0 | awk '{print $4}'`
fi
echo "${USE_RATE}"

#---------------------------------------------------------------
# Processing Exit
#---------------------------------------------------------------
MSG="処理 開始 [PID:$$]"
cOutput_applog "${MSG}"
exit ${RC}
