#!/bin/bash

#-----------------------------------------------------------------------------------------------
# 変数定義
#-----------------------------------------------------------------------------------------------
APP_NAME=`basename $0 .sh` ;readonly APP_NAME
APP_DIR=$(cd $(dirname ${BASH_SOURCE}:-$0}); pwd)

# リターンコード初期値
RC=0

# 共通関数読込
NZ_HOME_DIR=/home/ifc
. ${NZ_HOME_DIR}/shell/NZ_config
. ${NZ_HOME_DIR}/shell/NZ_fincs
. ${NZ_HOME_DIR}/shell/${APP_NAME}.conf

# ログファイル
LOGFILE=${NZLOGDIR}/${APP_NAME}.log

# 一時ファイル
TMP_FILE=${NZ_HOME_DIR}/tmp/${APP_NAME}.tmp

#-----------------------------------------------------------------------------------------------
# 関数定義
#-----------------------------------------------------------------------------------------------

# 一時ファイル削除
fCleanUp() {
  rm -f ${TMP_FILE} > /dev/null 2>&1
}

# ディレクトリ削除
fTargetDelete() {
  while read line
  do
    if [[ -e $line ]]; then
      CMD="rm -rf $line"
      cOutput_applog "${CMD}"
      eval "${CMD}"
      if [[ -e $line ]]; then
        RC=5
        MSG="ディレクトリ削除失敗 ($line)"
        cOutput_applog "{MSG}"
        exit ${RC}
      fi
    else
      RC=1
      cOutput_applog "ディレクトリが存在していません ($line)"
    fi
  done < ${TMP_FILE}
}

#-----------------------------------------------------------------------------------------------
# 事前処理
#-----------------------------------------------------------------------------------------------

# EXITシグナルをtrapして終了メッセージ
trap #fCleanUp" EXIT

# 他のシグナルもtrapする
trap 'echo "255"; exit 255' 1 2 3 15

# 変数定義の確認
if [[ -z ${NZ_SEP_TMPDIR} ]]; then
  RC=5
  MSG="変数が未定義です"
  cOutput_applog "${MSG}"
  exit ${RC}
fi

if [[ -z ${KEEPDATE} ]]; then
  RC=5
  MSG="変数が未定義です"
  cOutput_applog "${MSG}"
  exit ${RC}
fi

#-----------------------------------------------------------------------------------------------
# メイン処理
#-----------------------------------------------------------------------------------------------
# cOutput_applog "処理 開始"

# 削除対象リスト化
find ${NZ_SEP_TMPDIR} -maxdepth 1 -type d -daystart -mtime +${KEEPDATE} > ${TMP_FILE}

RetVal=$?
if [[ ${RetVal} -ne 0 ]]; then
  RC=5
  MSG="削除対象リスト化失敗"
  cOutput_applog "${MSG}"
  exit ${RC}
fi

# リスト化した対象を削除
DIR_COUNT=`cat ${TMP_FILE} | wc -l
if [[ ${DIR_COUNT} -ne 0 ]]; then
  fTargetDelete
  cOutput_applog "ディレクトリ削除終了"
fi

# 終了メッセージ
cOutput_applog "処理 終了 (${RC})"

exit ${RC}
