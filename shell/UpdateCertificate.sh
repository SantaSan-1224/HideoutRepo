#!/bin/bash
#-----------------------------------------------------------------
# File Name	:	NZ_UpdateCertificate.sh
# Description	:	Update of Certification File
#
# Return Code	:	0:Normal End
#			9:Abnormal End
#
#-----------------------------------------------------------------
#-----------------------------------------------------------------
# 変数定義
#-----------------------------------------------------------------
# ファイル名・ディレクトリ・ホスト
APP_NAME=`basename $0 .sh` ;readonly APP_NAME
APP_DIR=$(cd $(dirname ${BASH_SOURCE:-$0}); pwd)
MY_HOSTNAME=`uname -n`	;readonly MY_HOSTNAME

# 戻り値初期値
RC=0

# 共通関数・設定ファイル読込
NZ_HOME_DIR=/home/ifc
NZ_TMP_DIR=${NZ_HOME_DIR}/tmp
. ${NZ_HOME_DIR}/shell/NZ_config
. ${NZ_LIB_DIR}/NZ_funcs
. ${NZ_LIB_DIR}/${APP_NAME}.conf


#-----------------------------------------------------------------
# 関数定義
#-----------------------------------------------------------------
# 異常終了メッセージ出力
fErrorEnd () {
	local MSG_STR="$@"

	cOutput_applog "${MSG_STR}"
	RC=9

	# 終了メッセージ
	cOutput_applog "${APP_NAME} 終了 (RC=${RC})"
	exit ${RC}
}

# SSL設定ファイル書き換え
fModSslConf () {
	local RC=0
	local SSL_CONF=/etc/httpd/conf.d/ssl.conf
	local EXCLUDE_CERTFILE_STR='^SSLCertificateFile'
	local EXCLUDE_CHAINFILE_STR='^SSLCertificateChainFile'


#-----------------------------------------------------------------
# 事前処理
#-----------------------------------------------------------------
# EXITシグナルをtrap
trap 'echo "255"; exit 255' 1 2 3 15

# rootで実行されているか確認
if [[ ${EUID:-${UID}} -ne 0 ]]; then
	fErrorEnd "rootで実行されていません 処理を中止します。"
fi

# 引数チェック
if [[ #? -lt 1 ]]; then
	fErrorEnd "引数が設定されていません。"
fi

#-----------------------------------------------------------------
# メイン処理
#-----------------------------------------------------------------
# 処理開始メッセージ
cOutput_applog "${APP_NAME} 開始"

# 引数取得
SRV_CERTFILE=$1
CNT_CERTFILE=$2

# 中間証明書更新無しの場合
[ -z ${CNT_CERTFILE} ] && CNTCERT_ANSYN_FLG=0
# 中間証明書更新有の場合
[ -n ${CNT_CERTFILE} ] && CNTCERT_ANSYN_FLG=1

# フルパス
SRV_CERTPATH=${NZ_TMP_DIR}/${SRV_CERTFILE}

# サーバ証明書権限変更・移動
if [[ -e ${SRV_CERTPATH } ]]; then
	chmod 644 ${SRV_CERTPATH} && chown root:root ${SRV_CERTPATH}
	
	[[ $? -eq 0 ]] && mv ${SRV_CERTPATH} ${NZ_CSRDIR}/.
fi

# 中間証明書権限変更(更新ありのみ)
if [[ ${CNT_CERTFILE} -eq 1 ]]
	CNT_CERTPATH=${NZ_TMP_DIR}/${CNT_CERTFILE}

	if [[ -e ${CNT_CERTPATH} ]]; then
		chmod 644 ${CNT_CERTPATH} && chown root:root ${CNT_CERTPATH}

		[[ $? -eq 0 ]] && mv ${CNT_CERTPATH} ${NZ_CSRDIR}/.
	fi

fi

# SSL設定ファイルの更新
fModSslConf
