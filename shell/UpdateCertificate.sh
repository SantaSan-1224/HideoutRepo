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
RC=255

# 共通関数・設定ファイル読込
HOME_DIR=/home
TMP_DIR=${HOME_DIR}/tmp
. ${HOME_DIR}/shell/config
. ${HOME_DIR}/shell/funcs
. ${HOME_DIR}/shell/${APP_NAME}.conf

# チェックサムシェル
CHKSUM_SHELL=/${HOME_DIR}/shell/ChackSum.sh

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

# ファイルバックアップ
fFileBackup() {
	local TGT_FILE="$1"
	local RC=0

	if [[ ! -f ${TGT_FILE} ]]; then
		cOutput_applog "${TGT_FILE}が存在しませんのでファイルのバックアップはしません。"
		RC=255
		return ${RC}
	fi

	local BK_FILE=$(dirname ${TGT_FILE})/$(basename ${TGT_FILE}).`date "+%Y%m%d"`

	CMD="cp -p ${TGT_FILE} ${BK_FILE}"
	cOutput_applog "FileBackup: ${CMD}"
	eval "${CMD}"

	# 無駄なバックアップは削除
	LAST1_BK_FILE=`ls -1t ${TGT_FILE}.* | head -n 1`
	LAST2_BK_FILE=`ls -1t ${TGT_FILE}.* | head -n 2 | tail -n 1`

	if [[ ${LAST1_BK_FILE} != ${LAST2_BK_FILE} ]]; then
		diff ${LAST1_BK_FILE} ${LAST2_BK_FILE} >/dev/null 2>&1
		RESULT=$?
		if [[ ${RESULT} -eq 0 ]]; then
			MSG="Not Modified. remove ${LAST2_BK_FILE}"
			cOutput_applog "${MSG}"
			rm -f ${LAST2_BK_FILE}
		fi
	fi

	return ${RC}

}

# SSL設定ファイル書き換え
fModSslConf () {
	local RC=0
	local SSL_CONF=/etc/httpd/conf.d/ssl.conf
	local EXCLUDE_CERTFILE_STR='^SSLCertificateFile'
	local EXCLUDE_CHAINFILE_STR='^SSLCertificateChainFile'
	
	local RESULT1=`grep ${EXCLUDE_CERTFILE_STR} ${SSL_CONF} | wc -l`
	local RESULT2=`grep ${EXCLUDE_CHAINFILE_STR} ${SSL_CONF} | wc -l`

	if [[ ${RESULT1} -eq 1 ]] && [[ ${RESULT2} -eq 1 ]]; then
		fFileBackup ${SSL_CONF}
		local LAST_EXCLUDE_LINE1=`grep -e ${EXCLUDE_CERTFILE_STR} -n ${SSL_CONF} | \
			sort -n | tail -1 | sed -e 's/:.*//g'`
		sed -i -e 's/^SSLCertificateFile/#SSLCertificateFile/' ${SSL_CONF} && \
			sed -i -e "${LAST_EXCLUDE_LINE1}a SSLCertificateFile ${SRV_CERTPATH}" \
			${SSL_CONF}
		local LAST_EXCLUDE_LINE2=`grep -e ${EXCLUDE_CHAINFILE_STR} -n ${SSL_CONF} | \
			sort -n | tail -1 | sed -e 's/:.*//g'`
		sed -i -e 's/^SSLCertificateChainFile/#SSLCertificateChainFile/' ${SSL_CONF} && \
			sed -i -e "${LAST_EXCLUDE_LINE2}a SSLCertificateChainFile ${CNT_CERTPATH}" | \
			${SSL_CONF}

		# 確認
		local CNF_EXCLUDE_STR1=`grep '^SSLCertificateFile' ${SSL_CONF} | cut -d ' ' -f 2`
		local CNF_EXCLUDE_STR2=`grep '^SSLCertificateChainFile' ${SSL_CONF} | cut -d ' ' -f 2`

		if [[ "${CNF_EXCLUDE_STR1}" = "${SRV_SERTPATH}" ]]; then
			cOutput_applog "SSLCertificateFile Check OK!"
		else
			fErrorEnd "SSLCertificateFile Check NG!"
		fi

		if [[ "${CNF_EXCLUDE_STR2}" = "${CNT_CERTPATH}" ]]; then
			cOutput_applog "SSLCertificateChainFile Check OK!"
		else
			fErrorEnd "SSLCertificateChainFile Check NG!"
		fi

	elif [[ ${RESULT1} -eq 1 ]] && [[ ${RESULT2} -eq 0 ]]; then
		fFileBackup ${SSL_CONF}
		local LAST_EXCLUDE_LINE=`grep -e ${EXCLUDE_CERTFILE_STR} -n ${SSL_CONF} | \
			sort -n | tail -1 | sed -e 's/:.*//g'`
		sed -i -e 's/^SSLCertificateFile/#SSLCertificateFile/' ${SSL_CONF} && \
			sed -i -e "${LAST_EXCLUDE_LINE}a SSLCertificateFile ${SRV_CERTPATH}" \
			${SSL_CONF}
		
		# 確認
		local CNF_EXCLUDE_STR=`grep '^SSLCertificateFile' ${SSL_CONF} | cut -d ' ' -f 2`

		if [[ "${CNF_EXCLUDE_STR}" = "${SRV_CERTPATH}" ]]; then
			cOutput_applog "SSLCertificatFile Check OK!"
		else
			fErrorEnd "SSLCertificateFile Check NG!"
		fi

	else
		fErrorEnd "設定ファイル(${SSL_CONF})に誤りがあります"
	
	fi

}

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
SRV_CERTPATH=${TMP_DIR}/${SRV_CERTFILE}

# サーバ証明書権限変更・移動
if [[ -e ${SRV_CERTPATH } ]]; then
	chmod 644 ${SRV_CERTPATH} && chown root:root ${SRV_CERTPATH}
	
	[[ $? -eq 0 ]] && mv -f ${SRV_CERTPATH} ${NZ_CSRDIR}/.
fi

# 中間証明書権限変更(更新ありのみ)
if [[ ${CNT_CERTFILE} -eq 1 ]]
	CNT_CERTPATH=${TMP_DIR}/${CNT_CERTFILE}

	if [[ -e ${CNT_CERTPATH} ]]; then
		chmod 644 ${CNT_CERTPATH} && chown root:root ${CNT_CERTPATH}

		[[ $? -eq 0 ]] && mv -f ${CNT_CERTPATH} ${CSRDIR}/.
	fi

fi

# SSL設定ファイルの更新
fModSslConf

# 秘密鍵ファイル移動とパスフレーズ削除
if [[ -e ${PRIV_NEWFILE} ]]; then
	fFileBackup ${PRIVFILE}
	RC=$?
	if [[ ${RC} -eq 0 ]]; then
		mv -f ${PRIV_NEWFILE} ${PRIVFILE}
		RC=$?

		if [[ ${RC} -eq 0 ]]; then
			CMD="openssl rsa -in ${PRIVFILE} -out ${PRIVFILE} \
				-passout pass:${PRIVKEYPWD}
			cOutput_applog "${CMD}"
			eval "${CMD}"
			RC=$?
			if [[ ${RC} -eq 0 ]]; then
				${CHKSUM_SHELL} create
			else
				fErrorEnd "パスフレーズの削除失敗"
			fi
		fi
	fi
fi

#-----------------------------------------------------------------
# 事後処理
#-----------------------------------------------------------------
# 処理終了メッセージ
cOutput_applog "${APP_NAME} 終了 (RC=${RC})"
exit ${RC}
