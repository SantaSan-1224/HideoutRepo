#!/bin/bash

#--------------------------------------------------------------------
# Drfine of General Variable
#--------------------------------------------------------------------
APP_NAME=$(basename $0 .sh) ;readonly APP_NAME
APP_DIR=$(cd $(dirname ${BASH_SOURCE:-$0}); pwd)
MY_HOSTNAME=$(uname -n) ;readonly MY_HOSTNAME
MY_OS=$(uname) ;readonly MY_OS
ARG_NUM=$# ;readonly ARG_NUM

#--------------------------------------------------------------------
# Read Environment Definition
#--------------------------------------------------------------------
APP_HOME=${APP_DIR} ;readonly APP_HOME
INPUT_FILE=${APP_HOME}/server_params_${MY_HOSTNAME}

if [ -f ${INPUT_FILE} ]; then
    source ${INPUT_FILE}
else
    echo  "There is no read file(${INPUT_FILE})"
    exit 255
fi

INPUT_FILE=${OS_USER_LIST_FILE_NAME}
if [ ! -f ${INPUT_FILE} ]; then
	echo "There is no read file(${INPUT_FILE})"
	exit 255
fi

#--------------------------------------------------------------------
# Individual Variable Definition
#--------------------------------------------------------------------

# ログファイルの定義
LOG_DATE=$(date +'%Y%m%d_%H%M%S')
LOGFILE=${APP_HOME}/KY_${APP_NAME}_${MY_HOSTNAME}_${LOG_DATE}.log

# インストールモジュールの場所
MODULE_CP_SRC_DIR=${APP_HOME}/modules

# インフラツール群のコピー元ファイルの配置場所
INFRA_TOOLS_CP_SRC_DIR=${APP_HOME}/scripts

# 一時ファイル
TMP_FILE_MAIN=${APP_HOME}/${APP_NAME}.tmpm.$$
TMP_FILE_FUNC=${APP_HOME}/${APP_NAME}.tmpf.$$
TMP_FILE_FUNC2=${APP_HOME}/${APP_NAME}.tmpf2.$$

#--------------------------------------------------------------------
# Individual Function Definition
#--------------------------------------------------------------------
fCleanup() {
        rm -f ${TMP_FILE_MAIN} >/dev/null 2>&1
        rm -f ${TMP_FILE_FUNC} >/dev/null 2>&1
        rm -f ${TMP_FILE_FUNC2} >/dev/null 2>&1
}

fOutputLog () {
	local MSG_STR="$@"

	if [ ! -f ${LOGFILE} ]; then
		touch ${LOGFILE}
	fi

	#local E_DATE=`date +"%Y-%m-%d %H:%M:%S.%3N"`
	echo "${MSG_STR}"
	echo "${MSG_STR}" >>${LOGFILE}
}

fOutputFileToLog () {
	
	local FILE_TO_LOG="$1"

	while IFS= read fline || [ -n "$fline" ]
	do
		fOutputLog "${fline}"
	done <${FILE_TO_LOG}
}

fPrintHeader () {

	local P_CHK_NAME="$1"
	local P_CHK_DESC="$2"
	E_DATE=$(date +"%Y-%m-%d %H:%M:%S")

	fOutputLog "######################### ${P_CHK_NAME} #########################"
	fOutputLog "# 処理概説   : ${P_CHK_DESC}"
	fOutputLog "# 処理ホスト : ${MY_HOSTNAME}"
	fOutputLog "# 処理実施日 : ${E_DATE}"
	fOutputLog ""
}

fDel_coment_and_blankline() {
	local TGT_FILE=$1
	local RET_VAL=255

	if [ -f ${TGT_FILE} ]; then
		cat ${TGT_FILE} | sed -e 's/#.*//' -e '/^[\s\t]*$/d'
		RET_VAL=$?
	else
		RET_VAL=1
	fi

	return ${RET_VAL}
}

fFileBackup () {
	local TGT_FILE="$1"
	local RC=0

	if [ ! -f ${TGT_FILE} ];then
		fOutputLog "${TGT_FILE}が存在しませんのでファイルのバックアップはしません"
		RC=255
		return ${RC}
	fi

	local BK_FILE=$(dirname ${TGT_FILE})/$(basename ${TGT_FILE}).$(date "+%Y%m%d_%H%M%S")

	CMD="cp -p ${TGT_FILE} ${BK_FILE}"
	fOutputLog "FileBackup: ${CMD}"
	eval "${CMD}" >${TMP_FILE_FUNC} 2>&1

	fOutputFileToLog ${TMP_FILE_FUNC}

	# 無駄なバックアップは削除
	LAST1_BK_FILE=$(ls -1t ${TGT_FILE}.* | head -n 1)
	LAST2_BK_FILE=$(ls -1t ${TGT_FILE}.* | head -n 2 | tail -n 1)

	if [ ${LAST1_BK_FILE} != ${LAST2_BK_FILE} ];then
		diff ${LAST1_BK_FILE} ${LAST2_BK_FILE} >/dev/null 2>&1
		RESULT=$?
		if [ ${RESULT} -eq 0 ]; then
			MSG="Not modified. remove ${LAST2_BK_FILE}"
			fOutputLog "${MSG}"
			rm -f ${LAST2_BK_FILE}
		fi
	fi

	return ${RC}
}

fAppendStringToFile () {
	local TGT_FILE="$1"
	local ADD_STR="$2"

	grep "${ADD_STR}" ${TGT_FILE} >/dev/null 2>&1
	RESULT=$?
	if [ ${RESULT} -ne 0 ];then
		fOutputLog "Add: ${ADD_STR} -> ${TGT_FILE}"
		echo ${ADD_STR} >>${TGT_FILE}
	fi
}

fExitForReboot () {

	# 再起動を促すメッセージを表示
	fOutputLog ""
	MSG="OSを再起動して設定を有効化してください"
	fOutputLog ""
	fOutputLog "${MSG}"
	fOutputLog ""
	
	# 再起動して際にNICが有効になるかチェック
	local DEV_NAME=$(nmcli connection show | grep -i ethernet | awk '{print $5}' | grep [a-zA-Z])
	CMD="grep -i onboot /etc/sysconfig/network-scripts/ifcfg-${DEV_NAME} | grep -i yes"
	fOutputLog "${CMD}"
	eval "${CMD}"
	RESULT=$?
	if [ ${RESULT} -ne 0 ];then
		local DEV_NAME=$(nmcli connection show | grep -i ethernet | awk '{print $4}')
		fOutputLog ""
		fOutputLog "※※ 注意 !!! ※※"
		fOutputLog "起動時にNICが有効になるように設定されていません。自動接続するように設定を変更してから再起動してください"
		fOutputLog "Ex) # nmcli c m ${DEV_NAME} connection.autoconnect yes"
		fOutputLog ""
	fi
	
	exit 1
}

fChangeParamsOnCnfFile () {

	local TGT_FILE=$1
	local GREP_STR="$2"
	local ADD_STR="$3"

	local LAST_LINE=$(grep -E "${GREP_STR}" -n ${TGT_FILE} | sort -n | tail -1 | sed -e 's/:.*//g')
	if [ -n "${LAST_LINE}" ];then
		sed -i -e "/^#/!s/\(${GREP_STR}\)/#\1/" ${TGT_FILE}
		sed -i -e "${LAST_LINE}a ${ADD_STR}" ${TGT_FILE}
	else
		echo "${ADD_STR}" >>${TGT_FILE}
	fi
}

fCheck_SvrRes () {

	CHK_NAME="Server Resource"
	CHK_DESC="受領したサーバがオーダーと相違ないか確認"
	fPrintHeader "${CHK_NAME}" "${CHK_DESC}"

	fOutputLog "システム名称 : ${SYSTEM_NAME}"
	fOutputLog ""

	# OS
	fOutputLog "SystemRelease(オーダー): ${OS_RELEASE}"
	local REAL_VAL=$(cat /etc/system-release)
	fOutputLog "SystemRelease(実機)    : ${REAL_VAL}"
	fOutputLog ""

	# vCPU
	fOutputLog "vCPU数(オーダー) : ${VCPU_NUM}"
	local REAL_VAL=$(cat /proc/cpuinfo | grep -c processor)
	fOutputLog "vCPU数(実機)     : ${REAL_VAL}"
	fOutputLog ""

	# Memory & Swap
		fOutputLog "Memory Size(オーダー) : ${P_MEM}G"
		local REAL_VAL=$(free -h | grep Mem: | awk '{print $2}')
		fOutputLog "Memory Size(実機)     : ${REAL_VAL}"
		fOutputLog "Swap Size(オーダー) : ${P_SWAP}G"
		local REAL_VAL=$(free -h | grep Swap: | awk '{print $2}')
		fOutputLog "Swap Size(実機)     : ${REAL_VAL}"
		fOutputLog ""

	# Disk Size
	for device in "${!DISKSIZE_[@]}"
	do
		fOutputLog "DiskSize(オーダー) : ${device}: ${DISKSIZE_[${device}]}GB"
		local REAL_DISK_SIZE=$(parted -s ${device} p | grep ${device} | awk '{print $2,$3}')
		fOutputLog "DiskSize(実機)     : ${REAL_DISK_SIZE}"
		fOutputLog ""
	done

	# Network
	local REAL_VAL=$(ip address | grep inet | grep -v "127.0.0.1" | awk '{print $2}')
	key=inet
	fOutputLog "IP Address(オーダー) : ${OS_NW_PARAM_[$key]}"
	fOutputLog "IP Address(実機)     : ${REAL_VAL}"

	local REAL_VAL=$(ip route | grep default | awk '{print $3}')
	key=default_gw
	fOutputLog "Default route(オーダー) : ${OS_NW_PARAM_[$key]}"
	fOutputLog "Default route(実機)     : ${REAL_VAL}"
        fOutputLog ""
	
        fOutputLog "==== 実機ネットワーク関連情報 ===="
	CMD="nmcli device status"
	fOutputLog "${CMD}"
	NW_VAL_STR=$(eval "${CMD}")
	fOutputLog "${NW_VAL_STR}"
	fOutputLog ""
	
	local NW_DEV_NAME=$(nmcli device status | grep ethernet | cut -d ' ' -f 1)

	CMD="nmcli device show ${NW_DEV_NAME}"
	fOutputLog "${CMD}"
	NW_VAL_STR=$(eval "${CMD}")
	fOutputLog "${NW_VAL_STR}"
	fOutputLog ""
}

fModWgetProxy () {
	local WGETRC_FILE=/etc/wgetrc
	local PROXY_STR=''

	if [ ! -f ${WGETRC_FILE} ];then
		fOutputLog "${WGETRC_FILE} not exist"
		return 255
	fi

	fDel_coment_and_blankline ${WGETRC_FILE} >${TMP_FILE_FUNC}

	if [ -z "${OS_NW_PARAM_[http_proxy]}" ];then
		RESULT0=0
		RESULT1=0
		RESULT2=0
	else
		PROXY_STR='https_proxy'
		grep "${PROXY_STR}" ${TMP_FILE_FUNC} | grep "${OS_NW_PARAM_[http_proxy]}" >/dev/null 2>&1
		RESULT0=$?

		PROXY_STR='http_proxy'
		grep "${PROXY_STR}" ${TMP_FILE_FUNC} | grep "${OS_NW_PARAM_[http_proxy]}" >/dev/null 2>&1
		RESULT1=$?

		grep "use_proxy" ${TMP_FILE_FUNC} | grep "on" >/dev/null 2>&1
		RESULT2=$?
	fi

	RESULT=$(( RESULT0 + RESULT1 + RESULT2 ))

	if [ ${RESULT} -ne 0 ];then
		fOutputLog "${WGETRC_FILE}を変更します"
		fOutputLog ""

		fFileBackup ${WGETRC_FILE}

		if [ ${RESULT0} -ne 0 ];then

			PROXY_STR='https_proxy'
			local LAST_PROXY_LINE=$(grep -e "${PROXY_STR}" -n ${WGETRC_FILE} | sort -n | tail -1 | sed -e 's/:.*//g')
			sed -i -e "s/^${PROXY_STR}/#${PROXY_STR}/" ${WGETRC_FILE}
			sed -i -e "${LAST_PROXY_LINE}a ${PROXY_STR} = ${OS_NW_PARAM_[http_proxy]}" ${WGETRC_FILE}
		fi

		if [ ${RESULT1} -ne 0 ];then

			PROXY_STR='http_proxy'
			local LAST_PROXY_LINE=$(grep -e "${PROXY_STR}" -n ${WGETRC_FILE} | sort -n | tail -1 | sed -e 's/:.*//g')
			sed -i -e "s/^${PROXY_STR}/#${PROXY_STR}/" ${WGETRC_FILE}
			sed -i -e "${LAST_PROXY_LINE}a ${PROXY_STR} = ${OS_NW_PARAM_[http_proxy]}" ${WGETRC_FILE}
		fi

		if [ ${RESULT2} -ne 0 ];then

			PROXY_STR='use_proxy'
			local LAST_PROXY_LINE=$(grep -e "${PROXY_STR}" -n ${WGETRC_FILE} | sort -n | tail -1 | sed -e 's/:.*//g')
			sed -i -e "s/^${PROXY_STR}/#${PROXY_STR}/" ${WGETRC_FILE}
			sed -i -e "${LAST_PROXY_LINE}a ${PROXY_STR} = on" ${WGETRC_FILE}
		fi
	else
		fOutputLog "${WGETRC_FILE} : OK"
		
		fOutputFileToLog ${TMP_FILE_FUNC}
		fOutputLog ""
	fi

	# 確認
	CMD='wget -O /dev/null http://yahoo.co.jp'
	fOutputLog "${CMD}"
	eval "${CMD}" >${TMP_FILE_FUNC} 2>&1
	RESULT=$?
	
	if [ ${RESULT} -ne 0 ];then
		fOutputLog "wget : NG"
		fOutputFileToLog ${WGETRC_FILE}
		fOutputLog ""
	else
		fOutputLog "wget : OK"
	fi
}

fChangeServiceWithEnabled () {

	local TGT_SERVICE="$1"
	local TGT_ENABLED="$2"
	
	case "${TGT_ENABLED}" in
		"enabled"|"enable")
			CMD="systemctl enable ${TGT_SERVICE}" ;;
		"disabled"|"disable")
			CMD="systemctl disable ${TGT_SERVICE}" ;;
		"masked"|"mask")
			CMD="systemctl mask ${TGT_SERVICE}" ;;
		*)
			fOutputLog "よくわかりません 1:[${TGT_SERVICE}] 2:[${TGT_ENABLED}]" ;;
	esac

	fOutputLog "${CMD}"
	eval "${CMD}" >${TMP_FILE_FUNC2} 2>&1
	RESULT=$?
	if [ ${RESULT} -ne 0 ];then
		fOutputFileToLog ${TMP_FILE_FUNC2}
	fi
}

fSetAutostartServices () {

	local RC=0

	fOutputLog "Autostart Service Check"

	# is-enabled
	for service in "${!OS_SERVICE_[@]}"
	do
		SERVICE_ENABLED=$(systemctl is-enabled ${service} 2>/dev/null)

		if [ -z "${SERVICE_ENABLED}" ];then
			fOutputLog "${service} : searvice not exist"
			
		elif [ "${SERVICE_ENABLED}" = "${OS_SERVICE_[${service}]}" ];then
			fOutputLog "${service} [${OS_SERVICE_[${service}]}] : OK"
		else
			fOutputLog "${service} [設計->${OS_SERVICE_[${service}]}, 実機->${SERVICE_ENABLED}] : Set to ${OS_SERVICE_[${service}]}"
			fChangeServiceWithEnabled "${service}" "${OS_SERVICE_[${service}]}"
			RC=$(( ${RC} + 1 ))
		fi
	done

	fOutputLog ""

	return ${RC}
}

fChengeSystemdiskToInode32 () {

	local RC=255
	local FSTAB_FILE=/etc/fstab

	CHK_CMD="mount | grep xfs | grep inode64"
	fOutputLog "${CHK_CMD}"
	eval "${CHK_CMD}" >${TMP_FILE_MAIN} 2>&1
	RESULT=$?

	if [ ${RESULT} -eq 0 ]; then

		fOutputFileToLog ${TMP_FILE_MAIN}
		fOutputLog "${SYSTEM_DISK_[devname]} のマウントオプションを変更します"
		fFileBackup ${FSTAB_FILE}
		CMD="grep 'xfs' -n ${FSTAB_FILE} | grep -v 'inode32' | sort -n | sed -e 's/:.*//g'"
		XFS_INODE64_LINE=$(eval "${CMD}")

		for linenum in ${XFS_INODE64_LINE}
		do
			sed -i -e "${linenum} s/defaults/defaults,inode32/g" ${FSTAB_FILE}	
		done

		local WC_XFS=$(grep 'xfs' ${FSTAB_FILE} | wc -l)
		local WC_INODE32=$(grep 'inode32' ${FSTAB_FILE} | wc -l)
		if [ ${WC_XFS} -eq ${WC_INODE32} -a ${WC_XFS} -gt 0 ];then
			RC=0
		else
			RC=1
		fi
	else
		fOutputLog "${SYSTEM_DISK_[devname]} inode32 : OK"
		fOutputFileToLog ${TMP_FILE_MAIN}
		mount | grep 'xfs' >${TMP_FILE_FUNC} 2>&1
		fOutputFileToLog ${TMP_FILE_FUNC}
	fi

	return ${RC}
}

fChangeKernelParams () {

	local RC=255
	local RC1=1
	local RC2=1

	local LIMITS_FILE=/etc/security/limits.conf
	local SYSCTL_FILE=/etc/sysctl.conf

	for key in "${!KERNEL_LIMITS_[@]}"
	do
		grep "${KERNEL_LIMITS_[$key]}" ${LIMITS_FILE} >/dev/null 2>&1
		RESULT=$?
		if [ ${RESULT} -ne 0 ];then
			echo "${KERNEL_LIMITS_[$key]}" >>${LIMITS_FILE}
			RC1=0
		fi
	done
	sed -i -e "/# End of file/d" ${LIMITS_FILE}

	if [ ${RC1} -eq 0 ];then
		fOutputFileToLog ${LIMITS_FILE}
		fOutputLog ""
	else
		fOutputLog "${LIMITS_FILE} : OK"
	fi
	echo "# End of file" >>${LIMITS_FILE}

	for key in "${!KERNEL_SYSCTL_[@]}"
	do
		grep "${KERNEL_SYSCTL_[$key]}" ${SYSCTL_FILE} >/dev/null 2>&1
		RESULT=$?
		if [ ${RESULT} -ne 0 ];then
			echo "${key} = ${KERNEL_SYSCTL_[$key]}" >>${SYSCTL_FILE}
			RC2=0
		fi
	done

	if [ ${RC2} -eq 0 ];then
		fOutputFileToLog ${SYSCTL_FILE}
		fOutputLog ""
	else
		fOutputLog "${SYSCTL_FILE} : OK"
	fi
	
	RC=$(( RC1 * RC2 ))

	return ${RC}
}

fCheckSelinux () {
	local RC=0


	key="selinux"
	CMD="getenforce"
	local SELINUX_STATUS=$(eval "${CMD}")
	fOutputLog "${CMD}: ${SELINUX_STATUS}"

	if [ "${SELINUX_STATUS}" = "${OS_BASIC_PARAM_[$key]}" ];then
		fOutputLog "SELINUX [設計->${SELINUX_STATUS}] : OK"
	else
		fOutputLog ""
		fOutputLog "SELINUXの設定を変更します"
		fOutputLog ""
		CMD="setenforce 0"
		fOutputLog "${CMD}"
		eval ${CMD}
	
		local SELINUX_CNF_FILE=/etc/selinux/config
		fFileBackup ${SELINUX_CNF_FILE}
		local LAST_SELINUX_LINE=$(grep -e 'SELINUX=' -n ${SELINUX_CNF_FILE} | sort -n | tail -1 | sed -e 's/:.*//g')
		CMD="sed -i -e '${LAST_SELINUX_LINE}d' ${SELINUX_CNF_FILE}"
		fOutputLog ${CMD}
		eval ${CMD}
		CMD="sed -i -e '${LAST_SELINUX_LINE}i SELINUX=${OS_BASIC_PARAM_[$key]}' ${SELINUX_CNF_FILE}"
		fOutputLog ${CMD}
		eval ${CMD}

		RC=1
	fi

	fOutputLog ""
	return ${RC}
}

fCheckTimzoneAndLocale () {
	local RC=0

	# TimeZone
	key="timezone"
	CMD="timedatectl status"
	local TIMEZONE_STR=$(eval "${CMD}" | grep "${OS_BASIC_PARAM_[$key]}")
	TIMEZONE_STR=$(echo ${TIMEZONE_STR} | sed -e 's/^\s+//')
	if [ -n "${TIMEZONE_STR}" ];then

		fOutputLog "${TIMEZONE_STR} : OK"
	else
		TIMEZONE_STR=$(eval "${CMD}" | grep "zone" | sed -e 's/^\s+//')
		fOutputLog "${TIMEZONE_STR}"
		fOutputLog ""
		fOutputLog "Time zone を変更します"
		fOutputLog ""
		CMD="timedatectl set-timezone ${OS_BASIC_PARAM_[$key]}"
		fOutputLog "${CMD}"
		eval "${CMD}"

		RC=1
	fi
	fOutputLog ""

	# Locale
	key="locale"
	CMD="localectl status"
	local LOCALE_STR=$(eval "${CMD}" | grep "Locale:" | awk -F'=' '{print $2}')
	if [ "${LOCALE_STR}" = "${OS_BASIC_PARAM_[$key]}" ];then
		fOutputLog "${CMD} : ${LOCALE_STR} : OK"
	else
		fOutputLog ""
		fOutputLog "System Locale の設定を変更します"
		fOutputLog ""
		CMD="localectl set-locale LANG=${OS_BASIC_PARAM_[$key]}"
		fOutputLog "${CMD}"
		eval ${CMD}

		RC=1
	fi

	fOutputLog ""
	return ${RC}
}

fCheckResolvConf () {
	local RC=0
	local RESOLV_CNF_FILE=/etc/resolv.conf
	local SEARCH_STR=''

	fDel_coment_and_blankline ${RESOLV_CNF_FILE} >${TMP_FILE_FUNC}

	key="search"
	SEARCH_STR=${OS_NW_PARAM_[$key]}
	if [ -z "${SEARCH_STR}" ];then
		fOutputLog "search domain not defined"
		RESULT0=0
	else
		grep ${key} ${TMP_FILE_FUNC} | grep ${SEARCH_STR} >/dev/null 2>&1
		RESULT0=$?
	fi
	
	key="nameserver"
	SEARCH_STR=$(echo ${OS_NW_PARAM_[$key]} | cut -d ',' -f 1)
	grep ${key} ${TMP_FILE_FUNC} | grep ${SEARCH_STR} >/dev/null 2>&1
	RESULT1=$?

	SEARCH_STR=$(echo ${OS_NW_PARAM_[$key]} | cut -d ',' -f 2)
	grep ${key} ${TMP_FILE_FUNC} | grep ${SEARCH_STR} >/dev/null 2>&1
	RESULT2=$?

	RESULT=$(( RESULT0 + RESULT1 + RESULT2 ))

	if [ ${RESULT} -ne 0 ];then
		fOutputLog "${RESOLV_CNF_FILE} を変更します"
		fOutputLog ""
		fFileBackup ${RESOLV_CNF_FILE}

		if [ ${RESULT0} -ne 0 ];then
			key=search
			local LAST_SEARCH_LINE=$(grep -e "${key}" -n ${RESOLV_CNF_FILE} | sort -n | tail -1 | sed -e 's/:.*//g')
			if [ "${LAST_SEARCH_LINE}" = '' ];then
				LAST_SEARCH_LINE=1
			fi
			sed -i -e "s/^${key}/#${key}/" ${RESOLV_CNF_FILE}
			sed -i -e "${LAST_SEARCH_LINE}a ${key} ${OS_NW_PARAM_[$key]}" ${RESOLV_CNF_FILE}
		fi

		if [ ${RESULT1} -ne 0 -o ${RESULT2} -ne 0 ];then
			key=nameserver
			local LAST_SEARCH_LINE=$(grep -e "${key}" -n ${RESOLV_CNF_FILE} | sort -n | tail -1 | sed -e 's/:.*//g')
			sed -i -e "s/^${key}/#${key}/" ${RESOLV_CNF_FILE}
			sed -i -e "${LAST_SEARCH_LINE}a ${key} $(echo ${OS_NW_PARAM_[$key]} | cut -d ',' -f 2)" ${RESOLV_CNF_FILE}
			sed -i -e "${LAST_SEARCH_LINE}a ${key} $(echo ${OS_NW_PARAM_[$key]} | cut -d ',' -f 1)" ${RESOLV_CNF_FILE}
		fi

		# 確認
		key="search"
		SEARCH_STR=${OS_NW_PARAM_[$key]}
		grep ${key} ${RESOLV_CNF_FILE} | grep ${SEARCH_STR} >/dev/null 2>&1
		RESULT0=$?
		
		key="nameserver"
		SEARCH_STR=$(echo ${OS_NW_PARAM_[$key]} | cut -d ',' -f 1)
		grep ${key} ${RESOLV_CNF_FILE} | grep ${SEARCH_STR} >/dev/null 2>&1
		RESULT1=$?
	
		SEARCH_STR=$(echo ${OS_NW_PARAM_[$key]} | cut -d ',' -f 2)
		grep ${key} ${RESOLV_CNF_FILE} | grep ${SEARCH_STR} >/dev/null 2>&1
		RESULT2=$?
	
		RESULT=$(( RESULT0 + RESULT1 + RESULT2 ))
		
		if [ ${RESULT} -ne 0 ];then
			fOutputLog "${RESOLV_CNF_FILE}の変更に失敗しました。ファイル内容を確認してください"
			fOutputLog ""
		else
			fOutputFileToLog ${RESOLV_CNF_FILE}
		fi
	else
		fOutputLog "${RESOLV_CNF_FILE} : OK"
		fOutputFileToLog ${RESOLV_CNF_FILE}
	fi

	fOutputLog ""
	return ${RC}
}

fCheckSystemdLoglevel () {
	local RC=0
	local SYSTEMD_CNF_FILE=/etc/systemd/system.conf
	
	key=LogLevel
	SEARCH_STR=${OS_BASIC_PARAM_[$key]}

	fDel_coment_and_blankline ${SYSTEMD_CNF_FILE} >${TMP_FILE_FUNC} 2>&1

	grep ${key} ${TMP_FILE_FUNC} | grep ${SEARCH_STR} >/dev/null 2>&1
	RESULT=$?

	if [ ${RESULT} -ne 0 ];then
		fOutputLog "${SYSTEMD_CNF_FILE} を変更します"
		fOutputLog ""

		local LAST_LOGLEVEL_LINE=$(grep -e "${key}" -n ${SYSTEMD_CNF_FILE} | sort -n | tail -1 | sed -e 's/:.*//g')
		sed -i -e "s/^${key}/#${key}/" ${SYSTEMD_CNF_FILE}
		sed -i -e "${LAST_LOGLEVEL_LINE}a ${key}=${SEARCH_STR}" ${SYSTEMD_CNF_FILE}

		RC=1
	else
		fOutputLog "${SYSTEMD_CNF_FILE} : OK"
	fi

	fDel_coment_and_blankline ${SYSTEMD_CNF_FILE} >${TMP_FILE_FUNC} 2>&1
	fOutputFileToLog ${TMP_FILE_FUNC}

	fOutputLog ""
	return ${RC}
}

fInstallRootCertificate () {
	
	local CA_TRUST_LIST=${ROOT_CERT_FILENAME_LIST:=McAfeeSaaSWebOperations.cer}

	for CA_TRUST in $(IFS=' ';echo ${CA_TRUST_LIST})
	do
		ROOT_CERT_FILE=${MODULE_CP_SRC_DIR}/${CA_TRUST}
		if [ -f ${ROOT_CERT_FILE} ];then
			fOutputLog "Install CA [${ROOT_CERT_FILE}]"
			cp -f ${ROOT_CERT_FILE} /etc/pki/ca-trust/source/anchors/.
		else
			fOutputLog "インストールするルート証明書がありません[${ROOT_CERT_FILE}]"
		fi
	done

	CMD='update-ca-trust extract'
	fOutputLog "${CMD}"
	eval ${CMD}
}


fModOsBasicFiles () {
	# check name set
	CHK_NAME="OS Basic Configration" 
	CHK_DESC="OSの初期設定の確認と変更"
	fPrintHeader "${CHK_NAME}" "${CHK_DESC}"
	
	local REBOOT_FLG=0

	#
	# SELinux 設定
	#
	fCheckSelinux
	RESULT=$?
	if [ ${RESULT} -ne 0 ];then
		REBOOT_FLG=1
	fi

	#
	# TimeZone, LOCALE 設定
	#
	fCheckTimzoneAndLocale
	RESULT=$?
	if [ ${RESULT} -ne 0 ];then
		REBOOT_FLG=1
	fi

	#
	# DNS 設定
	#
	fCheckResolvConf
	RESULT=$?
	if [ ${RESULT} -ne 0 ];then
		REBOOT_FLG=1
	fi

	#
	# systemd ログレベル 変更
	#
	fCheckSystemdLoglevel
	RESULT=$?
	if [ ${RESULT} -ne 0 ];then
		REBOOT_FLG=1
	fi
	
	#
	# IPv6 無効化
	#
	fDisableIpv6 
	RESULT=$?
	if [ ${RESULT} -ne 0 ];then
		REBOOT_FLG=1
	fi

	#
	# 自動起動サービス設定
	#
	fSetAutostartServices
	RESULT=$?
	if [ ${RESULT} -ne 0 ];then
		REBOOT_FLG=1
	fi

	#
	# SSL CA (McAfee)
	#
	fInstallRootCertificate

	#
	# SystemDiskのxfs,inode64 -> inode32
	#
	if [ ${Change_inode32_enable} -eq 1 ];then
		
		fChengeSystemdiskToInode32
		RESULT=$?
		if [ ${RESULT} -eq 0 ];then
			REBOOT_FLG=1
		fi
	fi

	#
	# Kernel Parameters
	#
	if [ ${Change_KernelParameters_enable} -eq 1 ];then

		fChangeKernelParams
		RESULT=$?
		if [ ${RESULT} -eq 0 ];then
			REBOOT_FLG=1
		fi
	fi

	#
	# 再起動を促す
	#
	if [ ${REBOOT_FLG} -ne 0 ];then

		fExitForReboot 
	fi	

	fOutputLog ""
}

fModYumConf () {

	local RC=255
	local RESULT=255
	local YUM_CNF=/etc/yum.conf
	local EXCLUDE_REG_STR='kernel* redhat-release* unbound* initscripts.* *-firmware'
	local PROXY_STR="proxy=${OS_NW_PARAM_[http_proxy]}"

	fDel_coment_and_blankline ${YUM_CNF} >${TMP_FILE_FUNC}

	local CNF_EXCLUDE_STR=$(grep 'exclude=' ${TMP_FILE_FUNC} | cut -d '=' -f 2)
	if [ "${CNF_EXCLUDE_STR}" = "${EXCLUDE_REG_STR}" ];then
		RESULT1=0
	else
		RESULT1=1
	fi

	if [ -z "${PROXY_STR}" ];then
		RESULT2=0
		echo aaa
	else
		grep "${PROXY_STR}" ${TMP_FILE_FUNC} >/dev/null 2>&1
		RESULT2=$?
	fi

	RESULT=$(( ${RESULT1} + ${RESULT2} ))

	if [ ${RESULT} -ne 0 ];then
		fOutputLog "${YUM_CNF} を変更します"
		fFileBackup ${YUM_CNF}

		local LAST_EXCLUDE_LINE=$(grep -e 'exclude=' -n ${YUM_CNF} | sort -n | tail -1 | sed -e 's/:.*//g')
		if [ -n "${LAST_EXCLUDE_LINE}" ];then
			sed -i -e 's/^exclude/#exclude/' ${YUM_CNF}
			sed -i -e "${LAST_EXCLUDE_LINE}a exclude=${EXCLUDE_REG_STR}" ${YUM_CNF}
		else
			echo "exclude=${EXCLUDE_REG_STR}" >>${YUM_CNF}
		fi

		if [ ${RESULT2} -ne 0 ];then
			echo "${PROXY_STR}" >>${YUM_CNF}
		fi
		

		# 確認
		local CNF_EXCLUDE_STR=$(grep '^exclude=' ${YUM_CNF} | cut -d '=' -f 2)
		if [ "${CNF_EXCLUDE_STR}" = "${EXCLUDE_REG_STR}" ];then
			RESULT1=0
		else
			RESULT1=1
		fi

		grep "${PROXY_STR}" ${YUM_CNF} >/dev/null 2>&1
		RESULT2=$?

		RC=$(( ${RESULT1} + ${RESULT2} ))
			
	else
		fOutputLog "${YUM_CNF} : OK"
		RC=0
	fi

	return ${RC}
}

fYumUpdate () {
	# check name set
	CHK_NAME="Yum update"
	CHK_DESC="yum updateでシステムを最新化する"
	fPrintHeader "${CHK_NAME}" "${CHK_DESC}"

	local CMD=''
	local RC=255

	# yumが利用できるようにyum.confを書き替える
	fModYumConf
	RC=$?
	if [ ${RC} -ne 0 ];then
		return ${RC}
	fi

	# アップデートチェック
	yum clean all
	yum repolist 
	
	CMD='yum -y update'

	read -p "${CMD}を実行しますか？ (y/N): " ANSYN
	case "${ANSYN}" in
		[yY]*)
			cat /dev/null >${TMP_FILE_FUNC2}
			fOutputLog "${CMD}"
			eval "${CMD}" | tee -a ${TMP_FILE_FUNC2}
			RESULT=$?
			if [ ${RESULT} -ne 0 ];then
				fOutputLog ""
				fOutputLog "yum updateに失敗しました"
				fOutputLog ""
				RC=1
			else
				grep "No packages marked for update" ${TMP_FILE_FUNC2} >/dev/null 2>&1
				RESULT=$?
				if [ ${RESULT} -ne 0 ];then

					# 再起動を促す
					fExitForReboot 
				fi
			fi	
			;;
		*)
			fOutputLog "${CMD}を中止します。"
			RC=1
			return ${RC}
			;;
	esac

	# rpmnew
	find / -name *rpmnew >${TMP_FILE_FUNC} 2>&1
	if [ -s ${TMP_FILE_FUNC} ];then
		fOutputLog "exist *rpmnew"

		fOutputFileToLog ${TMP_FILE_FUNC}
		fOutputLog ""
	fi

	return ${RC}
}

fInstallGraphicalUserInterface () {

	local RC=255
	local CMD=''

	# GUI　環境導入
	fOutputLog ""
	fOutputLog "Graphical User Interfaceに必要なパッケージを導入します"
	fOutputLog ""

	# yumが利用できるようにyum.confを書き替える
	fModYumConf
	RC=$?
	if [ ${RC} -ne 0 ];then
		return ${RC}
	fi

	# GUI導入
	case "${ZONE_FLG}" in
		"0")
			fOutputLog "Zone : Teijin zone"
			#CMD='yum -y groupinstall "Server with GUI" --exclude="hyperv* *unbound* *libreswan* libvirt* gnome-boxes*"'
			#CMD='yum -y groupinstall "Server with GUI" --exclude="hyperv* *unbound* *libreswan* libvirt* gnome-boxes* kmod-* vdo-*"'
			GUI_CMD='yum -y groupinstall "Server with GUI" --exclude="hyperv* *unbound* *libreswan*"'
			VNC_CMD='yum -y --disablerepo=* --enablerepo=rhui-rhel-7-server-rhui-rpms install tigervnc-server'
			;;
		"1")
			fOutputLog "Zone : Oracle zone"
			GUI_CMD='yum -y groupinstall "Server with GUI"'
			VNC_CMD='yum -y tigervnc-server'
			;;
		*)
			fOutputLog "Zone : Undefined stop!!!"
			exit 255
			;;
	esac

	fOutputLog "${GUI_CMD}"
	eval "${GUI_CMD}"
	RC=$?

	# VNC Server 導入
	fOutputLog "${VNC_CMD}"
	eval "${VNC_CMD}" 
	RC=$(( ${RC} + $? ))

	# VNC Server設定
	local TGT_FILE=/etc/systemd/system/vncserver@:1.service
	
	if [ ! -f ${TGT_FILE} ];then
		cp -p /lib/systemd/system/vncserver@.service ${TGT_FILE}
	fi

	# VNCユーザの設定(vncuserが存在する場合のみ設定)
	local VNC_USER=vncuser
	id ${VNC_USER} >/dev/null 2>&1
	RESULT=$?
	if [ ${RESULT} -eq 0 ];then
		fOutputLog "${VNC_USER} で接続できるように ${TGT_FILE} を編集します"
		sed -i -e "/^#/!s/<USER>/${VNC_USER}/" ${TGT_FILE}
	
		systemctl daemon-reload
		fOutputLog "以下の手順でVNC用パスワードを設定後、VNCサーバを起動してください"
		fOutputLog "--------------------------------------------------------------------"
		fOutputLog "su - ${VNC_USER}"
		fOutputLog "vncpasswd"
		fOutputLog "Password: xxxxxxxx"
		fOutputLog "Verify: xxxxxxxx"
		fOutputLog " (Would you like to enter a view-only password (y/n)? 　← n 応答)"
		fOutputLog "exit"
		fOutputLog "systemctl start vncserver@:1.service"
		fOutputLog "systemctl enable vncserver@:1.service" 
		fOutputLog "--------------------------------------------------------------------"
	else
		fOutputLog "${VNC_USER} が存在しないので ${TGT_FILE} の編集はスキップします"
	fi

	fOutputLog ""

	return ${RC}
}

fChkDefaultRunLevel () {
	# check name set
	CHK_NAME="Check Defalut runlevel"
	CHK_DESC="デフォルト・ランレベルの確認と設定変更"
	fPrintHeader "${CHK_NAME}" "${CHK_DESC}"

	#
	# RunLevel 設定
	#
	key="runlevel"
	CMD="systemctl get-default"
	local DEFAULT_RUNLEVEL=$(eval "${CMD}")
	fOutputLog "runlevel設計値: ${OS_BASIC_PARAM_[$key]}"
	fOutputLog "runlevel現在値: ${DEFAULT_RUNLEVEL}"
	if [ "${DEFAULT_RUNLEVEL}" = "${OS_BASIC_PARAM_[$key]}" ];then
		:	
	else

		# runlevel　変更
		fOutputLog ""
		fOutputLog "Default Runlevel の設定を変更します"
		fOutputLog ""
		CMD="systemctl set-default ${OS_BASIC_PARAM_[$key]}"
		fOutputLog "${CMD}"
		eval ${CMD}

		# graphical.target ?
		if [ "${OS_BASIC_PARAM_[$key]}" = "graphical.target" ];then

			fInstallGraphicalUserInterface
			RESULT=$?
			if [ ${RESULT} -ne 0 ];then
				fOutputLog ""
				fOutputLog "Graphical User Interfaceの導入に失敗しました"
				fOutputLog ""
			else
				fOutputLog "Graphical User Interfaceを導入しました"
				# 再起動を促す
				fExitForReboot 
			fi	
		fi
	fi

	fOutputLog ""
}

fDisableIpv6 () {

	RC=255

	# /etc/hosts変更
	HOSTS_FILE=/etc/hosts
	sed -i -e 's/^::1/#::1/' ${HOSTS_FILE}

	CMD='netstat -nltu | grep tcp6 | grep -v "6[04][03][12]" | grep -v "5[49][430][1-4]"'
	fOutputLog "${CMD}"
	eval "${CMD}" >${TMP_FILE_FUNC} 2>&1
	RESULT=$?

	if [ ${RESULT} -eq 0 ];then
		
		fOutputFileToLog ${TMP_FILE_FUNC}
		fOutputLog ""
		fOutputLog "IPv6無効化設定を実施します"

		#
		# /etc/sysctl.d/でIPv6無効化
		#
		local SYSCTL_DEFALT_CNF=/etc/sysctl.conf
		local SYSCTL_D=/etc/sysctl.d/
		local IPV6_DISABLE_DEFALT_STR='net.ipv6.conf.default.disable_ipv6 = 1'
		local IPV6_DISABLE_ALL_STR='net.ipv6.conf.all.disable_ipv6 = 1'

		ls -1 ${SYSCTL_D} >${TMP_FILE_FUNC2}
		cat /dev/null >${TMP_FILE_FUNC} 2>&1
		while IFS= read line || [ -n "$line" ]
		do
			fDel_coment_and_blankline ${SYSCTL_D}/${line} | \
			grep -v "${IPV6_DISABLE_ALL_STR}" | grep -v "${IPV6_DISABLE_DEFALT_STR}" >>${TMP_FILE_FUNC}

		done <${TMP_FILE_FUNC2}

		fDel_coment_and_blankline ${SYSCTL_DEFALT_CNF} | \
		grep -v "${IPV6_DISABLE_ALL_STR}" | grep -v "${IPV6_DISABLE_DEFALT_STR}" >>${TMP_FILE_FUNC}

		if [ -s ${TMP_FILE_FUNC} ];then
			fOutputLog "[${SYSCTL_DEFALT_CNF}]と[${SYSCTL_D}]内のファイル内容を確認してください"
			fOutputFileToLog ${TMP_FILE_FUNC}
		else
			touch ${SYSCTL_D}/disable_ipv6.conf
			grep "${IPV6_DISABLE_ALL_STR}" ${SYSCTL_D}/disable_ipv6.conf >/dev/null 2>&1
			RESULT=$?
			if [ ${RESULT} -ne 0 ];then
				CMD="echo ${IPV6_DISABLE_ALL_STR} >>${SYSCTL_D}/disable_ipv6.conf"
				fOutputLog "${CMD}"
				eval "${CMD}" >${TMP_FILE_FUNC2} 2>&1
				fOutputFileToLog ${TMP_FILE_FUNC2}
			fi

			grep "${IPV6_DISABLE_DEFALT_STR}" ${SYSCTL_D}/disable_ipv6.conf >/dev/null 2>&1
			RESULT=$?
			if [ ${RESULT} -ne 0 ];then
				CMD="echo ${IPV6_DISABLE_DEFALT_STR} >>${SYSCTL_D}/disable_ipv6.conf"
				fOutputLog "${CMD}"
				eval "${CMD}" >${TMP_FILE_FUNC2} 2>&1
				fOutputFileToLog ${TMP_FILE_FUNC2}
			fi
		fi

		# 
		# M/W個別無効化
		#

		# SSHD
		local SSHD_CNF_FILE=/etc/ssh/sshd_config
		if [ -f ${SSHD_CNF_FILE} ];then
			fDisableIpv6Sshd
		fi
		# vsftpd
		local VSFTOD_CNF_FILE=/etc/vsftpd/vsftpd.conf
		if [ -f ${VSFTOD_CNF_FILE} ];then
			fModVsftpdConf
		fi
		
		RC=1	
	else
		fOutputLog "ipv6 disabled : OK"
		RC=0
	fi
	
	fOutputLog ""

	return ${RC}
}

fCheckOrCreateLV () {
	local ARG_KEY="$1"

	local LV_SIZE=$(echo ${EXT_DISK_PART_[${ARG_KEY}]} | cut -d "," -f 1)
	local LV_NAME=$(echo ${EXT_DISK_PART_[${ARG_KEY}]} | cut -d "," -f 2)
	local VG_NAME=$(echo ${EXT_DISK_PART_[${ARG_KEY}]} | cut -d "," -f 3)
	local LV_FS=$(echo ${EXT_DISK_PART_[${ARG_KEY}]}   | cut -d "," -f 4)

	# サイズ不正は何もせずエラーリターン
	local LV_SIZE_GB=$(echo ${LV_SIZE} | sed -e 's/GB//')
	if [ ${LV_SIZE_GB} -le 0 ]; then
		return 255
	fi

	CMD="lvdisplay -C | grep ${LV_NAME}"
	RESULT_STR=$(eval "${CMD}")

	if [ -z "${RESULT_STR}" ];then
		fOutputLog "${CMD} : no ${LV_NAME}"
		CMD="lvcreate -L ${LV_SIZE} -n ${LV_NAME} ${VG_NAME}"
		fOutputLog "${CMD}"
		eval "${CMD}"
		
		case "${LV_FS}" in
			"swap")
				CMD="mkswap /dev/${VG_NAME}/${LV_NAME}"
				fOutputLog "${CMD}"
				eval "${CMD}"
				CMD="swapon /dev/${VG_NAME}/${LV_NAME}"
				;;
			*)
				CMD="mkfs.${LV_FS} /dev/${VG_NAME}/${LV_NAME}"
				fOutputLog "${CMD}"
				;;
		esac

		eval "${CMD}"
		
	else
		fOutputLog "${CMD}"
		fOutputLog "${RESULT_STR}"
	fi
}

fCheckOrCreateMountPoint () {
	
	local ARG_KEY="$1"

	local FSTAB_FILE=/etc/fstab

	local LV_SIZE=$(echo ${EXT_DISK_PART_[${ARG_KEY}]} | cut -d "," -f 1)
	local LV_NAME=$(echo ${EXT_DISK_PART_[${ARG_KEY}]} | cut -d "," -f 2)
	local VG_NAME=$(echo ${EXT_DISK_PART_[${ARG_KEY}]} | cut -d "," -f 3)
	local LV_FS=$(echo ${EXT_DISK_PART_[${ARG_KEY}]}   | cut -d "," -f 4)
	local MNT_POT=$(echo ${EXT_DISK_PART_[${ARG_KEY}]} | cut -d "," -f 5)

	local UUID_STR=$(blkid -s UUID | grep ${VG_NAME}-${LV_NAME} | cut -d "=" -f 2 | sed -e 's/\"//g' -e 's/ //g')

	# サイズ不正は何もせずエラーリターン
	local LV_SIZE_GB=$(echo ${LV_SIZE} | sed -e 's/GB//')
	if [ ${LV_SIZE_GB} -le 0 ]; then
		return 255
	fi

	if [ "${ARG_KEY}" != "swap" ]; then

		mkdir -p ${MNT_POT}

		mountpoint ${MNT_POT} >/dev/null 2>&1
		RESULT=$?
		if [ ${RESULT} -ne 0 ];then
			mount /dev/${VG_NAME}/${LV_NAME} ${MNT_POT}
			
		else
			CMD="ls -l ${MNT_POT}"
			fOutputLog "${CMD}"
			eval "${CMD}"
		fi
	fi
	
	grep "${UUID_STR}" ${FSTAB_FILE} >/dev/null
	RESULT=$?
	
	echo "[${UUID_STR}]"
	if [ ${RESULT} -ne 0 ];then
		echo "UUID=${UUID_STR} ${MNT_POT}			${LV_FS}  defaults        0 0" >>${FSTAB_FILE}
	fi
}

fCreateExtDiskToLVM () {

	# check name set
	CHK_NAME="Create Ext Disk to LVM"
	CHK_DESC="拡張DiskをLVMで構成し、マウントする"
	fPrintHeader "${CHK_NAME}" "${CHK_DESC}"

	#---
	# PV
	#---
	CMD="pvdisplay -C | grep ${EXT_DISK_[devname]}"
	RESULT_STR=$(eval "${CMD}")

	if [ -z "${RESULT_STR}" ];then
		fOutputLog "${CMD} : no PV"
		CMD="parted -s -a optimal ${EXT_DISK_[devname]} -- mklabel ${EXT_DISK_[devlabel]} mkpart primary 1 -1 set 1 lvm on"
		fOutputLog "${CMD}"
		eval "${CMD}"
		
		CMD="pvcreate ${EXT_DISK_[devname]}1"
		fOutputLog "${CMD}"
		eval "${CMD}"
	else
		fOutputLog "${CMD}"
		fOutputLog "${RESULT_STR}"
	fi
	fOutputLog ""

	#---
	# VG
	#---
	CMD="vgdisplay -C | grep ${EXT_DISK_[vgname]}"
	RESULT_STR=$(eval "${CMD}")

	if [ -z "${RESULT_STR}" ];then
		fOutputLog "${CMD} : no VG"
		CMD="vgcreate ${EXT_DISK_[vgname]} ${EXT_DISK_[devname]}1"
		fOutputLog "${CMD}"
		eval "${CMD}"
	else
		fOutputLog "${CMD}"
		fOutputLog "${RESULT_STR}"
	fi
	fOutputLog ""

	#---
	# LV
	#---
	for volkey in "${!EXT_DISK_PART_[@]}"
	do
		fCheckOrCreateLV ${volkey}
		fCheckOrCreateMountPoint ${volkey}
	done

	local STR=$(df -Th)
	fOutputLog "${STR}"
	fOutputLog ""

	local STR=$(free -b)
	fOutputLog "${STR}"
	fOutputLog ""

	local STR=$(swapon -s)
	fOutputLog "${STR}"
	fOutputLog ""
}

fNewUsers () {
	local OS_USER_LIST=$1
	
	cat /dev/null >${TMP_FILE_MAIN}
	cat /dev/null >${TMP_FILE_FUNC}
	cat /dev/null >${TMP_FILE_FUNC2}

	cat ${OS_USER_LIST} | sed -e 's/^#.*//' -e '/^[\s\t]*$/d' >${TMP_FILE_FUNC2}

	for line in $(cat ${TMP_FILE_FUNC2})
	do
		USER_NAME=$(echo $line | cut -d ':' -f 1)
		USER_PASS=$(echo $line | cut -d ':' -f 2)
		USER_UID=$(echo $line | cut -d ':' -f 3)
		USER_GID=$(echo $line | cut -d ':' -f 4)
		USER_HOME=$(echo $line | cut -d ':' -f 6)
		USER_SHELL=$(echo $line | cut -d ':' -f 7)
		
		local DIR_NAME_HOME=$(dirname ${USER_HOME})
		if [ -d ${DIR_NAME_HOME} ]; then
			fOutputLog "AddUser [${USER_NAME}] PARAMS => PASSWD:${USER_PASS}, UID:${USER_UID}, GID:${USER_GID}, HOME:${USER_HOME}, SHELL:${USER_SHELL}"
			echo $line >>${TMP_FILE_FUNC}
		else
			fOutputLog "User [${USER_NAME}] USER_HOME[${USER_HOME}] のベースディレクトリが不在の為、ユーザ作成をスキップします"
		fi
	done
	fOutputLog ""

	# User 追加
	for line in $(cat ${TMP_FILE_FUNC})
	do
		USER_NAME=$(echo $line | cut -d ':' -f 1)
		USER_PASS=$(echo $line | cut -d ':' -f 2)
		USER_UID=$(echo $line | cut -d ':' -f 3)
		USER_GID=$(echo $line | cut -d ':' -f 4)
		USER_HOME=$(echo $line | cut -d ':' -f 6)
		USER_SHELL=$(echo $line | cut -d ':' -f 7)

		groupadd -g ${USER_GID} ${USER_NAME} >/dev/null 2>&1
		useradd -u ${USER_UID} -g ${USER_GID} -d ${USER_HOME} -m -s ${USER_SHELL} ${USER_NAME} >/dev/null 2>&1

		# wheelグループに追加
		if [ ${USER_NAME} = "vncuser" -o ${USER_NAME} = "ifc" ]; then
			usermod -aG wheel ${USER_NAME} >/dev/null 2>&1
		fi
		
		# umask変更,LANG変更,LC_ALL変更
		if [ ${USER_NAME} = "adm_priv" ];then
			:
		elif [ ${USER_NAME} = "adminfunc" ];then
			LANG_STR='export LANG=C'
			LC_ALL_STR='export LC_ALL=C'

			grep "${LANG_STR}" ${USER_HOME}/.bash_profile >/dev/null 2>&1
			RESULT1=$?
			grep "${LC_ALL_STR}" ${USER_HOME}/.bash_profile >/dev/null 2>&1
			RESULT2=$?
			RESULT=$(( ${RESULT1} + ${RESULT2} ))

			if [ ${RESULT} -ne 0 ];then

				fFileBackup ${USER_HOME}/.bash_profile
				cat /dev/null >${TMP_FILE_FUNC2} 2>&1

				if [ ${RESULT1} -ne 0 ]; then
					CMD="echo ${LANG_STR} >>${USER_HOME}/.bash_profile"
					fOutputLog "${CMD}"
					eval "${CMD}" >>${TMP_FILE_FUNC2} 2>&1
				fi
	
				if [ ${RESULT2} -ne 0 ]; then
					CMD="echo ${LC_ALL_STR} >>${USER_HOME}/.bash_profile"
					fOutputLog "${CMD}"
					eval "${CMD}" >>${TMP_FILE_FUNC2} 2>&1
				fi
				
				ls -l ${USER_HOME}/.bash_profile* >>${TMP_FILE_FUNC2} 2>&1
				# ログ出力
				fOutputFileToLog ${TMP_FILE_FUNC2}
				fOutputLog ""
			fi
		else
			UMASK_STR='umask 002'
			grep "${UMASK_STR}" ${USER_HOME}/.bash_profile >${TMP_FILE_FUNC2} 2>&1
			RESULT=$?
	
			if [ ${RESULT} -ne 0 ]; then
				fFileBackup ${USER_HOME}/.bash_profile
				CMD="echo ${UMASK_STR} >>${USER_HOME}/.bash_profile"
				fOutputLog "${CMD}"
				eval "${CMD}" >${TMP_FILE_FUNC2} 2>&1
				
				ls -l ${USER_HOME}/.bash_profile*
				cat ${TMP_FILE_FUNC2}
			fi
		fi

		# パスワード変更
		echo "${USER_NAME}:${USER_PASS}" | chpasswd

		# 確認
		CMD="id -a ${USER_NAME}"
		fOutputLog "CheckCmds: ${CMD}"
		eval "${CMD}" >${TMP_FILE_FUNC2} 2>&1
		sudo -iu ${USER_NAME} env | grep LANG >>${TMP_FILE_FUNC2} 2>&1
		sudo -iu ${USER_NAME} env | grep LC_ALL >>${TMP_FILE_FUNC2} 2>&1

		# ログ出力
		fOutputFileToLog ${TMP_FILE_FUNC2}
		fOutputLog ""
	done
}

fAddOsBaseUser () {
	# check name set
	CHK_NAME="Add Basic Os Users"
	CHK_DESC="基本OSユーザの作成と確認"
	fPrintHeader "${CHK_NAME}" "${CHK_DESC}"

	OS_USER_LIST_FILE=${APP_HOME}/${OS_USER_LIST_FILE_NAME}

	if [ -f ${OS_USER_LIST_FILE} ];then
		
		chmod 700 ${OS_USER_LIST_FILE}
		fNewUsers ${OS_USER_LIST_FILE}
	else
		fOutputLog "OSユーザのリストファイルがありません[${OS_USER_LIST_FILE}]"
	fi
}

fSedSudoers () {
	# check name set
	CHK_NAME="Modify sudoers"
	CHK_DESC="メンテナンスユーザと特権管理システムユーザのsudo設定確認と設定"
	fPrintHeader "${CHK_NAME}" "${CHK_DESC}"

	local SUDOERS_FILE=/etc/sudoers
	local MOD_FLG=0
	local EDITOR=''
	
	cat /dev/null >${TMP_FILE_MAIN}

	# Runas_Alias APL = <ApplicationUser> 追記
	local ADD_STR="Runas_Alias APL = ${RUNAS_ALIAS_APL}"
	grep "^${ADD_STR}" ${SUDOERS_FILE} >/dev/null 2>&1
	RESULT=$?
	if [ ${RESULT} -ne 0 ];then
		local LAST_LINE_NUM=$(grep -E "User_Alias\s+ADMINS\s+=" -n ${SUDOERS_FILE} | sort -n | tail -1 | sed -e 's/:.*//g')
		sed -e "${LAST_LINE_NUM}a ${ADD_STR}" ${SUDOERS_FILE} | EDITOR=tee visudo >/dev/null 2>>${TMP_FILE_MAIN}
		MOD_FLG=1
	fi

	# Cmnd_Alias SYSTEMCTL = /usr/bin/systemctl 追記
	local ADD_STR="Cmnd_Alias SYSTEMCTL = /usr/bin/systemctl"
	grep "^${ADD_STR}" ${SUDOERS_FILE} >/dev/null 2>&1
	RESULT=$?
	if [ ${RESULT} -ne 0 ];then

		local LAST_LINE_NUM=$(grep -E "Cmnd_Alias\s+SERVICES\s+=" -n ${SUDOERS_FILE} | sort -n | tail -1 | sed -e 's/:.*//g')
		sed -e "${LAST_LINE_NUM}a ${ADD_STR}" ${SUDOERS_FILE} | EDITOR=tee visudo >/dev/null 2>>${TMP_FILE_MAIN}
		MOD_FLG=1
	fi

	# ifc
	local SUDO_USER=ifc
	local PRE_ADD_STR="Defaults:${SUDO_USER} !requiretty"
	local ADD_STR="${SUDO_USER} ALL=(ALL) NOPASSWD: ALL"
	grep "^${ADD_STR}" ${SUDOERS_FILE} >/dev/null 2>&1
	RESULT=$?
	if [ ${RESULT} -ne 0 ];then

		echo "${PRE_ADD_STR}" | EDITOR='tee -a' visudo >/dev/null 2>>${TMP_FILE_MAIN}
		echo "${ADD_STR}" | EDITOR='tee -a' visudo >/dev/null 2>>${TMP_FILE_MAIN}
		MOD_FLG=1
	fi

	# apladm
	local SUDO_USER=apladm
	local PRE_ADD_STR="Defaults:${SUDO_USER} !requiretty"
	local ADD_STR="${SUDO_USER} ALL=(APL) NOPASSWD: ALL, !SYSTEMCTL"
	grep "^${ADD_STR}" ${SUDOERS_FILE} >/dev/null 2>&1
	RESULT=$?
	if [ ${RESULT} -ne 0 ];then

		echo "${PRE_ADD_STR}" | EDITOR='tee -a' visudo >/dev/null 2>>${TMP_FILE_MAIN}
		echo "${ADD_STR}" | EDITOR='tee -a' visudo >/dev/null 2>>${TMP_FILE_MAIN}
		MOD_FLG=1
	fi

	# User privilege specification
	local SUDO_USER=adminfunc
	local PRE_ADD_STR="Defaults:${SUDO_USER} !requiretty"
	local ADD_STR="${SUDO_USER} ALL=(ALL) NOPASSWD: /bin/passwd, /bin/grep, /bin/awk"
	grep "^${ADD_STR}" ${SUDOERS_FILE} >/dev/null 2>&1
	RESULT=$?
	if [ ${RESULT} -ne 0 ];then

		echo "${PRE_ADD_STR}" | EDITOR='tee -a' visudo >/dev/null 2>>${TMP_FILE_MAIN}
		echo "${ADD_STR}" | EDITOR='tee -a' visudo >/dev/null 2>>${TMP_FILE_MAIN}
		MOD_FLG=1
	fi

	# ログ表示
	if [ ${MOD_FLG} -eq 1 ];then

		fOutputLog "${SUDOERS_FILE}を変更しました"
		fOutputLog ""
		fDel_coment_and_blankline ${SUDOERS_FILE} >${TMP_FILE_FUNC2}
		echo "--" >>${TMP_FILE_FUNC2}
		cat ${TMP_FILE_MAIN}| sort -u >>${TMP_FILE_FUNC2}
	else
		fDel_coment_and_blankline ${SUDOERS_FILE} >${TMP_FILE_FUNC2}
	fi

	fOutputFileToLog ${TMP_FILE_FUNC2}

	fOutputLog ""
}

fMakeInfraDirectory () {
	# check name set
	CHK_NAME="Infra Directory"
	CHK_DESC="インフラディレクトリの作成と確認"
	fPrintHeader "${CHK_NAME}" "${CHK_DESC}"

	local DIR_MOD=''
	local DIR_OWNER=''
	cat /dev/null >${TMP_FILE_FUNC}

	# Make Directory
	for key in "${!NZ_DIRECTORY_[@]}"
	do
		#fOutputLog "Directory: [ ${key} ]"
		DIR_MOD=$(echo ${NZ_DIRECTORY_[$key]} | awk -F',' '{print $1}')
		DIR_OWNER=$(echo ${NZ_DIRECTORY_[$key]} | awk -F',' '{print $2}')

		CMD="mkdir -p ${key}"
		eval "${CMD}"
		CMD="chmod ${DIR_MOD} ${key}"
		eval "${CMD}"
		CMD="chown ${DIR_OWNER} ${key}"
		eval "${CMD}"
	done

	#
	# Check Directory
	#
	CMD="ls -l /home/ifc" 
	fOutputLog "CheckCmds: ${CMD}"
	eval "${CMD}" >${TMP_FILE_FUNC}

	# ログ出力
	fOutputFileToLog ${TMP_FILE_FUNC}
	fOutputLog ""

	local PERF_DIR=/home/ifc/perf

	CMD="ls -l ${PERF_DIR}" 
	fOutputLog "CheckCmds: ${CMD}"
	eval "${CMD}" >${TMP_FILE_FUNC}

	# ログ出力
	fOutputFileToLog ${TMP_FILE_FUNC}
	fOutputLog ""
	
	for cdir in $(ls -1 ${PERF_DIR})
	do
		CMD="ls -l ${PERF_DIR}/$cdir"
		fOutputLog "CheckCmds: ${CMD}"
		eval "${CMD}" >${TMP_FILE_FUNC} 2>&1

		# ログ出力
		fOutputFileToLog ${TMP_FILE_FUNC}
		fOutputLog ""
	done

	local LOG_DIR=/home/ifc/log

	CMD="ls -l ${LOG_DIR}" 
	fOutputLog "CheckCmds: ${CMD}"
	eval "${CMD}" >${TMP_FILE_FUNC}

	# ログ出力
	fOutputFileToLog ${TMP_FILE_FUNC}
	fOutputLog ""
}

fCheckSepInstall () {
	local RC=255
	cat /dev/null >${TMP_FILE_FUNC}

	CHK_CMD="/opt/Symantec/symantec_antivirus/sav info --product"
	fOutputLog "${CHK_CMD}"
	eval "${CHK_CMD}" >>${TMP_FILE_FUNC} 2>&1
	RESULT=$?
	if [ ${RESULT} -ne 0 ]; then
		fOutputLog "SEP not installed or in troubled"
		fOutputFileToLog "${TMP_FILE_FUNC}"
		RC=${RESULT}
	else
		fOutputLog "SEP installed [ $(cat ${TMP_FILE_FUNC}) ]"
		RC=0
	fi

	fOutputLog ""

	return ${RC}
}

fCheckSepStatus () {
	local RC=255

	CMD="/opt/Symantec/symantec_antivirus/sav info --defs"
	fOutputLog "${CMD}"
	eval "${CMD}" >${TMP_FILE_FUNC} 2>&1
	RESULT1=$?
	RC=${RESULT}

	# 結果出力
	fOutputFileToLog ${TMP_FILE_FUNC}
	fOutputLog ""

	CMD="/opt/Symantec/symantec_antivirus/sav info --autoprotect"
	fOutputLog "${CMD}"
	eval "${CMD}" >${TMP_FILE_FUNC} 2>&1
	RESULT2=$?

	# 結果出力
	fOutputFileToLog ${TMP_FILE_FUNC}
	fOutputLog ""

	RC=$(( ${RESULT1} + ${RESULT2} ))

	return ${RC}
}

fInstallSep () {

	local TMP_SEP_DIR=${APP_HOME}/SEP
	cat /dev/null >${TMP_FILE_FUNC}

	# SEPアーカイブ展開
	mkdir -p ${TMP_SEP_DIR}
	cp ${MODULE_CP_SRC_DIR}/${SEP_ARCHIVE_NAME} ${TMP_SEP_DIR}/.
	unzip -o ${TMP_SEP_DIR}/${SEP_ARCHIVE_NAME} -d ${TMP_SEP_DIR} >>${TMP_FILE_FUNC} 2>&1
        fOutputFileToLog ${TMP_FILE_FUNC}
        fOutputLog ""

	cat /dev/null >${TMP_FILE_FUNC}

	# SEP インストール
	CMD="chmod u+x ${TMP_SEP_DIR}/install.sh"
	fOutputLog "${CMD}"
	eval "${CMD}" >>${TMP_FILE_FUNC} 2>&1

	CMD="${TMP_SEP_DIR}/install.sh -i"
	fOutputLog "${CMD}"
	eval "${CMD}" >>${TMP_FILE_FUNC} 2>&1
	RESULT=$?
	RC=${RESULT}

	fOutputFileToLog ${TMP_FILE_FUNC}
	fOutputLog ""
}

fInstallRpmPkg () {
	local RC=0
	local PRE_PKG="$1"

	cat /dev/null >${TMP_FILE_FUNC}

	fOutputLog "${PRE_PKG}のインストールを試行します"

	# /etc/yum.confを一時的に変更してKernel-develをインストールできるようにする
	local YUM_CNF='/etc/yum.conf'
	local BK_FILE=$(dirname ${YUM_CNF})/$(basename ${YUM_CNF}).tmp.$$

	CMD="cp -p ${YUM_CNF} ${BK_FILE}"
        fOutputLog "FileBackup: ${CMD}"
        eval "${CMD}" >${TMP_FILE_FUNC} 2>&1
	fOutputFileToLog ${TMP_FILE_FUNC}

	sed -i -e 's/exclude=kernel/#exclude=kernel/' ${YUM_CNF}

	CMD="yum -y install ${PRE_PKG}"
	fOutputLog "${CMD}"
	eval "${CMD}" >${TMP_FILE_FUNC} 2>&1

	# ログ出力
        fOutputFileToLog ${TMP_FILE_FUNC}

	# /etc/yum.confを戻す
	CMD="cp -p ${BK_FILE} ${YUM_CNF}"
        fOutputLog "FileRestore: ${CMD}"
        eval "${CMD}" >${TMP_FILE_FUNC} 2>&1
        fOutputFileToLog ${TMP_FILE_FUNC}
	rm -f ${BK_FILE}

	cat /dev/null >${TMP_FILE_FUNC}
	return ${RC}
}

fCheckSepPrecondition () {
	local RC=255
	local PRE_PKG=''
	local PRE_PKG_FILENAME=''

	cat /dev/null >${TMP_FILE_FUNC}

	# inode64 check
		CHK_CMD="mount | grep xfs | grep inode64"
		fOutputLog "FileSystem Check: [ ${CHK_CMD} ]"
		eval ${CHK_CMD} >/dev/null 2>&1
		RESULT=$?
	if [ ${RESULT} -eq 0 ];then
		fOutputLog "FileSystemがxfsかつinode64が有効になっています"
		RC=255
	else
		fOutputLog "FileSystem: OK"
		RC=0

		# kernel-devel check
		PRE_PKG="kernel-devel-$(uname -r)"
		PRE_PKG_FILENAME=${PRE_PKG}.rpm

		CHK_CMD="rpm -qa | grep ${PRE_PKG}"
		fOutputLog "PKG check ${PRE_PKG}: [ ${CHK_CMD} ]"
		eval "${CHK_CMD}" >/dev/null 2>&1
		RESULT=$?
		if [ ${RESULT} -ne 0 ];then
			fOutputLog "PKG: ${PRE_PKG} [NG]"
			# Kernel-delel install
			fInstallRpmPkg "${PRE_PKG}" "${PRE_PKG_FILENAME}"
			RC=$?
		else
			fOutputLog "PKG: ${PRE_PKG} [OK]"
			RC=0
		fi

		# yumが利用できるようにyum.confを書き替える
		fModYumConf
		RC=$?
		if [ ${RC} -ne 0 ];then
			return ${RC}
		fi

		# 64bitモジュールの導入
		CMD="yum -y install libX11 libgcc gcc glibc ncompress sharutils"
		fOutputLog "${CMD}"
		eval "${CMD}" >${TMP_FILE_FUNC} 2>&1
		RESULT=$?
		# ログ出力
		fOutputFileToLog ${TMP_FILE_FUNC}

		# 32bitモジュールの導入
		case "${ZONE_FLG}" in
			"0")
				fOutputLog "Zone : Teijin zone"
				CMD="yum -y --disablerepo=* --enablerepo=rhui-rhel-7-server-rhui-rpms install glibc.i686 libgcc.i686 libX11.i686"
				;;
			"1")
				fOutputLog "Zone : Oracle zone"
				CMD="yum -y install glibc.i686 libgcc.i686 libX11.i686"
				;;
			*)
				fOutputLog "Zone : Undefined stop!!!"
				exit 255
				;;
		esac

		fOutputLog "${CMD}"
		eval "${CMD}" >${TMP_FILE_FUNC} 2>&1
		RESULT=$?
		# ログ出力
		fOutputFileToLog ${TMP_FILE_FUNC}
	fi

	cat /dev/null >${TMP_FILE_FUNC}
	return ${RC}
}

fDeployInfraTools () {
	# check name set
	CHK_NAME="Infra Tools Deploy and Set"
	CHK_DESC="インフラスクリプトの配置と設定"
	fPrintHeader "${CHK_NAME}" "${CHK_DESC}"

	# Deploy files
	for key in "${!NZ_FILE_[@]}"
	do
		fOutputLog "File: [ ${key} ]"
		FILE_PATH=$(echo ${NZ_FILE_[$key]} | awk -F',' '{print $1}')
		FILE_OWNER=$(echo ${NZ_FILE_[$key]} | awk -F',' '{print $2}')
		FILE_MOD=$(echo ${NZ_FILE_[$key]} | awk -F',' '{print $3}')

		INFRA_FILE="${FILE_PATH}/${key}"

		if [ -f ${INFRA_TOOLS_CP_SRC_DIR}/${key} ];then
		
			if [ -f ${INFRA_FILE} ];then
				ls -l ${INFRA_FILE} >${TMP_FILE_FUNC} 2>&1
				# ログ出力
				fOutputFileToLog ${TMP_FILE_FUNC}
			else

				CMD="cp ${INFRA_TOOLS_CP_SRC_DIR}/${key} ${INFRA_FILE}"
				fOutputLog "${CMD}"
				eval "${CMD}"
	
				CMD="chmod ${FILE_MOD} ${INFRA_FILE}"
				fOutputLog "${CMD}"
				eval "${CMD}"
	
				CMD="chown ${FILE_OWNER} ${INFRA_FILE}"
				fOutputLog "${CMD}"
				eval "${CMD}"
			fi
		else
			fOutputLog "${INFRA_TOOLS_CP_SRC_DIR}/${key}が存在しません"
			fOutputLog "${INFRA_FILE}は配置できませんでした"
		fi
	done

	fOutputLog ""
}

fSetCronAndRcLocal () {
	# check name set
	CHK_NAME="Cron and rc.local setting"
	CHK_DESC="Cron と rc.localの設定と確認"
	fPrintHeader "${CHK_NAME}" "${CHK_DESC}"

	# CRON Setting
	local CRON_BK_FILE=${APP_HOME}/crontab_bk.txt
	crontab -l >${CRON_BK_FILE} 2>&1
	RESULT=$?
	if [ ${RESULT} -eq 0 ];then
		fFileBackup ${CRON_BK_FILE}
	fi

	echo "${CRON_ENTRY_LIST}" >${TMP_FILE_MAIN}

	crontab -u root ${TMP_FILE_MAIN}

	local CRONTAB_STR=$(crontab -l)
	fOutputLog "--- crontab root ---"
	fOutputLog "${CRONTAB_STR}"

	# rc.local Setting
	local RCLOCAL_FILE=/etc/rc.local

	echo "${RCLOCAL_ENTRY_LIST}" >${TMP_FILE_MAIN}

	for entry in "${RCLOCAL_ENTRY_LIST}"
	do
		grep "${entry}" ${RCLOCAL_FILE} >/dev/null 2>&1
		if [ $? -ne 0 ];then
			echo "${entry}" >>${RCLOCAL_FILE}
		fi
	done
	fOutputLog "--- ${RCLOCAL_FILE} ---"
	fOutputFileToLog ${RCLOCAL_FILE}

	chmod u+x /etc/rc.d/rc.local

	fOutputLog ""
}

fUpdate_motd () {
	local P_SYSNAME="$1"
	local P_BUNNERFILE="$2"

	fFileBackup ${P_BUNNERFILE}

	local IP_ADDR=$(hostname -I)
	local RELEASE=$(cat /etc/redhat-release | sed -e 's/Red Hat Enterprise Linux/RHEL/' -e 's/release //')

	echo "" >${P_BUNNERFILE}
	echo "System Name : ${P_SYSNAME}" >>${P_BUNNERFILE}
	echo "IP Address  : ${IP_ADDR}" >>${P_BUNNERFILE}
	echo "Host Name   : ${MY_HOSTNAME}" >>${P_BUNNERFILE}
	echo "OS          : ${RELEASE}" >>${P_BUNNERFILE}
	echo "Charset     : ${LANG}" >>${P_BUNNERFILE}
	echo "" >>${P_BUNNERFILE}

	fOutputLog "--- ${P_BUNNERFILE} ---"
	fOutputFileToLog ${P_BUNNERFILE}
}

fUpdate_bashrc () {
	local P_SYSNAME="$1"
	local P_BASHRC="$2"

	fFileBackup ${P_BASHRC}

	# プロンプトの色を決める
	# m     ->  指定なし
	# 0;30m ->  Black
	# 0;31m ->  Red
	# 0;32m ->  Green
	# 0;33m ->  Yellow
	# 0;34m ->  Blue
	# 0;35m ->  Purple
	# 0;36m ->  Cyan
	# 0;37m ->  White
	local COLOR_NUM=31
	echo ${P_SYSNAME} | grep -e "検証" -e "開発" -e "テスト" >/dev/null 2>&1
	RESULT=$?
	if [ ${RESULT} -eq 0 ];then
		COLOR_NUM=34
	fi

	echo ${P_SYSNAME} | grep -e "準本番" >/dev/null 2>&1
	RESULT=$?
	if [ ${RESULT} -eq 0 ];then
		COLOR_NUM=32
	fi

	sed -i -e "s|PS1=\"|PS1=\"\\\e[${COLOR_NUM};1m[${P_SYSNAME}]\\\e[m\\\n|" ${P_BASHRC}

	local OLDFILE=$(ls -1t ${P_BASHRC}* | head -n 2 | tail -n 1)
	fOutputLog "--- diff ${P_BASHRC} ${OLDFILE} ---"
	diff ${P_BASHRC} ${OLDFILE} >${TMP_FILE_FUNC}
	fOutputFileToLog ${TMP_FILE_FUNC}
}

fUpdateBunner () {
	# check name set
	CHK_NAME="Update motd (Login Bunner)"
	CHK_DESC="誤操作防止設定 (ログインバナー設定)"
	fPrintHeader "${CHK_NAME}" "${CHK_DESC}"

	local MSG_FILE=/etc/motd
	local BASHRC_FILE=/etc/bashrc

	grep "${SYSTEM_NAME}" ${MSG_FILE} >/dev/null 2>&1
	RESULT=$?

	if [ ${RESULT} -eq 0 ];then
		# ログ出力
		fOutputFileToLog ${MSG_FILE}
		fOutputLog ""
	else
		# update bunner file
		fOutputLog "update ${MSG_FILE}"
		fUpdate_motd "${SYSTEM_NAME}" ${MSG_FILE}
		fOutputLog ""

		# update bunner prompt
		fOutputLog "update ${BASHRC_FILE}"
		fUpdate_bashrc "${SYSTEM_NAME}" ${BASHRC_FILE}
		fOutputLog ""
	fi
}

fYumInstall () {
	TGT_PKG_NAME="$1"
	RC=255

	fModYumConf
	RC=$?
	if [ ${RC} -ne 0 ];then
		return ${RC}
	fi

	CMD="yum -y install ${TGT_PKG_NAME}"
	fOutputLog "${CMD}"
	eval "${CMD}" | grep "${TGT_PKG_NAME}" >${TMP_FILE_FUNC} 2>&1
	RC=$?

	if [ ${RC} -ne 0 ];then

		fOutputLog ""
		fOutputLog "Install failed : ${TGT_PKG_NAME}"
		fOutputLog ""
		fOutputFileToLog ${TMP_FILE_FUNC}
	else
		fOutputFileToLog ${TMP_FILE_FUNC}
	fi

	return ${RC}
}

fModVsftpdConf () {

	local VSFTOD_CNF_FILE=/etc/vsftpd/vsftpd.conf
	local SERVICE_RESTART_FLG=0

	fDel_coment_and_blankline ${VSFTOD_CNF_FILE} >${TMP_FILE_FUNC2} 2>&1
	fFileBackup ${VSFTOD_CNF_FILE}

	for key in "${!VSFTPD_CONF_[@]}"
	do
		KEY_LINE_STR=$(grep "${key}=" ${TMP_FILE_FUNC2})
		KEY_LINE_VALUE=$(grep "${key}=" ${TMP_FILE_FUNC2} | cut -d "=" -f 2)

		if [ "${KEY_LINE_VALUE}" != "${VSFTPD_CONF_[$key]}" ];then

			fOutputLog "${key} のエントリを変更します (${KEY_LINE_STR}) -> (${key}=${VSFTPD_CONF_[$key]})"
			local LAST_LINE=$(grep -e "${key}=" -n ${VSFTOD_CNF_FILE} | sort -n | tail -1 | sed -e 's/:.*//g')
			if [ -n "${LAST_LINE}" ];then
				sed -i -e "s/${key}\=/#${key}\=/" ${VSFTOD_CNF_FILE}
				sed -i -e "${LAST_LINE}a ${key}=${VSFTPD_CONF_[$key]}" ${VSFTOD_CNF_FILE}

			else
				echo "${key}=${VSFTPD_CONF_[$key]}" >>${VSFTOD_CNF_FILE}
			fi
			SERVICE_RESTART_FLG=1
		else

			fOutputLog "${KEY_LINE_STR}"
		fi
	done

	if [ ${SERVICE_RESTART_FLG} -ne 0 ];then
		CMD="systemctl restart vsftpd"
		fOutputLog "${CMD}"
		eval "${CMD}"
	else
		fOutputLog "${VSFTOD_CNF_FILE} : ALL OK"
	fi
}

fSetupVsftpd () {
	fYumInstall vsftpd

	# vsftpd.conf 変更
	fModVsftpdConf
	
	# user_list 変更
	local VSFTPD_USER_LISTFILE=/etc/vsftpd/user_list
	fOutputLog ""
	fOutputLog "check: ${VSFTPD_USER_LISTFILE}"

	sed -i -e '/^#/!s/^/#/' ${VSFTPD_USER_LISTFILE}
	
	for allow_user in ${VSFTPD_USER_LIST}
	do
		sed -i -e "s/^#\(${allow_user}\)$/\1/" ${VSFTPD_USER_LISTFILE}
		grep "^${allow_user}" ${VSFTPD_USER_LISTFILE} >/dev/null 2>&1
		RESULT=$?
		if [ ${RESULT} -ne 0 ];then
			fOutputLog "Add: ${allow_user} -> ${VSFTPD_USER_LISTFILE}"
			echo ${allow_user} >>${VSFTPD_USER_LISTFILE}
		else
			fOutputLog "${allow_user} exist in ${VSFTPD_USER_LISTFILE}"
		fi
	done
}

fSetupTelnetServer () {
	fYumInstall telnet-server

	#
	# ipv6 無効化
	#
	TGT_FILE=/etc/systemd/system/telnet.socket

	if [ ! -f ${TGT_FILE} ];then
		cp -p /usr/lib/systemd/system/telnet.socket ${TGT_FILE}
	fi
	sed -i -e 's/^ListenStream=23/ListenStream=0.0.0.0:23/' ${TGT_FILE}

	#
	# Tcp-Wrappersを使用するようにする
	#
	TGT_FILE=/etc/systemd/system/telnet@.service

	if [ ! -f ${TGT_FILE} ];then
		cp -p /usr/lib/systemd/system/telnet@.service ${TGT_FILE}
	fi
	sed -i -e 's/^ExecStart=-\/usr\/sbin\/in\.telnetd/ExecStart=\@\/usr\/sbin\/tcpd \/usr\/sbin\/in\.telnetd/' ${TGT_FILE}

	systemctl daemon-reload
	systemctl restart telnet.socket
}

fSetupTcpwrappers () {
	fYumInstall tcp_wrappers
}

fSetupWget () {
	fYumInstall wget
	fOutputLog ""

	# wgetrc 変更
	fModWgetProxy
}

fDisableIpv6Sshd () {

	local SSHD_CNF_FILE=/etc/ssh/sshd_config
	local ADDR_FAMILY_STR='AddressFamily inet'

	if [ ! -f ${SSHD_CNF_FILE} ];then
		fOutputLog "${SSHD_CNF_FILE}が存在しません"
		return 255
	fi

	fDel_coment_and_blankline ${SSHD_CNF_FILE} | grep "${ADDR_FAMILY_STR}" >/dev/null 2>&1
	RESULT=$?
	if [ ${RESULT} -ne 0 ];then
		fOutputLog "IPv6無効化します。変更ファイル[${SSHD_CNF_FILE}]"
		local LAST_ADDR_FAMILY_LINE=$(grep -e "AddressFamily" -n ${SSHD_CNF_FILE} | sort -n | tail -1 | sed -e 's/:.*//g')
		sed -i -e "s/^AddressFamily/#AddressFamily/" ${SSHD_CNF_FILE}
		sed -i -e "${LAST_ADDR_FAMILY_LINE}a ${ADDR_FAMILY_STR}" ${SSHD_CNF_FILE}

		CMD="systemctl restart sshd"
		fOutputLog "${CMD}"
		eval "${CMD}" >/dev/null 2>&1
	else
		fOutputLog "sshd IPv6無効化設定 : OK"
	fi
	
	return 0
}

fSetupSshd () {

	fYumInstall openssh-server
	fOutputLog ""

	# IPv6無効化
	fDisableIpv6Sshd
	fOutputLog ""

	# サービス有効化
	fChangeServiceWithEnabled sshd enable
	fOutputLog ""
}

fSetupZabbixAgt () {

	local TMP_ZABBIX_DIR=${APP_HOME}/Zabbix
	local ZABBIX_CNF=/etc/zabbix/zabbix_agentd.conf

	cat /dev/null >${TMP_FILE_FUNC}

	# インストール
	mkdir -p ${TMP_ZABBIX_DIR}
	cp ${MODULE_CP_SRC_DIR}/zabbix-* ${TMP_ZABBIX_DIR}/.

	rpm -ivh ${TMP_ZABBIX_DIR}/zabbix-agent-2.2.*.rpm ${TMP_ZABBIX_DIR}/zabbix-2.2.*.rpm >/dev/null 2>&1
	rpm -ivh ${TMP_ZABBIX_DIR}/zabbix-sender-2.2.*.rpm >/dev/null 2>&1

	CMD="rpm -qa | grep 'zabbix-'"
	fOutputLog "${CMD}"
	eval "${CMD}" >${TMP_FILE_FUNC} 2>&1
	if [ -s ${TMP_FILE_FUNC} ];then
		fOutputFileToLog ${TMP_FILE_FUNC}
	else
		fOutputLog "Zabbix Agentのインストールに不具合があります"
	fi
	fOutputLog ""
	
	# 設定ファイル編集
	fDel_coment_and_blankline ${ZABBIX_CNF} >${TMP_FILE_FUNC2} 2>&1

	fFileBackup ${ZABBIX_CNF}

	for key in "${!ZABBIX_CONF_[@]}"
	do
		KEY_STR=$(grep "${key}=" ${TMP_FILE_FUNC2} | grep "${ZABBIX_CONF_[$key]}")
		RESULT=$?
		if [ ${RESULT} -ne 0 ];then
			local LAST_KEY_LINE=$(grep -e "${key}=" -n ${ZABBIX_CNF} | sort -n | tail -1 | sed -e 's/:.*//g')
			sed -i -e "s/^${key}\=/#${key}\=/" ${ZABBIX_CNF}
			sed -i -e "${LAST_KEY_LINE}a ${key}=${ZABBIX_CONF_[$key]}" ${ZABBIX_CNF}
			local NG_KEY_STR=$(grep "${key}=" ${TMP_FILE_FUNC2})
			fOutputLog "${key}=${ZABBIX_CONF_[$key]} : NG (${NG_KEY_STR})"
			KEY_STR=$(grep "${key}=" ${ZABBIX_CNF} | grep "${ZABBIX_CONF_[$key]}")
			fOutputLog "エントリ変更　⇒　(${KEY_STR})"

		else
			fOutputLog "${key}=${ZABBIX_CONF_[$key]} : OK"
		fi
	done
	
	# サービス有効化
	fChangeServiceWithEnabled zabbix-agent enable

	# ログパーミッション変更
	systemctl stop zabbix-agent.service
	sleep 2
	chown zabbix:zabbix /var/log/zabbix/zabbix_agentd.log
	systemctl start zabbix-agent.service
}

fInstallBasicModule () {
	# check name set
	CHK_NAME="Install Basic Modules"
	CHK_DESC="必要な基本パッケージの導入"
	fPrintHeader "${CHK_NAME}" "${CHK_DESC}"

	for PKG in $(IFS=' ';echo ${INSTALLED_PACKAGE_LIST} )
	do
		fOutputLog "Installed check [${PKG}]"

		case "${PKG}" in
			"telnet-server")
				fSetupTelnetServer ;;
			"vsftpd")
				fSetupVsftpd ;;
			"tcp_wrappers")
				fSetupTcpwrappers ;;
			"zabbix-agent")
				fSetupZabbixAgt ;;
			"wget")
				fSetupWget ;;
			"openssh-server")
				fSetupSshd ;;
			*)
				fYumInstall "${PKG}"
				;;
		esac	
		fOutputLog ""
	done
}


fCheckAndSetTcpWrappers () {

	local HOSTS_ALLOW_FILE=/etc/hosts.allow
	local HOSTS_DENY_FILE=/etc/hosts.deny
	
	#
	# hosts.allow追記
	#
	fOutputLog "Check and Modify ${HOSTS_ALLOW_FILE}"
	fFileBackup ${HOSTS_ALLOW_FILE}
	# FTP
	for base in ${HOST_ALLOW_FTP_BASE}
	do
		ALLOW_STR="vsftpd: ${ACC_NW_RANGE_[$base]}"
		fAppendStringToFile ${HOSTS_ALLOW_FILE} "${ALLOW_STR}"
	done
	# TELNET
	for base in ${HOST_ALLOW_TELNET_BASE}
	do
		ALLOW_STR="in.telnetd: ${ACC_NW_RANGE_[$base]}"
		fAppendStringToFile ${HOSTS_ALLOW_FILE} "${ALLOW_STR}"
	done
	# SSH
	for base in ${HOST_ALLOW_SSH_BASE}
	do
		ALLOW_STR="sshd: ${ACC_NW_RANGE_[$base]}"
		fAppendStringToFile ${HOSTS_ALLOW_FILE} "${ALLOW_STR}"
	done
	fOutputFileToLog ${HOSTS_ALLOW_FILE}
	fOutputLog ""

	#
	# hosts.deny追記
	#
	fOutputLog "Check and Modify ${HOSTS_DENY_FILE}"
	fFileBackup ${HOSTS_DENY_FILE}
	
	# ALL
	for base in ${HOST_DENY_ALL}
	do
		DENY_STR="ALL: ${base}"
		fAppendStringToFile ${HOSTS_DENY_FILE} "${DENY_STR}"
	done
	fOutputFileToLog ${HOSTS_DENY_FILE}
	fOutputLog ""
}

fCheckAndSetPam () {

	local PAM_SU_FILE=/etc/pam.d/su
	local PAM_REMOTE_FILE=/etc/pam.d/remote
	local PAM_ACCESS_FILE=/etc/security/access.conf

	#
	# /etc/pam.d/su
	#
	fOutputLog "Check and Modify ${PAM_SU_FILE}"
	PAM_STR="auth\s+required\s+pam_wheel.so\s+root_only"
	grep -E "^${PAM_STR}" ${PAM_SU_FILE} >/dev/null 2>&1
	RESULT=$?
	if [ ${RESULT} -ne 0 ];then
		fFileBackup ${PAM_SU_FILE}
		local LAST_LINE_NUM=$(grep -E "auth\s+required\s+pam_wheel.so" -n ${PAM_SU_FILE} | sort -n | tail -1 | sed -e 's/:.*//g')
		sed -i -e "${LAST_LINE_NUM}a auth           required        pam_wheel.so root_only" ${PAM_SU_FILE}
	fi
	fOutputFileToLog ${PAM_SU_FILE}	
	fOutputLog ""

	#
	# /etc/pam.d/remote
	#
	fOutputLog "Check and Modify ${PAM_REMOTE_FILE}"
	PAM_STR="account\s+required\s+pam_access.so"
	grep -E "^${PAM_STR}" ${PAM_REMOTE_FILE} >/dev/null 2>&1
	RESULT=$?
	if [ ${RESULT} -ne 0 ];then
		fFileBackup ${PAM_REMOTE_FILE}
		sed -i -e "1a account    required     pam_access.so" ${PAM_REMOTE_FILE}
	fi
	fOutputFileToLog ${PAM_REMOTE_FILE}	
	fOutputLog ""

	#
	# /etc/security/access.conf
	#
	fOutputLog "Check and Modify ${PAM_ACCESS_FILE}"
	fFileBackup ${PAM_ACCESS_FILE}

	OLDIFS=${IFS}
	IFS=$'\n'
	for entry in ${SECURITY_ACCESS_LIST}
	do
		grep "^${entry}" ${PAM_ACCESS_FILE} >/dev/null 2>&1
		RESULT=$?
		if [ ${RESULT} -ne 0 ];then
			echo "${entry}" >>${PAM_ACCESS_FILE}
		fi
	done
	IFS=${OLDIFS}
	fOutputFileToLog ${PAM_ACCESS_FILE}	
	fOutputLog ""
}

fChangeBaseModuleSetting () {
	# check name set
	CHK_NAME="Check and Setting Base Module"
	CHK_DESC="基本ミドルウェアの設定と確認"
	fPrintHeader "${CHK_NAME}" "${CHK_DESC}"

	fCheckAndSetTcpWrappers
	fCheckAndSetPam
	
	fOutputLog ""
}

fCehckOrInstallSep () {
	# check name set
	CHK_NAME="SEP Install and Check"
	CHK_DESC="SEPのインストール"
	fPrintHeader "${CHK_NAME}" "${CHK_DESC}"

	fCheckSepInstall
	RESULT=$?
	if [ ${RESULT} -ne 0 ]; then
		# SEP install
		fCheckSepPrecondition
		RESULT=$?
		if [ ${RESULT} -ne 0 ]; then
			fOutputLog "SEPインストールの前提条件が整っていませんのでインストールはスキップします"
		else
			fInstallSep
			local SLEEP_TIME=20
			local MAX_SLEEP=180
			fOutputLog "SEPのインストールが完了しました。Liveupdateが終了するまで${MAX_SLEEP}秒機します"
			i=0
			while [ $i -le ${MAX_SLEEP} ]
			do
				sleep ${SLEEP_TIME}
				i=$(($i + ${SLEEP_TIME}))
				echo "${i}秒経過"
			done
			
			fCheckSepInstall
			fCheckSepStatus
		fi
	else
		fCheckSepStatus
	fi
}

fCheckOrSetNtpsync () {
	# check name set
	CHK_NAME="NTP synchronization"
	CHK_DESC="NTPサーバとの時刻同期の設定と確認"
	fPrintHeader "${CHK_NAME}" "${CHK_DESC}"

	local NTP_CNF_FILE="/etc/chrony.conf"
	local NTP_OPT_FILE="/etc/sysconfig/chronyd"
	local SERVICE_REBOOT_FLG=0

	# NTP設定ファイルチェック
	fDel_coment_and_blankline ${NTP_CNF_FILE} >${TMP_FILE_FUNC2}
	grep "${OS_NW_PARAM_[ntp]}" ${TMP_FILE_FUNC2} >${TMP_FILE_FUNC} 2>&1
	RESULT=$?

	if [ ${RESULT} -eq 0 ];then
		# ログ出力
		fOutputLog "参照先時刻同期サーバ設定"
		fOutputFileToLog ${TMP_FILE_FUNC}
		fOutputLog ""
	else
		fOutputLog "NTPサーバ[${OS_NW_PARAM_[ntp]}]と同期する設定がされていません。設定変更します。"
		fOutputLog ""

		# chrony.confの編集
		fFileBackup ${NTP_CNF_FILE}
		local LAST_NTPSERVER_LINE=$(grep -e '^server' -n ${NTP_CNF_FILE} | sort -n | tail -1 | sed -e 's/:.*//g')
		if [ -n "${LAST_NTPSERVER_LINE}" ];then
			sed -i -e 's/^server/#server/' ${NTP_CNF_FILE} 
			sed -i -e "${LAST_NTPSERVER_LINE}a server ${OS_NW_PARAM_[ntp]} iburst" ${NTP_CNF_FILE}
			# 設定反映フラグON
			SERVICE_REBOOT_FLG=1
		else
			fOutputLog "${NTP_CNF_FILE}にNTPサーバの設定が見つかりませんでした。内容を確認してください"
			fOutputLog ""
		fi
	fi

	# NTP環境変数ファイルチェック
	grep 'OPTIONS="-4"' ${NTP_OPT_FILE} >${TMP_FILE_FUNC} 2>&1
	RESULT=$?

	if [ ${RESULT} -eq 0 ];then
		# ログ出力
		fOutputLog "NTPオプション設定"
		fOutputFileToLog ${TMP_FILE_FUNC}
		fOutputLog ""
	else
		fOutputLog "NTP環境変数がipv4でListenする設定がされていません。設定変更します。"
		fOutputLog ""

		sed -i -e '/OPTIONS=""/d' ${NTP_OPT_FILE} 
		echo 'OPTIONS="-4"' >>${NTP_OPT_FILE}

		# 設定反映フラグON
		SERVICE_REBOOT_FLG=1

	fi
	
	cat /dev/null >${TMP_FILE_FUNC} 2>&1

	if [ ${SERVICE_REBOOT_FLG} -ne 0 ];then
		# 設定反映
		CMD="systemctl restart chronyd"
		fOutputLog "${CMD}"
		eval "${CMD}" >>${TMP_FILE_FUNC} 2>&1
		fOutputLog ""
		sleep 10
	fi

	#
	# 確認
	#
	CMD="chronyc -n sources"
	eval "${CMD}" >>${TMP_FILE_FUNC} 2>&1
	echo "" >>${TMP_FILE_FUNC} 2>&1
	CMD="chronyc tracking"
	eval "${CMD}" >>${TMP_FILE_FUNC} 2>&1
	echo "" >>${TMP_FILE_FUNC} 2>&1

	# ログ出力
	fOutputFileToLog ${TMP_FILE_FUNC}
	fOutputLog ""
}

fSetForApl () {

	# check name set
	CHK_NAME="Setting for Application"
	CHK_DESC="アプリケーション向けの設定と確認"
	fPrintHeader "${CHK_NAME}" "${CHK_DESC}"
	
	local APL_MAINTENANCE_USER=apladm

	id ${APL_MAINTENANCE_USER} >/dev/null 2>&1
	RESULT1=$?
	id ${APL_USER_ID} >/dev/null 2>&1
	RESULT2=$?

	if [ $(( ${RESULT1} + ${RESULT2} )) -eq 0 ];then
		CMD="usermod -aG ${APL_USER_ID} ${APL_MAINTENANCE_USER}"
		fOutputLog "${CMD}"
		eval "${CMD}"
		local APL_ID_STR=$(sudo -u ${APL_MAINTENANCE_USER} id)
		fOutputLog "${APL_ID_STR}"
		fOutputLog ""

		APL_USER_HOME=$(grep ${APL_USER_ID} /etc/passwd | cut -d ':' -f 6)
		CMD="chmod 775 ${APL_USER_HOME}"
		fOutputLog "${CMD}"
		eval "${CMD}"
		CMD="ls -l $(dirname ${APL_USER_HOME}) | grep $(basename ${APL_USER_HOME})"
		fOutputLog "${CMD}"
		local LS_STR=$(eval "${CMD}")
		fOutputLog "${LS_STR}"
		fOutputLog ""
	fi

	for apldir in "${!APL_DIRECTORY_[@]}"
	do
		if [ -d "${apldir}" ];then
			APL_DIR_PARM=$(echo ${APL_DIRECTORY_[${apldir}]} | cut -d ',' -f 1)
			APL_DIR_OWN=$(echo ${APL_DIRECTORY_[${apldir}]} | cut -d ',' -f 2)
			CMD="chmod ${APL_DIR_PARM} ${apldir}"
			fOutputLog "${CMD}"
			eval "${CMD}"
			CMD="chown ${APL_DIR_OWN} ${apldir}"
			fOutputLog "${CMD}"
			eval "${CMD}"
			CMD="ls -l $(dirname ${apldir}) | grep $(basename ${apldir})"
			fOutputLog "${CMD}"
			local LS_STR=$(eval "${CMD}")
			fOutputLog "${LS_STR}"
			fOutputLog ""
		else
			fOutputLog "${apldir} が存在しません"
		fi
	done

	fOutputLog ""
}

#--------------------------------------------------------------------
# メイン処理(main)
#--------------------------------------------------------------------
# EXITシグナルをtrapして終了メッセージ
trap "fOutputLog '${APP_NAME}を終了.' ; fCleanup" EXIT
# 他のシグナルもtrap
trap 'fOutputLog "trapped."; exit 255' 1 2 3 15

# rootで実行しないとダメなので
if [ ${EUID:-${UID}} -ne 0 ]; then
	fOutputLog ""
	fOutputLog "rootで実行でKnight動かないyo! "
	fOutputLog ""
	exit 255
fi

#----------------------------------------
# Server Resource check
#----------------------------------------

fCheck_SvrRes

read -p "このサーバに対して変更作業を実施します。よろしいですか? (y/N):" ANSYN
case "${ANSYN}" in
	[yY]*)
		fOutputLog ""
		;;
	*)
		fOutputLog ""
		fOutputLog "変更を中止します。"
		fOutputLog ""
		exit 1
		;;
esac

#----------------------------------------
# Modify OS Basic Configration Files 
#----------------------------------------
if [ ${Mod_OsBasicFiles_enable} -eq 1 ];then
	
	# SELINUX, LOCALE, TIMEZONE
	fModOsBasicFiles
	
	if [ ${YumUpdate_enable} -eq 1 ];then
		# YUM UPDATE
		fYumUpdate
	fi

	# NTP
	fCheckOrSetNtpsync 
fi

#----------------------------------------
# Install Basic modules
#----------------------------------------
if [ ${Install_Basic_modules_enable} -eq 1 ];then
	fInstallBasicModule
	fChangeBaseModuleSetting
fi

#----------------------------------------
# Create Extra Disk to LVM and Patition 
#----------------------------------------
if [ ${Create_ExtDisk_enable} -eq 1 ];then
	fCreateExtDiskToLVM
fi

#----------------------------------------
# Add OS Basic User 
#----------------------------------------
if [ ${Add_OsBaseUser_enable} -eq 1 ];then
	fAddOsBaseUser
	fSedSudoers
fi

#----------------------------------------
# Infra Directory Make and Check
#----------------------------------------
if [ ${Make_InfraDir_enable} -eq 1 ];then
	fMakeInfraDirectory
	fDeployInfraTools

	# Cron and rc.local
	if [ ${Set_CronAndRcLocal_enable} -eq 1 ];then
		fSetCronAndRcLocal
	fi
fi

#----------------------------------------
# Update Motd (Login Bunner)
#----------------------------------------
if [ ${Update_motd_enable} -eq 1 ];then
	fUpdateBunner
fi

#----------------------------------------
# SEP check or install
#----------------------------------------
if [ ${InstallSep_enable} -eq 1 ];then
	fCehckOrInstallSep
fi

#----------------------------------------
# For Application Setting
#----------------------------------------
if [ ${SetForApl_enable} -eq 1 ];then

	# Apldirectoryの権限等
	fSetForApl
	# runlevelのチェックと変更
	fChkDefaultRunLevel 
fi

E_DATE=$(date +"%Y-%m-%d %H:%M:%S")
fOutputLog "#----------------------------------------------------------------------#"
fOutputLog "# ${APP_NAME} end ${E_DATE}"
fOutputLog "# LogFile -> ${LOGFILE}"
fOutputLog "#----------------------------------------------------------------------#"

#
## End of File.
