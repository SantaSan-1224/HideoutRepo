#!/bin/bash

#----------------------------------------------------------
# Definition of Variable
#----------------------------------------------------------
APP_NAME=$(basename $0) ;readonly   APP_NAME
APP_DIR=$(cd $(dirname ${BASH_SOURCE:-$0}); pwd)
MY_HOSTNAME=$(uname -n) ;readonly MY_HOSTNAME

# Initial Value Set of ReturnCode
RC=0

#Set of Common Function and Environment Variable
HOME_DIR=/home/test
. ${HOME_DIR}/shell/config
. ${HOME_DIR}/shell/funcs

#----------------------------------------------------------
# Function
#----------------------------------------------------------

# FileBackup
fFileBackup() {
    local TGT_FILE="$1"
    local RC=0

    if [[ ! -f ${TGT_FILE} ]]; then
        cOutput_applog "${TGT_FILE}が存在しませんのでファイルのバックアップはしません。"
        RC=255
        return ${RC}
    fi

    local BK_FILE=$(dirname ${TGT_FILE})/$(basename ${TGT_FILE}).$(date "+%Y%m%d")

    CMD="cp -p ${TGT_FILE} ${BK_FILE}"
    cOutput_applog "FileBackup: ${CMD}"
    eval "${CMD}" 2>&1

    LAST1_BK_FILE=$(ls -1t ${TGT_FILE}.* | head -n 1)
    LAST2_BK_FILE=$(ls -1t ${TGT_FILE}.* | head -n 2 | tail -n 1)

    if [[ ${LAST1_BK_FILE} != ${LAST2_BK_FILE} ]]; then
        diff ${LAST2_BK_FILE} ${LAST1_BK_FILE} >/dev/null 2>&1
        RESULT=$?
        if [[ ${RESULT} -eq 0 ]]; then
            MSG="Not modified. remove ${LAST2_BK_FILE}"
            cOutput_applog "${MSG}"
            rm -f ${LAST2_BK_FILE}
        fi
    fi

    return ${RC}

}

# Create CSR File
fCreateCsrFile() {
    local PRIVATEKEY_FILE=${PRIVFILE}
    local PRIVATENEWKEY_FILE=${PRIVNEWFILE}
    local RND_FILE=${RNDFILE}
    local PRIVFILE_PASSWD=${PRIVKEYPWD}
    local HOST=${SRV_HOSTNAME}
    local CSRFILE=${CSRDIR}/${HOST}_$(date "+%Y%m%d").csr
    local DISTNAME="/C=JP/ST=Tokyo/L=Minato/O=ABC CORPORATION.¥
            /OU=""/CN=$2"
    
    fFileBackup ${PRIVATEKEY_FILE}

    if [[ -e ${RND_FILE}]] && [[ ! -e ${PRIVATEKEY_FILE} ]]; then
        RND_CMD="openssl md5 * > ${RND_FILE}"
        cOutput_applog "${RND_CMD}"
        eval "${RND_CMD}"
        RET=$?
        if [[ ${RET} -ne 0 ]]; then
            fErrorEnd "Failed to create random number file"
        fi

        MAKEFILE_CMD="openssl genrsa -rand ${RND_FILE} ¥
            des3 2048 -passout pass:${PRIVFILE_PASSWD} > ${PRIVATENEWKEY_FILE}"
        cOutput_applog "${MAKEFILE_CMD}"
        eval "${MAKEFILE_CMD}"
        RET=$?
        if [[ ${RET} -ne 0 ]]; then
            fErrorEnd "Failed to create private key"
        fi

        MAKECSR_CMD="openssl req -new -key ${PRIVATENEWKEY_FILE} ¥
            -out ${CSRFILE} -subj ${DISTNAME}"
        cOutput_applog "${MAKECSR_CMD}"
        eval "${MAKECSR_CMD}"
        RET=$?
        if [[ ${RET} -ne 0 ]]; then
            fErrorEnd "Failed to create CSR"
        fi
    fi

}

# Abnormal termination message output
fErrorEnd () {
    local MSG_STR="$@"

    cOutput_applog ${MSG_STR}
    RC=9

    cOutput_applog "${APP_NAME} End (RC=${RC})"
    exit ${RC}
}

#----------------------------------------------------------
# Pre-Processing
#----------------------------------------------------------
# Trap the EXIT signal
trap 'echo "255"; exit 255' 1 2 3 15

# Check if it is running as root
if [[ ${EUID:-${UID}} -ne 0 ]]; then
    fErrorEnd "The process is interrupted because it is not running as root"
fi

# Argument Check
if [[ #? -ne 2 ]]; then
    fErrorEnd "No Argument Set"
fi

#----------------------------------------------------------
# Main
#----------------------------------------------------------
# Processing start message
cOutput_applog "${APP_NAME} Start"

# Get Argument
COMMON_NAME=$1

fCreateCsrFile ${MY_HOSTNAME} ${COMMON_NAME}

#----------------------------------------------------------
# End
#----------------------------------------------------------
# Processing end message
cOutput_applog "${APP_NAME} End (RC=${RC})"
exit ${RC}