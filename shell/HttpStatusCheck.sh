#!/bin/bash

#----------------------------------------------------------
# Definition of Variable
#----------------------------------------------------------
APP_NAME=`basename $0`
APP_DIR=$(cd $(dirname ${BASH_SOURCE:-$0}); pwd)
MY_IP_ADDR=`ip -f inet -o addr | grep -v "127.0.0.1" | ¥
            cut -d¥ -f 7 | vud -d/ -f 1 | awk 'NR == 1'`
            
# Initial Value Set of Returncode 
RC=0

# Set of Common Function and Environment Variable
HOME_DIR=/home/ifc
. ${HOME_DIR}/shell/NZ_config
. ${HOME_DIR}/shell/NZ_funcs

#----------------------------------------------------------
# Function
#----------------------------------------------------------

# Usage
fUsage() {
  local SCRIPT_NAME=`basename $0`
  echo "" 1>&2
  echo "Usage: ${SCRIPT_NAME} [-h] <open|close>" 1>&2
  echo "" 1>&2
  echo "  ex1) ${SCRIPT_NAME} -h" 1>&2
  echo "  ex2) ${SCRIPT_NAME} open" 1>&2
  echo "" 1>&2
  echo " Options:" 1>&2
  echo "  -h, --help" 1>&2
  echo "" 1>&2
}

# Open Check
fServiceOpenCheck() {
  local STATUS_CD=$1
  # SwitchCheck
  if [[ ${STATUS_CD} -ne 200 ]]; then
    RC=9
    MSG="${APP_NAME} Switch Failed (RC=${RC}) [PID:$$]"
    cOutput_applog "${MSG}"
  else
    RC=0
    MSG="${APP_NAME} Switch Success (RC=${RC}) [PID:$$]"
    cOutput_applog "${MSG}"
  fi
}

# Close Check
fServiceCloseCheck() {
  local STATUS_CD=$1
  # SwitchCheck
  if [[ ${STATUS_CD} -ne 503 ]]; then
    RC=9
    MSG="${APP_NAME} Switch Failed (RC=${RC}) [PID:$$]"
    cOutput_applog "${MSG}"
  else
    RC=0
    MSG="${APP_NAME} Switch Success (RC=${RC}) [PID:$$]"
    cOutput_applog "${MSG}"
  fi
}

#----------------------------------------------------------
# Pre-Processing
#----------------------------------------------------------
# Trap the Signal and End Message
trap 'echo "255"; exit 255' 1 2 3 15

# Argument Check
ARG_COUNT=$#
if [[ ${ARG_COUNT} -ne 1 ]]; then
  RC=-1
  fUsage
  MSG="${APP_NAME} No Arguments Specified (RC=${RC}) [PID:$$]"
  cOutput_applog "${MSG}"
  echo "RC=${RC}"
  exit ${RC}
else
  MSG="${APP_NAME} Process Start (RC=${RC}) [PID:$$]"
  cOutput_applog "${MSG}"
fi

#----------------------------------------------------------
# Main
#----------------------------------------------------------
# Argument Get
SERVICE_MODE=$1

# HTTP StatusCode Get
HTTP_STATUS=`curl -I ${MY_IP_ADDR} 2>/dev/null | awk '{print $2}' | ¥
              head -n 1`

# Service Check
case ${SERVICE_MODE} in
  open)
    fServiceOpenCheck ${HTTP_STATUS}
    ;;
  close)
    fServiceCloseCheck ${HTTP_STATUS}
    ;;
  *)
    MSG="Illigal mode [${SERVICE_MODE}]"
    cOutput_applog "${MSG}"
    exit 255
    ;;
esac

#----------------------------------------------------------
# End
#----------------------------------------------------------
MSG="${APP_NAME} Process End (RC=${RC}) [PID:$$]"
cOutput_applog "${MSG}"
exit ${RC}
