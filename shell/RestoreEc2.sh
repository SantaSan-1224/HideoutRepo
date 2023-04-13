#!/bin/bash

# 個別設定ファイル読み込み
CONFFILE=/home/cloudshell-user/conf/RestoreEc2.conf
if [[ ! -e ${CONFFILE} ]]; then
    echo "設定ファイルが存在しません"
    exit 255
fi
source ${CONFFILE}

#-----------------------------------------------
# Define of Function
#-----------------------------------------------
funcVariableChk() {
        if [[ -z ${INST_ID} ]] \
                || [[ -z ${VOL_ID} ]] \
                || [[ -z ${AZ_NAME} ]] \
                || [[ -z ${VOL_TYPE} ]] \
                || [[ -z ${VOL_SIZE} ]] \
                || [[ -z ${ATTACH_DEVICE_NAME} ]]; then
                echo "変数が定義されていません"
                exit 255
        else
                echo "変数チェックOK"
        fi
}

funcEc2StatusChk() {
        INST_STATUS=$(aws ec2 describe-instances \
                --instance-ids ${INST_ID} \
                --query 'Reservations[*].Instances[].State.Name | [0]')
        INST_STATUS=$(echo \"${INST_STATUS}\" | sed -e "s/\"//g")
}
#-----------------------------------------------
# Main
#-----------------------------------------------
# 変数チェック
funcVariableChk

# 最新のスナップショットID確認
SNPSHT_ID=$(aws ec2 describe-snapshots \
                --filters Name=volume-id,Values=${VOL_ID} \
                --query 'reverse(sort_by(Snapshots,&StartTime))[0].SnapshotId')

echo ${SNPSHT_ID}

# スナップショットからボリュームを作成する
CMD="aws ec2 create-volume \
                --availability-zone ${AZ_NAME} \
                --volume-type ${VOL_TYPE} \
                --size ${VOL_SIZE} \
                --snapshot-id ${SNPSHT_ID} \
                >/dev/null "

echo "ボリュームを作成します"
eval "${CMD}"
if [[ $? -ne 0 ]]; then
        echo "スナップショットからボリュームの作成に失敗しました"
        exit 255
else
        echo "ボリュームの作成に成功しました"
fi

# ボリュームIDを取得する
NEWVOL_ID=$(aws ec2 describe-volumes \
                --filters Name=snapshot-id,Values="${SNPSHT_ID}" \
                --query 'reverse(sort_by(Volumes,&CreateTime))[0].VolumeId')
echo "${NEWVOL_ID}"

# インスタンス起動確認
funcEc2StatusChk
if [[ "${INST_STATUS}" == running ]]; then
        echo "起動中のためインスタンスを停止します"
        CMD="aws ec2 stop-instances \
                --instance-ids ${INST_ID} \
                >/dev/null "
        eval "${CMD}"
        echo "停止中のため30秒待機します"
        sleep 30
        funcEc2StatusChk
        if [[ "${INST_STATUS}" == stopped ]]; then
                echo "インスタンスが停止されました"
        else
                echo "インスタンスの停止が失敗しました。終了します"
                exit 255
        fi
elif [[ "${INST_STATUS}" == stopped ]]; then
        echo "インスタンスは停止中です。後続処理します"
else
        echo "不明なエラーが発生しました。終了します"
        exit 255
fi

# ボリュームをデタッチ
OLDVOL_ID=${VOL_ID}
CMD="aws ec2 detach-volume \
        --volume-id ${OLDVOL_ID} \
        >/dev/null "
echo "${OLDVOL_ID} をデタッチします"
eval "${CMD}"
if [[ $? -ne 0 ]]; then
        echo "ボリュームのデタッチに失敗しました。終了します"
        exit 255
else
        echo "ボリュームのデタッチに成功しました"
fi

# ボリュームをアタッチ
CMD="aws ec2 attach-volume \
        --device ${ATTACH_DEVICE_NAME} \
        --instance-id ${INST_ID} \
        --volume-id ${NEWVOL_ID} \
        >/dev/null "
echo "${NEWVOL_ID} をアタッチします"
eval "${CMD}"
if [[ $? -ne 0 ]]; then
        echo "ボリュームのアタッチに失敗しました。終了します"
        exit 255
else
        echo "ボリュームのアタッチに成功しました"
fi

# インスタンス起動
echo "インスタンスを起動します"
CMD="aws ec2 start-instances \
        --instance-ids ${INST_ID} \
        >/dev/null "
eval "${CMD}"
echo "インスタンス起動待ちのため60秒待機します"
sleep 60
funcEc2StatusChk
if [[ "${INST_STATUS}" == running ]]; then
        echo "インスタンスの起動が成功しました"
else
        echo "インスタンスの起動が失敗しました。終了します"
        exit 255
fi