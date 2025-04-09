# S3 Glacier Deep Archiveからの復元スクリプト
# FSx for Windows File ServerのアーカイブオブジェクトをS3から復元します

# パラメータ
param(
    [Parameter(Mandatory=$true)]
    [string]$CsvFilePath,  # 復元対象のパスが記載されたCSVファイルパス
    
    [Parameter(Mandatory=$true)]
    [string]$S3BucketName   # S3バケット名 (s3://の後の部分)
)

# 固定値の設定
$EndpointUrl = "https://s3.ap-northeast-1.amazonaws.com"  # VPCエンドポイントURL（環境に合わせて変更してください）

# ログ出力関数
function Write-Log {
    param(
        [string]$Message,
        [string]$Level = "INFO"
    )
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$timestamp] [$Level] $Message"
    Add-Content -Path "$PSScriptRoot\restore_log_$(Get-Date -Format 'yyyyMMdd').log" -Value "[$timestamp] [$Level] $Message"
}

# FSxパスをS3パスに変換する関数
function Convert-FsxPathToS3Path {
    param(
        [string]$FsxPath
    )
    
    # UNCパスの処理 (\\server\share\path\to\file → path/to/file)
    if ($FsxPath -match '\\\\([^\\]+)\\([^\\]+)(.*)') {
        $relativePath = $Matches[3]
        # 先頭のバックスラッシュを削除
        if ($relativePath.StartsWith('\')) {
            $relativePath = $relativePath.Substring(1)
        }
        # バックスラッシュをスラッシュに変換
        $s3RelativePath = $relativePath -replace '\\', '/'
        return $s3RelativePath
    }
    
    Write-Log "パスの変換に失敗しました: $FsxPath" "ERROR"
    return $null
}

# S3オブジェクトの復元リクエストを送信する関数
function Request-S3ObjectRestoration {
    param(
        [string]$S3Key
    )
    
    try {
        $command = "aws s3api restore-object --bucket $S3BucketName --key `"$S3Key`" --restore-request 'Days:30,GlacierJobParameters:{`"Tier`":`"Bulk`"}' --profile Operation --endpoint-url $EndpointUrl"
        Write-Log "実行コマンド: $command" "DEBUG"
        
        $result = Invoke-Expression $command
        Write-Log "オブジェクト復元リクエストを送信しました: s3://$S3BucketName/$S3Key" "INFO"
        return $true
    }
    catch {
        Write-Log "オブジェクト復元リクエストの送信に失敗しました: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# S3からのオブジェクト存在確認関数
function Test-S3ObjectExists {
    param(
        [string]$S3Key
    )
    
    try {
        $command = "aws s3api head-object --bucket $S3BucketName --key `"$S3Key`" --profile Operation --endpoint-url $EndpointUrl"
        $result = Invoke-Expression $command 2>&1
        if ($LASTEXITCODE -eq 0) {
            return $true
        }
        return $false
    }
    catch {
        return $false
    }
}

# メイン処理
try {
    Write-Log "=== S3 Glacier Deep Archive 復元処理を開始します ===" "INFO"
    
    # CSVファイルの存在確認
    if (-not (Test-Path $CsvFilePath)) {
        Write-Log "CSVファイルが見つかりません: $CsvFilePath" "ERROR"
        exit 1
    }
    
    # CSVファイルを読み込む
    $restoreTargets = Import-Csv -Path $CsvFilePath -Header "Path", "Type"
    Write-Log "CSVファイルから $(($restoreTargets | Measure-Object).Count) 件の復元対象を読み込みました" "INFO"
    
    # 復元ステータス追跡用のリスト
    $restoreStatus = @()
    $totalRequestCount = 0
    $successRequestCount = 0
    $failedRequestCount = 0
    $unfoundObjectCount = 0
    
    # 各対象ごとに処理
    foreach ($target in $restoreTargets) {
        $fsxPath = $target.Path
        $type = $target.Type.ToLower()
        
        # FSxパスをS3パスに変換
        $s3RelativePath = Convert-FsxPathToS3Path -FsxPath $fsxPath
        if (-not $s3RelativePath) {
            continue
        }
        
        Write-Log "処理: $fsxPath ($type)" "INFO"
        Write-Log "S3相対パス: $s3RelativePath" "DEBUG"
        
        if ($type -eq "file") {
            $totalRequestCount++
            
            # ファイルが存在するか確認
            if (Test-S3ObjectExists -S3Key $s3RelativePath) {
                # ファイルの復元リクエスト
                $success = Request-S3ObjectRestoration -S3Key $s3RelativePath
                if ($success) {
                    $successRequestCount++
                    $restoreStatus += [PSCustomObject]@{
                        OriginalPath = $fsxPath
                        S3Key = $s3RelativePath
                        Type = $type
                        RequestStatus = "Success"
                        ObjectFound = "Yes"
                    }
                } else {
                    $failedRequestCount++
                    $restoreStatus += [PSCustomObject]@{
                        OriginalPath = $fsxPath
                        S3Key = $s3RelativePath
                        Type = $type
                        RequestStatus = "Failed"
                        ObjectFound = "Yes"
                    }
                }
            } else {
                $unfoundObjectCount++
                Write-Log "オブジェクトが見つかりません: s3://$S3BucketName/$s3RelativePath" "WARNING"
                $restoreStatus += [PSCustomObject]@{
                    OriginalPath = $fsxPath
                    S3Key = $s3RelativePath
                    Type = $type
                    RequestStatus = "Not Requested"
                    ObjectFound = "No"
                }
            }
        }
        elseif ($type -eq "folder") {
            # フォルダの場合、先頭にスラッシュがない場合は追加
            if (-not $s3RelativePath.EndsWith('/')) {
                $s3RelativePath = "$s3RelativePath/"
            }
            
            # フォルダ内のオブジェクトをリスト
            $command = "aws s3 ls s3://$S3BucketName/$s3RelativePath --recursive --profile Operation --endpoint-url $EndpointUrl"
            $objects = Invoke-Expression $command
            
            Write-Log "フォルダ内のオブジェクトを検索しています..." "INFO"
            
            $objectCount = 0
            $folderSuccessCount = 0
            $folderFailedCount = 0
            
            foreach ($object in $objects) {
                if ($object -match "\s+(\d+)\s+.+\s+(.+)$") {
                    $objectKey = $Matches[2]
                    $objectCount++
                    $totalRequestCount++
                    
                    # 各オブジェクトの復元リクエスト
                    $success = Request-S3ObjectRestoration -S3Key $objectKey
                    if ($success) {
                        $successRequestCount++
                        $folderSuccessCount++
                        $restoreStatus += [PSCustomObject]@{
                            OriginalPath = $fsxPath
                            S3Key = $objectKey
                            Type = "file"
                            RequestStatus = "Success"
                            ObjectFound = "Yes"
                            ParentFolder = $fsxPath
                        }
                    } else {
                        $failedRequestCount++
                        $folderFailedCount++
                        $restoreStatus += [PSCustomObject]@{
                            OriginalPath = $fsxPath
                            S3Key = $objectKey
                            Type = "file"
                            RequestStatus = "Failed"
                            ObjectFound = "Yes"
                            ParentFolder = $fsxPath
                        }
                    }
                }
            }
            
            if ($objectCount -eq 0) {
                $unfoundObjectCount++
                Write-Log "フォルダ内にオブジェクトが見つかりません: s3://$S3BucketName/$s3RelativePath" "WARNING"
                $restoreStatus += [PSCustomObject]@{
                    OriginalPath = $fsxPath
                    S3Key = $s3RelativePath
                    Type = $type
                    RequestStatus = "Not Requested"
                    ObjectFound = "No"
                }
            } else {
                Write-Log "フォルダ内の $objectCount 個のオブジェクトに対して復元リクエストを送信しました (成功: $folderSuccessCount, 失敗: $folderFailedCount)" "INFO"
            }
        }
        else {
            Write-Log "不明なタイプです: $type" "ERROR"
        }
    }
    
    # 復元ステータスをCSVに出力
    $statusCsvPath = "$PSScriptRoot\restore_status_$(Get-Date -Format 'yyyyMMddHHmmss').csv"
    $restoreStatus | Export-Csv -Path $statusCsvPath -NoTypeInformation
    Write-Log "復元ステータスをCSVに出力しました: $statusCsvPath" "INFO"
    
    # 復元リクエスト結果の集計
    $originalTargetCount = ($restoreTargets | Measure-Object).Count
    $processedTargetCount = ($restoreStatus | Group-Object -Property OriginalPath | Measure-Object).Count
    $successTargetCount = ($restoreStatus | Where-Object { $_.RequestStatus -eq "Success" } | Group-Object -Property OriginalPath | Measure-Object).Count
    
    # 復元リクエスト突き合わせチェック
    $missingTargets = $restoreTargets | Where-Object {
        $targetPath = $_.Path
        $processedTargets = $restoreStatus | Where-Object { $_.OriginalPath -eq $targetPath }
        ($processedTargets | Measure-Object).Count -eq 0
    }
    
    $missingCount = ($missingTargets | Measure-Object).Count
    
    # 結果判定
    $overallSuccess = ($missingCount -eq 0) -and ($successRequestCount -gt 0)
    $resultStatus = if ($overallSuccess) { "成功" } else { "一部処理に問題あり" }
    
    # 復元結果レポートの作成
    $reportPath = "$PSScriptRoot\restore_report_$(Get-Date -Format 'yyyyMMddHHmmss').txt"
    @"
=== S3 Glacier Deep Archive 復元リクエストレポート ===
実行日時: $(Get-Date -Format "yyyy/MM/dd HH:mm:ss")

【処理結果】
総合判定: $resultStatus

【入力CSVファイル】
読み込んだCSVファイル: $CsvFilePath
CSVファイル内の復元対象数: $originalTargetCount 件

【処理結果サマリー】
処理された復元対象数: $processedTargetCount 件
正常に復元リクエストが完了した対象数: $successTargetCount 件
処理されなかった対象数: $missingCount 件

【詳細統計】
送信された復元リクエスト総数: $totalRequestCount 件
成功した復元リクエスト数: $successRequestCount 件
失敗した復元リクエスト数: $failedRequestCount 件
見つからなかったオブジェクト数: $unfoundObjectCount 件

【注意事項】
・復元リクエストが完了しても、実際のファイル復元には最大48時間かかる場合があります
・復元が完了すると、FSx上の.arcファイルは自動的に元のファイルに戻ります
・復元リクエストの詳細は以下のCSVファイルを参照してください:
  $statusCsvPath

【復元ログ】
詳細なログは以下のファイルを参照してください:
$PSScriptRoot\restore_log_$(Get-Date -Format 'yyyyMMdd').log
"@ | Out-File -FilePath $reportPath -Encoding utf8
    
    # 処理されなかった対象がある場合は追記
    if ($missingCount -gt 0) {
        "【処理されなかった対象】" | Out-File -FilePath $reportPath -Encoding utf8 -Append
        foreach ($missing in $missingTargets) {
            "・$($missing.Path) ($($missing.Type))" | Out-File -FilePath $reportPath -Encoding utf8 -Append
        }
        "`n処理されなかった対象については、パスの指定が正しいか、S3に対応するオブジェクトが存在するかを確認してください。" | Out-File -FilePath $reportPath -Encoding utf8 -Append
    }
    
    Write-Log "復元リクエストレポートを作成しました: $reportPath" "INFO"
    
    # レポート表示
    Get-Content $reportPath
    
    # スクリプトの終了コードを設定
    if ($overallSuccess) {
        Write-Log "復元リクエスト処理が正常に完了しました" "INFO"
        exit 0
    } else {
        Write-Log "一部の復元リクエスト処理に問題がありました" "WARNING"
        exit 1
    }
}
catch {
    Write-Log "予期せぬエラーが発生しました: $($_.Exception.Message)" "ERROR"
    Write-Log "スタックトレース: $($_.ScriptStackTrace)" "ERROR"
    exit 1
}
