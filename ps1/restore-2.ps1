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

# S3オブジェクトの復元状態を確認する関数
function Get-S3ObjectRestorationStatus {
    param(
        [string]$S3Key
    )
    
    try {
        $command = "aws s3api head-object --bucket $S3BucketName --key `"$S3Key`" --profile Operation --endpoint-url $EndpointUrl"
        $result = Invoke-Expression $command | ConvertFrom-Json
        
        if ($result.Restore) {
            if ($result.Restore -match "ongoing-request=\"false\"") {
                return "COMPLETED"
            }
            elseif ($result.Restore -match "ongoing-request=\"true\"") {
                return "IN_PROGRESS"
            }
        }
        return "NOT_STARTED"
    }
    catch {
        Write-Log "オブジェクト状態の確認に失敗しました: $($_.Exception.Message)" "ERROR"
        return "ERROR"
    }
}

# S3からオブジェクトをダウンロードしてFSxにコピーする関数
function Copy-S3ObjectToFsx {
    param(
        [string]$S3Key,
        [string]$FsxPath,
        [string]$Type
    )
    
    try {
        # FSxパスの親ディレクトリを確認・作成
        $parentDir = Split-Path -Parent $FsxPath
        if (-not (Test-Path $parentDir)) {
            New-Item -Path $parentDir -ItemType Directory -Force | Out-Null
            Write-Log "ディレクトリを作成しました: $parentDir" "INFO"
        }
        
        if ($Type -eq "file") {
            # ファイルの場合、S3からダウンロードして直接コピー
            $tempFile = [System.IO.Path]::GetTempFileName()
            $command = "aws s3 cp s3://$S3BucketName/$S3Key `"$tempFile`" --profile Operation --endpoint-url $EndpointUrl"
            Invoke-Expression $command
            
            # .arcファイルのチェック
            $arcFilePath = "$FsxPath.arc"
            if (Test-Path $arcFilePath) {
                Remove-Item -Path $arcFilePath -Force
                Write-Log ".arcファイルを削除しました: $arcFilePath" "INFO"
            }
            
            # 元のファイルパスにコピー
            Copy-Item -Path $tempFile -Destination $FsxPath -Force
            Remove-Item -Path $tempFile -Force
            Write-Log "ファイルを復元しました: $FsxPath" "INFO"
        }
        elseif ($Type -eq "folder") {
            # フォルダの場合、S3のプレフィックスにマッチするすべてのオブジェクトをリスト
            $command = "aws s3 ls s3://$S3BucketName/$S3Key/ --recursive --profile Operation --endpoint-url $EndpointUrl"
            $objects = Invoke-Expression $command
            
            $totalObjects = 0
            $restoredObjects = 0
            
            # オブジェクトごとに処理
            foreach ($object in $objects) {
                if ($object -match "\s+(\d+)\s+.+\s+(.+)$") {
                    $objectKey = $Matches[2]
                    $totalObjects++
                    
                    # S3からの相対パスをFSxパスに変換
                    $relativePath = $objectKey.Substring($S3Key.Length).TrimStart('/')
                    $targetFsxPath = Join-Path -Path $FsxPath -ChildPath ($relativePath -replace '/', '\')
                    
                    # .arcファイルのチェック
                    $arcFilePath = "$targetFsxPath.arc"
                    if (Test-Path $arcFilePath) {
                        # 親ディレクトリを確認・作成
                        $targetParentDir = Split-Path -Parent $targetFsxPath
                        if (-not (Test-Path $targetParentDir)) {
                            New-Item -Path $targetParentDir -ItemType Directory -Force | Out-Null
                        }
                        
                        # S3からダウンロード
                        $tempFile = [System.IO.Path]::GetTempFileName()
                        $downloadCommand = "aws s3 cp s3://$S3BucketName/$objectKey `"$tempFile`" --profile Operation --endpoint-url $EndpointUrl"
                        Invoke-Expression $downloadCommand
                        
                        # 元のファイルパスにコピー
                        Copy-Item -Path $tempFile -Destination $targetFsxPath -Force
                        Remove-Item -Path $tempFile -Force
                        Remove-Item -Path $arcFilePath -Force
                        
                        $restoredObjects++
                        Write-Log "フォルダ内ファイルを復元しました: $targetFsxPath" "INFO"
                    }
                }
            }
            
            Write-Log "フォルダ復元完了: $FsxPath ($restoredObjects/$totalObjects ファイル)" "INFO"
        }
        
        return $true
    }
    catch {
        Write-Log "ファイル復元に失敗しました: $($_.Exception.Message)" "ERROR"
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
            # ファイルの復元リクエスト
            $success = Request-S3ObjectRestoration -S3Key $s3RelativePath
            if ($success) {
                $restoreStatus += [PSCustomObject]@{
                    OriginalPath = $fsxPath
                    S3Key = $s3RelativePath
                    Type = $type
                    RequestStatus = "Requested"
                    CompletionStatus = "Pending"
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
            foreach ($object in $objects) {
                if ($object -match "\s+(\d+)\s+.+\s+(.+)$") {
                    $objectKey = $Matches[2]
                    $objectCount++
                    
                    # 各オブジェクトの復元リクエスト
                    $success = Request-S3ObjectRestoration -S3Key $objectKey
                    if ($success) {
                        $restoreStatus += [PSCustomObject]@{
                            OriginalPath = $fsxPath
                            S3Key = $objectKey
                            Type = "file"
                            RequestStatus = "Requested"
                            CompletionStatus = "Pending"
                            ParentFolder = $fsxPath
                        }
                    }
                }
            }
            
            Write-Log "フォルダ内の $objectCount 個のオブジェクトに対して復元リクエストを送信しました" "INFO"
        }
        else {
            Write-Log "不明なタイプです: $type" "ERROR"
        }
    }
    
    # 復元ステータスをCSVに出力
    $statusCsvPath = "$PSScriptRoot\restore_status_$(Get-Date -Format 'yyyyMMddHHmmss').csv"
    $restoreStatus | Export-Csv -Path $statusCsvPath -NoTypeInformation
    Write-Log "復元ステータスをCSVに出力しました: $statusCsvPath" "INFO"
    
    # ステータス確認プロセスの開始
    Write-Log "=== 復元ステータスの定期確認を開始します ===" "INFO"
    Write-Log "このプロセスはバックグラウンドで動作し、復元が完了したオブジェクトを順次FSxに反映します" "INFO"
    Write-Log "復元には数時間から最大12時間程度かかる場合があります" "INFO"
    
    $pendingItems = $restoreStatus | Where-Object { $_.CompletionStatus -eq "Pending" }
    $checkCount = 0
    
    while (($pendingItems | Measure-Object).Count -gt 0) {
        $checkCount++
        Write-Log "ステータス確認 #$checkCount - 残り $($pendingItems.Count) アイテム" "INFO"
        
        foreach ($item in $pendingItems) {
            $status = Get-S3ObjectRestorationStatus -S3Key $item.S3Key
            
            if ($status -eq "COMPLETED") {
                Write-Log "復元完了: $($item.S3Key)" "INFO"
                
                # FSxへのコピー
                if ($item.ParentFolder) {
                    # フォルダ内のアイテム
                    $relativePath = $item.S3Key.Substring($s3RelativePath.Length).TrimStart('/')
                    $targetFsxPath = Join-Path -Path $item.ParentFolder -ChildPath ($relativePath -replace '/', '\')
                    Copy-S3ObjectToFsx -S3Key $item.S3Key -FsxPath $targetFsxPath -Type "file"
                }
                else {
                    # 直接指定されたファイル
                    Copy-S3ObjectToFsx -S3Key $item.S3Key -FsxPath $item.OriginalPath -Type $item.Type
                }
                
                # ステータス更新
                $item.CompletionStatus = "Completed"
            }
            elseif ($status -eq "ERROR") {
                Write-Log "復元エラー: $($item.S3Key)" "ERROR"
                $item.CompletionStatus = "Error"
            }
        }
        
        # 更新されたステータスをCSVに出力
        $restoreStatus | Export-Csv -Path $statusCsvPath -NoTypeInformation
        
        # 次の確認までの待機
        Write-Log "30分後に再確認します..." "INFO"
        Start-Sleep -Seconds 1800  # 30分待機
        
        # 保留中のアイテムを再取得
        $pendingItems = $restoreStatus | Where-Object { $_.CompletionStatus -eq "Pending" }
    }
    
    # 復元結果の集計
    $completed = ($restoreStatus | Where-Object { $_.CompletionStatus -eq "Completed" } | Measure-Object).Count
    $errors = ($restoreStatus | Where-Object { $_.CompletionStatus -eq "Error" } | Measure-Object).Count
    $total = ($restoreStatus | Measure-Object).Count
    
    Write-Log "=== 復元処理が完了しました ===" "INFO"
    Write-Log "合計: $total アイテム" "INFO"
    Write-Log "成功: $completed アイテム" "INFO"
    Write-Log "エラー: $errors アイテム" "INFO"
    Write-Log "詳細ステータス: $statusCsvPath" "INFO"
    
    # 復元結果レポートの作成
    $reportPath = "$PSScriptRoot\restore_report_$(Get-Date -Format 'yyyyMMddHHmmss').txt"
    @"
=== S3 Glacier Deep Archive 復元レポート ===
実行日時: $(Get-Date -Format "yyyy/MM/dd HH:mm:ss")

【復元結果サマリー】
合計処理対象: $total アイテム
復元成功: $completed アイテム
復元失敗: $errors アイテム

【詳細】
詳細なステータス情報は以下のCSVファイルを参照してください:
$statusCsvPath

【復元ログ】
詳細なログは以下のファイルを参照してください:
$PSScriptRoot\restore_log_$(Get-Date -Format 'yyyyMMdd').log
"@ | Out-File -FilePath $reportPath -Encoding utf8
    
    Write-Log "復元レポートを作成しました: $reportPath" "INFO"
    
    # レポート表示
    Get-Content $reportPath
}
catch {
    Write-Log "予期せぬエラーが発生しました: $($_.Exception.Message)" "ERROR"
    Write-Log "スタックトレース: $($_.ScriptStackTrace)" "ERROR"
    exit 1
}
