<#
.SYNOPSIS
    S3 Glacier Deep Archiveに保存されたオブジェクトの復元リクエストを行うスクリプト
.DESCRIPTION
    このスクリプトはS3 Glacier Deep Archiveに保存されたオブジェクトの復元リクエストを行います。
    復元リクエスト対象のS3パスはFSxパス形式から自動的に導出されます。
.PARAMETER ConfigPath
    設定ファイルのパス
.PARAMETER SourceBucket
    復元リクエスト対象のS3バケット名
.PARAMETER RootFolder
    パス変換のベースとなるルートフォルダパス（例: "\\fsx\share"）
.PARAMETER Days
    復元したオブジェクトをS3内に保持する日数（デフォルト: 7）
.PARAMETER Tier
    復元リクエストのティア（Standard/Bulk/Expedited、デフォルト: Standard）
.PARAMETER LogPath
    ログファイルの保存先パス（デフォルト: カレントディレクトリ）
.EXAMPLE
    .\S3GlacierRestore.ps1 -ConfigPath "restore_targets.csv" -SourceBucket "my-archive-bucket" -RootFolder "\\fsx\share"
#>

param (
    [Parameter(Mandatory=$true)]
    [string]$ConfigPath,
    
    [Parameter(Mandatory=$true)]
    [string]$SourceBucket,
    
    [Parameter(Mandatory=$true)]
    [string]$RootFolder,
    
    [Parameter(Mandatory=$false)]
    [int]$Days = 7,
    
    [Parameter(Mandatory=$false)]
    [ValidateSet('Standard', 'Bulk', 'Expedited')]
    [string]$Tier = 'Standard',
    
    [Parameter(Mandatory=$false)]
    [string]$LogPath = ".\S3GlacierRestore_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
)

# AWScliがインストールされているか確認
function Test-AwsCli {
    try {
        $null = aws --version
        return $true
    } catch {
        return $false
    }
}

# ログ出力関数
function Write-Log {
    param (
        [string]$Message,
        [string]$Type = "INFO"
    )
    
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [$Type] $Message"
    
    Write-Host $logMessage
    Add-Content -Path $LogPath -Value $logMessage -Encoding UTF8
}

# 設定ファイルを読み込む関数
function Import-TargetConfig {
    param (
        [string]$ConfigPath
    )
    
    if (-not (Test-Path -Path $ConfigPath)) {
        Write-Log "設定ファイルが見つかりません: $ConfigPath" -Type "ERROR"
        exit 1
    }
    
    try {
        # UTF-8でCSVを読み込む
        $targets = Import-Csv -Path $ConfigPath -Encoding UTF8
        return $targets
    } catch {
        # エンコーディングが原因でエラーが発生した場合、他のエンコーディングを試みる
        try {
            Write-Log "UTF-8エンコーディングでの読み込みに失敗。代替エンコーディングで試行します。" -Type "WARNING"
            $targets = Import-Csv -Path $ConfigPath -Encoding Default
            return $targets
        } catch {
            Write-Log "設定ファイルの読み込みに失敗しました: $($_.Exception.Message)" -Type "ERROR"
            exit 1
        }
    }
}

# 入力パス形式をS3キーに変換する関数
function Convert-PathToS3Key {
    param (
        [string]$InputPath
    )
    
    # ルートフォルダを除去して、パスセパレータを置換
    $s3Key = $InputPath.Replace($RootFolder, "").TrimStart("\").Replace("\", "/")
    
    # URLエンコードが必要な文字（日本語など）をエンコード
    # ただし、スラッシュ(/)はエンコードしない
    $parts = $s3Key.Split('/')
    $encodedParts = $parts | ForEach-Object { 
        [System.Web.HttpUtility]::UrlEncode($_, [System.Text.Encoding]::UTF8).Replace('+', '%20')
    }
    $encodedS3Key = $encodedParts -join '/'
    
    return $encodedS3Key
}

# ファイルの復元リクエストを実行する関数
function Request-ObjectRestore {
    param (
        [string]$FilePath
    )
    
    # 入力パスからS3キーを取得
    $s3Key = Convert-PathToS3Key -InputPath $FilePath
    
    Write-Log "復元リクエストを開始: $FilePath -> s3://$SourceBucket/$s3Key"
    
    try {
        # JSON形式でリストア構成を生成
        $restoreConfig = @{
            Days = $Days
            GlacierJobParameters = @{
                Tier = $Tier
            }
        } | ConvertTo-Json -Compress
        
        # S3オブジェクトの復元リクエスト
        $restoreResult = aws s3api restore-object --bucket $SourceBucket --key $s3Key --restore-request $restoreConfig 2>&1
        
        if ($LASTEXITCODE -eq 0) {
            Write-Log "復元リクエスト成功: s3://$SourceBucket/$s3Key" -Type "SUCCESS"
            return $true
        } else {
            if ($restoreResult -match "RestoreAlreadyInProgress") {
                Write-Log "復元リクエスト既に実行中: s3://$SourceBucket/$s3Key" -Type "WARNING"
                return $true
            } elseif ($restoreResult -match "InvalidObjectState") {
                Write-Log "復元リクエスト失敗 (既に復元済み、または別のストレージクラス): s3://$SourceBucket/$s3Key" -Type "WARNING"
                return $false
            } else {
                Write-Log "復元リクエスト失敗: s3://$SourceBucket/$s3Key" -Type "ERROR"
                Write-Log "エラー: $restoreResult" -Type "ERROR"
                return $false
            }
        }
    } catch {
        Write-Log "例外発生: s3://$SourceBucket/$s3Key" -Type "ERROR"
        Write-Log "エラー: $($_.Exception.Message)" -Type "ERROR"
        return $false
    }
}

# フォルダに対応するプレフィックスのオブジェクト復元リクエストを処理する関数
function Process-RestoreRequests {
    param (
        [string]$FolderPath
    )
    
    Write-Log "フォルダパスに対する復元処理を開始: $FolderPath"
    
    # 入力パスからS3プレフィックスを取得
    $s3Prefix = Convert-PathToS3Key -InputPath $FolderPath
    
    # S3バケット内のオブジェクトを一覧表示
    try {
        Write-Log "S3からオブジェクト一覧を取得中: $s3Prefix"
        $s3Objects = aws s3 ls "s3://$SourceBucket/$s3Prefix" --recursive | Where-Object { $_ -notmatch '^\s*$' }
        
        if ($null -eq $s3Objects -or $s3Objects.Count -eq 0) {
            Write-Log "指定されたプレフィックスにオブジェクトが見つかりません: $s3Prefix" -Type "WARNING"
            return
        }
        
        Write-Log "対象オブジェクト数: $($s3Objects.Count)"
        
        # 各オブジェクトに対してリストアリクエストを実行
        foreach ($objectLine in $s3Objects) {
            # 行を解析してS3キーを抽出
            $line = $objectLine.Trim()
            $parts = $line -split '\s+'
            
            if ($parts.Count -ge 4) {
                $s3Key = [string]::Join(" ", $parts[3..$parts.Count])
                
                # S3キーからログ表示用のパスを構築
                $displayPath = "s3://$SourceBucket/$s3Key"
                
                # 復元リクエストを実行
                Write-Log "オブジェクト復元リクエスト処理: $displayPath"
                
                $restoreConfig = @{
                    Days = $Days
                    GlacierJobParameters = @{
                        Tier = $Tier
                    }
                } | ConvertTo-Json -Compress
                
                try {
                    $restoreResult = aws s3api restore-object --bucket $SourceBucket --key $s3Key --restore-request $restoreConfig 2>&1
                    
                    if ($LASTEXITCODE -eq 0) {
                        Write-Log "復元リクエスト成功: $displayPath" -Type "SUCCESS"
                    } else {
                        if ($restoreResult -like "*RestoreAlreadyInProgress*") {
                            Write-Log "復元リクエスト既に実行中: $displayPath" -Type "WARNING"
                        } elseif ($restoreResult -like "*InvalidObjectState*") {
                            Write-Log "復元リクエスト失敗 (既に復元済み、または別のストレージクラス): $displayPath" -Type "WARNING"
                        } else {
                            Write-Log "復元リクエスト失敗: $displayPath" -Type "ERROR"
                            Write-Log "エラー: $restoreResult" -Type "ERROR"
                        }
                    }
                } catch {
                    Write-Log "例外発生: $displayPath" -Type "ERROR"
                    Write-Log "エラー: $($_.Exception.Message)" -Type "ERROR"
                }
            } else {
                Write-Log "S3オブジェクト行の解析に失敗: $line" -Type "ERROR"
            }
        }
    } catch {
        Write-Log "S3オブジェクト一覧の取得に失敗: $($_.Exception.Message)" -Type "ERROR"
    }
    
    Write-Log "フォルダパスに対する復元処理が完了: $FolderPath"
}

# メイン処理
function Start-Restore {
    # System.Web名前空間を読み込み（URLエンコーディング用）
    Add-Type -AssemblyName System.Web
    
    # 初期化
    if (-not (Test-Path -Path $LogPath)) {
        $null = New-Item -Path $LogPath -ItemType File -Force -Encoding UTF8
    }
    
    Write-Log "復元リクエスト処理を開始します。"
    Write-Log "設定ファイル: $ConfigPath"
    Write-Log "対象S3バケット: $SourceBucket"
    Write-Log "ルートフォルダ: $RootFolder"
    Write-Log "復元保持期間: $Days 日"
    Write-Log "復元ティア: $Tier"
    
    # AWS CLIの存在確認
    if (-not (Test-AwsCli)) {
        Write-Log "AWS CLIがインストールされていません。スクリプトを終了します。" -Type "ERROR"
        exit 1
    }
    
    # 設定ファイルを読み込む
    $targets = Import-TargetConfig -ConfigPath $ConfigPath
    
    # 成功・失敗カウンター
    $totalSuccessCount = 0
    $totalFailureCount = 0
    
    # 各ターゲットを処理
    foreach ($target in $targets) {
        # 必須フィールドの確認
        if ([string]::IsNullOrEmpty($target.Path)) {
            Write-Log "設定エラー: Pathフィールドが空です。" -Type "ERROR"
            continue
        }
        
        $path = $target.Path
        $type = if ($target.Type) { $target.Type } else { "" }
        
        # タイプに応じて処理
        if ($type -eq "folder") {
            Process-RestoreRequests -FolderPath $path
        } elseif ($type -eq "file") {
            $result = Request-ObjectRestore -FilePath $path
            if ($result) {
                $totalSuccessCount++
            } else {
                $totalFailureCount++
            }
        } else {
            Write-Log "タイプが指定されていないか、不明です。タイプを明示的に指定してください。: $path" -Type "ERROR"
        }
    }
    
    Write-Log "すべての復元リクエストが完了しました。"
    Write-Log "注意: Glacier Deep Archive からの復元には最大48時間かかる場合があります。"
    Write-Log "ログファイル: $LogPath"
}

# 実行
Start-Restore
