<#
.SYNOPSIS
    FSx for Windows File ServerからS3バケットへファイルを転送し、Glacier Deep Archiveに変更するスクリプト
.DESCRIPTION
    このスクリプトは指定されたフォルダからAWS S3バケットへファイルを転送し、
    Glacier Deep Archiveストレージクラスに変更します。
    転送後、元のファイルは空にし、拡張子を.arcに変更します。
.PARAMETER SourceFolders
    転送元のフォルダパスの配列
.PARAMETER DestinationBucket
    転送先のS3バケット名
.PARAMETER LogPath
    ログファイルの保存先パス（デフォルト: カレントディレクトリ）
.PARAMETER CsvPath
    CSVファイルの保存先パス（デフォルト: カレントディレクトリ）
.EXAMPLE
    .\FSxToS3Glacier.ps1 -SourceFolders @("\\fsx\share\folder1", "\\fsx\share\folder2") -DestinationBucket "my-archive-bucket"
#>

param (
    [Parameter(Mandatory=$true)]
    [string[]]$SourceFolders,
    
    [Parameter(Mandatory=$true)]
    [string]$DestinationBucket,
    
    [Parameter(Mandatory=$false)]
    [string]$LogPath = ".\FSxToS3Glacier_$(Get-Date -Format 'yyyyMMdd_HHmmss').log",
    
    [Parameter(Mandatory=$false)]
    [string]$CsvPath = ".\FSxToS3Glacier_$(Get-Date -Format 'yyyyMMdd_HHmmss').csv"
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
    Add-Content -Path $LogPath -Value $logMessage
}

# CSVにエントリを追加する関数
function Add-CsvEntry {
    param (
        [string]$SourcePath,
        [string]$S3Path,
        [string]$Status,
        [string]$Message = ""
    )
    
    $csvObject = [PSCustomObject]@{
        Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        SourcePath = $SourcePath
        S3Path = $S3Path
        Status = $Status
        Message = $Message
    }
    
    $csvObject | Export-Csv -Path $CsvPath -Append -NoTypeInformation -Force
}

# メイン処理
function Start-Migration {
    # 初期化
    if (-not (Test-Path -Path $LogPath)) {
        $null = New-Item -Path $LogPath -ItemType File -Force
    }
    
    if (-not (Test-Path -Path $CsvPath)) {
        $header = "Timestamp,SourcePath,S3Path,Status,Message"
        Set-Content -Path $CsvPath -Value $header -Force
    }
    
    Write-Log "処理を開始します。"
    Write-Log "転送元フォルダ: $($SourceFolders -join ', ')"
    Write-Log "転送先S3バケット: $DestinationBucket"
    
    # AWS CLIの存在確認
    if (-not (Test-AwsCli)) {
        Write-Log "AWS CLIがインストールされていません。スクリプトを終了します。" -Type "ERROR"
        exit 1
    }
    
    # 各フォルダを処理
    foreach ($sourceFolder in $SourceFolders) {
        if (-not (Test-Path -Path $sourceFolder)) {
            Write-Log "フォルダが存在しません: $sourceFolder" -Type "ERROR"
            continue
        }
        
        Write-Log "フォルダの処理を開始: $sourceFolder"
        
        # フォルダ内のすべてのファイルを取得
        $files = Get-ChildItem -Path $sourceFolder -File -Recurse
        
        Write-Log "対象ファイル数: $($files.Count)"
        
        # 各ファイルを処理
        foreach ($file in $files) {
            $relativePath = $file.FullName.Replace($sourceFolder, "").TrimStart("\")
            $s3Key = $relativePath.Replace("\", "/")
            $s3Path = "s3://$DestinationBucket/$s3Key"
            
            Write-Log "ファイル処理: $($file.FullName) -> $s3Path"
            
            try {
                # S3にアップロード
                $uploadResult = aws s3 cp $file.FullName $s3Path --storage-class DEEP_ARCHIVE 2>&1
                
                if ($LASTEXITCODE -eq 0) {
                    Write-Log "アップロード成功: $($file.FullName)" -Type "SUCCESS"
                    
                    # 元ファイルを空にして拡張子を.arcに変更
                    $arcFilePath = [System.IO.Path]::ChangeExtension($file.FullName, ".arc")
                    
                    # 既存の.arcファイルがあれば削除
                    if (Test-Path -Path $arcFilePath) {
                        Remove-Item -Path $arcFilePath -Force
                    }
                    
                    # ファイルを空にして拡張子を変更
                    Set-Content -Path $file.FullName -Value "" -Force
                    Rename-Item -Path $file.FullName -NewName $arcFilePath -Force
                    
                    Write-Log "元ファイルを空にして拡張子を.arcに変更しました: $arcFilePath" -Type "INFO"
                    
                    # CSVに成功を記録
                    Add-CsvEntry -SourcePath $file.FullName -S3Path $s3Path -Status "SUCCESS"
                } else {
                    Write-Log "アップロード失敗: $($file.FullName)" -Type "ERROR"
                    Write-Log "エラー: $uploadResult" -Type "ERROR"
                    
                    # CSVに失敗を記録
                    Add-CsvEntry -SourcePath $file.FullName -S3Path $s3Path -Status "FAILED" -Message $uploadResult
                }
            } catch {
                Write-Log "例外発生: $($file.FullName)" -Type "ERROR"
                Write-Log "エラー: $($_.Exception.Message)" -Type "ERROR"
                
                # CSVに失敗を記録
                Add-CsvEntry -SourcePath $file.FullName -S3Path $s3Path -Status "FAILED" -Message $_.Exception.Message
            }
        }
        
        Write-Log "フォルダの処理が完了しました: $sourceFolder"
    }
    
    Write-Log "すべての処理が完了しました。"
    Write-Log "ログファイル: $LogPath"
    Write-Log "CSVファイル: $CsvPath"
}

# 実行
Start-Migration