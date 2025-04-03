<#
.SYNOPSIS
    FSx for Windows File ServerからS3バケットへファイルを転送し、Glacier Deep Archiveに変更するスクリプト
.DESCRIPTION
    このスクリプトは指定されたフォルダ/ファイルからAWS S3バケットへファイルを転送し、
    Glacier Deep Archiveストレージクラスに変更します。
    転送元の指定は設定ファイルから読み込みます。
    転送後、元のファイルは空にし、拡張子を.arcに変更します。
.PARAMETER ConfigPath
    設定ファイルのパス
.PARAMETER DestinationBucket
    転送先のS3バケット名
.PARAMETER RootFolder
    転送元のルートフォルダ（S3パス生成時に除外する部分、例: "\\fsx\share"）
.PARAMETER LogPath
    ログファイルの保存先パス（デフォルト: カレントディレクトリ）
.PARAMETER CsvPath
    CSVファイルの保存先パス（デフォルト: カレントディレクトリ）
.EXAMPLE
    .\FSxToS3Glacier.ps1 -ConfigPath "targets.csv" -DestinationBucket "my-archive-bucket" -RootFolder "\\fsx\share"
#>

param (
    [Parameter(Mandatory=$true)]
    [string]$ConfigPath,
    
    [Parameter(Mandatory=$true)]
    [string]$DestinationBucket,
    
    [Parameter(Mandatory=$true)]
    [string]$RootFolder,
    
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
        [DateTime]$ProcessDate,
        [string]$SourcePath,
        [string]$SourceFileName,
        [string]$S3Path,
        [long]$FileSize
    )
    
    $csvObject = [PSCustomObject]@{
        ProcessDate = $ProcessDate.ToString("yyyy-MM-dd")
        SourcePath = $SourcePath
        SourceFileName = $SourceFileName
        S3Path = $S3Path
        FileSize = $FileSize
    }
    
    $csvObject | Export-Csv -Path $CsvPath -Append -NoTypeInformation -Force
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
        $targets = Import-Csv -Path $ConfigPath
        return $targets
    } catch {
        Write-Log "設定ファイルの読み込みに失敗しました: $($_.Exception.Message)" -Type "ERROR"
        exit 1
    }
}

# ファイルを処理する関数
function Process-File {
    param (
        [string]$FilePath,
        [string]$Type
    )
    
    if (-not (Test-Path -Path $FilePath -PathType Leaf)) {
        Write-Log "ファイルが存在しません: $FilePath" -Type "ERROR"
        return $false
    }
    
    $fileInfo = Get-Item -Path $FilePath
    $fileSize = $fileInfo.Length
    $fileName = $fileInfo.Name
    $sourcePath = $fileInfo.DirectoryName
    $processDate = Get-Date
    
    # S3パスを構築（RootFolderを除外する）
    $s3Key = $FilePath.Replace($RootFolder, "").TrimStart("\").Replace("\", "/")
    $s3Path = "s3://$DestinationBucket/$s3Key"
    
    Write-Log "ファイル処理: $FilePath -> $s3Path"
    
    try {
        # S3にアップロード
        $uploadResult = aws s3 cp $FilePath $s3Path --storage-class DEEP_ARCHIVE 2>&1
        
        if ($LASTEXITCODE -eq 0) {
            Write-Log "アップロード成功: $FilePath" -Type "SUCCESS"
            
            # 元ファイルを空にして拡張子を.arcに変更
            $arcFilePath = [System.IO.Path]::ChangeExtension($FilePath, ".arc")
            
            # 既存の.arcファイルがあれば削除
            if (Test-Path -Path $arcFilePath) {
                Remove-Item -Path $arcFilePath -Force
            }
            
            # ファイルを空にして拡張子を変更
            Set-Content -Path $FilePath -Value "" -Force
            Rename-Item -Path $FilePath -NewName $arcFilePath -Force
            
            Write-Log "元ファイルを空にして拡張子を.arcに変更しました: $arcFilePath" -Type "INFO"
            
            # CSVに成功を記録
            Add-CsvEntry -ProcessDate $processDate -SourcePath $sourcePath -SourceFileName $fileName -S3Path $s3Path -FileSize $fileSize
            return $true
        } else {
            Write-Log "アップロード失敗: $FilePath" -Type "ERROR"
            Write-Log "エラー: $uploadResult" -Type "ERROR"
            return $false
        }
    } catch {
        Write-Log "例外発生: $FilePath" -Type "ERROR"
        Write-Log "エラー: $($_.Exception.Message)" -Type "ERROR"
        return $false
    }
}

# フォルダを処理する関数
function Process-Folder {
    param (
        [string]$FolderPath,
        [string]$Type
    )
    
    if (-not (Test-Path -Path $FolderPath -PathType Container)) {
        Write-Log "フォルダが存在しません: $FolderPath" -Type "ERROR"
        return
    }
    
    Write-Log "フォルダの処理を開始: $FolderPath"
    
    # フォルダ内のすべてのファイルを取得
    $files = Get-ChildItem -Path $FolderPath -File -Recurse
    
    Write-Log "対象ファイル数: $($files.Count)"
    
    # 成功・失敗カウンター
    $successCount = 0
    $failureCount = 0
    
    # 各ファイルを処理
    foreach ($file in $files) {
        $processDate = Get-Date
        $fileSize = $file.Length
        $fileName = $file.Name
        $sourcePath = $file.DirectoryName
        
        # S3パスを構築（RootFolderを除外する）
        $s3Key = $file.FullName.Replace($RootFolder, "").TrimStart("\").Replace("\", "/")
        $s3Path = "s3://$DestinationBucket/$s3Key"
        
        Write-Log "ファイル処理: $($file.FullName) -> $s3Path"
        
        try {
            # S3にアップロード
            $uploadResult = aws s3 cp $file.FullName $s3Path --storage-class DEEP_ARCHIVE 2>&1
            
            if ($LASTEXITCODE -eq 0) {
                Write-Log "アップロード成功: $($file.FullName)" -Type "SUCCESS"
                $successCount++
                
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
                Add-CsvEntry -ProcessDate $processDate -SourcePath $sourcePath -SourceFileName $fileName -S3Path $s3Path -FileSize $fileSize
            } else {
                Write-Log "アップロード失敗: $($file.FullName)" -Type "ERROR"
                Write-Log "エラー: $uploadResult" -Type "ERROR"
                $failureCount++
            }
        } catch {
            Write-Log "例外発生: $($file.FullName)" -Type "ERROR"
            Write-Log "エラー: $($_.Exception.Message)" -Type "ERROR"
            $failureCount++
        }
    }
    
    Write-Log "フォルダの処理が完了しました: $FolderPath"
    Write-Log "結果: 成功=$successCount, 失敗=$failureCount"
}

# メイン処理
function Start-Migration {
    # 初期化
    if (-not (Test-Path -Path $LogPath)) {
        $null = New-Item -Path $LogPath -ItemType File -Force
    }
    
    if (-not (Test-Path -Path $CsvPath)) {
        $header = "ProcessDate,SourcePath,SourceFileName,S3Path,FileSize"
        Set-Content -Path $CsvPath -Value $header -Force
    }
    
    Write-Log "処理を開始します。"
    Write-Log "設定ファイル: $ConfigPath"
    Write-Log "転送先S3バケット: $DestinationBucket"
    Write-Log "ルートフォルダ: $RootFolder"
    
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
        
        # パスのタイプを判定
        if (Test-Path -Path $path -PathType Container) {
            # フォルダ
            Process-Folder -FolderPath $path -Type $type
        } elseif (Test-Path -Path $path -PathType Leaf) {
            # ファイル
            $result = Process-File -FilePath $path -Type $type
            if ($result) {
                $totalSuccessCount++
            } else {
                $totalFailureCount++
            }
        } else {
            Write-Log "パスが存在しません: $path" -Type "ERROR"
            continue
        }
    }
    
    Write-Log "すべての処理が完了しました。"
    Write-Log "ログファイル: $LogPath"
    Write-Log "CSVファイル: $CsvPath"
}

# 実行
Start-Migration
