# 閉域環境でのWindows Server 2022サービス化

## 方法1: PowerShellスクリプト + タスクスケジューラ【推奨】

### 1.1 理由
- **インターネット不要**: Windows標準機能のみ使用
- **追加ソフト不要**: 外部ツールのダウンロード不要
- **シンプル**: 設定が簡単で管理しやすい
- **ログ管理**: 独自のログ管理機能を実装可能

### 1.2 サービス起動スクリプト

**StreamlitService.ps1**
```powershell
<#
.SYNOPSIS
    Streamlitアプリケーションサービス
.DESCRIPTION
    ファイルアーカイブ履歴管理システムをサービスとして起動・監視
#>

param(
    [string]$AppPath = "C:\archive_system\streamlit_app.py",
    [int]$Port = 8501,
    [string]$LogDir = "C:\archive_system\logs\service",
    [string]$ConfigPath = "C:\archive_system\config\archive_config.json",
    [int]$RestartDelay = 30
)

# ログディレクトリ作成
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force
}

# ログファイルパス
$LogFile = Join-Path $LogDir "streamlit_service_$(Get-Date -Format 'yyyyMMdd').log"
$PidFile = Join-Path $LogDir "streamlit.pid"
$ErrorLog = Join-Path $LogDir "streamlit_error_$(Get-Date -Format 'yyyyMMdd').log"

# ログ出力関数
function Write-ServiceLog {
    param([string]$Message, [string]$Level = "INFO")
    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $LogEntry = "[$Timestamp] [$Level] $Message"
    $LogEntry | Out-File -FilePath $LogFile -Append -Encoding UTF8
    Write-Host $LogEntry
}

# プロセス停止関数
function Stop-StreamlitProcess {
    if (Test-Path $PidFile) {
        $PID = Get-Content $PidFile -ErrorAction SilentlyContinue
        if ($PID) {
            try {
                Stop-Process -Id $PID -Force -ErrorAction SilentlyContinue
                Write-ServiceLog "既存のStreamlitプロセス($PID)を停止しました"
            } catch {
                Write-ServiceLog "プロセス停止エラー: $_" "ERROR"
            }
        }
        Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
    }
}

# プロセス健全性チェック関数
function Test-StreamlitHealth {
    try {
        $Response = Invoke-WebRequest -Uri "http://localhost:$Port/_stcore/health" -TimeoutSec 10 -UseBasicParsing
        return $Response.StatusCode -eq 200
    } catch {
        return $false
    }
}

# メイン処理開始
Write-ServiceLog "=== Streamlitサービス開始 ==="
Write-ServiceLog "アプリケーションパス: $AppPath"
Write-ServiceLog "ポート: $Port"
Write-ServiceLog "ログディレクトリ: $LogDir"

# 設定ファイル確認
if (-not (Test-Path $ConfigPath)) {
    Write-ServiceLog "設定ファイルが見つかりません: $ConfigPath" "ERROR"
    exit 1
}

# 作業ディレクトリ変更
$WorkingDir = Split-Path $AppPath -Parent
Set-Location $WorkingDir
Write-ServiceLog "作業ディレクトリ: $WorkingDir"

# 既存プロセス停止
Stop-StreamlitProcess

# メインループ
while ($true) {
    try {
        Write-ServiceLog "Streamlitプロセスを起動中..."
        
        # Streamlit起動
        $ProcessInfo = New-Object System.Diagnostics.ProcessStartInfo
        $ProcessInfo.FileName = "python"
        $ProcessInfo.Arguments = "-m streamlit run `"$AppPath`" --server.port $Port --server.address 0.0.0.0 --server.headless true"
        $ProcessInfo.UseShellExecute = $false
        $ProcessInfo.RedirectStandardOutput = $true
        $ProcessInfo.RedirectStandardError = $true
        $ProcessInfo.WorkingDirectory = $WorkingDir
        
        $Process = New-Object System.Diagnostics.Process
        $Process.StartInfo = $ProcessInfo
        $Process.Start() | Out-Null
        
        # プロセスID保存
        $Process.Id | Out-File -FilePath $PidFile -Encoding UTF8
        Write-ServiceLog "Streamlitプロセス開始 (PID: $($Process.Id))"
        
        # 起動待機
        Start-Sleep -Seconds 10
        
        # 健全性チェック
        $HealthCheckCount = 0
        while ($HealthCheckCount -lt 30) {
            if (Test-StreamlitHealth) {
                Write-ServiceLog "Streamlitサービスが正常に起動しました"
                break
            }
            Start-Sleep -Seconds 2
            $HealthCheckCount++
        }
        
        if ($HealthCheckCount -eq 30) {
            Write-ServiceLog "Streamlitサービスの起動に失敗しました" "ERROR"
            $Process.Kill()
            throw "起動タイムアウト"
        }
        
        # 監視ループ
        while (-not $Process.HasExited) {
            Start-Sleep -Seconds 30
            
            # 健全性チェック
            if (-not (Test-StreamlitHealth)) {
                Write-ServiceLog "健全性チェック失敗 - プロセスを再起動します" "WARNING"
                $Process.Kill()
                break
            }
        }
        
        # プロセス終了検知
        if ($Process.HasExited) {
            $ExitCode = $Process.ExitCode
            Write-ServiceLog "Streamlitプロセスが終了しました (ExitCode: $ExitCode)" "WARNING"
            
            # エラーログ収集
            if ($Process.StandardError -and -not $Process.StandardError.EndOfStream) {
                $ErrorOutput = $Process.StandardError.ReadToEnd()
                if ($ErrorOutput.Trim()) {
                    Write-ServiceLog "エラー出力: $ErrorOutput" "ERROR"
                    $ErrorOutput | Out-File -FilePath $ErrorLog -Append -Encoding UTF8
                }
            }
        }
        
    } catch {
        Write-ServiceLog "予期しないエラーが発生しました: $_" "ERROR"
    }
    
    # 再起動待機
    Write-ServiceLog "再起動までの待機中... ($RestartDelay秒)"
    Start-Sleep -Seconds $RestartDelay
}
```

### 1.3 サービス停止スクリプト

**StopStreamlitService.ps1**
```powershell
# Streamlitサービス停止スクリプト
param(
    [string]$LogDir = "C:\archive_system\logs\service"
)

$PidFile = Join-Path $LogDir "streamlit.pid"
$LogFile = Join-Path $LogDir "streamlit_service_$(Get-Date -Format 'yyyyMMdd').log"

function Write-ServiceLog {
    param([string]$Message, [string]$Level = "INFO")
    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $LogEntry = "[$Timestamp] [$Level] $Message"
    $LogEntry | Out-File -FilePath $LogFile -Append -Encoding UTF8
    Write-Host $LogEntry
}

Write-ServiceLog "=== Streamlitサービス停止処理開始 ==="

# PIDファイルから停止
if (Test-Path $PidFile) {
    $PID = Get-Content $PidFile -ErrorAction SilentlyContinue
    if ($PID) {
        try {
            Stop-Process -Id $PID -Force
            Write-ServiceLog "プロセス($PID)を正常に停止しました"
        } catch {
            Write-ServiceLog "プロセス停止エラー: $_" "ERROR"
        }
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
}

# Streamlitプロセスを強制終了
Get-Process -Name "python" -ErrorAction SilentlyContinue | Where-Object {
    $_.CommandLine -like "*streamlit*"
} | ForEach-Object {
    Write-ServiceLog "Streamlitプロセス($($_.Id))を強制終了します"
    Stop-Process -Id $_.Id -Force
}

Write-ServiceLog "=== Streamlitサービス停止処理完了 ==="
```

### 1.4 タスクスケジューラ設定スクリプト

**SetupStreamlitTask.ps1**
```powershell
# 管理者権限で実行
if (-NOT ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Write-Error "このスクリプトは管理者権限で実行してください"
    exit 1
}

$TaskName = "ArchiveHistoryStreamlitService"
$ScriptPath = "C:\archive_system\StreamlitService.ps1"
$StopScriptPath = "C:\archive_system\StopStreamlitService.ps1"
$User = "SYSTEM"  # またはサービス専用ユーザ
$Description = "ファイルアーカイブ履歴管理システム - Streamlitサービス"

# 既存タスクの削除
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue

# 起動タスク作成
$Action = New-ScheduledTaskAction -Execute "PowerShell.exe" -Argument "-ExecutionPolicy Bypass -WindowStyle Hidden -File `"$ScriptPath`""
$Trigger = New-ScheduledTaskTrigger -AtStartup
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
$Principal = New-ScheduledTaskPrincipal -UserID $User -LogonType ServiceAccount -RunLevel Highest

Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal -Description $Description

# 停止タスク作成
$StopTaskName = "ArchiveHistoryStreamlitServiceStop"
$StopAction = New-ScheduledTaskAction -Execute "PowerShell.exe" -Argument "-ExecutionPolicy Bypass -WindowStyle Hidden -File `"$StopScriptPath`""
$StopTrigger = New-ScheduledTaskTrigger -AtLogOn  # またはシャットダウン時
$StopSettings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

Register-ScheduledTask -TaskName $StopTaskName -Action $StopAction -Trigger $StopTrigger -Settings $StopSettings -Principal $Principal -Description "$Description - 停止処理"

Write-Host "タスクが正常に作成されました:"
Write-Host "  - 起動タスク: $TaskName"
Write-Host "  - 停止タスク: $StopTaskName"
Write-Host ""
Write-Host "管理コマンド:"
Write-Host "  開始: Start-ScheduledTask -TaskName '$TaskName'"
Write-Host "  停止: Stop-ScheduledTask -TaskName '$TaskName'"
Write-Host "  状態: Get-ScheduledTask -TaskName '$TaskName'"
```

## 方法2: Windowsサービス化（sc.exe使用）

### 2.1 サービス用バッチファイル

**StreamlitServiceWrapper.bat**
```batch
@echo off
setlocal

REM 設定
set APP_PATH=C:\archive_system\streamlit_app.py
set LOG_DIR=C:\archive_system\logs\service
set PYTHON_PATH=python

REM ログディレクトリ作成
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM 作業ディレクトリ変更
cd /d "C:\archive_system"

REM Streamlit起動
%PYTHON_PATH% -m streamlit run "%APP_PATH%" --server.port 8501 --server.address 0.0.0.0 --server.headless true > "%LOG_DIR%\streamlit_output.log" 2>&1
```

### 2.2 サービス登録

```cmd
REM 管理者権限のコマンドプロンプトで実行
sc create "ArchiveHistoryApp" binPath= "C:\archive_system\StreamlitServiceWrapper.bat" start= auto DisplayName= "Archive History Management App"

REM サービス開始
sc start "ArchiveHistoryApp"

REM サービス状態確認
sc query "ArchiveHistoryApp"
```

## 方法3: Python-Windows-Service（pywin32使用）

### 3.1 サービス用Pythonスクリプト

**StreamlitWindowsService.py**
```python
import win32serviceutil
import win32service
import win32event
import servicemanager
import subprocess
import time
import sys
import os
import logging
from pathlib import Path

class StreamlitService(win32serviceutil.ServiceFramework):
    _svc_name_ = "ArchiveHistoryApp"
    _svc_display_name_ = "Archive History Management App"
    _svc_description_ = "ファイルアーカイブ履歴管理システム"
    
    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.process = None
        self.is_running = True
        
        # ログ設定
        log_dir = Path("C:/archive_system/logs/service")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            filename=log_dir / "streamlit_service.log",
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def SvcStop(self):
        """サービス停止"""
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.logger.info("サービス停止要求を受信")
        
        if self.process:
            try:
                self.process.terminate()
                self.process.wait(timeout=10)
                self.logger.info("Streamlitプロセスを正常に停止")
            except:
                self.process.kill()
                self.logger.warning("Streamlitプロセスを強制終了")
        
        self.is_running = False
        win32event.SetEvent(self.hWaitStop)
    
    def SvcDoRun(self):
        """サービス実行"""
        self.logger.info("サービス開始")
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        
        # 作業ディレクトリ変更
        os.chdir("C:/archive_system")
        
        while self.is_running:
            try:
                # Streamlit起動
                self.logger.info("Streamlitプロセス起動中...")
                self.process = subprocess.Popen([
                    sys.executable, "-m", "streamlit", "run", 
                    "streamlit_app.py",
                    "--server.port", "8501",
                    "--server.address", "0.0.0.0",
                    "--server.headless", "true"
                ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                # プロセス監視
                while self.is_running:
                    if self.process.poll() is not None:
                        self.logger.warning("Streamlitプロセスが終了しました")
                        break
                    
                    # 停止イベント確認
                    if win32event.WaitForSingleObject(self.hWaitStop, 1000) == win32event.WAIT_OBJECT_0:
                        break
                
                # 自動再起動（サービス停止でない場合）
                if self.is_running and self.process.poll() is not None:
                    self.logger.info("30秒後に再起動します...")
                    time.sleep(30)
                    
            except Exception as e:
                self.logger.error(f"エラーが発生しました: {e}")
                time.sleep(30)

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(StreamlitService)
```

### 3.2 サービス登録・管理

```cmd
REM サービス登録
python StreamlitWindowsService.py install

REM サービス開始
python StreamlitWindowsService.py start

REM サービス停止
python StreamlitWindowsService.py stop

REM サービス削除
python StreamlitWindowsService.py remove
```

## 推奨構成とセキュリティ

### サービス専用ユーザー作成

```powershell
# サービス専用ユーザー作成
$UserName = "StreamlitService"
$Password = ConvertTo-SecureString "複雑なパスワード" -AsPlainText -Force

New-LocalUser -Name $UserName -Password $Password -Description "Streamlitサービス専用アカウント" -PasswordNeverExpires

# 必要な権限付与
Add-LocalGroupMember -Group "Log on as a service" -Member $UserName
```

### ファイアウォール設定

```powershell
# Windows Firewall設定
New-NetFirewallRule -DisplayName "Streamlit Archive App" -Direction Inbound -LocalPort 8501 -Protocol TCP -Action Allow
```

### 監視設定

```powershell
# イベントログ監視
$EventLogName = "Application"
$Source = "ArchiveHistoryApp"

# イベントソース作成
if (-not [System.Diagnostics.EventLog]::SourceExists($Source)) {
    [System.Diagnostics.EventLog]::CreateEventSource($Source, $EventLogName)
}

# 監視スクリプト例
while ($true) {
    try {
        $Response = Invoke-WebRequest -Uri "http://localhost:8501/_stcore/health" -TimeoutSec 5
        if ($Response.StatusCode -ne 200) {
            throw "HTTP Error: $($Response.StatusCode)"
        }
    } catch {
        Write-EventLog -LogName $EventLogName -Source $Source -EventId 1001 -EntryType Error -Message "Service health check failed: $_"
    }
    Start-Sleep -Seconds 60
}
```

## 閉域環境での推奨方法

**方法1のPowerShell + タスクスケジューラ**が最も適しています：

✅ **メリット**
- Windows標準機能のみ使用
- 外部ツール不要
- 豊富なログ機能
- 自動再起動機能
- 健全性チェック機能

❌ **デメリット**
- 少し複雑な設定
- PowerShellの知識が必要

この方法で進めますか？具体的な設定手順をサポートします！