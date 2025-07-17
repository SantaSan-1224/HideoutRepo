# Setup Streamlit Service Task (Fixed Encoding Version)
# This script sets up Streamlit as a Windows Scheduled Task

# Check if running as Administrator
if (-NOT ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Write-Error "This script must be run as Administrator"
    exit 1
}

$TaskName = "ArchiveHistoryStreamlitService"
$ScriptPath = "C:\temp\archive\archive_system\StreamlitService.ps1"
$StopScriptPath = "C:\temp\archive\archive_system\StopStreamlitService.ps1"
$User = "SYSTEM"
$Description = "Archive History Management System - Streamlit Service"

Write-Host "Setting up Streamlit service task..." -ForegroundColor Green

# Remove existing task if exists
Write-Host "Removing existing task (if exists)..." -ForegroundColor Yellow
try {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Existing task removed successfully" -ForegroundColor Green
} catch {
    Write-Host "No existing task found or removal failed" -ForegroundColor Yellow
}

# Create startup task
Write-Host "Creating startup task..." -ForegroundColor Yellow
$Action = New-ScheduledTaskAction -Execute "PowerShell.exe" -Argument "-ExecutionPolicy Bypass -WindowStyle Hidden -File `"$ScriptPath`""
$Trigger = New-ScheduledTaskTrigger -AtStartup
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
$Principal = New-ScheduledTaskPrincipal -UserID $User -LogonType ServiceAccount -RunLevel Highest

try {
    Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal -Description $Description
    Write-Host "Startup task created successfully: $TaskName" -ForegroundColor Green
} catch {
    Write-Error "Failed to create startup task: $_"
    exit 1
}

# Create stop task
Write-Host "Creating stop task..." -ForegroundColor Yellow
$StopTaskName = "ArchiveHistoryStreamlitServiceStop"
$StopAction = New-ScheduledTaskAction -Execute "PowerShell.exe" -Argument "-ExecutionPolicy Bypass -WindowStyle Hidden -File `"$StopScriptPath`""
$StopTrigger = New-ScheduledTaskTrigger -AtLogOn
$StopSettings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

try {
    Register-ScheduledTask -TaskName $StopTaskName -Action $StopAction -Trigger $StopTrigger -Settings $StopSettings -Principal $Principal -Description "$Description - Stop Process"
    Write-Host "Stop task created successfully: $StopTaskName" -ForegroundColor Green
} catch {
    Write-Warning "Failed to create stop task: $_"
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Task Setup Completed Successfully!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Tasks created:" -ForegroundColor White
Write-Host "  - Startup task: $TaskName" -ForegroundColor Green
Write-Host "  - Stop task: $StopTaskName" -ForegroundColor Green
Write-Host ""
Write-Host "Management commands:" -ForegroundColor White
Write-Host "  Start:  Start-ScheduledTask -TaskName '$TaskName'" -ForegroundColor Yellow
Write-Host "  Stop:   Stop-ScheduledTask -TaskName '$TaskName'" -ForegroundColor Yellow
Write-Host "  Status: Get-ScheduledTask -TaskName '$TaskName'" -ForegroundColor Yellow
Write-Host ""
Write-Host "Next steps:" -ForegroundColor White
Write-Host "1. Create StreamlitService.ps1 script" -ForegroundColor Cyan
Write-Host "2. Test the service:" -ForegroundColor Cyan
Write-Host "   Start-ScheduledTask -TaskName '$TaskName'" -ForegroundColor Yellow
Write-Host "3. Check status:" -ForegroundColor Cyan
Write-Host "   Get-ScheduledTask -TaskName '$TaskName'" -ForegroundColor Yellow