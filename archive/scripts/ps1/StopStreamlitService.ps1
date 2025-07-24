# Stop Streamlit Service Script
# Gracefully stops the Streamlit service

param(
    [string]$LogDir = "C:\temp\archive\archive_system\logs\service"
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

Write-ServiceLog "=== Streamlit Service Stop Process Started ==="

# Stop process using PID file
if (Test-Path $PidFile) {
    $PID = Get-Content $PidFile -ErrorAction SilentlyContinue
    if ($PID) {
        try {
            Stop-Process -Id $PID -Force
            Write-ServiceLog "Process (PID: $PID) stopped successfully"
        } catch {
            Write-ServiceLog "Error stopping process: $_" "ERROR"
        }
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
}

# Force stop any remaining Streamlit processes
Get-Process -Name "python" -ErrorAction SilentlyContinue | Where-Object {
    $_.CommandLine -like "*streamlit*"
} | ForEach-Object {
    Write-ServiceLog "Force stopping Streamlit process (PID: $($_.Id))"
    Stop-Process -Id $_.Id -Force
}

Write-ServiceLog "=== Streamlit Service Stop Process Completed ==="