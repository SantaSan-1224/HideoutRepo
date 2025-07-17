# Streamlit Service Script
# Runs Streamlit app as a Windows service with monitoring and auto-restart

param(
    [string]$AppPath = "C:\temp\archive\archive_system\streamlit_app.py",
    [int]$Port = 8501,
    [string]$LogDir = "C:\temp\archive\archive_system\logs\service",
    [int]$RestartDelay = 30
)

# Create log directory
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

# Log file paths
$LogFile = Join-Path $LogDir "streamlit_service_$(Get-Date -Format 'yyyyMMdd').log"
$PidFile = Join-Path $LogDir "streamlit.pid"
$ErrorLog = Join-Path $LogDir "streamlit_error_$(Get-Date -Format 'yyyyMMdd').log"

# Logging function
function Write-ServiceLog {
    param([string]$Message, [string]$Level = "INFO")
    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $LogEntry = "[$Timestamp] [$Level] $Message"
    $LogEntry | Out-File -FilePath $LogFile -Append -Encoding UTF8
    Write-Host $LogEntry
}

# Stop existing process function
function Stop-StreamlitProcess {
    if (Test-Path $PidFile) {
        $PID = Get-Content $PidFile -ErrorAction SilentlyContinue
        if ($PID) {
            try {
                Stop-Process -Id $PID -Force -ErrorAction SilentlyContinue
                Write-ServiceLog "Stopped existing Streamlit process (PID: $PID)"
            } catch {
                Write-ServiceLog "Error stopping process: $_" "ERROR"
            }
        }
        Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
    }
}

# Health check function
function Test-StreamlitHealth {
    try {
        $Response = Invoke-WebRequest -Uri "http://localhost:$Port/_stcore/health" -TimeoutSec 10 -UseBasicParsing
        return $Response.StatusCode -eq 200
    } catch {
        return $false
    }
}

# Main process start
Write-ServiceLog "=== Streamlit Service Starting ==="
Write-ServiceLog "Application Path: $AppPath"
Write-ServiceLog "Port: $Port"
Write-ServiceLog "Log Directory: $LogDir"

# Check if application file exists
if (-not (Test-Path $AppPath)) {
    Write-ServiceLog "Application file not found: $AppPath" "ERROR"
    exit 1
}

# Change working directory
$WorkingDir = Split-Path $AppPath -Parent
Set-Location $WorkingDir
Write-ServiceLog "Working Directory: $WorkingDir"

# Stop existing process
Stop-StreamlitProcess

# Main service loop
while ($true) {
    try {
        Write-ServiceLog "Starting Streamlit process..."
        
        # Create process info
        $ProcessInfo = New-Object System.Diagnostics.ProcessStartInfo
        $ProcessInfo.FileName = "python"
        $ProcessInfo.Arguments = "-m streamlit run `"$AppPath`" --server.port $Port --server.address 0.0.0.0 --server.headless true"
        $ProcessInfo.UseShellExecute = $false
        $ProcessInfo.RedirectStandardOutput = $true
        $ProcessInfo.RedirectStandardError = $true
        $ProcessInfo.WorkingDirectory = $WorkingDir
        
        # Start process
        $Process = New-Object System.Diagnostics.Process
        $Process.StartInfo = $ProcessInfo
        $Process.Start() | Out-Null
        
        # Save process ID
        $Process.Id | Out-File -FilePath $PidFile -Encoding UTF8
        Write-ServiceLog "Streamlit process started (PID: $($Process.Id))"
        
        # Wait for startup
        Start-Sleep -Seconds 10
        
        # Health check
        $HealthCheckCount = 0
        while ($HealthCheckCount -lt 30) {
            if (Test-StreamlitHealth) {
                Write-ServiceLog "Streamlit service started successfully"
                break
            }
            Start-Sleep -Seconds 2
            $HealthCheckCount++
        }
        
        if ($HealthCheckCount -eq 30) {
            Write-ServiceLog "Streamlit service startup failed (timeout)" "ERROR"
            $Process.Kill()
            throw "Startup timeout"
        }
        
        # Monitoring loop
        while (-not $Process.HasExited) {
            Start-Sleep -Seconds 30
            
            # Health check
            if (-not (Test-StreamlitHealth)) {
                Write-ServiceLog "Health check failed - restarting process" "WARNING"
                $Process.Kill()
                break
            }
        }
        
        # Process exit handling
        if ($Process.HasExited) {
            $ExitCode = $Process.ExitCode
            Write-ServiceLog "Streamlit process exited (ExitCode: $ExitCode)" "WARNING"
            
            # Collect error output
            if ($Process.StandardError -and -not $Process.StandardError.EndOfStream) {
                $ErrorOutput = $Process.StandardError.ReadToEnd()
                if ($ErrorOutput.Trim()) {
                    Write-ServiceLog "Error output: $ErrorOutput" "ERROR"
                    $ErrorOutput | Out-File -FilePath $ErrorLog -Append -Encoding UTF8
                }
            }
        }
        
    } catch {
        Write-ServiceLog "Unexpected error occurred: $_" "ERROR"
    }
    
    # Wait before restart
    Write-ServiceLog "Waiting before restart... ($RestartDelay seconds)"
    Start-Sleep -Seconds $RestartDelay
}