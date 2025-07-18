# Streamlit Service Script (Fixed Version)
# Runs Streamlit app as a Windows service with enhanced error handling

param(
    [string]$AppPath = "C:\temp\archive\archive_system\web\streamlit_app.py",
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

# System startup delay (avoid early startup issues)
Write-ServiceLog "Waiting for system startup completion..."
Start-Sleep -Seconds 60

# Check if application file exists
if (-not (Test-Path $AppPath)) {
    Write-ServiceLog "Application file not found: $AppPath" "ERROR"
    exit 1
}

# Change working directory
$WorkingDir = Split-Path $AppPath -Parent
Set-Location $WorkingDir
Write-ServiceLog "Working Directory: $WorkingDir"

# Check config file
$ConfigPath = Join-Path $WorkingDir "config\archive_config.json"
if (-not (Test-Path $ConfigPath)) {
    Write-ServiceLog "Config file not found: $ConfigPath" "ERROR"
    
    # Try to copy from parent directory
    $ParentConfigPath = "C:\temp\archive\archive_system\config\archive_config.json"
    if (Test-Path $ParentConfigPath) {
        Write-ServiceLog "Copying config from parent directory..."
        $ConfigDir = Join-Path $WorkingDir "config"
        if (-not (Test-Path $ConfigDir)) {
            New-Item -ItemType Directory -Path $ConfigDir -Force | Out-Null
        }
        Copy-Item $ParentConfigPath $ConfigPath -Force
        Write-ServiceLog "Config file copied successfully"
    } else {
        Write-ServiceLog "Parent config file also not found: $ParentConfigPath" "ERROR"
        exit 1
    }
}

# Verify Python and Streamlit
try {
    $PythonVersion = & python --version 2>&1
    Write-ServiceLog "Python version: $PythonVersion"
    
    $StreamlitVersion = & python -m streamlit version 2>&1
    Write-ServiceLog "Streamlit check: $StreamlitVersion"
} catch {
    Write-ServiceLog "Python or Streamlit check failed: $_" "ERROR"
    exit 1
}

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
        $ProcessStarted = $Process.Start()
        
        if (-not $ProcessStarted) {
            throw "Failed to start process"
        }
        
        # Save process ID
        $Process.Id | Out-File -FilePath $PidFile -Encoding UTF8
        Write-ServiceLog "Streamlit process started (PID: $($Process.Id))"
        
        # Wait for startup
        Write-ServiceLog "Waiting for Streamlit startup..."
        Start-Sleep -Seconds 15
        
        # Health check with extended timeout
        $HealthCheckCount = 0
        $HealthCheckSuccess = $false
        while ($HealthCheckCount -lt 40) {
            if (Test-StreamlitHealth) {
                Write-ServiceLog "Streamlit service started successfully"
                $HealthCheckSuccess = $true
                break
            }
            Start-Sleep -Seconds 3
            $HealthCheckCount++
            Write-ServiceLog "Health check attempt $($HealthCheckCount)/40..."
        }
        
        if (-not $HealthCheckSuccess) {
            Write-ServiceLog "Streamlit service startup failed (timeout after 2 minutes)" "ERROR"
            
            # Collect error output
            if ($Process.StandardError -and -not $Process.StandardError.EndOfStream) {
                $ErrorOutput = $Process.StandardError.ReadToEnd()
                if ($ErrorOutput.Trim()) {
                    Write-ServiceLog "Startup error output: $ErrorOutput" "ERROR"
                    $ErrorOutput | Out-File -FilePath $ErrorLog -Append -Encoding UTF8
                }
            }
            
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
                    Write-ServiceLog "Exit error output: $ErrorOutput" "ERROR"
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