# Debug Version - Streamlit Service Script
# Enhanced logging for troubleshooting

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
$LogFile = Join-Path $LogDir "streamlit_debug_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
$PidFile = Join-Path $LogDir "streamlit.pid"
$ErrorLog = Join-Path $LogDir "streamlit_error_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"

# Enhanced logging function
function Write-ServiceLog {
    param([string]$Message, [string]$Level = "INFO")
    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff"
    $LogEntry = "[$Timestamp] [$Level] [PID:$PID] $Message"
    try {
        $LogEntry | Out-File -FilePath $LogFile -Append -Encoding UTF8
        Write-Host $LogEntry
    } catch {
        Write-Host "Logging failed: $_"
    }
}

# Stop existing process function
function Stop-StreamlitProcess {
    Write-ServiceLog "Checking for existing processes..."
    
    # Check PID file
    if (Test-Path $PidFile) {
        $OldPID = Get-Content $PidFile -ErrorAction SilentlyContinue
        Write-ServiceLog "Found PID file with PID: $OldPID"
        if ($OldPID) {
            try {
                $OldProcess = Get-Process -Id $OldPID -ErrorAction SilentlyContinue
                if ($OldProcess) {
                    Write-ServiceLog "Stopping old process (PID: $OldPID)"
                    Stop-Process -Id $OldPID -Force
                    Start-Sleep -Seconds 3
                } else {
                    Write-ServiceLog "Old process (PID: $OldPID) not found - already stopped"
                }
            } catch {
                Write-ServiceLog "Error stopping old process: $_" "ERROR"
            }
        }
        Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
        Write-ServiceLog "PID file removed"
    } else {
        Write-ServiceLog "No PID file found"
    }
    
    # Check for any Streamlit processes
    $StreamlitProcesses = Get-Process -Name "python" -ErrorAction SilentlyContinue | Where-Object { 
        try {
            (Get-WmiObject Win32_Process -Filter "ProcessId=$($_.Id)").CommandLine -like "*streamlit*"
        } catch {
            $false
        }
    }
    
    if ($StreamlitProcesses) {
        Write-ServiceLog "Found $($StreamlitProcesses.Count) existing Streamlit processes"
        foreach ($proc in $StreamlitProcesses) {
            Write-ServiceLog "Stopping Streamlit process (PID: $($proc.Id))"
            Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
        }
        Start-Sleep -Seconds 3
    } else {
        Write-ServiceLog "No existing Streamlit processes found"
    }
}

# Health check function
function Test-StreamlitHealth {
    try {
        $Response = Invoke-WebRequest -Uri "http://localhost:$Port/_stcore/health" -TimeoutSec 5 -UseBasicParsing
        return $Response.StatusCode -eq 200
    } catch {
        return $false
    }
}

# Port check function
function Test-PortAvailable {
    param([int]$PortNumber)
    try {
        $Listener = [System.Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()
        $PortInUse = $Listener | Where-Object { $_.Port -eq $PortNumber }
        return $PortInUse.Count -eq 0
    } catch {
        Write-ServiceLog "Port check failed: $_" "ERROR"
        return $false
    }
}

# Main process start
Write-ServiceLog "=== DEBUG Streamlit Service Starting ==="
Write-ServiceLog "Application Path: $AppPath"
Write-ServiceLog "Port: $Port"
Write-ServiceLog "Log Directory: $LogDir"
Write-ServiceLog "Process ID: $PID"

# System startup delay
Write-ServiceLog "Waiting for system startup completion..."
Start-Sleep -Seconds 30  # Reduced for debugging

# Check if application file exists
Write-ServiceLog "Checking application file..."
if (-not (Test-Path $AppPath)) {
    Write-ServiceLog "Application file not found: $AppPath" "ERROR"
    exit 1
}
Write-ServiceLog "Application file found: $AppPath"

# Change working directory
$WorkingDir = Split-Path $AppPath -Parent
Write-ServiceLog "Changing to working directory: $WorkingDir"
Set-Location $WorkingDir
Write-ServiceLog "Current location: $(Get-Location)"

# Check config file
$ConfigPath = Join-Path $WorkingDir "config\archive_config.json"
Write-ServiceLog "Checking config file: $ConfigPath"
if (-not (Test-Path $ConfigPath)) {
    Write-ServiceLog "Config file not found: $ConfigPath" "ERROR"
    
    # Try to copy from parent directory
    $ParentConfigPath = "C:\temp\archive\archive_system\config\archive_config.json"
    Write-ServiceLog "Trying parent config: $ParentConfigPath"
    if (Test-Path $ParentConfigPath) {
        Write-ServiceLog "Copying config from parent directory..."
        $ConfigDir = Join-Path $WorkingDir "config"
        if (-not (Test-Path $ConfigDir)) {
            New-Item -ItemType Directory -Path $ConfigDir -Force | Out-Null
            Write-ServiceLog "Created config directory: $ConfigDir"
        }
        Copy-Item $ParentConfigPath $ConfigPath -Force
        Write-ServiceLog "Config file copied successfully"
    } else {
        Write-ServiceLog "Parent config file also not found: $ParentConfigPath" "ERROR"
        exit 1
    }
} else {
    Write-ServiceLog "Config file found: $ConfigPath"
}

# Check port availability
Write-ServiceLog "Checking port $Port availability..."
if (-not (Test-PortAvailable -PortNumber $Port)) {
    Write-ServiceLog "Port $Port is already in use" "WARNING"
    netstat -an | findstr ":$Port" | ForEach-Object { Write-ServiceLog "Port usage: $_" }
}

# Verify Python and Streamlit
Write-ServiceLog "Verifying Python environment..."
try {
    $PythonVersion = & python --version 2>&1
    Write-ServiceLog "Python version: $PythonVersion"
    
    $StreamlitVersion = & python -m streamlit version 2>&1
    Write-ServiceLog "Streamlit version: $StreamlitVersion"
} catch {
    Write-ServiceLog "Python or Streamlit check failed: $_" "ERROR"
    exit 1
}

# Stop existing process
Write-ServiceLog "Stopping existing processes..."
Stop-StreamlitProcess

# Check port again after cleanup
Write-ServiceLog "Checking port after cleanup..."
Start-Sleep -Seconds 5
$PortStatus = Test-PortAvailable -PortNumber $Port
Write-ServiceLog "Port $Port available: $PortStatus"

# Single attempt (no loop for debugging)
try {
    Write-ServiceLog "Starting Streamlit process..."
    
    # Create process info
    $ProcessInfo = New-Object System.Diagnostics.ProcessStartInfo
    $ProcessInfo.FileName = "python"
    $ProcessInfo.Arguments = "-m streamlit run `"$AppPath`" --server.port $Port --server.address 0.0.0.0 --server.headless true --logger.level debug"
    $ProcessInfo.UseShellExecute = $false
    $ProcessInfo.RedirectStandardOutput = $true
    $ProcessInfo.RedirectStandardError = $true
    $ProcessInfo.WorkingDirectory = $WorkingDir
    
    Write-ServiceLog "Command: $($ProcessInfo.FileName) $($ProcessInfo.Arguments)"
    Write-ServiceLog "Working Directory: $($ProcessInfo.WorkingDirectory)"
    
    # Start process
    $Process = New-Object System.Diagnostics.Process
    $Process.StartInfo = $ProcessInfo
    $ProcessStarted = $Process.Start()
    
    if (-not $ProcessStarted) {
        throw "Failed to start process"
    }
    
    Write-ServiceLog "Process started successfully"
    Write-ServiceLog "Process ID: $($Process.Id)"
    Write-ServiceLog "Process Name: $($Process.ProcessName)"
    
    # Save process ID
    $Process.Id | Out-File -FilePath $PidFile -Encoding UTF8
    Write-ServiceLog "PID saved to file: $PidFile"
    
    # Monitor for initial errors
    Write-ServiceLog "Monitoring initial startup (30 seconds)..."
    for ($i = 1; $i -le 30; $i++) {
        Start-Sleep -Seconds 1
        
        # Check if process is still running
        if ($Process.HasExited) {
            Write-ServiceLog "Process exited early (after $i seconds)" "ERROR"
            Write-ServiceLog "Exit Code: $($Process.ExitCode)" "ERROR"
            
            # Get error output
            if ($Process.StandardError -and -not $Process.StandardError.EndOfStream) {
                $ErrorOutput = $Process.StandardError.ReadToEnd()
                Write-ServiceLog "Error Output: $ErrorOutput" "ERROR"
                $ErrorOutput | Out-File -FilePath $ErrorLog -Append -Encoding UTF8
            }
            
            # Get standard output
            if ($Process.StandardOutput -and -not $Process.StandardOutput.EndOfStream) {
                $StandardOutput = $Process.StandardOutput.ReadToEnd()
                Write-ServiceLog "Standard Output: $StandardOutput" "INFO"
            }
            
            exit 1
        }
        
        # Check health every 5 seconds
        if ($i % 5 -eq 0) {
            if (Test-StreamlitHealth) {
                Write-ServiceLog "Health check successful at $i seconds"
                Write-ServiceLog "=== Streamlit Service Started Successfully ==="
                
                # Wait indefinitely (for debugging)
                while (-not $Process.HasExited) {
                    Start-Sleep -Seconds 30
                    if (-not (Test-StreamlitHealth)) {
                        Write-ServiceLog "Health check failed during monitoring" "WARNING"
                        break
                    }
                }
                
                Write-ServiceLog "=== Process monitoring ended ==="
                exit 0
            } else {
                Write-ServiceLog "Health check failed at $i seconds"
            }
        }
    }
    
    Write-ServiceLog "Startup monitoring completed - health check never succeeded" "ERROR"
    
    # Final error collection
    if ($Process.StandardError -and -not $Process.StandardError.EndOfStream) {
        $ErrorOutput = $Process.StandardError.ReadToEnd()
        Write-ServiceLog "Final Error Output: $ErrorOutput" "ERROR"
        $ErrorOutput | Out-File -FilePath $ErrorLog -Append -Encoding UTF8
    }
    
    if ($Process.StandardOutput -and -not $Process.StandardOutput.EndOfStream) {
        $StandardOutput = $Process.StandardOutput.ReadToEnd()
        Write-ServiceLog "Final Standard Output: $StandardOutput" "INFO"
    }
    
    $Process.Kill()
    exit 1
    
} catch {
    Write-ServiceLog "Unexpected error occurred: $_" "ERROR"
    Write-ServiceLog "Error details: $($_.Exception.ToString())" "ERROR"
    exit 1
}