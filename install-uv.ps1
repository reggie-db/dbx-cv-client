<#
.SYNOPSIS
    Install UV (Python package manager) if not already installed.
.EXAMPLE
    .\install-uv.ps1
#>

$ErrorActionPreference = "Stop"

# Check if UV is already installed
if (Get-Command uv -ErrorAction SilentlyContinue) {
    Write-Host "uv is already installed"
    exit 0
}

Write-Host "Installing uv..."
Invoke-RestMethod https://astral.sh/uv/install.ps1 | Invoke-Expression

# Refresh PATH
$env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")

# Verify installation
if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    Write-Error "uv installation failed"
    exit 1
}

Write-Host "uv installed successfully"

