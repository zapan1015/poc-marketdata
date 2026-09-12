<#
.SYNOPSIS
    compact_vhdx.ps1 — Windows PowerShell script for WSL2 ext4.vhdx disk compaction
.DESCRIPTION
    WSL2 virtual disk (ext4.vhdx) grows monotonically. This script terminates WSL
    and uses diskpart to compact the virtual disk file.
    Run in an elevated PowerShell prompt (Administrator).
#>

[CmdletBinding()]
param(
    [string]$DistroName = "Ubuntu"
)

Write-Host "=== [WSL2 VHDX Compactor] ===" -ForegroundColor Cyan

# 1. Check WSL state and shutdown
Write-Host "Shutting down WSL2 instances..." -ForegroundColor Yellow
wsl --shutdown
Start-Sleep -Seconds 3

# 2. Search for ext4.vhdx file location
$localAppData = [Environment]::GetFolderPath("LocalApplicationData")
$candidates = Get-ChildItem -Path "$localAppData\Packages" -Filter "ext4.vhdx" -Recurse -ErrorAction SilentlyContinue |
              Select-Object -ExpandProperty FullName

if (-not $candidates) {
    # Check Docker Desktop WSL data paths as well
    $candidates = Get-ChildItem -Path "$localAppData\Docker\wsl" -Filter "ext4.vhdx" -Recurse -ErrorAction SilentlyContinue |
                  Select-Object -ExpandProperty FullName
}

if (-not $candidates) {
    Write-Warning "Could not automatically locate ext4.vhdx. Please specify the path manually."
    exit 1
}

foreach ($vhdxPath in $candidates) {
    Write-Host "Target VHDX: $vhdxPath" -ForegroundColor Green
    $sizeBefore = (Get-Item $vhdxPath).Length / 1GB
    Write-Host ("Size before compaction: {0:N2} GB" -f $sizeBefore)

    # 3. Create temporary diskpart script
    $diskpartScript = [System.IO.Path]::GetTempFileName()
    @"
select vdisk file="$vhdxPath"
attach vdisk readonly
compact vdisk
detach vdisk
exit
"@ | Set-Content -Path $diskpartScript -Encoding ASCII

    Write-Host "Executing diskpart compaction..." -ForegroundColor Yellow
    Start-Process -FilePath "diskpart.exe" -ArgumentList "/s `"$diskpartScript`"" -Wait -NoNewWindow

    Remove-Item $diskpartScript -Force -ErrorAction SilentlyContinue

    $sizeAfter = (Get-Item $vhdxPath).Length / 1GB
    Write-Host ("Size after compaction: {0:N2} GB" -f $sizeAfter) -ForegroundColor Green
    Write-Host ("Freed: {0:N2} GB" -f ($sizeBefore - $sizeAfter)) -ForegroundColor Cyan
}

Write-Host "Compaction completed." -ForegroundColor Green
