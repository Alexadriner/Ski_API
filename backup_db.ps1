param(
    [Parameter(Mandatory = $false)]
    [string]$DbName = "ski_api",

    [Parameter(Mandatory = $false)]
    [string]$DbUser = "root",

    [Parameter(Mandatory = $false)]
    [string]$DbHost = "localhost",

    [Parameter(Mandatory = $false)]
    [int]$DbPort = 3306,

    [Parameter(Mandatory = $false)]
    [string]$OutputDir = "",

    [Parameter(Mandatory = $false)]
    [switch]$Compress,

    [Parameter(Mandatory = $false)]
    [string]$DbPassword = "",

    [Parameter(Mandatory = $false)]
    [string]$MySqlDumpPath = "mysqldump"
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($OutputDir)) {
    $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
    $OutputDir = Join-Path $repoRoot "backups"
}

if (-not (Test-Path $OutputDir)) {
    New-Item -Path $OutputDir -ItemType Directory | Out-Null
}

$timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$fileBase = "backup_${DbName}_$timestamp.sql"
$sqlPath = Join-Path $OutputDir $fileBase
$errPath = Join-Path $OutputDir "backup_${DbName}_$timestamp.err.log"

Write-Host "Creating backup..."
Write-Host "  DB:      $DbName"
Write-Host "  User:    $DbUser"
Write-Host "  Host:    $DbHost`:$DbPort"
Write-Host "  Output:  $sqlPath"

$mysqldumpCmd = Get-Command $MySqlDumpPath -ErrorAction SilentlyContinue
if (-not $mysqldumpCmd) {
    throw "mysqldump was not found. Set -MySqlDumpPath to mysqldump.exe (full path) or add it to PATH."
}

$dumpArgs = @(
    "-h", $DbHost,
    "-P", "$DbPort",
    "-u", $DbUser,
    "--single-transaction",
    "--routines",
    "--triggers",
    $DbName
)

if ([string]::IsNullOrWhiteSpace($DbPassword)) {
    # Prompt for password interactively.
    $dumpArgs = @("-p") + $dumpArgs
} else {
    # Non-interactive password mode.
    $dumpArgs = @("--password=$DbPassword") + $dumpArgs
}

& $mysqldumpCmd.Source @dumpArgs 1> $sqlPath 2> $errPath

if ($LASTEXITCODE -ne 0) {
    $details = ""
    if (Test-Path $errPath) {
        $details = (Get-Content $errPath -Raw).Trim()
    }
    if ([string]::IsNullOrWhiteSpace($details)) {
        throw "mysqldump failed with exit code $LASTEXITCODE"
    }
    throw "mysqldump failed with exit code $LASTEXITCODE. Details: $details"
}

if (Test-Path $errPath) {
    Remove-Item $errPath -Force
}

if ($Compress) {
    $zipPath = "$sqlPath.zip"
    if (Test-Path $zipPath) {
        Remove-Item $zipPath -Force
    }
    Compress-Archive -Path $sqlPath -DestinationPath $zipPath -CompressionLevel Optimal
    Remove-Item $sqlPath -Force
    Write-Host "Backup created: $zipPath"
} else {
    Write-Host "Backup created: $sqlPath"
}
