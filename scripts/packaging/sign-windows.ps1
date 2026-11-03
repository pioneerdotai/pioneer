[CmdletBinding()]
param([Parameter(Mandatory = $true)][string]$Path)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
. (Join-Path $PSScriptRoot "windows-signing.ps1")
$workDir = Join-Path $env:TEMP ("pioneer-sign-" + [Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $workDir | Out-Null
$context = $null
try {
    $required = ([string]$env:WINDOWS_SIGNING_REQUIRED).Trim().ToLowerInvariant() -in @("true", "1", "yes")
    $context = New-SigningContext -SigningRequired $required -WorkDir $workDir
    Sign-Artifact -Path $Path -SigningContext $context
}
finally {
    Remove-Item -Path $workDir -Recurse -Force -ErrorAction SilentlyContinue
}
