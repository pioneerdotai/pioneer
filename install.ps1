[CmdletBinding()]
param(
    [ValidateSet("stable", "beta", "canary")]
    [string]$Channel = "stable",

    [string]$Version,

    [switch]$NoStart,

    [switch]$ForceStart,

    [string]$LocalAssetFile,

    [string]$LocalChecksumsFile
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

function Write-Log {
    param([string]$Message)
    Write-Host "[pioneer-install] $Message"
}

function Fail {
    param([string]$Message)
    throw "[pioneer-install] $Message"
}

function Normalize-Tag {
    param([string]$Raw)

    if ([string]::IsNullOrWhiteSpace($Raw)) {
        Fail "version is empty"
    }

    if ($Raw.StartsWith("v")) {
        return $Raw
    }

    return "v$Raw"
}

function Resolve-ReleaseTag {
    param(
        [string]$ReleaseApiBase,
        [string]$ChannelName,
        [string]$PinnedVersion
    )

    if (-not [string]::IsNullOrWhiteSpace($PinnedVersion)) {
        return Normalize-Tag -Raw $PinnedVersion
    }

    if ($ChannelName -eq "stable") {
        $latest = Invoke-RestMethod -Uri "$ReleaseApiBase/latest"
        if (-not $latest.tag_name) {
            Fail "latest release does not include tag_name"
        }
        return [string]$latest.tag_name
    }

    $releases = Invoke-RestMethod -Uri "$ReleaseApiBase?per_page=100"
    $match = $releases | Where-Object {
        $_.tag_name -and $_.tag_name.ToString().Contains("-$ChannelName")
    } | Select-Object -First 1

    if (-not $match) {
        Fail "failed to find release for channel '$ChannelName'"
    }

    return [string]$match.tag_name
}

function Get-ArchSuffix {
    $arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString().ToLowerInvariant()
    switch ($arch) {
        "x64" { return "x86_64" }
        "arm64" { return "aarch64" }
        default { Fail "unsupported architecture: $arch" }
    }
}

function Parse-Checksum {
    param(
        [string]$ChecksumsPath,
        [string]$AssetName
    )

    foreach ($line in Get-Content -Path $ChecksumsPath) {
        if ($line -match "^sha256:([0-9a-fA-F]+)\s+$([Regex]::Escape($AssetName))$") {
            return $Matches[1].ToLowerInvariant()
        }
    }

    Fail "checksum for $AssetName not found in $ChecksumsPath"
}

function Run-BootstrapInstaller {
    param(
        [string]$InstallerBinary,
        [string]$AssetPath,
        [string]$ChecksumsPath
    )

    $args = @(
        "install",
        "--source", "local",
        "--asset", $AssetPath,
        "--checksums", $ChecksumsPath,
        "--managed-by", "script"
    )
    if ($NoStart.IsPresent) {
        $args += "--no-start"
    }
    if ($ForceStart.IsPresent) {
        $args += "--force-start"
    }

    & $InstallerBinary @args
}

$repo = if ($env:PIONEER_RELEASE_REPO) { $env:PIONEER_RELEASE_REPO } else { "pioneerdotai/pioneer" }
$releaseApiBase = if ($env:PIONEER_RELEASE_API_BASE) { $env:PIONEER_RELEASE_API_BASE } else { "https://api.github.com/repos/$repo/releases" }
$releaseDownloadBase = if ($env:PIONEER_RELEASE_DOWNLOAD_BASE) { $env:PIONEER_RELEASE_DOWNLOAD_BASE } else { "https://github.com/$repo/releases/download" }

$assetPath = ""
$checksumsPath = ""
$arch = Get-ArchSuffix
$assetName = "pioneer-gateway-windows-$arch.zip"

$tempDir = Join-Path $env:TEMP ("pioneer-install-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tempDir | Out-Null

try {
    $effectiveLocalAssetFile = if (-not [string]::IsNullOrWhiteSpace($LocalAssetFile)) {
        $LocalAssetFile
    }
    else {
        $env:PIONEER_LOCAL_ASSET_FILE
    }
    $effectiveLocalChecksumsFile = if (-not [string]::IsNullOrWhiteSpace($LocalChecksumsFile)) {
        $LocalChecksumsFile
    }
    else {
        $env:PIONEER_LOCAL_CHECKSUMS_FILE
    }

    $usingLocalAssets = (-not [string]::IsNullOrWhiteSpace($effectiveLocalAssetFile)) -or
        (-not [string]::IsNullOrWhiteSpace($effectiveLocalChecksumsFile))

    if ($usingLocalAssets) {
        if ([string]::IsNullOrWhiteSpace($effectiveLocalAssetFile)) {
            Fail "PIONEER_LOCAL_ASSET_FILE is required when using local assets"
        }
        if ([string]::IsNullOrWhiteSpace($effectiveLocalChecksumsFile)) {
            Fail "PIONEER_LOCAL_CHECKSUMS_FILE is required when using local assets"
        }
        if (-not (Test-Path -Path $effectiveLocalAssetFile -PathType Leaf)) {
            Fail "local asset file does not exist: $effectiveLocalAssetFile"
        }
        if (-not (Test-Path -Path $effectiveLocalChecksumsFile -PathType Leaf)) {
            Fail "local checksums file does not exist: $effectiveLocalChecksumsFile"
        }

        $assetPath = $effectiveLocalAssetFile
        $checksumsPath = $effectiveLocalChecksumsFile
        $assetName = [System.IO.Path]::GetFileName($assetPath)
        Write-Log "using local bundled asset $assetName"
    }
    else {
        $tag = Resolve-ReleaseTag -ReleaseApiBase $releaseApiBase -ChannelName $Channel -PinnedVersion $Version
        $assetPath = Join-Path $tempDir $assetName
        $checksumsPath = Join-Path $tempDir "SHA256SUMS"

        $assetUrl = "$releaseDownloadBase/$tag/$assetName"
        $checksumsUrl = "$releaseDownloadBase/$tag/SHA256SUMS"

        Write-Log "downloading $assetName from $tag"
        Invoke-WebRequest -UseBasicParsing -Uri $assetUrl -OutFile $assetPath
        Invoke-WebRequest -UseBasicParsing -Uri $checksumsUrl -OutFile $checksumsPath
    }

    $expected = Parse-Checksum -ChecksumsPath $checksumsPath -AssetName $assetName
    $actual = (Get-FileHash -Path $assetPath -Algorithm SHA256).Hash.ToLowerInvariant()

    if ($expected -ne $actual) {
        Fail "checksum mismatch for $assetName"
    }

    $extractDir = Join-Path $tempDir "extract"
    New-Item -ItemType Directory -Path $extractDir | Out-Null
    Expand-Archive -Path $assetPath -DestinationPath $extractDir -Force

    $installerBinary = Get-ChildItem -Path $extractDir -Filter "pioneer.exe" -Recurse | Select-Object -First 1
    if (-not $installerBinary) {
        Fail "downloaded archive does not contain pioneer.exe"
    }

    Run-BootstrapInstaller -InstallerBinary $installerBinary.FullName -AssetPath $assetPath -ChecksumsPath $checksumsPath
}
finally {
    Remove-Item -Path $tempDir -Recurse -Force -ErrorAction SilentlyContinue
}
