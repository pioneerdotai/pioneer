<#
.SYNOPSIS
Install or update the Pioneer Gateway (server variant by default).
.PARAMETER Headless
Install the server Gateway without computer use. Overrides PIONEER_INSTALL_COMPUTER_USE.
.PARAMETER ComputerUse
Install the Gateway with its complete CUA driver package. Overrides PIONEER_INSTALL_COMPUTER_USE.
.DESCRIPTION
PIONEER_INSTALL_COMPUTER_USE=1/true/yes/on selects computer use when no variant
switch is supplied. Use Get-Help ./install.ps1 -Detailed for parameter help.
#>
[CmdletBinding()]
param(
    [ValidateSet("stable", "beta", "canary")]
    [string]$Channel = "stable",

    [string]$Version,

    [switch]$NoStart,

    [switch]$ForceStart,

    [switch]$ComputerUse,

    [switch]$Headless,

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

function Expand-GatewayPackage {
    param([string]$AssetPath, [string]$Destination)

    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $archive = [System.IO.Compression.ZipFile]::OpenRead($AssetPath)
    try {
        $seen = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
        $files = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
        $total = [long]0
        foreach ($entry in $archive.Entries) {
            $directory = $entry.FullName.EndsWith('/')
            $name = if ($directory) { $entry.FullName.Substring(0, $entry.FullName.Length - 1) } else { $entry.FullName }
            $parts = $name.Split('/')
            $mode = (($entry.ExternalAttributes -shr 16) -band 0xffff)
            $kind = $mode -band 0xf000
            $expectedKind = if ($directory) { 0x4000 } else { 0x8000 }
            if ([string]::IsNullOrEmpty($name) -or $name.Contains('\') -or $name.Contains(':') -or
                $name -match '\p{Cc}' -or $kind -notin @(0, 0x8000, 0x4000) -or
                ($kind -ne 0 -and $kind -ne $expectedKind) -or
                -not $seen.Add($name)) {
                Fail "unsafe or duplicate Gateway archive member: $($entry.FullName)"
            }
            foreach ($part in $parts) {
                if ([string]::IsNullOrEmpty($part) -or $part -eq '.' -or $part -eq '..' -or
                    $part.EndsWith('.') -or $part.EndsWith(' ') -or
                    $part -match '^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\.|$)') {
                    Fail "unsafe Gateway archive path: $($entry.FullName)"
                }
            }
            $total += $entry.Length
            if ($entry.Length -lt 0 -or $entry.Length -gt 512MB -or $total -gt 2GB -or
                $seen.Count -gt 10000 -or ($directory -and $entry.Length -ne 0)) {
                Fail "Gateway archive exceeds extraction budget"
            }
            if (-not $directory) { [void]$files.Add($name) }
        }
        foreach ($name in $seen) {
            $parent = $name
            while ($parent.Contains('/')) {
                $parent = $parent.Substring(0, $parent.LastIndexOf('/'))
                if ($files.Contains($parent)) { Fail "Gateway archive member conflicts with a parent file" }
            }
        }
        foreach ($entry in $archive.Entries) {
            if ($entry.FullName.EndsWith('/')) { continue }
            $outputPath = Join-Path $Destination $entry.FullName
            [void][System.IO.Directory]::CreateDirectory([System.IO.Path]::GetDirectoryName($outputPath))
            $source = $entry.Open()
            try {
                $output = [System.IO.File]::Open($outputPath, [System.IO.FileMode]::CreateNew)
                try {
                    $buffer = [byte[]]::new(1048576)
                    $written = [long]0
                    while (($count = $source.Read($buffer, 0, $buffer.Length)) -gt 0) {
                        $written += $count
                        if ($written -gt $entry.Length -or $written -gt 512MB) {
                            Fail "Gateway archive member exceeds declared size"
                        }
                        $output.Write($buffer, 0, $count)
                    }
                }
                finally { $output.Dispose() }
            }
            finally { $source.Dispose() }
            if ((Get-Item -LiteralPath $outputPath).Length -ne $entry.Length) {
                Fail "Gateway archive member size mismatch"
            }
        }
    }
    finally { $archive.Dispose() }
}

function Assert-GatewayArchitecture {
    param([string]$Binary, [string]$Architecture)
    $stream = [System.IO.File]::OpenRead($Binary)
    $reader = [System.IO.BinaryReader]::new($stream)
    try {
        if ($stream.Length -lt 64 -or $reader.ReadUInt16() -ne 0x5a4d) { Fail "invalid Gateway PE executable" }
        $stream.Position = 0x3c
        $offset = $reader.ReadUInt32()
        if ($offset -gt $stream.Length - 6) { Fail "invalid Gateway PE header" }
        $stream.Position = $offset
        $expected = if ($Architecture -eq 'aarch64') { 0xaa64 } else { 0x8664 }
        if ($reader.ReadUInt32() -ne 0x00004550 -or $reader.ReadUInt16() -ne $expected) {
            Fail "Gateway executable does not match this OS and architecture"
        }
    }
    finally { $reader.Dispose() }
}

function Run-PackageInstaller {
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
    if ($LASTEXITCODE -ne 0) { Fail "Gateway installer exited with code $LASTEXITCODE" }
}

$repo = if ($env:PIONEER_RELEASE_REPO) { $env:PIONEER_RELEASE_REPO } else { "pioneerdotai/pioneer" }
$releaseApiBase = if ($env:PIONEER_RELEASE_API_BASE) { $env:PIONEER_RELEASE_API_BASE } else { "https://api.github.com/repos/$repo/releases" }
$releaseDownloadBase = if ($env:PIONEER_RELEASE_DOWNLOAD_BASE) { $env:PIONEER_RELEASE_DOWNLOAD_BASE } else { "https://github.com/$repo/releases/download" }

$assetPath = ""
$checksumsPath = ""
$arch = Get-ArchSuffix
$computerUseEnabled = $false
if ($env:PIONEER_INSTALL_COMPUTER_USE) {
    switch -Regex ($env:PIONEER_INSTALL_COMPUTER_USE) {
        "^(1|true|yes|on)$" { $computerUseEnabled = $true; break }
        "^(0|false|no|off)$" { $computerUseEnabled = $false; break }
        default { Fail "invalid PIONEER_INSTALL_COMPUTER_USE value; expected 1/true/yes or 0/false/no" }
    }
}
if ($ComputerUse.IsPresent) {
    $computerUseEnabled = $true
}
if ($Headless.IsPresent) {
    $computerUseEnabled = $false
}
$assetSuffix = if ($computerUseEnabled) { "-computer-use-package" } else { "" }
$assetName = "pioneer-gateway-windows-$arch$assetSuffix.zip"

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
        $inputsDir = Join-Path $tempDir 'inputs'
        [void][System.IO.Directory]::CreateDirectory($inputsDir)
        $snapshotAsset = Join-Path $inputsDir $assetName
        $snapshotChecksums = Join-Path $inputsDir 'SHA256SUMS'
        Copy-Item -LiteralPath $assetPath -Destination $snapshotAsset
        Copy-Item -LiteralPath $checksumsPath -Destination $snapshotChecksums
        $assetPath = $snapshotAsset
        $checksumsPath = $snapshotChecksums
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
    Expand-GatewayPackage -AssetPath $assetPath -Destination $extractDir

    $installerBinary = Join-Path $extractDir "pioneer.exe"
    if (-not (Test-Path -Path $installerBinary -PathType Leaf)) {
        Fail "downloaded archive does not contain pioneer.exe"
    }

    Assert-GatewayArchitecture -Binary $installerBinary -Architecture $arch
    Run-PackageInstaller -InstallerBinary $installerBinary -AssetPath $assetPath -ChecksumsPath $checksumsPath
}
finally {
    Remove-Item -Path $tempDir -Recurse -Force -ErrorAction SilentlyContinue
}
