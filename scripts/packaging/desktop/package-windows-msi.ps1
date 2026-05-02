[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("x86_64-pc-windows-msvc", "aarch64-pc-windows-msvc")]
    [string]$Target,

    [string]$OutDir,

    [string]$WixPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Test-True {
    param([string]$Value)

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $false
    }

    switch ($Value.Trim().ToLowerInvariant()) {
        "1" { return $true }
        "true" { return $true }
        "yes" { return $true }
        default { return $false }
    }
}

function Get-WorkspaceVersion {
    param([string]$RepoRoot)

    $cargoTomlPath = Join-Path $RepoRoot "Cargo.toml"
    $inWorkspaceSection = $false

    foreach ($line in Get-Content -Path $cargoTomlPath) {
        $trimmed = $line.Trim()
        if ($trimmed -eq "[workspace.package]") {
            $inWorkspaceSection = $true
            continue
        }

        if ($inWorkspaceSection -and $trimmed.StartsWith("[")) {
            break
        }

        if ($inWorkspaceSection -and $trimmed -match '^version\s*=\s*"([^"]+)"$') {
            return $Matches[1]
        }
    }

    throw "failed to read workspace.package version from $cargoTomlPath"
}

function Convert-ToProductVersion {
    param([string]$Raw)

    if ([string]::IsNullOrWhiteSpace($Raw)) {
        return $null
    }

    $value = $Raw.Trim()
    if ($value.StartsWith("v") -or $value.StartsWith("V")) {
        $value = $value.Substring(1)
    }

    if ($value -match '^(\d+)\.(\d+)\.(\d+)(?:[-+][0-9A-Za-z\.-]+)?$') {
        return "$($Matches[1]).$($Matches[2]).$($Matches[3])"
    }

    return $null
}

function Resolve-ProductVersion {
    param([string]$RepoRoot)

    $candidate = if ($env:PIONEER_DESKTOP_VERSION) {
        $env:PIONEER_DESKTOP_VERSION
    }
    elseif ($env:GITHUB_REF_NAME) {
        $env:GITHUB_REF_NAME
    }
    else {
        ""
    }

    $parsed = Convert-ToProductVersion -Raw $candidate
    if ($parsed) {
        return $parsed
    }

    $workspaceVersion = Get-WorkspaceVersion -RepoRoot $RepoRoot
    $parsedWorkspaceVersion = Convert-ToProductVersion -Raw $workspaceVersion
    if ($parsedWorkspaceVersion) {
        return $parsedWorkspaceVersion
    }

    throw "failed to resolve release version from PIONEER_DESKTOP_VERSION/GITHUB_REF_NAME/Cargo.toml"
}

function Resolve-SignToolPath {
    $cmd = Get-Command signtool.exe -ErrorAction SilentlyContinue
    if ($null -ne $cmd) {
        return $cmd.Source
    }

    $sdkBinRoot = "C:\Program Files (x86)\Windows Kits\10\bin"
    if (Test-Path -Path $sdkBinRoot) {
        $candidate = Get-ChildItem -Path $sdkBinRoot -Directory -ErrorAction SilentlyContinue |
            Sort-Object -Property Name -Descending |
            ForEach-Object { Join-Path $_.FullName "x64\signtool.exe" } |
            Where-Object { Test-Path -Path $_ } |
            Select-Object -First 1

        if ($candidate) {
            return $candidate
        }
    }

    throw "signtool.exe is not available"
}

function New-SigningContext {
    param(
        [bool]$SigningRequired,
        [string]$WorkDir
    )

    $context = @{
        Enabled = $false
        Mode = ""
        CertPath = ""
        CertPassword = ""
        Subject = ""
        TempCertPath = $null
        SignToolPath = ""
        TimestampUrl = if ($env:WINDOWS_SIGNING_TIMESTAMP_URL) { $env:WINDOWS_SIGNING_TIMESTAMP_URL } else { "http://timestamp.digicert.com" }
        FileDigest = if ($env:WINDOWS_SIGNING_FILE_DIGEST) { $env:WINDOWS_SIGNING_FILE_DIGEST } else { "SHA256" }
        TimestampDigest = if ($env:WINDOWS_SIGNING_TIMESTAMP_DIGEST) { $env:WINDOWS_SIGNING_TIMESTAMP_DIGEST } else { "SHA256" }
    }

    $certBase64 = $env:WINDOWS_SIGNING_CERT_BASE64
    $certPath = $env:WINDOWS_SIGNING_CERT_PATH
    $subject = $env:WINDOWS_SIGNING_SUBJECT_NAME

    if (-not [string]::IsNullOrWhiteSpace($certBase64)) {
        if ([string]::IsNullOrWhiteSpace($env:WINDOWS_SIGNING_CERT_PASSWORD)) {
            throw "WINDOWS_SIGNING_CERT_PASSWORD is required when WINDOWS_SIGNING_CERT_BASE64 is set"
        }

        $tempCertPath = Join-Path $WorkDir "windows-signing-cert.pfx"
        try {
            [System.IO.File]::WriteAllBytes($tempCertPath, [Convert]::FromBase64String($certBase64))
        }
        catch {
            throw "WINDOWS_SIGNING_CERT_BASE64 is not valid base64"
        }

        $context.Enabled = $true
        $context.Mode = "pfx"
        $context.CertPath = $tempCertPath
        $context.CertPassword = $env:WINDOWS_SIGNING_CERT_PASSWORD
        $context.TempCertPath = $tempCertPath
    }
    elseif (-not [string]::IsNullOrWhiteSpace($certPath)) {
        if (-not (Test-Path -Path $certPath)) {
            throw "WINDOWS_SIGNING_CERT_PATH does not exist: $certPath"
        }

        if ([string]::IsNullOrWhiteSpace($env:WINDOWS_SIGNING_CERT_PASSWORD)) {
            throw "WINDOWS_SIGNING_CERT_PASSWORD is required when WINDOWS_SIGNING_CERT_PATH is set"
        }

        $context.Enabled = $true
        $context.Mode = "pfx"
        $context.CertPath = $certPath
        $context.CertPassword = $env:WINDOWS_SIGNING_CERT_PASSWORD
    }
    elseif (-not [string]::IsNullOrWhiteSpace($subject)) {
        $context.Enabled = $true
        $context.Mode = "subject"
        $context.Subject = $subject
    }

    if ($SigningRequired -and -not $context.Enabled) {
        throw "Windows signing is required but no signing certificate configuration is provided"
    }

    if ($context.Enabled) {
        $context.SignToolPath = Resolve-SignToolPath
    }

    return $context
}

function Sign-Artifact {
    param(
        [string]$Path,
        [hashtable]$SigningContext
    )

    if (-not $SigningContext.Enabled) {
        return
    }

    if (-not (Test-Path -Path $Path)) {
        throw "cannot sign missing file: $Path"
    }

    $args = @(
        "sign",
        "/fd", $SigningContext.FileDigest,
        "/td", $SigningContext.TimestampDigest,
        "/tr", $SigningContext.TimestampUrl
    )

    switch ($SigningContext.Mode) {
        "pfx" {
            $args += @("/f", $SigningContext.CertPath, "/p", $SigningContext.CertPassword)
        }
        "subject" {
            $args += @("/n", $SigningContext.Subject)
        }
        default {
            throw "unsupported signing mode: $($SigningContext.Mode)"
        }
    }

    $args += $Path

    & $SigningContext.SignToolPath @args
    if ($LASTEXITCODE -ne 0) {
        throw "signtool failed with exit code $LASTEXITCODE for $Path"
    }
}

switch ($Target) {
    "x86_64-pc-windows-msvc" {
        $archLabel = "x86_64"
        $wixArch = "x64"
    }
    "aarch64-pc-windows-msvc" {
        $archLabel = "aarch64"
        $wixArch = "arm64"
    }
    default {
        throw "Unsupported target: $Target"
    }
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../../..")).Path
Set-Location $repoRoot

if ([string]::IsNullOrWhiteSpace($OutDir)) {
    $OutDir = Join-Path $repoRoot "dist"
}

New-Item -ItemType Directory -Path $OutDir -Force | Out-Null

if ([string]::IsNullOrWhiteSpace($WixPath)) {
    $wixCmd = Get-Command wix -ErrorAction SilentlyContinue
    if ($null -eq $wixCmd) {
        throw "WiX toolset CLI ('wix') is not available"
    }
    $WixPath = $wixCmd.Source
}

$signingRequired = Test-True -Value $env:WINDOWS_SIGNING_REQUIRED
$productVersion = Resolve-ProductVersion -RepoRoot $repoRoot
$upgradeCode = "4f0b8f79-0f9c-4b7f-a0d1-f9cf472a61b4"
$bundleUpgradeCode = "8d3f730a-1ef9-4d3d-a4cd-b36640bcc350"
$appIconPath = Join-Path $repoRoot "crates/desktop/assets/app-icon.ico"

if (-not (Test-Path -Path $appIconPath)) {
    throw "missing Windows app icon: $appIconPath"
}

cargo build --release -p pioneer-desktop --target $Target
cargo build --release -p pioneer-cli --target $Target

$workDir = Join-Path $env:TEMP ("pioneer-msi-" + [Guid]::NewGuid().ToString("N"))
$stageDir = Join-Path $workDir "stage"
New-Item -ItemType Directory -Path $stageDir -Force | Out-Null

$signingContext = $null

try {
    $signingContext = New-SigningContext -SigningRequired $signingRequired -WorkDir $workDir

    Copy-Item -Path "target/$Target/release/pioneer-app.exe" -Destination (Join-Path $stageDir "pioneer-app.exe") -Force
    $gatewayStageDir = Join-Path $stageDir "gateway"
    New-Item -ItemType Directory -Path $gatewayStageDir -Force | Out-Null

    $gatewayAssetName = "pioneer-gateway-windows-$archLabel.zip"
    $gatewayAssetPath = Join-Path $gatewayStageDir $gatewayAssetName
    Compress-Archive -Path "target/$Target/release/pioneer.exe" -DestinationPath $gatewayAssetPath -Force

    $gatewaySha256 = (Get-FileHash -Path $gatewayAssetPath -Algorithm SHA256).Hash.ToLowerInvariant()
    "sha256:$gatewaySha256 $gatewayAssetName" | Set-Content -Path (Join-Path $gatewayStageDir "SHA256SUMS") -NoNewline

    Copy-Item -Path "target/$Target/release/pioneer.exe" -Destination (Join-Path $gatewayStageDir "pioneer-bootstrap.exe") -Force

    $wxsPath = Join-Path $workDir "Pioneer.wxs"
    @"
<Wix xmlns="http://wixtoolset.org/schemas/v4/wxs">
  <Package Name="Pioneer" Manufacturer="Pioneer" Version="`$(var.Version)" UpgradeCode="`$(var.UpgradeCode)" Language="1033" Scope="perMachine">
    <MajorUpgrade DowngradeErrorMessage="A newer version of Pioneer is already installed." />
    <MediaTemplate EmbedCab="yes" />
    <Icon Id="PioneerAppIcon" SourceFile="`$(var.AppIconPath)" />
    <Property Id="ARPPRODUCTICON" Value="PioneerAppIcon" />

    <StandardDirectory Id="ProgramFiles64Folder">
      <Directory Id="INSTALLFOLDER" Name="Pioneer">
        <Component Id="DesktopExeComponent" Guid="*">
          <File Id="DesktopExeFile" Source="`$(var.StageDir)\\pioneer-app.exe" KeyPath="yes" />
        </Component>
        <Directory Id="GatewayBundleDir" Name="gateway">
          <Component Id="GatewayBootstrapComponent" Guid="*">
            <File Id="GatewayBootstrapFile" Source="`$(var.StageDir)\\gateway\\pioneer-bootstrap.exe" KeyPath="yes" />
          </Component>
          <Component Id="GatewayAssetComponent" Guid="*">
            <File Id="GatewayAssetFile" Source="`$(var.StageDir)\\gateway\\$gatewayAssetName" KeyPath="yes" />
          </Component>
          <Component Id="GatewayChecksumsComponent" Guid="*">
            <File Id="GatewayChecksumsFile" Source="`$(var.StageDir)\\gateway\\SHA256SUMS" KeyPath="yes" />
          </Component>
        </Directory>
      </Directory>
    </StandardDirectory>

    <Feature Id="MainFeature" Title="Pioneer" Level="1">
      <ComponentRef Id="DesktopExeComponent" />
      <ComponentRef Id="GatewayBootstrapComponent" />
      <ComponentRef Id="GatewayAssetComponent" />
      <ComponentRef Id="GatewayChecksumsComponent" />
    </Feature>
  </Package>
</Wix>
"@ | Set-Content -Path $wxsPath -NoNewline

    $msiName = "Pioneer-$archLabel.msi"
    $msiPath = Join-Path $OutDir $msiName

    & $WixPath build $wxsPath -arch $wixArch -d "Version=$productVersion" -d "UpgradeCode=$upgradeCode" -d "StageDir=$stageDir" -d "AppIconPath=$appIconPath" -o $msiPath
    if ($LASTEXITCODE -ne 0) {
        throw "wix build failed with exit code $LASTEXITCODE"
    }

    $bundleWxsPath = Join-Path $workDir "PioneerBundle.wxs"
    @"
<Wix xmlns="http://wixtoolset.org/schemas/v4/wxs" xmlns:bal="http://wixtoolset.org/schemas/v4/wxs/bal">
  <Bundle Name="Pioneer" Manufacturer="Pioneer" Version="`$(var.Version)" UpgradeCode="`$(var.BundleUpgradeCode)" IconSourceFile="`$(var.AppIconPath)">
    <BootstrapperApplication>
      <bal:WixStandardBootstrapperApplication Theme="hyperlinkLicense" LicenseUrl="https://pioneer.ai/license" />
    </BootstrapperApplication>
    <Chain>
      <MsiPackage SourceFile="`$(var.MsiPath)" />
    </Chain>
  </Bundle>
</Wix>
"@ | Set-Content -Path $bundleWxsPath -NoNewline

    $exeName = "Pioneer-$archLabel.exe"
    $exePath = Join-Path $OutDir $exeName

    & $WixPath build $bundleWxsPath -arch $wixArch -ext WixToolset.Bal.wixext -d "Version=$productVersion" -d "BundleUpgradeCode=$bundleUpgradeCode" -d "MsiPath=$msiPath" -d "AppIconPath=$appIconPath" -o $exePath
    if ($LASTEXITCODE -ne 0) {
        throw "wix bundle build failed with exit code $LASTEXITCODE"
    }

    Sign-Artifact -Path $msiPath -SigningContext $signingContext
    Sign-Artifact -Path $exePath -SigningContext $signingContext

    Write-Host "Created: $msiPath"
    Write-Host "Created: $exePath"
}
finally {
    if ($null -ne $signingContext -and $signingContext.TempCertPath) {
        Remove-Item -Path $signingContext.TempCertPath -Force -ErrorAction SilentlyContinue
    }
    Remove-Item -Path $workDir -Recurse -Force -ErrorAction SilentlyContinue
}
