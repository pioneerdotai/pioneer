[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("x86_64-pc-windows-msvc", "aarch64-pc-windows-msvc")]
    [string]$Target,

    [string]$OutDir,

    [string]$WixPath,

    [switch]$SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

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

. (Join-Path $PSScriptRoot "../windows-signing.ps1")

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
$OutDir = (Resolve-Path $OutDir).Path

if ([string]::IsNullOrWhiteSpace($WixPath)) {
    $wixCmd = Get-Command wix -ErrorAction SilentlyContinue
    if ($null -eq $wixCmd) {
        throw "WiX toolset CLI ('wix') is not available"
    }
    $WixPath = $wixCmd.Source
}

$signingRequired = Test-True -Value $env:WINDOWS_SIGNING_REQUIRED
$productVersion = Resolve-ProductVersion -RepoRoot $repoRoot
$appUrlScheme = if ([string]::IsNullOrWhiteSpace($env:PIONEER_APP_URL_SCHEME)) {
    "pioneer"
}
else {
    $env:PIONEER_APP_URL_SCHEME.Trim()
}
if ($appUrlScheme -notin @("pioneer", "pioneer-dev")) {
    throw "PIONEER_APP_URL_SCHEME must be either pioneer or pioneer-dev"
}
$env:PIONEER_APP_URL_SCHEME = $appUrlScheme
$upgradeCode = "4f0b8f79-0f9c-4b7f-a0d1-f9cf472a61b4"
$bundleUpgradeCode = "8d3f730a-1ef9-4d3d-a4cd-b36640bcc350"
$appIconPath = Join-Path $repoRoot "crates/desktop/assets/app-icon.ico"

if (-not (Test-Path -Path $appIconPath)) {
    throw "missing Windows app icon: $appIconPath"
}

# Always use prepared inputs; SkipBuild is retained for existing callers.
python scripts/packaging/desktop-inputs.py preflight --target $Target
if ($LASTEXITCODE -ne 0) { throw "Prepared Desktop/Gateway inputs are required" }

$workDir = Join-Path $env:TEMP ("pioneer-msi-" + [Guid]::NewGuid().ToString("N"))
$stageDir = Join-Path $workDir "stage"
New-Item -ItemType Directory -Path $stageDir -Force | Out-Null

$signingContext = $null

try {
    $signingContext = New-SigningContext -SigningRequired $signingRequired -WorkDir $workDir

    Copy-Item -Path "target/$Target/release/pioneer-app.exe" -Destination (Join-Path $stageDir "pioneer-app.exe") -Force
    $helperStagePath = Join-Path $stageDir "pioneer-app-updater.exe"
    Copy-Item -Path "target/$Target/release/pioneer-app-updater.exe" -Destination $helperStagePath -Force
    if (-not (Test-Path -Path $helperStagePath)) {
        throw "missing packaged desktop updater helper: $helperStagePath"
    }
    Sign-Artifact -Path $helperStagePath -SigningContext $signingContext

    $gatewayStageDir = Join-Path $stageDir "gateway"
    New-Item -ItemType Directory -Path $gatewayStageDir -Force | Out-Null

    $gatewayAssetName = "pioneer-gateway-windows-$archLabel-computer-use-package.zip"
    python scripts/packaging/desktop-inputs.py embed --target $Target --destination $gatewayStageDir
    if ($LASTEXITCODE -ne 0) { throw "Approved Gateway payload embedding failed" }
    Sign-Artifact -Path (Join-Path $stageDir "pioneer-app.exe") -SigningContext $signingContext

    # windows-stage must be called, including full ZIP identity/hash validation.
    bash scripts/packaging/desktop/check-package-contents.sh windows-stage $stageDir $Target
    if ($LASTEXITCODE -ne 0) { throw "Windows staging validation failed" }
    if ($env:PIONEER_SYMBOL_INPUTS_DIR) {
        $ledger = Join-Path $env:PIONEER_SYMBOL_INPUTS_DIR "packaged.json"
        foreach ($binary in @((Join-Path $stageDir "pioneer-app.exe"), (Join-Path $gatewayStageDir "pioneer-bootstrap.exe"), $helperStagePath)) {
            python scripts/packaging/symbol-inputs.py record $binary $ledger
            if ($LASTEXITCODE -ne 0) { throw "Packaged module inventory failed" }
        }
    }

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
          <RegistryKey Root="HKLM" Key="Software\Classes\$appUrlScheme">
            <RegistryValue Type="string" Value="URL:Pioneer Protocol" />
            <RegistryValue Name="URL Protocol" Type="string" Value="" />
            <RegistryKey Key="DefaultIcon">
              <RegistryValue Type="string" Value="&quot;[#DesktopExeFile]&quot;,0" />
            </RegistryKey>
            <RegistryKey Key="shell\open\command">
              <RegistryValue Type="string" Value="&quot;[#DesktopExeFile]&quot; &quot;%1&quot;" />
            </RegistryKey>
          </RegistryKey>
        </Component>
        <Component Id="DesktopUpdaterComponent" Guid="*">
          <File Id="DesktopUpdaterFile" Source="`$(var.StageDir)\\pioneer-app-updater.exe" KeyPath="yes" />
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
          <Component Id="GatewayProvenanceComponent" Guid="*">
            <File Id="GatewayProvenanceFile" Source="`$(var.StageDir)\\gateway\\provenance.json" KeyPath="yes" />
          </Component>
        </Directory>
      </Directory>
    </StandardDirectory>

    <Feature Id="MainFeature" Title="Pioneer" Level="1">
      <ComponentRef Id="DesktopExeComponent" />
      <ComponentRef Id="DesktopUpdaterComponent" />
      <ComponentRef Id="GatewayBootstrapComponent" />
      <ComponentRef Id="GatewayAssetComponent" />
      <ComponentRef Id="GatewayChecksumsComponent" />
      <ComponentRef Id="GatewayProvenanceComponent" />
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

    Sign-Artifact -Path $msiPath -SigningContext $signingContext

    $bundleWxsPath = Join-Path $workDir "PioneerBundle.wxs"
    @"
<Wix xmlns="http://wixtoolset.org/schemas/v4/wxs" xmlns:bal="http://wixtoolset.org/schemas/v4/wxs/bal">
  <Bundle Name="Pioneer" Manufacturer="Pioneer" Version="`$(var.Version)" UpgradeCode="`$(var.BundleUpgradeCode)" IconSourceFile="`$(var.AppIconPath)">
    <BootstrapperApplication>
      <bal:WixStandardBootstrapperApplication Theme="hyperlinkLicense" LicenseUrl="https://getpioneer.dev/license" />
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

    Sign-Artifact -Path $exePath -SigningContext $signingContext

    # Administrative extraction validates actual MSI payload without installing it.
    $extractDir = Join-Path $workDir "extracted"
    $extract = Start-Process msiexec.exe -ArgumentList @("/a", "`"$msiPath`"", "/qn", "TARGETDIR=`"$extractDir`"") -Wait -PassThru
    if ($extract.ExitCode -ne 0) { throw "MSI payload extraction failed: $($extract.ExitCode)" }
    $installedApp = @(Get-ChildItem -Path $extractDir -Filter "pioneer-app.exe" -Recurse)
    if ($installedApp.Count -ne 1) { throw "MSI does not contain exactly one Desktop executable" }
    $installedStage = $installedApp[0].Directory.FullName
    bash scripts/packaging/desktop/check-package-contents.sh windows-stage $installedStage $Target
    if ($LASTEXITCODE -ne 0) { throw "Built MSI payload validation failed" }
    foreach ($name in @("pioneer-app.exe", "pioneer-app-updater.exe")) {
        $expected = (Get-FileHash (Join-Path $stageDir $name) -Algorithm SHA256).Hash
        $actual = (Get-FileHash (Join-Path $installedStage $name) -Algorithm SHA256).Hash
        if ($expected -ne $actual) { throw "MSI executable identity mismatch: $name" }
    }

    Write-Host "Created: $msiPath"
    Write-Host "Created: $exePath"
}
finally {
    if ($null -ne $signingContext -and $signingContext.TempCertPath) {
        Remove-Item -Path $signingContext.TempCertPath -Force -ErrorAction SilentlyContinue
    }
    Remove-Item -Path $workDir -Recurse -Force -ErrorAction SilentlyContinue
}
