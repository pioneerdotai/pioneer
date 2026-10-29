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

    $signArguments = @(
        "sign",
        "/fd", $SigningContext.FileDigest,
        "/td", $SigningContext.TimestampDigest,
        "/tr", $SigningContext.TimestampUrl
    )

    switch ($SigningContext.Mode) {
        "pfx" {
            $signArguments += @("/f", $SigningContext.CertPath, "/p", $SigningContext.CertPassword)
        }
        "subject" {
            $signArguments += @("/n", $SigningContext.Subject)
        }
        default {
            throw "unsupported signing mode: $($SigningContext.Mode)"
        }
    }

    $signArguments += $Path

    & $SigningContext.SignToolPath @signArguments
    if ($LASTEXITCODE -ne 0) {
        throw "signtool failed with exit code $LASTEXITCODE for $Path"
    }
    & $SigningContext.SignToolPath verify /pa $Path
    if ($LASTEXITCODE -ne 0) {
        throw "signtool verification failed for $Path"
    }
}
