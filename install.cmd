@echo off
setlocal

set "SCRIPT_URL=%PIONEER_INSTALL_POWERSHELL_URL%"
if "%SCRIPT_URL%"=="" set "SCRIPT_URL=https://pioneer.ai/install.ps1"

set "TMP_PS1=%TEMP%\pioneer-install-%RANDOM%%RANDOM%.ps1"

powershell -NoProfile -NonInteractive -ExecutionPolicy Bypass -Command "Invoke-WebRequest -UseBasicParsing -Uri '%SCRIPT_URL%' -OutFile '%TMP_PS1%'"
if errorlevel 1 (
  echo [pioneer-install] failed to download install.ps1 from %SCRIPT_URL%
  del /f /q "%TMP_PS1%" >nul 2>&1
  exit /b 1
)

powershell -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "%TMP_PS1%" %*
set "EXIT_CODE=%ERRORLEVEL%"

del /f /q "%TMP_PS1%" >nul 2>&1
exit /b %EXIT_CODE%
