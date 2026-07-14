@echo off
setlocal

cd /d "%~dp0"
title Send Moneymaker Labels to Google Docs

set "PYTHON_CMD="

if exist ".venv\Scripts\python.exe" (
    set "PYTHON_CMD=.venv\Scripts\python.exe"
) else (
    where python >nul 2>nul
    if not errorlevel 1 set "PYTHON_CMD=python"
)

if "%PYTHON_CMD%"=="" (
    where py >nul 2>nul
    if not errorlevel 1 set "PYTHON_CMD=py -3"
)

if "%PYTHON_CMD%"=="" (
    echo Could not find Python.
    echo Install Python 3, then run this file again.
    pause
    exit /b 1
)

echo Checking required Python packages...
%PYTHON_CMD% -c "import googleapiclient, google_auth_oauthlib" >nul 2>nul
if errorlevel 1 (
    echo Installing Google Docs export packages...
    %PYTHON_CMD% -m pip install -r requirements.txt
    if errorlevel 1 (
        echo.
        echo Dependency install failed. Check the error above.
        pause
        exit /b 1
    )
)

set "GOOGLE_SECRET_FOUND="
if exist "google_client_secret.json" set "GOOGLE_SECRET_FOUND=google_client_secret.json"
if "%GOOGLE_SECRET_FOUND%"=="" (
    for %%F in (client_secret_*.json) do (
        if exist "%%F" set "GOOGLE_SECRET_FOUND=%%F"
    )
)

if "%GOOGLE_SECRET_FOUND%"=="" (
    echo.
    echo Missing Google OAuth client secret JSON.
    echo Download your Google OAuth Desktop app JSON and save it here:
    echo %CD%\google_client_secret.json
    echo.
    echo The original downloaded client_secret_*.json filename also works.
    echo.
    pause
    exit /b 1
)

echo.
echo Sending saved Moneymaker labels to Google Docs...
echo The first run may open a browser window for Google approval.
echo.

%PYTHON_CMD% export_labels_to_google_docs.py --cache-file stock_cache.sqlite %*
if errorlevel 1 (
    echo.
    echo Export failed. Check the message above.
    pause
    exit /b 1
)

echo.
echo Export complete.
pause
