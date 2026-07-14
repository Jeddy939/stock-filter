@echo off
setlocal

cd /d "%~dp0"
title Send Moneymaker Picks to Google Sheets

set "CACHE_FILE=stock_cache.sqlite"
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

if not exist "%CACHE_FILE%" (
    echo.
    echo Could not find the saved picks database:
    echo %CD%\%CACHE_FILE%
    echo.
    echo Put stock_cache.sqlite in this folder first.
    echo.
    pause
    exit /b 1
)

echo Checking required Python packages...
%PYTHON_CMD% -c "import googleapiclient, google_auth_oauthlib" >nul 2>nul
if errorlevel 1 (
    echo Installing Google export packages...
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
    echo Download the OAuth Desktop app JSON and save it in this folder.
    echo.
    echo Use either of these names:
    echo google_client_secret.json
    echo client_secret_*.json
    echo.
    pause
    exit /b 1
)

echo.
echo This is the one shared-picks sync tool.
echo The Google secret stays as the JSON file in this folder.
echo Do not paste anything from the secret JSON into this window.
echo.
set /p "USER_NAME=Your display name for shared picks, or press Enter to leave blank: "
echo.

if exist "moneymaker_shared_google.json" (
    echo Using the saved shared Google Sheet setting from moneymaker_shared_google.json.
    echo Sending and receiving saved picks now...
    %PYTHON_CMD% sync_picks_to_google_sheets.py --cache-file "%CACHE_FILE%" --user-name "%USER_NAME%"
) else (
    echo No shared Google Sheet is linked on this computer yet.
    echo.
    echo If you already have the shared Sheet, paste its Google Sheet link or ID.
    echo If this is the FIRST computer and no shared Sheet exists yet, type CREATE.
    echo.
    set /p "SHEET_INPUT=Google Sheet link/ID or CREATE: "
    echo.
    if /I "%SHEET_INPUT%"=="CREATE" (
        echo Creating the shared Google Sheet and syncing picks...
        %PYTHON_CMD% sync_picks_to_google_sheets.py --cache-file "%CACHE_FILE%" --create --user-name "%USER_NAME%"
    ) else (
        if "%SHEET_INPUT%"=="" (
            echo No Sheet link or ID entered.
            echo Run this again and paste the real shared Google Sheet link, or type CREATE on the first computer only.
            pause
            exit /b 1
        )
        echo Linking to the shared Google Sheet and syncing picks...
        %PYTHON_CMD% sync_picks_to_google_sheets.py --cache-file "%CACHE_FILE%" --sheet "%SHEET_INPUT%" --user-name "%USER_NAME%"
    )
)

if errorlevel 1 (
    echo.
    echo Google Sheets sync failed. Check the message above.
    pause
    exit /b 1
)

echo.
echo Google Sheets sync complete.
pause
