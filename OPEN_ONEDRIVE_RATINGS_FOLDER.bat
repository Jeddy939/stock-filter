@echo off
setlocal

set "ONEDRIVE_ROOT="
if not "%OneDriveCommercial%"=="" set "ONEDRIVE_ROOT=%OneDriveCommercial%"
if "%ONEDRIVE_ROOT%"=="" if not "%OneDrive%"=="" set "ONEDRIVE_ROOT=%OneDrive%"

if "%ONEDRIVE_ROOT%"=="" (
    echo Could not find a OneDrive folder on this Windows account.
    pause
    exit /b 1
)

set "ONEDRIVE_RATINGS_DIR=%ONEDRIVE_ROOT%\MoneyMaker\ratings"
if not exist "%ONEDRIVE_RATINGS_DIR%" mkdir "%ONEDRIVE_RATINGS_DIR%"
start "" "%ONEDRIVE_RATINGS_DIR%"
