@echo off
setlocal

cd /d "%~dp0"

set "ONEDRIVE_ROOT="
if not "%OneDriveCommercial%"=="" set "ONEDRIVE_ROOT=%OneDriveCommercial%"
if "%ONEDRIVE_ROOT%"=="" if not "%OneDrive%"=="" set "ONEDRIVE_ROOT=%OneDrive%"

if "%ONEDRIVE_ROOT%"=="" (
    echo Could not find a OneDrive folder on this Windows account.
    echo Run START_MONEYMAKER.bat for local-only ratings.
    pause
    exit /b 1
)

set "MONEYMAKER_CENTRAL_RATINGS_DB=%ONEDRIVE_ROOT%\MoneyMaker\ratings\central_stock_ratings.sqlite"
if "%MONEYMAKER_RATER_NAME%"=="" set "MONEYMAKER_RATER_NAME=%USERNAME%"

call "%~dp0START_MONEYMAKER.bat"
