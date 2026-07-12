@echo off
setlocal

cd /d "%~dp0"

set "ONEDRIVE_ROOT="
if not "%OneDriveCommercial%"=="" set "ONEDRIVE_ROOT=%OneDriveCommercial%"
if "%ONEDRIVE_ROOT%"=="" if not "%OneDrive%"=="" set "ONEDRIVE_ROOT=%OneDrive%"

if "%ONEDRIVE_ROOT%"=="" (
    echo Could not find a OneDrive folder on this Windows account.
    pause
    exit /b 1
)

set "MONEYMAKER_CENTRAL_RATINGS_DB=%ONEDRIVE_ROOT%\MoneyMaker\ratings\central_stock_ratings.sqlite"

call "%~dp0EXPORT_RATING_ANALYSIS.bat"
