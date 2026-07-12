@echo off
setlocal

cd /d "%~dp0"
title Use OneDrive Ratings Database

set "ONEDRIVE_ROOT="
if not "%OneDriveCommercial%"=="" set "ONEDRIVE_ROOT=%OneDriveCommercial%"
if "%ONEDRIVE_ROOT%"=="" if not "%OneDrive%"=="" set "ONEDRIVE_ROOT=%OneDrive%"

if "%ONEDRIVE_ROOT%"=="" (
    echo Could not find a OneDrive folder on this Windows account.
    echo Sign in to OneDrive first, then run this again.
    pause
    exit /b 1
)

set "ONEDRIVE_RATINGS_DIR=%ONEDRIVE_ROOT%\MoneyMaker\ratings"
set "ONEDRIVE_RATINGS_DB=%ONEDRIVE_RATINGS_DIR%\central_stock_ratings.sqlite"

if not exist "%ONEDRIVE_RATINGS_DIR%" mkdir "%ONEDRIVE_RATINGS_DIR%"

if not exist "%ONEDRIVE_RATINGS_DB%" (
    if exist "%~dp0ratings\central_stock_ratings.sqlite" (
        echo Copying existing local ratings database to OneDrive...
        copy "%~dp0ratings\central_stock_ratings.sqlite" "%ONEDRIVE_RATINGS_DB%" >nul
    )
)

setx MONEYMAKER_CENTRAL_RATINGS_DB "%ONEDRIVE_RATINGS_DB%" >nul
if "%MONEYMAKER_RATER_NAME%"=="" setx MONEYMAKER_RATER_NAME "%USERNAME%" >nul

echo.
echo MoneyMaker is now configured to use this OneDrive ratings database:
echo %ONEDRIVE_RATINGS_DB%
echo.
echo Close and reopen MoneyMaker after running this setup.
echo You can view the file with OPEN_ONEDRIVE_RATINGS_FOLDER.bat.
pause
