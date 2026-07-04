@echo off
setlocal

cd /d "%~dp0"
title Send Existing MoneyMaker Ratings to Google Sheets

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

if "%MONEYMAKER_GOOGLE_SHEETS_WEBHOOK_URL%"=="" (
    echo Paste the Google Apps Script Web App URL below.
    echo It should look like https://script.google.com/macros/s/.../exec
    echo.
    set /p MONEYMAKER_GOOGLE_SHEETS_WEBHOOK_URL=Web App URL: 
)

if "%MONEYMAKER_GOOGLE_SHEETS_WEBHOOK_URL%"=="" (
    echo No Google Sheets URL entered.
    pause
    exit /b 1
)

if "%MONEYMAKER_RATER_NAME%"=="" (
    set /p MONEYMAKER_RATER_NAME=Rater name to use for imported ratings [press Enter for Windows username]: 
    if "%MONEYMAKER_RATER_NAME%"=="" set "MONEYMAKER_RATER_NAME=%USERNAME%"
)

if "%MONEYMAKER_GOOGLE_SHEETS_SECRET%"=="" (
    echo.
    echo Optional: if the Google Apps Script uses MONEYMAKER_SECRET, paste it here.
    echo Otherwise press Enter.
    set /p MONEYMAKER_GOOGLE_SHEETS_SECRET=Secret: 
)

echo.
echo Sending existing ratings to Google Sheets...
%PYTHON_CMD% send_existing_ratings_to_sheets.py --webhook-url "%MONEYMAKER_GOOGLE_SHEETS_WEBHOOK_URL%" --rater-name "%MONEYMAKER_RATER_NAME%" --secret "%MONEYMAKER_GOOGLE_SHEETS_SECRET%"

echo.
pause
