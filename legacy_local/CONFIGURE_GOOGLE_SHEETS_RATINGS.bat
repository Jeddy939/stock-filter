@echo off
setlocal

cd /d "%~dp0"
title Configure Google Sheets Ratings

echo Paste the Google Apps Script Web App URL below.
echo It should look like https://script.google.com/macros/s/.../exec
echo.
set /p SHEETS_URL=Web App URL: 

if "%SHEETS_URL%"=="" (
    echo No URL entered.
    pause
    exit /b 1
)

setx MONEYMAKER_GOOGLE_SHEETS_WEBHOOK_URL "%SHEETS_URL%" >nul

echo.
set /p RATER_NAME=Rater name to show in Sheets [press Enter for Windows username]: 
if "%RATER_NAME%"=="" set "RATER_NAME=%USERNAME%"
setx MONEYMAKER_RATER_NAME "%RATER_NAME%" >nul

echo.
echo Optional: if you set MONEYMAKER_SECRET in Apps Script properties, paste it here.
echo Otherwise just press Enter.
set /p SHEETS_SECRET=Secret: 
if not "%SHEETS_SECRET%"=="" setx MONEYMAKER_GOOGLE_SHEETS_SECRET "%SHEETS_SECRET%" >nul

echo.
echo Google Sheets ratings are configured.
echo Close and reopen MoneyMaker before rating stocks.
pause
