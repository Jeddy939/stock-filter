@echo off
setlocal

cd /d "%~dp0"
title Moneymaker Stock Filter

if "%MONEYMAKER_CENTRAL_RATINGS_DB%"=="" (
    if not exist "ratings" mkdir "ratings"
    set "MONEYMAKER_CENTRAL_RATINGS_DB=%~dp0ratings\central_stock_ratings.sqlite"
) else (
    for %%I in ("%MONEYMAKER_CENTRAL_RATINGS_DB%") do if not exist "%%~dpI" mkdir "%%~dpI"
)
if "%MONEYMAKER_RATER_NAME%"=="" set "MONEYMAKER_RATER_NAME=%USERNAME%"

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

echo Starting Moneymaker Stock Filter...
echo Central ratings database:
echo %MONEYMAKER_CENTRAL_RATINGS_DB%
echo.

%PYTHON_CMD% -c "import pandas, yfinance, curl_cffi, tqdm, googleapiclient, google_auth_oauthlib" >nul 2>nul
if errorlevel 1 (
    echo Installing required Python packages...
    %PYTHON_CMD% -m pip install -r requirements.txt
    if errorlevel 1 (
        echo.
        echo Dependency install failed. Check the error above.
        pause
        exit /b 1
    )
)

echo Opening http://localhost:8000/
start "" powershell -NoProfile -WindowStyle Hidden -Command "Start-Sleep -Seconds 2; Start-Process 'http://localhost:8000/'"
echo.
echo Leave this window open while using the app.
echo Press Ctrl+C in this window to stop the app.
echo.

%PYTHON_CMD% web_app.py --host 127.0.0.1 --port 8000

echo.
echo Moneymaker has stopped.
pause
