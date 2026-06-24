@echo off
setlocal

cd /d "%~dp0"
title Export MoneyMaker Rating Analysis

if not exist "ratings" mkdir "ratings"
if "%MONEYMAKER_CENTRAL_RATINGS_DB%"=="" set "MONEYMAKER_CENTRAL_RATINGS_DB=%~dp0ratings\central_stock_ratings.sqlite"

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

echo Exporting central rating analysis from:
echo %MONEYMAKER_CENTRAL_RATINGS_DB%
%PYTHON_CMD% export_rating_analysis.py --central-db "%MONEYMAKER_CENTRAL_RATINGS_DB%"
echo.
echo Done. Check the exports folder.
pause
