@echo off
setlocal

cd /d "%~dp0"
title Push MoneyMaker Ratings to GitHub

where git >nul 2>nul
if errorlevel 1 (
    echo Could not find Git.
    echo Install Git or push from GitHub Desktop.
    pause
    exit /b 1
)

echo Pulling latest GitHub changes first...
git pull --rebase origin main
if errorlevel 1 (
    echo.
    echo Git could not pull cleanly. Open this folder in GitHub Desktop or ask Codex to fix the conflict.
    pause
    exit /b 1
)

git add central_stock_ratings.json central_stock_ratings.jsonl
git diff --cached --quiet
if not errorlevel 1 (
    echo.
    echo No new rating changes to push.
    pause
    exit /b 0
)

for /f "tokens=1-3 delims=/ " %%a in ("%date%") do set "STAMP_DATE=%%c-%%b-%%a"
set "STAMP_TIME=%time::=-%"
set "STAMP_TIME=%STAMP_TIME: =0%"

git commit -m "Update central stock ratings %STAMP_DATE% %STAMP_TIME%"
if errorlevel 1 (
    echo.
    echo Git could not commit the rating changes.
    pause
    exit /b 1
)

git push origin HEAD:main
if errorlevel 1 (
    echo.
    echo Git could not push. Check your GitHub login, then run this again.
    pause
    exit /b 1
)

echo.
echo Ratings pushed to GitHub.
pause
