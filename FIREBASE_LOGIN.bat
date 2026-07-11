@echo off
setlocal
cd /d "%~dp0"
echo Opening Firebase sign-in in your browser...
npx.cmd --yes firebase-tools@latest login
if errorlevel 1 (
  echo.
  echo Firebase sign-in did not complete.
  pause
  exit /b 1
)
echo Selecting project moneymaker-aedf7...
npx.cmd --yes firebase-tools@latest use moneymaker-aedf7
echo Checking project access...
npx.cmd --yes firebase-tools@latest projects:list
pause
