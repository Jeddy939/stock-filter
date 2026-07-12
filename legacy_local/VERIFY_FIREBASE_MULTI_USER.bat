@echo off
setlocal
cd /d "%~dp0"
if "%FIREBASE_API_KEY%"=="" set "FIREBASE_API_KEY=AIzaSyA4tXcCkEv26i83WlM8k_dv-EubkjRCFRM"
if "%MONEYMAKER_SCAN_ID%"=="" set "MONEYMAKER_SCAN_ID=32"
if "%MONEYMAKER_TEST_TICKER%"=="" set "MONEYMAKER_TEST_TICKER=ABIG"
python firebase\verify_multi_user.py
if errorlevel 1 pause
endlocal
