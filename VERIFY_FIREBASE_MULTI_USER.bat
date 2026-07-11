@echo off
setlocal
cd /d "%~dp0"
if "%FIREBASE_API_KEY%"=="" set "FIREBASE_API_KEY=AIzaSyA4tXcCkEv26i83WlM8k_dv-EubkjRCFRM"
python firebase\verify_multi_user.py
if errorlevel 1 pause
endlocal
