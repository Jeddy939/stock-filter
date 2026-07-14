@echo off
setlocal
cd /d "%~dp0"
echo This applies the PostgreSQL schema, deploys the API and hosting, and keeps the existing data.
echo Run "gcloud auth login" first if this machine has no active Google Cloud account.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0firebase\DEPLOY_FIREBASE.ps1" -DeployOnly -ApplySchema
if errorlevel 1 pause
endlocal
