@echo off
setlocal

cd /d "%~dp0"
if not exist "ratings" mkdir "ratings"
start "" "%~dp0ratings"
