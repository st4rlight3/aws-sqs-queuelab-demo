@echo off
chcp 65001 >nul
cd /d "%~dp0"
set "QUEUELAB_PYTHON=%USERPROFILE%\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
if exist "%QUEUELAB_PYTHON%" (
  "%QUEUELAB_PYTHON%" app.py --open-browser
  pause
  exit /b
)
where py >nul 2>nul
if %errorlevel% equ 0 (
  py -3 app.py --open-browser
  pause
  exit /b
)
where python >nul 2>nul
if %errorlevel% equ 0 (
  python app.py --open-browser
  pause
  exit /b
)
echo Please install Python 3.10 or later from https://www.python.org/downloads/
pause
