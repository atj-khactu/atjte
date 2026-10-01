@echo off
REM Double-click me. Runs run_gateway.py beside this file with the default
REM Python, so it works even where .py files open in an editor.
cd /d "%~dp0"
python "%~dp0run_gateway.py" %*
if errorlevel 1 pause
