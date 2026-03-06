@echo off
title OSSN Italia v8 — AIS Backend
echo.
echo  Avvio backend OSSN...
echo  Apri il browser su: http://localhost:3001
echo  Premi CTRL+C per fermare.
echo.
py server.py 2>nul || python server.py
pause
