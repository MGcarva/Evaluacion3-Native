@echo off
REM Elimina TODO lo creado en AWS (instancia, disco, security group, llave) y verifica.
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -Command ". .\aws\cargar-credenciales.ps1; & .\aws\99-destruir.ps1"
pause
