@echo off
REM Ejecuta escenarios en la EC2 y trae evidencias *-aws a evidencias\
REM Uso: pruebas-aws.bat [todo|broker|reinicio|generador|base|traer]   (por defecto: todo)
cd /d "%~dp0"
set ESC=%1
if "%ESC%"=="" set ESC=todo
powershell -NoProfile -ExecutionPolicy Bypass -Command ". .\aws\cargar-credenciales.ps1; & .\aws\04-evidencias.ps1 -Escenario %ESC%"
pause
