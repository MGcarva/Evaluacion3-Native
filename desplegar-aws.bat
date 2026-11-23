@echo off
REM Despliega TODO en AWS: red, llave, EC2, Docker y el sistema completo.
REM Antes: pega las credenciales del laboratorio en aws\credenciales.txt
REM Uso:  desplegar-aws.bat            (solo desplegar, ~10 min)
REM       desplegar-aws.bat pruebas    (desplegar + todos los escenarios + evidencias, ~35 min)
REM TODOS los puertos (SSH, Grafana, Prometheus, Kafka UI en solo lectura y Kafka) quedan abiertos a TODO
REM internet para presentar desde cualquier red. Kafka no tiene autenticacion: es temporal.
REM Para restringir todo a tu IP: quitar -AbrirATodos de las dos lineas de abajo. Al terminar: destruir-aws.bat
cd /d "%~dp0"
if not exist "aws\credenciales.txt" (
  copy "aws\credenciales.ejemplo.txt" "aws\credenciales.txt" >nul
  echo Pega tus credenciales en aws\credenciales.txt, guarda, cierra el bloc de notas y vuelve a ejecutar.
  notepad "aws\credenciales.txt"
  exit /b 1
)
if /i "%1"=="pruebas" (
  powershell -NoProfile -ExecutionPolicy Bypass -File aws\desplegar-todo.ps1 -ConPruebas -AbrirATodos
) else (
  powershell -NoProfile -ExecutionPolicy Bypass -File aws\desplegar-todo.ps1 -AbrirATodos
)
pause
