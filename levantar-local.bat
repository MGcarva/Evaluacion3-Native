@echo off
REM Levanta TODO en local (Docker Desktop): Kafka, ZooKeeper, Streams, Prometheus, Grafana.
cd /d "%~dp0"
if not exist ".env" copy ".env.example" ".env" >nul
docker compose up -d --build
docker compose ps -a
echo.
echo Grafana: http://localhost:3000   Prometheus: http://localhost:9090
pause
