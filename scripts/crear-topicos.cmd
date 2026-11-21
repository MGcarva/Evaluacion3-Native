@echo off
REM Crea los topicos (idempotente). Hace lo mismo que el servicio kafka-init de docker compose,
REM que ya se ejecuta solo al hacer "docker compose up -d". Configuracion en .env.
docker compose run --rm kafka-init
