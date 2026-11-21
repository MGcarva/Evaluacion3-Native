#!/usr/bin/env bash
# Ejecuta un comando y guarda comando + fecha + salida en evidencias/<nombre>.txt
# Uso: ./scripts/evidencia.sh 10-isr-antes "./scripts/kafka.sh isr"
# Variable opcional: EVIDENCIA_ENTORNO=local|aws. Las claves AWS se reemplazan por <REDACTADO>.
set -u
cd "$(dirname "$0")/.."
NOMBRE=$1; shift
COMANDO="$*"
mkdir -p evidencias
SALIDA=$(bash -c "$COMANDO" 2>&1 | sed -E \
  -e 's/(AKIA|ASIA)[A-Z0-9]{16}/<REDACTADO>/g' \
  -e 's/([Aa][Ww][Ss]_[Ss][Ee][Cc][Rr][Ee][Tt]_[Aa][Cc][Cc][Ee][Ss][Ss]_[Kk][Ee][Yy]|[Aa][Ww][Ss]_[Ss][Ee][Ss][Ss][Ii][Oo][Nn]_[Tt][Oo][Kk][Ee][Nn]|[Pp]assword|[Tt]oken)([[:space:]]*[=:][[:space:]]*)[^[:space:]]+/\1\2<REDACTADO>/g')
{
  echo "# Evidencia $NOMBRE"
  echo "# Fecha (UTC): $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "# Entorno: ${EVIDENCIA_ENTORNO:-local}"
  echo
  echo "\$ $COMANDO"
  echo
  echo "$SALIDA"
} > "evidencias/$NOMBRE.txt"
echo "$SALIDA"
echo "Evidencia guardada en evidencias/$NOMBRE.txt"
