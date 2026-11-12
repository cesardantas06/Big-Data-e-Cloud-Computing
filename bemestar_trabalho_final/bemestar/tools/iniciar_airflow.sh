#!/usr/bin/env bash
# Inicia o Airflow (interface em http://localhost:8080). Ctrl+C encerra.
# Uso:  wsl -d Ubuntu-24.04 -- bash tools/iniciar_airflow.sh
set -euo pipefail
if [ ! -f "$HOME/airflow-trabalho.sh" ]; then
    echo "Airflow não instalado. Rode antes: bash tools/instalar_airflow.sh"; exit 1
fi
source "$HOME/airflow-trabalho.sh"
echo "Quando aparecer 'Airflow is ready', abra http://localhost:8080 . Mantenha este terminal aberto."
exec airflow standalone
