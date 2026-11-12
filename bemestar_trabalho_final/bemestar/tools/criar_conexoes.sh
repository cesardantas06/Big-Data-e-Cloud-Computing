#!/usr/bin/env bash
# Plano B: cria as conexões e a Variable do DAG pela linha de comando (a interface é o caminho principal).
# Uso:  bash tools/criar_conexoes.sh
set -euo pipefail
[ -f "$HOME/airflow-trabalho.sh" ] && source "$HOME/airflow-trabalho.sh"
KEY="${AWS_ACCESS_KEY_ID:-admin}"; SECRET="${AWS_SECRET_ACCESS_KEY:-bigdata-secret}"   # credenciais do laboratório local
airflow connections delete lake_s3  >/dev/null 2>&1 || true
airflow connections delete api_fonte >/dev/null 2>&1 || true
airflow connections add lake_s3 --conn-type aws --conn-login "$KEY" --conn-password "$SECRET" \
    --conn-extra '{"endpoint_url": "http://127.0.0.1:8333", "region_name": "us-east-1"}'
airflow connections add api_fonte --conn-type http --conn-schema http --conn-host 127.0.0.1 --conn-port 8001
CHAVE="${1:-${PSEUDO_CHAVE:?informe a chave: bash tools/criar_conexoes.sh "<chave>" (ou defina PSEUDO_CHAVE)}}"
airflow variables set pseudo_secret "$CHAVE"
echo "Conexões lake_s3 e api_fonte e Variable pseudo_secret criadas."
