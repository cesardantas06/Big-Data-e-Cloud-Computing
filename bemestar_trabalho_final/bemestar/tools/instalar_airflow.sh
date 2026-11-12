#!/usr/bin/env bash
# Instala o Apache Airflow 3.1.8 (Ubuntu/WSL) e aponta para a pasta dags/ deste projeto.
# Uso:  wsl -d Ubuntu-24.04 -- bash tools/instalar_airflow.sh      (pode rodar de novo)
set -euo pipefail

AIRFLOW_VERSION="3.1.8"
VENV="${AIRFLOW_VENV:-$HOME/airflow-venv}"
PROJETO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "Projeto: $PROJETO"

echo "[1/5] Ambiente virtual Python ($VENV)"
if ! python3 -m venv /tmp/_teste_venv >/dev/null 2>&1; then
    echo "      Instalando o python3-venv (o Ubuntu vai pedir a senha)."
    sudo apt-get update -qq
    sudo apt-get install -y -qq python3-venv
fi
rm -rf /tmp/_teste_venv
[ -x "$VENV/bin/python" ] || "${PYTHON_BIN:-python3}" -m venv "$VENV"
source "$VENV/bin/activate"
PY="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

echo "[2/5] Apache Airflow $AIRFLOW_VERSION + providers amazon (S3) e http"
pip install -q --upgrade pip
pip install -q "apache-airflow==$AIRFLOW_VERSION" \
    apache-airflow-providers-amazon \
    apache-airflow-providers-http \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$PY.txt"

echo "[3/5] Bibliotecas do pipeline (mesmas versões do .venv)"
pip install -q "duckdb==1.5.6" "pandas==2.3.3" "scikit-learn==1.9.1" boto3 pyarrow

echo "[4/5] Configuração (~/airflow-trabalho.sh)"
cat > "$HOME/airflow-trabalho.sh" <<EOT
# Ambiente do Airflow do Trabalho Final. Gerado por tools/instalar_airflow.sh
source "$VENV/bin/activate"
export AIRFLOW_HOME="\$HOME/airflow-trabalho"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJETO/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
# Laboratório local: a interface abre sem tela de login.
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True
EOT
source "$HOME/airflow-trabalho.sh"
airflow db migrate >/dev/null 2>&1

echo "[5/5] Conferindo o DAG"
airflow dags reserialize >/dev/null 2>&1 || true
ERROS="$(airflow dags list-import-errors -o plain 2>/dev/null | grep -v -i 'no data found' || true)"
if [ -n "$ERROS" ]; then echo "ERRO ao carregar dags/:"; echo "$ERROS"; exit 1; fi
airflow dags list -o plain 2>/dev/null | grep -v -i "dag_id" || true
echo
echo "INSTALAÇÃO CONCLUÍDA: Airflow $AIRFLOW_VERSION lendo $PROJETO/dags"
