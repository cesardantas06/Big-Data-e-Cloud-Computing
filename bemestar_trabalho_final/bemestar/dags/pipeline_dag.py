"""DAG da Drogaria Bem-Estar (cenário 3): fontes → Landing → Lake → Bronze → Silver → gate → Gold → produto.

Este arquivo responde a três perguntas, nesta ordem:
    1. ONDE estão os sistemas?  Connections do Airflow (lidas pelo id, sem senha no código)
    2. O QUE executar?          sensores e comandos `python -m pipeline...`
    3. EM QUE ORDEM?            as setas >> no fim do arquivo

Antes de ativar, cadastre no Airflow (Admin):
    Connection  api_fonte      tipo HTTP                 → a API do seu cenário
    Connection  lake_s3        tipo Amazon Web Services  → o SeaweedFS
    Variable    pseudo_secret  a chave da pseudonimização (Etapa 7)
"""
from __future__ import annotations

import shlex
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param, TaskGroup, task
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

PROJETO = Path(__file__).resolve().parents[1]      # a pasta do projeto (dags/ fica dentro dela)
sys.path.insert(0, str(PROJETO))
from pipeline.config import BUCKET  # noqa: E402  (precisa do sys.path acima)

PYTHON = shlex.quote(sys.executable)               # o Python do Airflow, com duckdb, boto3...

# ═════════════════════════════════════════════════════════════════════════
# 1. ONDE: as conexões
# ═════════════════════════════════════════════════════════════════════════
# {{ conn.<id>.<campo> }} é preenchido pelo Airflow na hora de executar a tarefa.
LAKE_ENV = {
    "AWS_ACCESS_KEY_ID": "{{ conn.lake_s3.login }}",
    "AWS_SECRET_ACCESS_KEY": "{{ conn.lake_s3.password }}",
    "S3_ENDPOINT_URL": "{{ conn.lake_s3.extra_dejson.endpoint_url }}",
    "S3_BUCKET": BUCKET,
}
API_URL = "{{ conn.api_fonte.schema or 'http' }}://{{ conn.api_fonte.host }}:{{ conn.api_fonte.port }}"
BASE_ENV = {"PYTHONUTF8": "1"}
CREDENCIAL_RECUSADA = {"403", "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"}


def py(modulo: str, argumentos: str = "") -> str:
    """Monta o comando `python -m <modulo> <argumentos>`."""
    return f"{PYTHON} -m {modulo} {argumentos}".strip()


with DAG(
    dag_id="bemestar_pipeline",
    description="Fontes → Landing → Lake → Bronze → Silver → Gate → Gold → Produto",
    schedule="0 6 * * *",                          # todo dia às 06:00
    start_date=pendulum.datetime(2026, 10, 1, tz="America/Sao_Paulo"),
    catchup=False,                                 # não executa os dias passados ao ligar
    max_active_runs=1,                             # o DuckDB aceita um único escritor
    params={
        "max_quarentena_pct": Param(3, type="number", minimum=0, maximum=100,
                                    description="Percentual máximo de quarentena aceito pelo gate."),
    },
    default_args={
        "owner": "trabalho-final",
        "cwd": str(PROJETO),                       # comandos rodam na pasta do projeto
        "env": BASE_ENV,
        "append_env": True,
        "retries": 0,
    },
    tags=["trabalho-final"],
    doc_md=__doc__,
) as dag:

    # ═════════════════════════════════════════════════════════════════════
    # 2. O QUE: as tarefas
    # ═════════════════════════════════════════════════════════════════════
    with TaskGroup("sensores"):
        aguardar_api = HttpSensor(
            task_id="aguardar_api",
            http_conn_id="api_fonte",
            endpoint="health",
            poke_interval=5, timeout=60, mode="poke",
            retries=10, retry_delay=timedelta(seconds=5),   # API fora do ar → insiste ~1 min
        )

        @task.sensor(task_id="aguardar_lake", poke_interval=5, timeout=60, mode="poke")
        def aguardar_lake() -> bool:
            s3 = S3Hook(aws_conn_id="lake_s3").get_conn()
            try:
                s3.head_bucket(Bucket=BUCKET)
                return True
            except ClientError as erro:
                codigo = str(erro.response.get("Error", {}).get("Code", ""))
                if codigo in CREDENCIAL_RECUSADA:          # senha errada: esperar não resolve
                    raise AirflowFailException(f"SeaweedFS recusou a credencial ({codigo}). Confira a Connection lake_s3.")
                return False
            except NoCredentialsError:
                raise AirflowFailException("Connection lake_s3 sem login/senha.")
            except BotoCoreError:
                return False                               # fora do ar: tenta no próximo poke

        lake_ok = aguardar_lake()

    with TaskGroup("ingestao", default_args={"retries": 1, "retry_delay": timedelta(seconds=10)}):
        el_sqlite = BashOperator(task_id="el_sqlite", bash_command=py("pipeline.el.sqlite"))
        el_csv = BashOperator(task_id="el_csv", bash_command=py("pipeline.el.arquivos", "--pasta csv"))
        el_json = BashOperator(task_id="el_json", bash_command=py("pipeline.el.arquivos", "--pasta json"))
        el_eventos = BashOperator(task_id="el_eventos", bash_command=py("pipeline.el.arquivos", "--pasta events"))
        el_api = BashOperator(task_id="el_api", bash_command=py("pipeline.el.api", f"--api-url {API_URL}"),
                              retries=2)

    sincronizar = BashOperator(task_id="sincronizar_landing", bash_command=py("pipeline.lake.sync"),
                               env={**BASE_ENV, **LAKE_ENV}, retries=2, retry_delay=timedelta(seconds=10))

    with TaskGroup("transformacao"):
        bronze = BashOperator(task_id="bronze", bash_command=py("pipeline.transform.bronze"))
        silver = BashOperator(task_id="silver", bash_command=py("pipeline.transform.silver"))
        gate = BashOperator(task_id="gate_qualidade",
                            bash_command=py("pipeline.transform.gate",
                                            "--max-quarentena-pct {{ params.max_quarentena_pct }}"))

    with TaskGroup("warehouse"):
        gold = BashOperator(task_id="gold", bash_command=py("pipeline.transform.gold"),
                            env={**BASE_ENV, "PSEUDO_CHAVE": "{{ var.value.pseudo_secret }}"})
        validar = BashOperator(task_id="validar", bash_command=py("pipeline.transform.validar"))

    with TaskGroup("publicacao", default_args={"retries": 2, "retry_delay": timedelta(seconds=10)}):
        publicar = [
            BashOperator(task_id=f"publicar_{prefixo}",
                         bash_command=py("pipeline.lake.sync", f"--pasta {pasta} --prefixo {prefixo}" + (" --espelhar" if prefixo != "landing" else "")),
                         env={**BASE_ENV, **LAKE_ENV})
            for pasta, prefixo in [("data/lake/bronze", "bronze"), ("data/lake/silver", "silver"),
                                   ("data/lake/quarentena", "quarentena"), ("data/qualidade", "qualidade"),
                                   ("data/warehouse", "warehouse")]
        ]

    produto = BashOperator(task_id="produto_de_dados", bash_command=py("pipeline.produto.executar"),
                           env={**BASE_ENV, **LAKE_ENV, "PSEUDO_CHAVE": "{{ var.value.pseudo_secret }}"})

    # ═════════════════════════════════════════════════════════════════════
    # 3. EM QUE ORDEM
    # ═════════════════════════════════════════════════════════════════════
    aguardar_api >> el_api
    [el_sqlite, el_csv, el_json, el_eventos, el_api, lake_ok] >> sincronizar
    sincronizar >> bronze >> silver >> gate >> gold >> validar
    validar >> publicar
    validar >> produto
