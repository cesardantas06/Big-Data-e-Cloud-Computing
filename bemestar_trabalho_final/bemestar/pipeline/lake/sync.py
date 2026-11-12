"""Envia uma pasta local para o Data Lake (SeaweedFS, protocolo S3).

Uso (na pasta do projeto):
    python -m pipeline.lake.sync --criar-bucket                 # uma vez
    python -m pipeline.lake.sync                                # Landing → landing/...
    python -m pipeline.lake.sync --pasta data/lake/bronze --prefixo bronze
    python -m pipeline.lake.sync --pasta data/lake/silver --prefixo silver --espelhar
    python -m pipeline.lake.sync --listar                       # resumo do bucket

Só envia arquivos NOVOS ou ALTERADOS: o SHA-256 de cada arquivo vai junto,
como metadado do objeto, e é comparado antes do envio.

--espelhar: depois do envio, APAGA do bucket os objetos do prefixo que não existem mais
na pasta local. Use só para camadas DERIVADAS e reconstruídas a cada execução (silver,
quarentena, warehouse, qualidade): sem isso, uma partição que sumiu da Silver continuaria
no Lake e os consumidores leriam dado velho. NUNCA use em landing/ (é o histórico bruto).

Credenciais e endereço vêm de variáveis de ambiente (no Airflow, da Connection):
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_ENDPOINT_URL, S3_BUCKET
"""
from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from ..config import BUCKET, LANDING, RAIZ, S3_ENDPOINT_PADRAO
from ..util import sha256_arquivo


def cliente_s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", S3_ENDPOINT_PADRAO),   # SeaweedFS, não a AWS
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "admin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "bigdata-secret"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


def criar_bucket(s3, bucket: str) -> str:
    try:
        s3.head_bucket(Bucket=bucket)
        return "já existia"
    except ClientError as erro:
        if str(erro.response.get("Error", {}).get("Code")) not in {"404", "NoSuchBucket", "NotFound"}:
            raise                                      # 403 = credencial errada: não esconda
        s3.create_bucket(Bucket=bucket)
        return "criado"


def sincronizar(s3, bucket: str, pasta: Path, prefixo: str, espelhar: bool = False) -> dict[str, int]:
    if not pasta.is_dir():
        raise SystemExit(f"Pasta não encontrada: {pasta}")
    enviados = iguais = 0
    chaves_locais: set[str] = set()
    for arquivo in sorted(p for p in pasta.rglob("*") if p.is_file() and not p.name.endswith(".tmp")):
        chave = f"{prefixo}/{arquivo.relative_to(pasta).as_posix()}"
        chaves_locais.add(chave)
        impressao = sha256_arquivo(arquivo)
        try:
            remoto = s3.head_object(Bucket=bucket, Key=chave)
            if remoto.get("Metadata", {}).get("sha256") == impressao:
                iguais += 1
                continue
        except ClientError as erro:
            if str(erro.response.get("Error", {}).get("Code")) not in {"404", "NoSuchKey", "NotFound"}:
                raise
        s3.upload_file(str(arquivo), bucket, chave, ExtraArgs={"Metadata": {"sha256": impressao}})
        enviados += 1
    resultado = {"enviados": enviados, "ja_estavam_no_lake": iguais}
    if espelhar:
        obsoletos = []
        for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefixo + "/"):
            obsoletos += [o["Key"] for o in pagina.get("Contents", []) if o["Key"] not in chaves_locais]
        for i in range(0, len(obsoletos), 1000):
            s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in obsoletos[i:i + 1000]]})
        resultado["removidos_do_lake"] = len(obsoletos)
    return resultado


def listar(s3, bucket: str) -> dict[str, int]:
    contagem: Counter = Counter()
    for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for objeto in pagina.get("Contents", []):
            contagem[objeto["Key"].split("/", 1)[0]] += 1
    return dict(contagem)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sincroniza uma pasta com o Data Lake.")
    parser.add_argument("--pasta", type=Path, default=LANDING)
    parser.add_argument("--prefixo", default="landing")
    parser.add_argument("--criar-bucket", action="store_true")
    parser.add_argument("--listar", action="store_true")
    parser.add_argument("--espelhar", action="store_true",
                        help="remove do bucket o que não existe mais localmente (só camadas derivadas)")
    args = parser.parse_args()
    bucket = os.getenv("S3_BUCKET", BUCKET)
    s3 = cliente_s3()
    if args.criar_bucket:
        resultado = {"bucket": bucket, "status": criar_bucket(s3, bucket)}
    elif args.listar:
        resultado = {"bucket": bucket, "objetos_por_prefixo": listar(s3, bucket)}
    else:
        pasta = args.pasta if args.pasta.is_absolute() else RAIZ / args.pasta
        resultado = {"bucket": bucket, "prefixo": args.prefixo,
                     **sincronizar(s3, bucket, pasta, args.prefixo, args.espelhar)}
    print(json.dumps(resultado, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
