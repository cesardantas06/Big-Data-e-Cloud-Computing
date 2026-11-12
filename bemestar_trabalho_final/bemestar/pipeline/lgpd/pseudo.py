"""Pseudonimização com HMAC-SHA256.

    pseudo('1512')  →  '9f2c1a...'   (sempre o MESMO resultado para a mesma entrada)

Por que não basta sha256(email)? Porque qualquer pessoa pode calcular o sha256
de uma lista de e-mails/CPFs conhecidos e comparar ("ataque de dicionário"). Com HMAC,
o resultado depende de uma CHAVE SECRETA: sem a chave, não há como refazer a conta.

A chave vem da variável de ambiente PSEUDO_CHAVE e NUNCA fica no código, no Git
ou no Data Lake. No Airflow, ela vem da Variable `pseudo_secret`.

Dado pseudonimizado CONTINUA sendo dado pessoal (LGPD art. 13, § 4º): quem tem a chave
consegue religar. Por isso a chave fica separada e o código só roda onde ela existe.
"""
from __future__ import annotations

import hashlib
import hmac
import os

import duckdb


def _chave() -> bytes:
    chave = os.getenv("PSEUDO_CHAVE")
    if not chave:
        raise SystemExit("Defina a variável de ambiente PSEUDO_CHAVE antes de construir a Silver/Gold.")
    return chave.encode("utf-8")


def pseudonimizar(valor: str | None) -> str | None:
    if valor is None:
        return None
    return hmac.new(_chave(), str(valor).encode("utf-8"), hashlib.sha256).hexdigest()[:20]


def registrar(con: duckdb.DuckDBPyConnection) -> None:
    """Disponibiliza pseudo(texto) dentro do SQL do DuckDB."""
    _chave()                                   # falha cedo, com mensagem clara
    con.create_function("pseudo", pseudonimizar, ["VARCHAR"], "VARCHAR")
