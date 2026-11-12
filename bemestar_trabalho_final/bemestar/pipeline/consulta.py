"""Consulta rápida no Warehouse ou em Parquet, sem abrir outro programa.

Uso (na pasta do projeto):
    python -m pipeline.consulta "SELECT * FROM gold.mart_perda_vencimento LIMIT 5"
    python -m pipeline.consulta sql/perguntas_negocio.sql
    python -m pipeline.consulta "SELECT count(*) FROM 'data/lake/silver/vendas/**/*.parquet'"

Abre o DuckDB em modo SOMENTE LEITURA (não atrapalha o DAG).
Num arquivo .sql, as consultas são separadas por ';' e os comentários '--' são exibidos.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb

from .config import FUSO, WAREHOUSE


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    argumento = sys.argv[1]
    arquivo = Path(argumento)
    texto = arquivo.read_text(encoding="utf-8") if argumento.endswith(".sql") and arquivo.exists() else argumento
    con = duckdb.connect(str(WAREHOUSE), read_only=True) if WAREHOUSE.exists() else duckdb.connect()
    con.execute(f"SET TimeZone = '{FUSO}'")
    for consulta in (c.strip() for c in texto.split(";")):
        linhas = consulta.splitlines()
        comentarios = [l for l in linhas if l.strip().startswith("--")]
        codigo = [l for l in linhas if l.strip() and not l.strip().startswith("--")]
        if comentarios:
            print("\n" + "\n".join(comentarios))
        if codigo:
            con.sql("\n".join(codigo)).show(max_rows=40, max_width=220)
    con.close()


if __name__ == "__main__":
    main()
