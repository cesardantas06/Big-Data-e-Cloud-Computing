"""Landing → Bronze (Parquet).

Uso (na pasta do projeto):
    python -m pipeline.transform.bronze

Cada arquivo da Landing vira UM Parquet na Bronze, no mesmo caminho relativo:
    landing/events/pdv_2026-06-01.jsonl → bronze/events/pdv_2026-06-01.parquet

Regras da Bronze:
  - NÃO limpa nada: todos os valores entram como TEXTO, exatamente como vieram.
    (Se o DuckDB adivinhar os tipos, '2026-06-01T09:59:38-03:00' vira um TIMESTAMP
     sem fuso e a informação de fuso se perde em silêncio.)
  - Exceção: JSON aninhado (listas dentro de objetos) mantém a estrutura (não ocorre aqui).
  - Acrescenta colunas de linhagem:
      _arquivo_origem  o arquivo da Landing de onde a linha veio
      _capturado_em    quando o EL trouxe esse arquivo (data de modificação na Landing)
      _carregado_em    quando esta execução da Bronze rodou
  - Incremental: arquivo que já tem Parquet atualizado é pulado.
  - Reconstruível: apagar data/lake/bronze e rodar de novo gera a mesma Bronze.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import duckdb

from ..config import BRONZE, LANDING
from ..util import agora_utc, caminho_sql

EXTENSOES = {".csv", ".json", ".jsonl"}


def leitura_como_texto(con: duckdb.DuckDBPyConnection, arquivo) -> str:
    """Devolve a expressão SQL que lê o arquivo com todas as colunas como VARCHAR."""
    p = caminho_sql(arquivo)
    if arquivo.suffix.lower() == ".csv":
        # all_varchar: nada de adivinhar tipos. O separador (, ou ;) é detectado sozinho.
        return f"read_csv('{p}', header=true, all_varchar=true)"
    formato = "newline_delimited" if arquivo.suffix.lower() == ".jsonl" else "array"
    descricao = con.execute(
        f"DESCRIBE SELECT * FROM read_json_auto('{p}', format='{formato}', sample_size=-1)").fetchall()
    aninhado = any(tipo.startswith(("STRUCT", "MAP")) or tipo.endswith("]") for _, tipo, *_ in descricao)
    if aninhado:
        return f"read_json_auto('{p}', format='{formato}', sample_size=-1)"
    colunas = ", ".join(f"'{nome}': 'VARCHAR'" for nome, *_ in descricao)
    return f"read_json('{p}', format='{formato}', columns={{{colunas}}})"


def construir() -> dict[str, int]:
    if not LANDING.is_dir():
        raise SystemExit(f"Landing não encontrada: {LANDING}")
    con = duckdb.connect()
    carregado_em = agora_utc().isoformat()
    convertidos = pulados = linhas = 0
    for arquivo in sorted(p for p in LANDING.rglob("*") if p.suffix.lower() in EXTENSOES):
        relativo = arquivo.relative_to(LANDING)
        destino = (BRONZE / relativo).with_suffix(".parquet")
        if destino.exists() and destino.stat().st_mtime >= arquivo.stat().st_mtime:
            pulados += 1
            continue
        capturado_em = datetime.fromtimestamp(arquivo.stat().st_mtime, tz=timezone.utc).isoformat()
        consulta = (
            f"SELECT *, '{caminho_sql(relativo.as_posix())}' AS _arquivo_origem, "
            f"TIMESTAMPTZ '{capturado_em}' AS _capturado_em, "
            f"TIMESTAMPTZ '{carregado_em}' AS _carregado_em "
            f"FROM {leitura_como_texto(con, arquivo)}"
        )
        destino.parent.mkdir(parents=True, exist_ok=True)
        temporario = destino.with_name(destino.name + ".tmp")
        con.execute(f"COPY ({consulta}) TO '{caminho_sql(temporario)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        os.replace(temporario, destino)
        linhas += con.execute(f"SELECT count(*) FROM read_parquet('{caminho_sql(destino)}')").fetchone()[0]
        convertidos += 1
    con.close()
    return {"convertidos": convertidos, "pulados": pulados, "linhas_novas": linhas}


def main() -> None:
    print(json.dumps(construir(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
