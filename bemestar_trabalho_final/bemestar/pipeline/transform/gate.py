"""Gate de qualidade: os dados desta execução podem seguir para a Gold?

Uso (na pasta do projeto):
    python -m pipeline.transform.gate --max-quarentena-pct 3

Regras BLOQUEANTES (código de saída 1 → a tarefa do Airflow falha e a Gold não roda):
  1. algum conjunto com quarentena acima do limite (global ou específico do conjunto);
  2. reconciliação INDEPENDENTE do relatório: o que está gravado nos Parquet da Silver e da
     Quarentena precisa bater com o relatório, e
         linhas da Bronze = versões substituídas + Silver + Quarentena
     ("toda linha da Bronze está na Silver ou na quarentena");
  3. volume mínimo: nenhum conjunto pode ficar sem linhas aprovadas.

Por que 3% e não 5%?  Os conjuntos grandes têm quarentena "normal" entre 0% e 1,3%
(ver data/qualidade/relatorio_historico.jsonl). 3% deixa folga de ~2,4× sobre o pior caso
normal, mas bloqueia um problema real (ex.: um sistema de loja passar a mandar a coluna de
quantidade quebrada: >3% do inventário). 5% deixaria passar quatro vezes o normal.

Exceção: `fornecedores` é uma dimensão de 9 linhas; UMA linha duplicada já é 11%. Para ela o limite
é em linhas (no máximo 2 reprovadas), não em porcentagem.
"""
from __future__ import annotations

import argparse
import json
import sys

import duckdb

from ..config import BRONZE, QUALIDADE, QUARENTENA, SILVER
from ..util import agora_utc, caminho_sql, gravar_json, ler_json

LIMITE_PADRAO = 3.0
# conjuntos pequenos demais para um limite percentual: máximo de LINHAS reprovadas
LIMITE_LINHAS = {"fornecedores": 2, "lojas": 2}


def contar(con, pasta) -> int:
    if not pasta.is_dir() or not any(pasta.rglob("*.parquet")):
        return 0
    return con.execute(
        f"SELECT count(*) FROM read_parquet('{caminho_sql(pasta)}/**/*.parquet', union_by_name=true)").fetchone()[0]


def avaliar(limite: float) -> dict:
    relatorio = ler_json(QUALIDADE / "relatorio_qualidade.json", None)
    if relatorio is None:
        raise SystemExit("Relatório de qualidade não encontrado. Rode a Silver antes.")
    con = duckdb.connect()
    bloqueios, resumo = [], {}
    for nome, r in relatorio["conjuntos"].items():
        if nome in LIMITE_LINHAS:
            if r["quarentena"] > LIMITE_LINHAS[nome]:
                bloqueios.append(f"{nome}: {r['quarentena']} linhas em quarentena (limite {LIMITE_LINHAS[nome]} linhas)")
        elif r["percentual_quarentena"] > limite:
            bloqueios.append(f"{nome}: {r['percentual_quarentena']}% em quarentena (limite {limite}%)")
        na_silver, na_quarentena = contar(con, SILVER / nome), contar(con, QUARENTENA / nome)
        if (na_silver, na_quarentena) != (r["aprovadas"], r["quarentena"]):
            bloqueios.append(f"{nome}: reconciliação falhou (Silver {na_silver} × relatório {r['aprovadas']}, "
                             f"Quarentena {na_quarentena} × relatório {r['quarentena']})")
        # conta a Bronze direto dos arquivos, sem confiar no número que a Silver escreveu
        na_bronze = contar(con, BRONZE / r["fonte_bronze"])
        if na_bronze != r["aprovadas"] + r["quarentena"] + (r["versoes_substituidas"] or 0):
            bloqueios.append(f"{nome}: Bronze {na_bronze} ≠ Silver {r['aprovadas']} + Quarentena {r['quarentena']} "
                             f"+ versões substituídas {r['versoes_substituidas']}")
        if r["aprovadas"] == 0:
            bloqueios.append(f"{nome}: nenhuma linha aprovada")
        resumo[nome] = {"bronze": na_bronze, "silver": na_silver, "quarentena": na_quarentena,
                        "versoes_substituidas": r["versoes_substituidas"], "pct_quarentena": r["percentual_quarentena"]}
    con.close()
    return {
        "avaliado_em": agora_utc().isoformat(),
        "limite_quarentena_pct": limite,
        "limites_em_linhas": LIMITE_LINHAS,
        "decisao": "BLOQUEADO" if bloqueios else "APROVADO",
        "bloqueios": bloqueios,
        "conjuntos": resumo,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Gate de qualidade entre Silver e Gold.")
    parser.add_argument("--max-quarentena-pct", type=float, default=LIMITE_PADRAO)
    args = parser.parse_args()
    decisao = avaliar(args.max_quarentena_pct)
    gravar_json(QUALIDADE / "gate.json", decisao)
    with (QUALIDADE / "gate_historico.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps({k: decisao[k] for k in ("avaliado_em", "limite_quarentena_pct", "decisao", "bloqueios")},
                           ensure_ascii=False) + "\n")
    print(json.dumps({k: decisao[k] for k in ("limite_quarentena_pct", "decisao", "bloqueios")}, ensure_ascii=False, indent=2))
    if decisao["bloqueios"]:
        sys.exit(1)                     # ← é ISTO que faz a tarefa do Airflow falhar


if __name__ == "__main__":
    main()
