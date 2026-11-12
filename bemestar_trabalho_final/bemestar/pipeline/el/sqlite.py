"""EL 1 — SQLite → Landing.

Uso (na pasta do projeto):
    python -m pipeline.el.sqlite

Para cada tabela de config.TABELAS grava um arquivo
    data/lake/landing/sqlite/<tabela>/<tabela>_<execucao>.jsonl
com as linhas novas ou alteradas desde a última execução.

Modos (config.TABELAS):
  - "incremental" com coluna de data (termina em "_em"): WHERE datetime(col) > marca.
        datetime() do SQLite converte qualquer offset para UTC, então a comparação
        continua correta mesmo que uma linha chegue com outro fuso (ex.: 'Z').
  - "incremental" com coluna numérica (item_id): WHERE col > marca (só inserções).
  - "completa": lê a tabela inteira e só grava se o conteúdo mudou (hash SHA-256).

Tabelas-filhas (config.FILHAS): além do filtro acima, captura também as linhas cujas
chaves-pai foram capturadas NESTA execução (ex.: itens das vendas novas ou alteradas).
Uma linha capturada duas vezes não é problema: a Silver fica com a versão mais recente.

Idempotência: a marca d'água só avança depois que os arquivos foram gravados
(o estado é salvo no fim, por Execucao.concluir()).
"""
from __future__ import annotations

import hashlib
import json
import sqlite3

from ..config import BANCO_SQLITE, CHAVES_PRIMARIAS, FILHAS, LANDING, TABELAS
from ..util import gravar_jsonl
from .estado import Execucao

MARCA = "_marca_ctrl"   # coluna auxiliar (não vai para a Landing)


def _expressao(coluna: str) -> str:
    """Expressão SQL comparável da coluna de controle."""
    return f"datetime({coluna})" if coluna.endswith("_em") else coluna


def capturar(ex: Execucao) -> dict[str, int]:
    if not BANCO_SQLITE.exists():
        raise SystemExit(f"Banco não encontrado: {BANCO_SQLITE}")
    # mode=ro: abre SOMENTE PARA LEITURA. Um EL nunca altera a fonte.
    con = sqlite3.connect(f"file:{BANCO_SQLITE.as_posix()}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    marcas = ex.estado.setdefault("marcas", {})          # tabela → maior valor já capturado
    impressoes = ex.estado.setdefault("impressoes", {})  # tabela → hash (modo "completa")
    novas_marcas: dict[str, object] = {}
    novas_impressoes: dict[str, str] = {}
    chaves: dict[str, list] = {}                         # tabela → chaves capturadas agora
    gravacoes: list[tuple[str, list[dict]]] = []
    resultado: dict[str, int] = {}
    try:
        for tabela, (modo, coluna) in TABELAS.items():
            pk = CHAVES_PRIMARIAS[tabela]
            if modo == "incremental":
                expr, marca = _expressao(coluna), marcas.get(tabela)
                filtro, parametros = ("", ()) if marca is None else (f"WHERE {expr} > ?", (marca,))
                brutas = [dict(l) for l in con.execute(
                    f"SELECT *, {expr} AS {MARCA} FROM {tabela} {filtro} ORDER BY {expr}", parametros)]
                if brutas:
                    novas_marcas[tabela] = max(l[MARCA] for l in brutas)
                linhas = [{k: v for k, v in l.items() if k != MARCA} for l in brutas]
                if tabela in FILHAS and chaves.get(FILHAS[tabela][0]):
                    pai, chave_pai = FILHAS[tabela]
                    ja = {l[pk] for l in linhas}
                    ids = chaves[pai]
                    for i in range(0, len(ids), 500):
                        bloco = ids[i:i + 500]
                        marcadores = ",".join("?" * len(bloco))
                        for l in con.execute(f"SELECT * FROM {tabela} WHERE {chave_pai} IN ({marcadores})", bloco):
                            if l[pk] not in ja:
                                linhas.append(dict(l))
                                ja.add(l[pk])
            else:
                linhas = [dict(l) for l in con.execute(f"SELECT * FROM {tabela}")]
                impressao = hashlib.sha256(
                    json.dumps(linhas, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
                if impressoes.get(tabela) == impressao:  # tabela igual à última captura
                    linhas = []
                else:
                    novas_impressoes[tabela] = impressao
            if linhas:
                gravacoes.append((tabela, linhas))
                chaves[tabela] = [l[pk] for l in linhas]
            resultado[tabela] = len(linhas)
    finally:
        con.close()

    # Só DEPOIS de ler tudo gravamos os arquivos; o estado é gravado por último (em concluir()).
    for tabela, linhas in gravacoes:
        gravar_jsonl(LANDING / "sqlite" / tabela / f"{tabela}_{ex.id}.jsonl", linhas)
    marcas.update(novas_marcas)
    impressoes.update(novas_impressoes)
    return resultado


def main() -> None:
    ex = Execucao("sqlite")
    ex.concluir(capturar(ex))


if __name__ == "__main__":
    main()
