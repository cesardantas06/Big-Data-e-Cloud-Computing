"""API simulada — Distribuidor — API de Pedidos de Reposição.

Pedidos de reposição feitos pela rede aos distribuidores, com prazos e entregas.

Como iniciar (na pasta do cenário, com o ambiente Python ativado):

    python -m uvicorn api.app:app --host 127.0.0.1 --port 8001

Documentação interativa: http://127.0.0.1:8001/docs

Paginação incremental: cada registro tem um campo `sequencia` crescente.
Guarde a maior sequência já capturada e peça só o que vem depois:

    GET /pedidos-reposicao?after_sequence=1500&limit=500&page=1
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

FONTES = Path(os.getenv("FONTES_DIR", Path(__file__).resolve().parent.parent / "fontes"))
ARQUIVO = FONTES / "api" / "pedidos_reposicao.jsonl"

app = FastAPI(title="Distribuidor — API de Pedidos de Reposição", version="1.0.0", description="Pedidos de reposição feitos pela rede aos distribuidores, com prazos e entregas.")


def registros():
    if not ARQUIVO.exists():
        raise HTTPException(status_code=503, detail=f"Arquivo de dados não encontrado: {ARQUIVO}")
    with ARQUIVO.open("r", encoding="utf-8") as stream:
        for linha in stream:
            if linha.strip():
                yield json.loads(linha)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "arquivo_existe": ARQUIVO.exists()}


@app.get("/pedidos-reposicao")
def listar(
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=1_000),
    after_sequence: int = Query(0, ge=0),
) -> dict:
    inicio = (page - 1) * limit
    itens: list[dict] = []
    elegiveis = 0
    has_more = False
    for registro in registros():
        if int(registro["sequencia"]) <= after_sequence:
            continue
        if elegiveis < inicio:
            elegiveis += 1
            continue
        if len(itens) < limit:
            itens.append(registro)
            elegiveis += 1
            continue
        has_more = True
        break
    return {
        "page": page,
        "limit": limit,
        "count": len(itens),
        "has_more": has_more,
        "after_sequence": after_sequence,
        "items": itens,
    }


@app.get("/pedidos-reposicao/{item_id}")
def obter(item_id: str) -> dict:
    """Devolve a versão mais recente do registro (a de maior sequência)."""
    encontrado = None
    for registro in registros():
        if str(registro.get("pedido_reposicao_id")) == item_id:
            encontrado = registro
    if encontrado is None:
        raise HTTPException(status_code=404, detail="Registro não encontrado")
    return encontrado


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.app:app", host="127.0.0.1", port=8001, reload=False)
