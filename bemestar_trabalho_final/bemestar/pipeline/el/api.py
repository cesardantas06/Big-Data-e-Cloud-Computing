"""EL 5 — API REST (distribuidor) → Landing.

Uso (na pasta do projeto):
    python -m pipeline.el.api                       # usa config.API_URL_PADRAO ou $API_URL
    python -m pipeline.el.api --api-url http://127.0.0.1:8001

Pede à API só os registros com sequencia > última sequência capturada,
página por página, até has_more = false. Grava tudo em UM arquivo:
    data/lake/landing/api/<recurso>/<recurso>_<execucao>.jsonl

Guarda TODAS as versões que chegam (um pedido que muda de status volta com sequência
maior): a escolha da versão vigente é feita na Silver.
"""
from __future__ import annotations

import argparse
import json
import os
import urllib.parse
import urllib.request

from ..config import API_RECURSO, API_URL_PADRAO, CONTROLE, LANDING
from .estado import Execucao

LIMITE_POR_PAGINA = 1000


def capturar(ex: Execucao, api_url: str) -> dict[str, int]:
    ultima = int(ex.estado.get("ultima_sequencia", 0))
    maior = ultima
    destino = LANDING / "api" / API_RECURSO / f"{API_RECURSO}_{ex.id}.jsonl"
    # As páginas vão primeiro para um arquivo temporário FORA da Landing.
    # Se a API cair na página 7, nada pela metade aparece na Landing, e a
    # próxima tentativa recomeça da mesma sequência sem duplicar.
    temporario = CONTROLE / "tmp" / destino.name
    temporario.parent.mkdir(parents=True, exist_ok=True)
    pagina, total = 1, 0
    try:
        with temporario.open("w", encoding="utf-8") as f:
            while True:
                parametros = urllib.parse.urlencode(
                    {"after_sequence": ultima, "limit": LIMITE_POR_PAGINA, "page": pagina})
                url = f"{api_url.rstrip('/')}/{API_RECURSO}?{parametros}"
                with urllib.request.urlopen(url, timeout=30) as resposta:
                    corpo = json.load(resposta)
                for item in corpo["items"]:
                    f.write(json.dumps(item, ensure_ascii=False) + "\n")
                    maior = max(maior, int(item["sequencia"]))
                    total += 1
                if not corpo["has_more"]:
                    break
                pagina += 1
    except BaseException:
        temporario.unlink(missing_ok=True)
        raise
    if total == 0:
        temporario.unlink()
    else:
        destino.parent.mkdir(parents=True, exist_ok=True)
        os.replace(temporario, destino)               # aparece na Landing completo, de uma vez
        ex.estado["ultima_sequencia"] = maior
    return {"registros": total, "paginas": pagina if total else 0, "ultima_sequencia": maior}


def main() -> None:
    parser = argparse.ArgumentParser(description="Captura incremental da API.")
    parser.add_argument("--api-url", default=os.getenv("API_URL", API_URL_PADRAO))
    args = parser.parse_args()
    ex = Execucao("api")
    ex.concluir(capturar(ex, args.api_url))


if __name__ == "__main__":
    main()
