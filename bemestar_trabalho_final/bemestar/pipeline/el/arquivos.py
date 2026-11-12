"""ELs 2, 3 e 4 — arquivos (CSV, JSON, JSONL) → Landing.

Uso (na pasta do projeto), um comando por fonte:
    python -m pipeline.el.arquivos --pasta csv
    python -m pipeline.el.arquivos --pasta json
    python -m pipeline.el.arquivos --pasta events

Regras:
  - arquivo novo                        → copiado com o mesmo caminho relativo;
  - arquivo já capturado e igual        → ignorado (idempotência);
  - arquivo já capturado e DIFERENTE    → copiado como NOVA VERSÃO
    (nome__<execucao>.ext), sem apagar a versão anterior.
A comparação usa o SHA-256 do conteúdo, guardado no "manifesto".
O manifesto só é gravado no fim (Execucao.concluir), depois que as cópias terminaram.
"""
from __future__ import annotations

import argparse

from ..config import FONTES, LANDING, PASTAS_ARQUIVOS
from ..util import copiar_atomico, sha256_arquivo
from .estado import Execucao


def capturar(ex: Execucao, pasta: str) -> dict[str, int]:
    origem = FONTES / pasta
    if not origem.is_dir():
        raise SystemExit(f"Pasta de origem não encontrada: {origem}")
    manifesto: dict[str, str] = ex.estado.setdefault("manifesto", {})
    novos = versoes = iguais = 0
    for arquivo in sorted(p for p in origem.rglob("*") if p.is_file()):
        relativo = arquivo.relative_to(origem).as_posix()
        impressao = sha256_arquivo(arquivo)
        anterior = manifesto.get(relativo)
        if anterior == impressao:
            iguais += 1
            continue
        destino = LANDING / pasta / relativo
        if anterior is None:
            novos += 1
        else:
            destino = destino.with_name(f"{destino.stem}__{ex.id}{destino.suffix}")
            versoes += 1
        copiar_atomico(arquivo, destino)
        manifesto[relativo] = impressao
    return {"novos": novos, "novas_versoes": versoes, "ja_capturados": iguais}


def main() -> None:
    parser = argparse.ArgumentParser(description="Captura arquivos de fontes/<pasta> para a Landing.")
    parser.add_argument("--pasta", required=True, choices=PASTAS_ARQUIVOS)
    args = parser.parse_args()
    ex = Execucao(f"arquivos_{args.pasta}")
    ex.concluir(capturar(ex, args.pasta))


if __name__ == "__main__":
    main()
