"""Funções pequenas usadas em várias etapas."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path


def agora_utc() -> datetime:
    return datetime.now(timezone.utc)


def novo_id_execucao() -> str:
    """Identificador da execução, ex.: 20261104T153012123456Z (com microssegundos,
    para dois programas iniciados no mesmo segundo não terem o mesmo id)."""
    return agora_utc().strftime("%Y%m%dT%H%M%S%fZ")


def sha256_arquivo(caminho: Path) -> str:
    """Impressão digital do conteúdo: muda se UM byte do arquivo mudar."""
    resumo = hashlib.sha256()
    with caminho.open("rb") as f:
        for bloco in iter(lambda: f.read(1024 * 1024), b""):
            resumo.update(bloco)
    return resumo.hexdigest()


def ler_json(caminho: Path, padrao: object) -> object:
    if not caminho.exists():
        return padrao
    return json.loads(caminho.read_text(encoding="utf-8"))


def gravar_json(caminho: Path, valor: object) -> None:
    """Grava de forma ATÔMICA: escreve num arquivo temporário e troca no fim.
    Se o programa cair no meio, o arquivo antigo continua inteiro."""
    caminho.parent.mkdir(parents=True, exist_ok=True)
    temporario = caminho.with_name(caminho.name + ".tmp")
    temporario.write_text(json.dumps(valor, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(temporario, caminho)


def gravar_jsonl(caminho: Path, documentos: list[dict]) -> None:
    """Um JSON por linha, gravado de forma atômica."""
    caminho.parent.mkdir(parents=True, exist_ok=True)
    temporario = caminho.with_name(caminho.name + ".tmp")
    with temporario.open("w", encoding="utf-8") as f:
        for doc in documentos:
            f.write(json.dumps(doc, ensure_ascii=False, default=str) + "\n")
    os.replace(temporario, caminho)


def copiar_atomico(origem: Path, destino: Path) -> None:
    """Copia o CONTEÚDO (copyfile, não copy2): a data de modificação da cópia
    passa a ser o momento da captura, que a Bronze registra em _capturado_em."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_name(destino.name + ".tmp")
    shutil.copyfile(origem, temporario)
    os.replace(temporario, destino)


def caminho_sql(caminho: Path | str) -> str:
    """Caminho no formato que o DuckDB aceita dentro de uma string SQL."""
    return str(caminho).replace("\\", "/").replace("'", "''")
