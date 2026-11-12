"""Estado e registro de uma execução de EL.

Cada EL tem o SEU arquivo de estado em data/lake/controle/estado_<nome>.json.
Assim os cinco ELs podem rodar ao mesmo tempo sem um sobrescrever o outro.

O estado guarda o que já foi capturado (marca d'água, sequência, manifesto de
arquivos). Ele só é gravado no FIM, depois que os dados chegaram à Landing:
se o EL cair no meio, a próxima execução recomeça do ponto anterior.
"""
from __future__ import annotations

import json

from ..config import CONTROLE
from ..util import agora_utc, gravar_json, ler_json, novo_id_execucao


class Execucao:
    def __init__(self, nome: str):
        self.nome = nome
        self.id = novo_id_execucao()
        self.inicio = agora_utc()
        self.arquivo_estado = CONTROLE / f"estado_{nome}.json"
        self.estado: dict = ler_json(self.arquivo_estado, {})

    def concluir(self, resumo: dict) -> None:
        fim = agora_utc()
        registro = {
            "el": self.nome,
            "execucao": self.id,
            "inicio": self.inicio.isoformat(),
            "fim": fim.isoformat(),
            "segundos": round((fim - self.inicio).total_seconds(), 2),
            "resultado": resumo,
        }
        gravar_json(self.arquivo_estado, self.estado)
        gravar_json(CONTROLE / "execucoes" / f"{self.id}_{self.nome}.json", registro)
        print(json.dumps(registro, ensure_ascii=False, indent=2))
