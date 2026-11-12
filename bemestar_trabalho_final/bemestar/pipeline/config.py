"""Configuração do projeto — Cenário 3: Drogaria Bem-Estar (rede de farmácias).

Este é o arquivo que muda de um cenário para outro nas etapas 2, 3 e 4.
Todo o resto do código lê os caminhos e os nomes daqui.

Os valores podem ser sobrescritos por variáveis de ambiente (usado nos testes, para
apontar para uma cópia de `fontes/`; em produção/Airflow vale o padrão abaixo).
"""
import os
from pathlib import Path

# ── Pastas ──────────────────────────────────────────────────────────────
RAIZ = Path(__file__).resolve().parents[1]                       # a pasta do projeto
FONTES = Path(os.getenv("BEMESTAR_FONTES", RAIZ / "fontes"))     # dados recebidos: NUNCA escreva aqui
DATA = Path(os.getenv("BEMESTAR_DATA", RAIZ / "data"))           # tudo o que o pipeline gera fica aqui

LAKE = DATA / "lake"
LANDING = LAKE / "landing"          # cópia bruta das fontes (Etapa 2)
BRONZE = LAKE / "bronze"            # Parquet sem limpeza, tudo texto (Etapa 4)
SILVER = LAKE / "silver"            # dados aprovados, tipados e padronizados (Etapa 5)
QUARENTENA = LAKE / "quarentena"    # dados reprovados, com o motivo (Etapa 5)
CONTROLE = LAKE / "controle"        # estado dos ELs e registro das execuções
QUALIDADE = DATA / "qualidade"      # relatório de qualidade e decisão do gate
WAREHOUSE = DATA / "warehouse" / "bemestar.duckdb"
PRODUTOS = DATA / "produtos"        # saídas do produto de dados (Etapa 8)

# ── Fonte 1: SQLite (sistema de vendas) ─────────────────────────────────
BANCO_SQLITE = FONTES / "sqlite" / "bemestar.db"

# tabela: (modo, coluna de controle)
#   "incremental" → captura só as linhas em que a coluna de controle é MAIOR
#                   que a última marca d'água (watermark) guardada
#   "completa"    → lê a tabela inteira, mas só grava na Landing se ela mudou
TABELAS = {
    "lojas": ("completa", None),
    "produtos": ("incremental", "atualizado_em"),
    "clientes": ("incremental", "atualizado_em"),
    "vendas": ("incremental", "atualizado_em"),
    "itens_venda": ("incremental", "item_id"),
}

# ── Fontes 2, 3 e 4: arquivos ───────────────────────────────────────────
PASTAS_ARQUIVOS = ["csv", "json", "events"]     # subpastas de fontes/

# ── Fonte 5: API do distribuidor ────────────────────────────────────────
API_URL_PADRAO = "http://127.0.0.1:8001"        # no Airflow, vem da Connection api_fonte
API_RECURSO = "pedidos-reposicao"               # GET /pedidos-reposicao

# ── Data Lake (SeaweedFS) ───────────────────────────────────────────────
# Credenciais do LABORATÓRIO LOCAL (iguais às do guia). Numa empresa viriam de um cofre.
S3_ENDPOINT_PADRAO = "http://127.0.0.1:8333"    # no Airflow, vem da Connection lake_s3
BUCKET = "bemestar-lake"

# ── Fuso horário de referência do negócio ───────────────────────────────
FUSO = "America/Sao_Paulo"

# ── Tabelas-filhas do SQLite ────────────────────────────────────────────
# filha: (tabela-pai, chave que liga a filha ao pai). Quando uma venda é capturada
# (nova OU alterada), os itens dela são capturados junto, mesmo que o item_id seja antigo.
FILHAS = {
    "itens_venda": ("vendas", "venda_id"),
}
# chave primária de cada tabela do SQLite (usada para ligar filhas aos pais)
CHAVES_PRIMARIAS = {
    "lojas": "loja_id", "produtos": "produto_id", "clientes": "cliente_id",
    "vendas": "venda_id", "itens_venda": "item_id",
}
