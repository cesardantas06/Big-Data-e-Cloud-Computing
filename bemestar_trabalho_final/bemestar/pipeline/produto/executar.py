"""Produto de dados — ponto de entrada chamado pelo DAG.

Uso (na pasta do projeto):
    python -m pipeline.produto.executar

O produto do Cenário 3 é o relatório diário de reposição (Etapa 8.4): gera o HTML a partir da
Gold e o publica no Lake em relatorios/, sem apagar os anteriores.
"""
from __future__ import annotations

from .relatorio import main

if __name__ == "__main__":
    main()
