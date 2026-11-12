"""Funções Python para usar DENTRO do SQL da Silver.

    from .validacoes import registrar_validacoes
    registrar_validacoes(s.con)
    # depois, numa regra:  "cpf_digitos(o.cpf) IS NULL"
"""
from __future__ import annotations

import re

import duckdb


def cpf_digitos(cpf: str | None) -> str | None:
    """Devolve os 11 dígitos do CPF se ele for VÁLIDO (formato 000.000.000-00 ou só números,
    e os dois dígitos verificadores corretos); senão, None.

    111.111.111-11 "fecha a conta" nos dígitos verificadores, mas é um CPF de teste/placeholder:
    sequências de um único dígito são rejeitadas."""
    if cpf is None:
        return None
    cpf = cpf.strip()
    if not re.fullmatch(r"\d{3}\.\d{3}\.\d{3}-\d{2}|\d{11}", cpf):
        return None
    numeros = [int(c) for c in cpf if c.isdigit()]
    if len(set(numeros)) == 1:
        return None
    for tamanho in (9, 10):
        soma = sum(d * p for d, p in zip(numeros[:tamanho], range(tamanho + 1, 1, -1)))
        if (soma * 10) % 11 % 10 != numeros[tamanho]:
            return None
    return "".join(str(n) for n in numeros)


def registrar_validacoes(con: duckdb.DuckDBPyConnection) -> None:
    # null_handling="special": a função recebe None (e devolve None) em vez de o DuckDB pular a linha
    con.create_function("cpf_digitos", cpf_digitos, ["VARCHAR"], "VARCHAR", null_handling="special")
