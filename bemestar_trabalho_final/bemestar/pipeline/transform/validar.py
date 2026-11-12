"""Valida a Gold depois de construída. Sai com código 1 se algo estiver errado.

Uso (na pasta do projeto):
    python -m pipeline.transform.validar

Verifica (1) o GRÃO de cada fato (nenhuma linha repetida), (2) a INTEGRIDADE das chaves substitutas,
(3) a RECONCILIAÇÃO com a Silver (a Gold não perde nem cria linhas/valores),
(4) invariantes dos marts e (5) LGPD: nenhuma coluna pessoal na Gold e nenhuma célula pequena de
receita retida exposta.
"""
from __future__ import annotations

import json
import sys

import duckdb

from ..config import FUSO, QUALIDADE, QUARENTENA, SILVER, WAREHOUSE
from ..util import agora_utc, caminho_sql, gravar_json

GRAOS = {
    "fato_venda_item": "item_id",
    "fato_estoque_lote": "sk_loja, sk_produto, lote_codigo, data_contagem",
    "fato_reposicao": "pedido_reposicao_id",
    "fato_devolucao": "evento_id",
    "mart_estoque_lote": "sk_loja, sk_produto, lote_codigo",
    "mart_estoque_loja_produto": "sk_loja, sk_produto",
    "mart_risco_vencimento": "sk_loja, sk_produto, lote_codigo",
    "mart_perda_vencimento": "sk_loja, sk_produto, lote_codigo",
    "mart_pontualidade_fornecedor": "sk_fornecedor",
    "mart_transferencia": "sk_produto, sk_loja_origem, sk_loja_destino",
    "dim_loja": "loja_id", "dim_produto": "produto_id", "dim_fornecedor": "fornecedor_id", "dim_data": "sk_data",
}
# (tabela, coluna, dimensão referenciada)
CHAVES = [
    ("fato_venda_item", "sk_data", "dim_data"), ("fato_venda_item", "sk_loja", "dim_loja"), ("fato_venda_item", "sk_produto", "dim_produto"),
    ("fato_estoque_lote", "sk_data", "dim_data"), ("fato_estoque_lote", "sk_loja", "dim_loja"), ("fato_estoque_lote", "sk_produto", "dim_produto"),
    ("fato_reposicao", "sk_data", "dim_data"), ("fato_reposicao", "sk_loja", "dim_loja"),
    ("fato_reposicao", "sk_produto", "dim_produto"), ("fato_reposicao", "sk_fornecedor", "dim_fornecedor"),
    ("fato_devolucao", "sk_loja", "dim_loja"), ("fato_devolucao", "sk_produto", "dim_produto"),
    ("dim_produto", "sk_fornecedor", "dim_fornecedor"),
    ("mart_estoque_loja_produto", "sk_loja", "dim_loja"), ("mart_estoque_loja_produto", "sk_produto", "dim_produto"),
    ("mart_transferencia", "sk_loja_origem", "dim_loja"), ("mart_transferencia", "sk_loja_destino", "dim_loja"),
]
COLUNAS_PROIBIDAS = {"cliente_id", "cliente_pseudo", "cpf", "cpf_pseudo", "email", "telefone", "data_nascimento",
                     "operador_id", "cliente_identificado", "contato_email", "cnpj"}


def validar() -> dict:
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    con.execute(f"SET TimeZone = '{FUSO}'")
    s = lambda n: f"read_parquet('{caminho_sql(SILVER / n)}/**/*.parquet', union_by_name=true)"
    resultados: list[dict] = []

    def checar(nome: str, ok: bool, detalhe: str = "") -> None:
        resultados.append({"verificacao": nome, "ok": bool(ok), "detalhe": detalhe})

    for tabela, chave in GRAOS.items():
        n, d = con.execute(f"SELECT count(*), count(DISTINCT ({chave})) FROM gold.{tabela}").fetchone()
        checar(f"grão único: {tabela} ({chave})", n == d, f"{n} linhas, {d} chaves distintas")
    for tabela, coluna, dim in CHAVES:
        chave_dim = coluna.replace("_origem", "").replace("_destino", "")      # sk_loja_origem → sk_loja
        nulas, orfas = con.execute(f"""SELECT count(*) FILTER (WHERE t.{coluna} IS NULL),
                                              count(*) FILTER (WHERE t.{coluna} IS NOT NULL AND d.{chave_dim} IS NULL)
                                       FROM gold.{tabela} t LEFT JOIN gold.{dim} d ON d.{chave_dim} = t.{coluna}""").fetchone()
        checar(f"chave {tabela}.{coluna} → {dim}", nulas == 0 and orfas == 0, f"{nulas} nulas, {orfas} órfãs")

    # reconciliação com a Silver
    q = lambda sql: con.execute(sql).fetchone()
    silver_itens = q(f"SELECT count(*), sum(quantidade) FROM {s('itens_venda')}")
    gold_itens = q("SELECT count(*), sum(quantidade) FROM gold.fato_venda_item")
    checar("Gold itens = Silver itens (linhas e unidades)", silver_itens == gold_itens, f"Silver {silver_itens} × Gold {gold_itens}")
    sv = q(f"SELECT sum(valor_total_centavos) FROM {s('vendas')} WHERE status = 'concluida'")[0]
    gv = q("SELECT sum(valor_centavos) FROM gold.fato_venda_item WHERE NOT venda_cancelada")[0]
    # Os itens que foram para a quarentena (ex.: produto inexistente) ainda compõem o total da venda:
    # total da venda = itens na Gold + itens na quarentena. Nada some sem explicação.
    qv = q(f"""SELECT coalesce(sum(try_cast(i.quantidade AS BIGINT) * try_cast(i.preco_unitario_centavos AS BIGINT)), 0)
               FROM read_parquet('{caminho_sql(QUARENTENA / 'itens_venda')}/**/*.parquet', union_by_name=true) i
               JOIN {s('vendas')} v ON v.venda_id = try_cast(i.venda_id AS BIGINT) WHERE v.status = 'concluida'""")[0]
    checar("valor das vendas concluídas = itens na Gold + itens na quarentena", sv == gv + qv,
           f"total das vendas {sv} × Gold {gv} + quarentena {qv}")
    checar("Gold estoque = Silver inventário",
           q(f"SELECT count(*) FROM {s('inventario')}")[0] == q("SELECT count(*) FROM gold.fato_estoque_lote")[0])
    checar("Gold reposição = Silver pedidos (versão vigente, sem duplicar)",
           q(f"SELECT count(*) FROM {s('pedidos_reposicao')}")[0] == q("SELECT count(*) FROM gold.fato_reposicao")[0])
    checar("Gold produtos = Silver produtos (reajuste substitui, não duplica)",
           q(f"SELECT count(*) FROM {s('produtos')}")[0] == q("SELECT count(*) FROM gold.dim_produto")[0])
    checar("pedidos em trânsito têm status coerente (sem data de entrega)",
           q("SELECT count(*) FROM gold.fato_reposicao WHERE (status = 'em_transito') <> (entregue_em IS NULL)")[0] == 0)

    # invariantes dos marts
    checar("estoque estimado nunca negativo", q("SELECT count(*) FROM gold.mart_estoque_lote WHERE estoque_estimado < 0")[0] == 0)
    checar("sobra na validade ≤ estoque do lote",
           q("SELECT count(*) FROM gold.mart_risco_vencimento WHERE sobra_na_validade > estoque_estimado OR sobra_na_validade < 0")[0] == 0)
    checar("perda por vencimento: unidades e valor positivos",
           q("SELECT count(*) FROM gold.mart_perda_vencimento WHERE unidades_perdidas <= 0 OR valor_perdido_centavos <= 0")[0] == 0)
    checar("um lote nunca está ao mesmo tempo em perda (já venceu) e em risco (ainda na prateleira)",
           q("""SELECT count(*) FROM gold.mart_perda_vencimento p
                JOIN gold.mart_risco_vencimento r USING (sk_loja, sk_produto, lote_codigo)""")[0] == 0)
    checar("pontualidade: % no prazo entre 0 e 100",
           q("SELECT count(*) FROM gold.mart_pontualidade_fornecedor WHERE pct_no_prazo NOT BETWEEN 0 AND 100")[0] == 0)
    checar("transferência: origem ≠ destino e quantidade > 0",
           q("SELECT count(*) FROM gold.mart_transferencia WHERE sk_loja_origem = sk_loja_destino OR quantidade <= 0")[0] == 0)
    checar("transferência não excede a necessidade do destino",
           q("""SELECT count(*) FROM (SELECT t.sk_produto, t.sk_loja_destino, sum(t.quantidade) q FROM gold.mart_transferencia t
                GROUP BY ALL) x JOIN gold.mart_estoque_loja_produto m ON m.sk_produto = x.sk_produto AND m.sk_loja = x.sk_loja_destino
                WHERE x.q > m.qtd_sugerida""")[0] == 0)

    # LGPD
    colunas = {c.lower() for (c,) in con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'gold'").fetchall()}
    proibidas = sorted(colunas & COLUNAS_PROIBIDAS)
    checar("LGPD: nenhuma coluna de pessoa na Gold", not proibidas, f"encontradas: {proibidas}" if proibidas else "nenhuma")
    nomes_em_fato = con.execute("""SELECT table_name FROM information_schema.columns WHERE table_schema = 'gold'
                                   AND column_name = 'nome' AND table_name NOT IN ('dim_loja', 'dim_produto', 'dim_fornecedor', 'mart_pontualidade_fornecedor')""").fetchall()
    checar("LGPD: coluna 'nome' só em dimensões de loja/produto/fornecedor", not nomes_em_fato, str(nomes_em_fato))
    checar("LGPD: receita retida só agregada, sem célula com menos de k eventos",
           q("SELECT count(*) FROM gold.mart_receita_retida m, gold.parametros p WHERE NOT m.suprimido AND m.eventos < p.k_minimo_receita")[0] == 0)
    con.close()
    falhas = [r for r in resultados if not r["ok"]]
    return {"validado_em": agora_utc().isoformat(), "total": len(resultados), "falhas": len(falhas), "verificacoes": resultados}


def main() -> None:
    resultado = validar()
    gravar_json(QUALIDADE / "validacao_gold.json", resultado)
    for r in resultado["verificacoes"]:
        print(("OK    " if r["ok"] else "FALHA ") + r["verificacao"] + (f"  [{r['detalhe']}]" if r["detalhe"] and not r["ok"] else ""))
    print(f"\n{resultado['total'] - resultado['falhas']}/{resultado['total']} verificações passaram.")
    if resultado["falhas"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
