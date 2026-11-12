"""PRODUTO — relatório diário de reposição, gerado pelo DAG e publicado no Data Lake.

Uso (na pasta do projeto):
    python -m pipeline.produto.relatorio                  # gera e publica
    python -m pipeline.produto.relatorio --sem-publicar   # só gera (para testar)

Lê SOMENTE a Gold (somente leitura, conexão curta). "Hoje" = gold.parametros.data_ref
(a última data de venda dos dados). Gera:
    data/produtos/relatorios/relatorio_AAAA-MM-DD.html     ← o arquivo que o comprador abre
    data/produtos/listas/repor_AAAA-MM-DD.csv              ← foto da lista "Repor" (para medir acerto depois)
    data/produtos/listas/risco_AAAA-MM-DD.csv              ← foto da lista de risco de vencimento
e envia para o bucket em relatorios/, SEM apagar os anteriores (cada dia tem o seu arquivo).

Regra de reposição (justificativa no relatório do trabalho, seção Produto):
    repor quando  (estoque vendável + em trânsito) / vendas por dia  <  prazo real p90 do distribuidor + folga
    quantidade    vendas por dia × (prazo p90 + ciclo) − estoque − em trânsito   (nível "pedir até")
"""
from __future__ import annotations

import argparse
import html
import os
from datetime import date

import duckdb
import pandas as pd

from ..config import BUCKET, FUSO, PRODUTOS, WAREHOUSE
from ..lake.sync import cliente_s3


# ── formatação pt-BR ─────────────────────────────────────────────────────
def inteiro(v) -> str:
    return "—" if pd.isna(v) else f"{int(v):,}".replace(",", ".")


def decimal(v, casas=1) -> str:
    return "—" if pd.isna(v) else f"{v:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def reais(centavos) -> str:
    return "—" if pd.isna(centavos) else "R$ " + decimal(centavos / 100, 2)


def data_br(v) -> str:
    return "—" if pd.isna(v) else pd.Timestamp(v).strftime("%d/%m/%Y")


def tabela(df: pd.DataFrame, colunas: list[tuple[str, str, callable]], vazio: str, classe_linha=None) -> str:
    """colunas: (coluna, título, formatador). Cada célula é escapada."""
    if df.empty:
        return f"<p class='vazio'>{html.escape(vazio)}</p>"
    cab = "".join(f"<th>{html.escape(t)}</th>" for _, t, _ in colunas)
    linhas = []
    for _, r in df.iterrows():
        cls = f" class='{classe_linha(r)}'" if classe_linha and classe_linha(r) else ""
        cel = "".join(f"<td class='{'n' if f in (inteiro, decimal, reais) else ''}'>{html.escape(str(f(r[c])))}</td>"
                      for c, _, f in colunas)
        linhas.append(f"<tr{cls}>{cel}</tr>")
    return f"<div class='rolagem'><table><thead><tr>{cab}</tr></thead><tbody>{''.join(linhas)}</tbody></table></div>"


texto = lambda v: "—" if pd.isna(v) else str(v)

ESTILO = """<style>
:root{--fg:#1d2733;--mut:#5d6b7a;--bg:#fff;--lin:#d9e0e7;--cab:#eef2f6;--ruptura:#fde8e8;--repor:#fff4de;--ok:#e6f4ea;--acc:#0b6b5d}
@media (prefers-color-scheme: dark){:root{--fg:#e6ebf0;--mut:#9aa8b6;--bg:#12181f;--lin:#2b3642;--cab:#1b242e;--ruptura:#4a2323;--repor:#4a3d1d;--ok:#1d3a28;--acc:#5fd0bd}}
*{box-sizing:border-box}body{font-family:system-ui,-apple-system,Segoe UI,sans-serif;margin:0;padding:1.5rem 1rem 4rem;background:var(--bg);color:var(--fg);line-height:1.45}
main{max-width:1180px;margin:0 auto}h1{font-size:1.6rem;margin:.2rem 0}h2{font-size:1.2rem;margin:2.2rem 0 .3rem;border-bottom:2px solid var(--acc);padding-bottom:.25rem}
h3{font-size:1rem;margin:1.3rem 0 .3rem}p.sub,p.nota{color:var(--mut);margin:.2rem 0 .8rem;font-size:.92rem}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:.7rem;margin:1rem 0}
.card{border:1px solid var(--lin);border-radius:8px;padding:.7rem .9rem;background:var(--cab)}.card b{display:block;font-size:1.5rem}.card span{font-size:.82rem;color:var(--mut)}
.rolagem{overflow-x:auto;margin:.4rem 0 1rem}table{border-collapse:collapse;width:100%;font-size:.88rem}
th,td{border:1px solid var(--lin);padding:.3rem .55rem;text-align:left;vertical-align:top}th{background:var(--cab);position:sticky;top:0}td.n{text-align:right;font-variant-numeric:tabular-nums}
tr.ruptura td{background:var(--ruptura)}tr.repor td{background:var(--repor)}tr.ok td{background:var(--ok)}.vazio{color:var(--mut);font-style:italic}
footer{margin-top:3rem;color:var(--mut);font-size:.82rem}code{background:var(--cab);padding:0 .25rem;border-radius:3px}
@media print{body{padding:0}h2{break-after:avoid}tr{break-inside:avoid}}
</style>"""


def consultar(con) -> dict[str, pd.DataFrame]:
    """Todas as consultas do relatório. Só tabelas gold.*"""
    q = lambda sql: con.execute(sql).df()
    d = {}
    d["parametros"] = q("SELECT * FROM gold.parametros")
    # Quanto cada loja já vai receber por transferência (para não pedir duas vezes a mesma coisa)
    d["repor"] = q("""
        WITH recebe AS (SELECT sk_produto, sk_loja_destino AS sk_loja, sum(quantidade) AS transf FROM gold.mart_transferencia GROUP BY ALL)
        SELECT f.nome AS fornecedor, l.nome AS loja, l.cidade, p.nome AS produto, p.categoria, p.controlado,
               m.situacao, m.estoque_vendavel, m.em_transito, m.taxa_diaria, m.dias_estoque, m.dias_posicao,
               m.prazo_p90_dias, m.qtd_sugerida, coalesce(r.transf, 0) AS transferencia_recebida,
               greatest(m.qtd_sugerida - coalesce(r.transf, 0), 0) AS qtd_a_pedir, p.preco_centavos,
               greatest(m.qtd_sugerida - coalesce(r.transf, 0), 0) * p.preco_centavos AS valor_estimado_centavos,
               f.pedido_minimo_reais
        FROM gold.mart_estoque_loja_produto m
        JOIN gold.dim_loja l USING (sk_loja) JOIN gold.dim_produto p USING (sk_produto)
        JOIN gold.dim_fornecedor f ON f.sk_fornecedor = m.sk_fornecedor
        LEFT JOIN recebe r USING (sk_loja, sk_produto)
        WHERE m.situacao IN ('ruptura', 'repor') AND m.qtd_sugerida > 0
        ORDER BY f.nome, (m.situacao = 'ruptura') DESC, m.dias_posicao NULLS FIRST, l.nome""")
    d["transferir"] = q("""
        SELECT p.nome AS produto, lo.nome AS origem, ld.nome AS destino, lo.cidade, t.quantidade,
               t.dias_estoque_origem, t.dias_estoque_destino
        FROM gold.mart_transferencia t JOIN gold.dim_produto p USING (sk_produto)
        JOIN gold.dim_loja lo ON lo.sk_loja = t.sk_loja_origem JOIN gold.dim_loja ld ON ld.sk_loja = t.sk_loja_destino
        ORDER BY t.quantidade DESC, p.nome""")
    d["risco"] = q("""
        SELECT l.nome AS loja, p.nome AS produto, r.lote_codigo, r.validade, r.dias_ate_vencer, r.estoque_estimado,
               r.taxa_diaria, r.sobra_na_validade, r.valor_em_risco_centavos, r.faixa
        FROM gold.mart_risco_vencimento r JOIN gold.dim_loja l USING (sk_loja) JOIN gold.dim_produto p USING (sk_produto), gold.parametros k
        WHERE r.sobra_na_validade > 0 AND r.dias_ate_vencer <= k.horizonte_risco_dias
        ORDER BY r.validade, r.valor_em_risco_centavos DESC""")
    d["fornecedores"] = q("""
        SELECT nome, prazo_cadastro_dias, pedidos_entregues, pct_no_prazo, atraso_medio_dos_atrasados_dias,
               prazo_real_medio_dias, prazo_real_p90_dias, em_transito, em_transito_atrasados
        FROM gold.mart_pontualidade_fornecedor ORDER BY pct_no_prazo""")
    d["perda_mes"] = q("""
        SELECT mes_vencimento, count(*) AS lotes, sum(unidades_perdidas) AS unidades, sum(valor_perdido_centavos) AS valor_centavos
        FROM gold.mart_perda_vencimento GROUP BY 1 ORDER BY 1""")
    d["perda_loja"] = q("""
        SELECT l.nome AS loja, count(*) AS lotes, sum(m.unidades_perdidas) AS unidades, sum(m.valor_perdido_centavos) AS valor_centavos
        FROM gold.mart_perda_vencimento m JOIN gold.dim_loja l USING (sk_loja) GROUP BY 1 ORDER BY valor_centavos DESC LIMIT 8""")
    d["perda_categoria"] = q("""
        SELECT p.categoria, sum(m.unidades_perdidas) AS unidades, sum(m.valor_perdido_centavos) AS valor_centavos
        FROM gold.mart_perda_vencimento m JOIN gold.dim_produto p USING (sk_produto) GROUP BY 1 ORDER BY valor_centavos DESC""")
    d["excesso"] = q("SELECT count(*) AS n FROM gold.mart_estoque_loja_produto WHERE situacao = 'excesso'")
    return d


def gerar_html(d: dict[str, pd.DataFrame], dia: date) -> str:
    par = d["parametros"].iloc[0]
    repor, transf, risco, forn = d["repor"], d["transferir"], d["risco"], d["fornecedores"]
    rupturas = int((repor.situacao == "ruptura").sum())
    a_pedir = repor[repor.qtd_a_pedir > 0]
    perda_total = d["perda_mes"].valor_centavos.sum() if not d["perda_mes"].empty else 0
    perda_un = d["perda_mes"].unidades.sum() if not d["perda_mes"].empty else 0
    critico = risco[risco.faixa == "critico_ate_30d"]
    p = []
    p.append(f"<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
             f"<title>Reposição {dia.isoformat()} — Drogaria Bem-Estar</title>{ESTILO}</head><body><main>")
    p.append(f"<h1>Relatório diário de reposição — Drogaria Bem-Estar</h1>"
             f"<p class='sub'>Posição de <b>{dia.strftime('%d/%m/%Y')}</b> (último dia de vendas nos dados) · 20 lojas · "
             f"vendas médias dos últimos {int(par.janela_taxa_dias)} dias · gerado só a partir da Gold</p>")
    p.append("<div class='cards'>"
             f"<div class='card'><b>{rupturas}</b><span>rupturas: sem estoque e sem pedido a caminho</span></div>"
             f"<div class='card'><b>{len(a_pedir)}</b><span>itens a pedir ({inteiro(a_pedir.qtd_a_pedir.sum())} un.)</span></div>"
             f"<div class='card'><b>{len(transf)}</b><span>transferências sugeridas ({inteiro(transf.quantidade.sum()) if len(transf) else 0} un.)</span></div>"
             f"<div class='card'><b>{len(critico)}</b><span>lotes com sobra vencendo em ≤ 30 dias</span></div>"
             f"<div class='card'><b>{reais(risco.valor_em_risco_centavos.sum())}</b><span>em risco de vencer (≤ {int(par.horizonte_risco_dias)} dias)</span></div>"
             f"<div class='card'><b>{reais(perda_total)}</b><span>perdidos por vencimento desde jun/2026 ({inteiro(perda_un)} un.)</span></div>"
             "</div>")

    # ── 1. REPOR ──
    p.append("<h2>1. Repor</h2><p class='sub'>Ordenado por distribuidor, rupturas primeiro, depois menor cobertura. "
             "<b>Dias de estoque</b> = estoque vendável ÷ vendas por dia. A quantidade é o nível "
             "“vendas/dia × (prazo p90 do distribuidor + ciclo de reposição)” menos o estoque, o que já está em trânsito e as transferências recebidas.</p>")
    if repor.empty:
        p.append("<p class='vazio'>Nada a repor hoje.</p>")
    for fornecedor, g in repor.groupby("fornecedor", sort=True):
        total = g.valor_estimado_centavos.sum()
        minimo = g.pedido_minimo_reais.iloc[0]
        status = "atinge o pedido mínimo" if total / 100 >= minimo else "abaixo do pedido mínimo: junte com o próximo pedido, exceto rupturas"
        p.append(f"<h3>{html.escape(fornecedor)} — {inteiro(g.qtd_a_pedir.sum())} un. · {reais(total)} · "
                 f"pedido mínimo {reais(minimo * 100)} ({status}) · prazo p90 {int(g.prazo_p90_dias.iloc[0])} dias</h3>")
        p.append(tabela(g, [("loja", "Loja", texto), ("produto", "Produto", texto), ("situacao", "Situação", texto),
                            ("estoque_vendavel", "Estoque", inteiro), ("em_transito", "Em trânsito", inteiro),
                            ("taxa_diaria", "Vendas/dia", lambda v: decimal(v, 2)), ("dias_estoque", "Dias de estoque", decimal),
                            ("transferencia_recebida", "Recebe transf.", inteiro), ("qtd_a_pedir", "A pedir", inteiro)],
                        "—", classe_linha=lambda r: "ruptura" if r.situacao == "ruptura" else "repor"))
    p.append("<p class='nota'>Valores estimados a <b>preço de venda</b>: o custo de compra não existe nas fontes, então o total é um teto para comparar com o pedido mínimo.</p>")

    # ── 2. TRANSFERIR ──
    p.append("<h2>2. Transferir</h2><p class='sub'>Produto sobrando numa loja (cobertura acima da meta ou lote que não vai vender até vencer) "
             "e faltando em outra <b>da mesma cidade</b>. A transferência chega antes do distribuidor e já foi descontada da seção Repor.</p>")
    p.append(tabela(transf, [("produto", "Produto", texto), ("origem", "De (sobra)", texto), ("destino", "Para (falta)", texto),
                             ("cidade", "Cidade", texto), ("quantidade", "Unidades", inteiro),
                             ("dias_estoque_origem", "Dias estoque origem", decimal), ("dias_estoque_destino", "Dias estoque destino", decimal)],
                    "Nenhuma oportunidade de transferência hoje."))
    p.append("<p class='nota'>Antes de transferir: produtos refrigerados (ex.: insulina) exigem transporte com cadeia de frio, e produtos controlados "
             "exigem a documentação de movimentação. Se não houver como cumprir, peça ao distribuidor em vez de transferir.</p>")

    # ── 3. RISCO DE VENCIMENTO ──
    p.append(f"<h2>3. Risco de vencimento (próximos {int(par.horizonte_risco_dias)} dias)</h2><p class='sub'>Pressupõe que cada loja vende primeiro o lote que vence antes "
             "e mantém o ritmo dos últimos 28 dias. <b>Sobra</b> = unidades que ainda estarão na prateleira na validade.</p>")
    p.append(tabela(risco, [("loja", "Loja", texto), ("produto", "Produto", texto), ("lote_codigo", "Lote", texto),
                            ("validade", "Validade", data_br), ("dias_ate_vencer", "Dias", inteiro), ("estoque_estimado", "Estoque do lote", inteiro),
                            ("taxa_diaria", "Vendas/dia", lambda v: decimal(v, 2)), ("sobra_na_validade", "Sobra prevista", inteiro),
                            ("valor_em_risco_centavos", "Valor em risco", reais)],
                    "Nenhum lote em risco no horizonte.",
                    classe_linha=lambda r: "ruptura" if r.dias_ate_vencer <= 14 else ("repor" if r.dias_ate_vencer <= 30 else "")))
    p.append("<p class='nota'>Ação sugerida: vermelho (≤ 14 dias) = promoção imediata ou devolução ao distribuidor; amarelo (≤ 30 dias) = promoção programada ou transferência para loja com giro maior.</p>")

    # ── 4. DISTRIBUIDORES ──
    p.append("<h2>4. Distribuidores: quem entrega fora do prazo</h2><p class='sub'>Prazo prometido × prazo real das entregas já feitas. "
             "O <b>p90</b> (9 em cada 10 entregas chegam em até esse prazo) é o prazo usado para decidir quando repor.</p>")
    p.append(tabela(forn, [("nome", "Distribuidor", texto), ("prazo_cadastro_dias", "Prazo combinado (dias)", inteiro),
                           ("pedidos_entregues", "Entregas", inteiro), ("pct_no_prazo", "% no prazo", decimal),
                           ("atraso_medio_dos_atrasados_dias", "Atraso médio dos atrasados (dias)", decimal),
                           ("prazo_real_medio_dias", "Prazo real médio", decimal), ("prazo_real_p90_dias", "Prazo real p90", inteiro),
                           ("em_transito", "Em trânsito", inteiro)],
                    "—", classe_linha=lambda r: "ruptura" if r.pct_no_prazo < 50 else ""))

    # ── 5. PERDA ──
    p.append("<h2>5. Perda por vencimento no período</h2><p class='sub'>Unidades que estavam na prateleira quando o lote venceu (última contagem − vendas até a validade), "
             "a preço de venda atual.</p>")
    p.append(tabela(d["perda_mes"], [("mes_vencimento", "Mês", lambda v: pd.Timestamp(v).strftime("%m/%Y")), ("lotes", "Lotes", inteiro),
                                     ("unidades", "Unidades", inteiro), ("valor_centavos", "Valor", reais)], "Sem perdas no período."))
    p.append("<h3>Lojas com maior perda</h3>")
    p.append(tabela(d["perda_loja"], [("loja", "Loja", texto), ("lotes", "Lotes", inteiro), ("unidades", "Unidades", inteiro), ("valor_centavos", "Valor", reais)], "—"))
    p.append("<h3>Por categoria</h3>")
    p.append(tabela(d["perda_categoria"], [("categoria", "Categoria", texto), ("unidades", "Unidades", inteiro), ("valor_centavos", "Valor", reais)], "—"))
    p.append(f"<p class='nota'>{int(d['excesso'].n.iloc[0])} combinações loja × produto estão com excesso (mais de {int(par.excesso_dias)} dias de cobertura): origem provável das perdas futuras.</p>")
    p.append("<footer>Fonte: warehouse DuckDB (schema <code>gold</code>), gerado em "
             f"{pd.Timestamp(par.gerado_em).strftime('%d/%m/%Y %H:%M')}. Estoque de hoje = última contagem da loja − vendas do lote desde então + entregas. "
             "Nenhum dado pessoal de clientes é usado neste relatório.</footer></main></body></html>")
    return "\n".join(p)


def gerar(dia: date | None = None) -> tuple[os.PathLike, os.PathLike, os.PathLike]:
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    con.execute(f"SET TimeZone = '{FUSO}'")
    d = consultar(con)
    con.close()
    dia = pd.Timestamp(dia or d["parametros"].iloc[0].data_ref).date()   # Timestamp → date puro (sem hora)
    pasta = PRODUTOS / "relatorios"
    pasta.mkdir(parents=True, exist_ok=True)
    listas = PRODUTOS / "listas"
    listas.mkdir(parents=True, exist_ok=True)
    destino = pasta / f"relatorio_{dia.isoformat()}.html"
    temporario = destino.with_name(destino.name + ".tmp")
    temporario.write_text(gerar_html(d, dia), encoding="utf-8")
    os.replace(temporario, destino)
    csv_repor, csv_risco = listas / f"repor_{dia.isoformat()}.csv", listas / f"risco_{dia.isoformat()}.csv"
    d["repor"].to_csv(csv_repor, index=False)
    d["risco"].to_csv(csv_risco, index=False)
    return destino, csv_repor, csv_risco


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sem-publicar", action="store_true")
    args = parser.parse_args()
    arquivo, csv_repor, csv_risco = gerar()
    print(f"Relatório gerado: {arquivo}")
    if not args.sem_publicar:
        s3, bucket = cliente_s3(), os.getenv("S3_BUCKET", BUCKET)
        s3.upload_file(str(arquivo), bucket, f"relatorios/{arquivo.name}", ExtraArgs={"ContentType": "text/html; charset=utf-8"})
        for csv in (csv_repor, csv_risco):
            s3.upload_file(str(csv), bucket, f"relatorios/listas/{csv.name}", ExtraArgs={"ContentType": "text/csv; charset=utf-8"})
        print(f"Publicado no Lake: relatorios/{arquivo.name} (+ listas CSV). Relatórios anteriores preservados.")


if __name__ == "__main__":
    main()
