"""Silver → Gold (modelo dimensional + marts, no DuckDB).

Uso (na pasta do projeto):
    python -m pipeline.transform.gold

A Gold é reconstruída a cada execução num arquivo temporário e só então troca o warehouse:
se algo falhar no meio, o warehouse anterior continua intacto.

══ GRÃO DE CADA FATO (uma frase) ═════════════════════════════════════════════════════
  fato_venda_item   uma linha = UM ITEM de uma venda (item_id), com o lote de onde saiu.
  fato_estoque_lote uma linha = UM LOTE de um produto numa loja, contado numa segunda-feira
                    (loja × produto × lote × data da contagem).
  fato_reposicao    uma linha = UM PEDIDO de reposição, na versão vigente (maior sequência).
  fato_devolucao    uma linha = UM EVENTO de devolução no caixa.

══ MARTS (uma tabela por pergunta de negócio) ════════════════════════════════════════
  mart_estoque_lote          grão loja × produto × lote       estoque estimado hoje, validade
  mart_estoque_loja_produto  grão loja × produto              dias de estoque, situação (repor/ok/...)
  mart_risco_vencimento      grão loja × produto × lote       unidades que devem sobrar na validade
  mart_perda_vencimento      grão loja × produto × lote       unidades/valor que venceram na prateleira
  mart_pontualidade_fornecedor grão fornecedor                % no prazo, atraso, prazo real (p90)
  mart_transferencia         grão produto × origem × destino  sobra numa loja ↔ falta noutra (mesma cidade)
  mart_receita_retida        grão loja × categoria × mês      agregado (k ≥ 5); LGPD: dado de saúde

══ LGPD ══════════════════════════════════════════════════════════════════════════════
  Nenhuma tabela da Gold tem cliente, CPF, nome, e-mail, telefone, nascimento nem operador.
  A venda já perdeu o vínculo com o cliente na Silver (só resta cliente_identificado, que nem
  chega aqui). Nada na Gold liga uma pessoa a um medicamento. Ver docs/inventario_lgpd.md.

══ HOJE DO RELATÓRIO ═════════════════════════════════════════════════════════════════
  Como os dados são de 2026, "hoje" = última data de venda presente na Silver (gold.parametros).
"""
from __future__ import annotations

import json
import os

import duckdb

from ..config import FUSO, SILVER, WAREHOUSE
from ..util import caminho_sql

# Parâmetros de negócio (ficam gravados em gold.parametros; o relatório lê de lá)
PARAMETROS = {
    "janela_taxa_dias": 28,        # janela para a média de vendas diárias
    "ciclo_reposicao_dias": 14,    # cobertura extra pedida além do prazo (ciclo de pedido de 2 semanas)
    "folga_dias": 1,               # folga sobre o prazo real p90 (o relatório é diário)
    "excesso_dias": 60,            # acima disto de cobertura, a loja tem excesso
    "doador_alvo_dias": 45,        # a loja doadora mantém esta cobertura depois de transferir
    "horizonte_risco_dias": 90,    # risco de vencimento só é acionável até esta distância
    "dias_minimos_transferencia": 14,  # lote precisa de ≥14 dias de validade para valer a pena mover
    "k_minimo_receita": 5,         # células de receita retida com menos eventos são suprimidas
}


def silver(con: duckdb.DuckDBPyConnection, nome: str) -> None:
    con.execute(f"""CREATE OR REPLACE TEMP VIEW silver_{nome} AS
                    SELECT * FROM read_parquet('{caminho_sql(SILVER / nome)}/**/*.parquet', union_by_name=true)""")


def construir() -> dict:
    WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)
    temporario = WAREHOUSE.with_name(WAREHOUSE.stem + "_construindo.duckdb")
    temporario.unlink(missing_ok=True)
    con = duckdb.connect(str(temporario))
    con.execute(f"SET TimeZone = '{FUSO}'")
    con.execute("CREATE SCHEMA gold")
    # clientes NÃO é lido de propósito: nada na Gold precisa de pessoa (minimização, LGPD art. 6º, III)
    for nome in ("lojas", "fornecedores", "produtos", "vendas", "itens_venda", "inventario", "eventos", "pedidos_reposicao"):
        silver(con, nome)

    # ── Parâmetros e "hoje" ─────────────────────────────────────────────
    colunas = ", ".join(f"{v} AS {k}" for k, v in PARAMETROS.items())
    con.execute(f"""CREATE TABLE gold.parametros AS
        SELECT (SELECT max(data) FROM silver_vendas) AS data_ref, {colunas}, current_timestamp AS gerado_em""")

    # ── Dimensões ───────────────────────────────────────────────────────
    con.execute("""CREATE TABLE gold.dim_data AS
        SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER) AS sk_data, CAST(d AS DATE) AS data,
               year(d) AS ano, month(d) AS mes, day(d) AS dia,
               CAST(date_trunc('week', d) AS DATE) AS semana_inicio, CAST(date_trunc('month', d) AS DATE) AS mes_inicio,
               isodow(d) AS dia_semana, isodow(d) >= 6 AS fim_de_semana
        FROM range(DATE '2026-05-01', DATE '2027-06-30', INTERVAL 1 DAY) AS t(d)""")
    con.execute("""CREATE TABLE gold.dim_loja AS
        SELECT row_number() OVER (ORDER BY loja_id) AS sk_loja, loja_id, nome, cidade, bairro, inaugurada_em
        FROM silver_lojas""")
    con.execute("""CREATE TABLE gold.dim_fornecedor AS
        SELECT row_number() OVER (ORDER BY fornecedor_id) AS sk_fornecedor, fornecedor_id, nome,
               prazo_entrega_dias AS prazo_cadastro_dias, pedido_minimo_reais
        FROM silver_fornecedores""")
    # Preço atual: o produto reajustado aparece UMA vez, com o preço novo (a Silver já tem a versão vigente).
    con.execute("""CREATE TABLE gold.dim_produto AS
        SELECT row_number() OVER (ORDER BY p.produto_id) AS sk_produto, p.produto_id, p.ean, p.nome, p.principio_ativo,
               p.categoria, p.controlado, p.preco_centavos, f.sk_fornecedor, p.atualizado_em AS preco_atualizado_em
        FROM silver_produtos p LEFT JOIN gold.dim_fornecedor f USING (fornecedor_id)""")

    # ── Fatos ───────────────────────────────────────────────────────────
    con.execute("""CREATE TABLE gold.fato_venda_item AS
        SELECT i.item_id, i.venda_id, CAST(strftime(v.data, '%Y%m%d') AS INTEGER) AS sk_data,
               l.sk_loja, p.sk_produto, i.lote_codigo, i.quantidade, i.preco_unitario_centavos,
               i.quantidade * i.preco_unitario_centavos AS valor_centavos,
               v.status = 'cancelada' AS venda_cancelada, v.vendido_em
        FROM silver_itens_venda i
        JOIN silver_vendas v USING (venda_id)
        JOIN gold.dim_loja l ON l.loja_id = v.loja_id
        JOIN gold.dim_produto p ON p.produto_id = i.produto_id""")
    con.execute("""CREATE TABLE gold.fato_estoque_lote AS
        SELECT CAST(strftime(i.data_contagem, '%Y%m%d') AS INTEGER) AS sk_data, l.sk_loja, p.sk_produto, i.lote_codigo,
               i.data_contagem, i.validade, i.quantidade,
               date_diff('day', i.data_contagem, i.validade) AS dias_para_vencer,
               i.validade < i.data_contagem AS vencido_na_contagem,
               i.quantidade * p.preco_centavos AS valor_estoque_centavos
        FROM silver_inventario i
        JOIN gold.dim_loja l ON l.loja_id = i.loja_id
        JOIN gold.dim_produto p ON p.produto_id = i.produto_id""")
    con.execute("""CREATE TABLE gold.fato_reposicao AS
        SELECT r.pedido_reposicao_id, r.sequencia, CAST(strftime(r.solicitado_em, '%Y%m%d') AS INTEGER) AS sk_data,
               l.sk_loja, p.sk_produto, f.sk_fornecedor, r.lote_codigo, r.validade_lote, r.quantidade,
               r.solicitado_em, r.previsto_para, r.entregue_em, r.status,
               date_diff('day', CAST(r.solicitado_em AS DATE), r.previsto_para) AS prazo_prometido_dias,
               date_diff('day', CAST(r.solicitado_em AS DATE), CAST(r.entregue_em AS DATE)) AS prazo_real_dias,
               date_diff('day', r.previsto_para, CAST(r.entregue_em AS DATE)) AS dias_atraso,
               date_diff('day', r.previsto_para, CAST(r.entregue_em AS DATE)) <= 0 AS no_prazo,
               (r.status = 'em_transito' AND r.previsto_para < (SELECT data_ref FROM gold.parametros)) AS em_transito_atrasado
        FROM silver_pedidos_reposicao r
        JOIN gold.dim_loja l ON l.loja_id = r.loja_id
        JOIN gold.dim_produto p ON p.produto_id = r.produto_id
        JOIN gold.dim_fornecedor f ON f.fornecedor_id = r.fornecedor_id""")
    con.execute("""CREATE TABLE gold.fato_devolucao AS
        SELECT e.evento_id, CAST(strftime(e.data, '%Y%m%d') AS INTEGER) AS sk_data, l.sk_loja, p.sk_produto,
               e.quantidade, e.ocorrido_em
        FROM silver_eventos e
        JOIN gold.dim_loja l ON l.loja_id = e.loja_id
        JOIN gold.dim_produto p ON p.produto_id = e.produto_id
        WHERE e.tipo = 'devolucao'""")

    # ── Mart: pontualidade dos distribuidores ───────────────────────────
    con.execute("""CREATE TABLE gold.mart_pontualidade_fornecedor AS
        SELECT f.sk_fornecedor, d.fornecedor_id, d.nome, d.prazo_cadastro_dias,
               count(*) FILTER (WHERE f.status = 'entregue') AS pedidos_entregues,
               count(*) FILTER (WHERE f.no_prazo) AS entregues_no_prazo,
               round(100.0 * count(*) FILTER (WHERE f.no_prazo) / nullif(count(*) FILTER (WHERE f.status = 'entregue'), 0), 1) AS pct_no_prazo,
               round(avg(f.dias_atraso), 2) AS atraso_medio_dias,
               round(avg(f.dias_atraso) FILTER (WHERE f.dias_atraso > 0), 2) AS atraso_medio_dos_atrasados_dias,
               max(f.dias_atraso) AS atraso_maximo_dias,
               round(avg(f.prazo_prometido_dias), 2) AS prazo_prometido_medio_dias,
               round(avg(f.prazo_real_dias), 2) AS prazo_real_medio_dias,
               CAST(ceil(quantile_cont(f.prazo_real_dias, 0.9)) AS INTEGER) AS prazo_real_p90_dias,
               count(*) FILTER (WHERE f.status = 'em_transito') AS em_transito,
               count(*) FILTER (WHERE f.em_transito_atrasado) AS em_transito_atrasados
        FROM gold.fato_reposicao f JOIN gold.dim_fornecedor d USING (sk_fornecedor)
        GROUP BY ALL ORDER BY pct_no_prazo""")

    # ── Posição de lotes (tabelas temporárias de apoio) ─────────────────
    con.execute("""CREATE TEMP TABLE venda_lote AS
        SELECT l.loja_id, p.produto_id, f.lote_codigo, d.data, f.quantidade
        FROM gold.fato_venda_item f
        JOIN gold.dim_loja l USING (sk_loja) JOIN gold.dim_produto p USING (sk_produto) JOIN gold.dim_data d USING (sk_data)
        WHERE NOT f.venda_cancelada AND f.lote_codigo IS NOT NULL""")
    con.execute("""CREATE TEMP TABLE ent_lote AS
        SELECT loja_id, produto_id, lote_codigo, CAST(entregue_em AS DATE) AS data_entrega,
               sum(quantidade) AS quantidade, min(validade_lote) AS validade_lote
        FROM silver_pedidos_reposicao WHERE status = 'entregue' AND lote_codigo IS NOT NULL GROUP BY ALL""")
    con.execute("""CREATE TEMP TABLE ult_contagem AS
        SELECT loja_id, max(data_contagem) AS d_ult FROM silver_inventario
        WHERE data_contagem <= (SELECT data_ref FROM gold.parametros) GROUP BY loja_id""")
    con.execute("""CREATE TEMP TABLE obs_lote AS   -- última observação física de cada lote
        SELECT loja_id, produto_id, lote_codigo, max(data_contagem) AS data_ult,
               arg_max(quantidade, data_contagem) AS q_ult, arg_max(validade, data_contagem) AS validade
        FROM silver_inventario WHERE data_contagem <= (SELECT data_ref FROM gold.parametros) GROUP BY ALL""")

    # ── Mart: estoque estimado hoje, por lote ───────────────────────────
    # estoque hoje = última contagem da loja − vendas do lote desde então + entregas do lote desde então.
    # (Validado nos dados: em 97% dos lotes, contagem seguinte = contagem − vendas + entregas, exato.)
    con.execute("""CREATE TABLE gold.mart_estoque_lote AS
        WITH base AS (
            SELECT i.loja_id, i.produto_id, i.lote_codigo, i.validade, i.quantidade AS q_contada, u.d_ult
            FROM silver_inventario i JOIN ult_contagem u ON u.loja_id = i.loja_id AND i.data_contagem = u.d_ult),
        entregas AS (
            SELECT e.loja_id, e.produto_id, e.lote_codigo, min(e.validade_lote) AS validade, sum(e.quantidade) AS q_entregue,
                   any_value(u.d_ult) AS d_ult
            FROM ent_lote e JOIN ult_contagem u USING (loja_id)
            WHERE e.data_entrega >= u.d_ult AND e.data_entrega <= (SELECT data_ref FROM gold.parametros) GROUP BY e.loja_id, e.produto_id, e.lote_codigo),
        juntos AS (
            SELECT coalesce(b.loja_id, e.loja_id) AS loja_id, coalesce(b.produto_id, e.produto_id) AS produto_id,
                   coalesce(b.lote_codigo, e.lote_codigo) AS lote_codigo, coalesce(b.validade, e.validade) AS validade,
                   coalesce(b.d_ult, e.d_ult) AS d_ult, coalesce(b.q_contada, 0) AS q_contada, coalesce(e.q_entregue, 0) AS q_entregue
            FROM base b FULL JOIN entregas e ON b.loja_id = e.loja_id AND b.produto_id = e.produto_id AND b.lote_codigo = e.lote_codigo),
        com_vendas AS (
            SELECT j.*, coalesce((SELECT sum(v.quantidade) FROM venda_lote v
                                  WHERE v.loja_id = j.loja_id AND v.produto_id = j.produto_id AND v.lote_codigo = j.lote_codigo
                                    AND v.data >= j.d_ult AND v.data <= (SELECT data_ref FROM gold.parametros)), 0) AS q_vendida
            FROM juntos j)
        SELECT l.sk_loja, p.sk_produto, c.lote_codigo, c.validade, c.d_ult AS data_contagem_base,
               c.q_contada, c.q_entregue AS q_entregue_desde, c.q_vendida AS q_vendida_desde,
               greatest(0, c.q_contada + c.q_entregue - c.q_vendida) AS estoque_estimado,
               c.q_contada + c.q_entregue - c.q_vendida < 0 AS estimativa_negativa_zerada,
               date_diff('day', (SELECT data_ref FROM gold.parametros), c.validade) AS dias_ate_vencer,
               greatest(0, c.q_contada + c.q_entregue - c.q_vendida) * p.preco_centavos AS valor_estoque_centavos
        FROM com_vendas c
        JOIN gold.dim_loja l ON l.loja_id = c.loja_id JOIN gold.dim_produto p ON p.produto_id = c.produto_id
        WHERE greatest(0, c.q_contada + c.q_entregue - c.q_vendida) > 0""")

    # ── Mart: risco de vencimento (FEFO: o lote que vence primeiro sai primeiro) ─
    # sobra = o que sobra do lote na validade, no ritmo atual de vendas, depois de vender os lotes que vencem antes.
    con.execute("""CREATE TABLE gold.mart_risco_vencimento AS
        WITH taxa AS (
            SELECT f.sk_loja, f.sk_produto, sum(f.quantidade) / CAST(p.janela_taxa_dias AS DOUBLE) AS taxa_diaria
            FROM gold.fato_venda_item f JOIN gold.dim_data d USING (sk_data), gold.parametros p
            WHERE NOT f.venda_cancelada AND d.data > p.data_ref - p.janela_taxa_dias AND d.data <= p.data_ref
            GROUP BY f.sk_loja, f.sk_produto, p.janela_taxa_dias),
        lotes AS (
            SELECT e.*, coalesce(t.taxa_diaria, 0) AS taxa_diaria,
                   sum(e.estoque_estimado) OVER (PARTITION BY e.sk_loja, e.sk_produto ORDER BY e.validade, e.lote_codigo) AS acumulado
            FROM gold.mart_estoque_lote e LEFT JOIN taxa t USING (sk_loja, sk_produto)
            WHERE e.dias_ate_vencer >= 0)      -- lote já vencido é perda (mart_perda_vencimento), não risco, e não entra na fila FEFO
        SELECT sk_loja, sk_produto, lote_codigo, validade, dias_ate_vencer, estoque_estimado, round(taxa_diaria, 3) AS taxa_diaria,
               CAST(ceil(least(estoque_estimado, greatest(0, acumulado - taxa_diaria * greatest(dias_ate_vencer, 0)))) AS INTEGER) AS sobra_na_validade,
               CAST(ceil(least(estoque_estimado, greatest(0, acumulado - taxa_diaria * greatest(dias_ate_vencer, 0)))) AS INTEGER)
                   * (SELECT preco_centavos FROM gold.dim_produto d WHERE d.sk_produto = lotes.sk_produto) AS valor_em_risco_centavos,
               CASE WHEN dias_ate_vencer <= 30 THEN 'critico_ate_30d'
                    WHEN dias_ate_vencer <= 90 THEN 'atencao_31_a_90d' ELSE 'longo_prazo' END AS faixa
        FROM lotes""")

    # ── Mart: perda por vencimento (o que JÁ venceu na prateleira) ──────
    con.execute("""CREATE TABLE gold.mart_perda_vencimento AS
        WITH calc AS (
            SELECT o.*, greatest(0, o.q_ult
                   + coalesce((SELECT sum(e.quantidade) FROM ent_lote e WHERE e.loja_id = o.loja_id AND e.produto_id = o.produto_id
                               AND e.lote_codigo = o.lote_codigo AND e.data_entrega >= o.data_ult AND e.data_entrega <= o.validade), 0)
                   - coalesce((SELECT sum(v.quantidade) FROM venda_lote v WHERE v.loja_id = o.loja_id AND v.produto_id = o.produto_id
                               AND v.lote_codigo = o.lote_codigo AND v.data >= o.data_ult AND v.data <= o.validade), 0)) AS unidades_perdidas
            -- "perdido" = venceu ANTES de hoje. O lote que vence hoje ainda está na prateleira e fica no risco
            -- (senão o mesmo lote seria contado nos dois marts).
            FROM obs_lote o WHERE o.validade < (SELECT data_ref FROM gold.parametros))
        SELECT l.sk_loja, p.sk_produto, c.lote_codigo, c.validade, CAST(date_trunc('month', c.validade) AS DATE) AS mes_vencimento,
               c.data_ult AS ultima_contagem, c.unidades_perdidas, c.unidades_perdidas * p.preco_centavos AS valor_perdido_centavos
        FROM calc c JOIN gold.dim_loja l ON l.loja_id = c.loja_id JOIN gold.dim_produto p ON p.produto_id = c.produto_id
        WHERE c.unidades_perdidas > 0""")

    # ── Mart: situação por loja × produto (dias de estoque e regra de reposição) ──
    con.execute("""CREATE TABLE gold.mart_estoque_loja_produto AS
        WITH par AS (SELECT * FROM gold.parametros),
        taxa AS (
            SELECT f.sk_loja, f.sk_produto, sum(f.quantidade) AS vendido_janela
            FROM gold.fato_venda_item f JOIN gold.dim_data d USING (sk_data), par p
            WHERE NOT f.venda_cancelada AND d.data > p.data_ref - p.janela_taxa_dias AND d.data <= p.data_ref
            GROUP BY f.sk_loja, f.sk_produto),
        est AS (
            SELECT sk_loja, sk_produto,
                   sum(estoque_estimado) FILTER (WHERE dias_ate_vencer >= 0) AS estoque_vendavel,
                   sum(estoque_estimado) FILTER (WHERE dias_ate_vencer < 0) AS estoque_vencido,
                   count(*) AS lotes
            FROM gold.mart_estoque_lote GROUP BY ALL),
        transito AS (
            SELECT sk_loja, sk_produto, sum(quantidade) AS em_transito
            FROM gold.fato_reposicao WHERE status = 'em_transito' GROUP BY ALL),
        universo AS (SELECT sk_loja, sk_produto FROM taxa UNION SELECT sk_loja, sk_produto FROM est),
        calc AS (
            SELECT u.sk_loja, u.sk_produto, pr.sk_fornecedor,
                   coalesce(e.estoque_vendavel, 0) AS estoque_vendavel, coalesce(e.estoque_vencido, 0) AS estoque_vencido,
                   coalesce(e.lotes, 0) AS lotes, coalesce(t.em_transito, 0) AS em_transito,
                   coalesce(k.vendido_janela, 0) / CAST(p.janela_taxa_dias AS DOUBLE) AS taxa_diaria,
                   coalesce(pf.prazo_real_p90_dias, d.prazo_cadastro_dias) AS prazo_p90_dias,
                   pf.prazo_real_medio_dias,
                   coalesce(pf.prazo_real_p90_dias, d.prazo_cadastro_dias) + p.folga_dias AS limite_reposicao_dias,
                   p.ciclo_reposicao_dias, p.excesso_dias
            FROM universo u CROSS JOIN par p
            JOIN gold.dim_produto pr USING (sk_produto)
            JOIN gold.dim_fornecedor d ON d.sk_fornecedor = pr.sk_fornecedor
            LEFT JOIN gold.mart_pontualidade_fornecedor pf ON pf.sk_fornecedor = pr.sk_fornecedor
            LEFT JOIN est e USING (sk_loja, sk_produto) LEFT JOIN taxa k USING (sk_loja, sk_produto)
            LEFT JOIN transito t USING (sk_loja, sk_produto))
        SELECT sk_loja, sk_produto, sk_fornecedor, lotes, estoque_vendavel, estoque_vencido, em_transito,
               round(taxa_diaria, 3) AS taxa_diaria,
               CASE WHEN taxa_diaria > 0 THEN round(estoque_vendavel / taxa_diaria, 1) END AS dias_estoque,
               CASE WHEN taxa_diaria > 0 THEN round((estoque_vendavel + em_transito) / taxa_diaria, 1) END AS dias_posicao,
               prazo_p90_dias, prazo_real_medio_dias, limite_reposicao_dias,
               CASE WHEN taxa_diaria > 0 THEN greatest(0, CAST(ceil(taxa_diaria * (prazo_p90_dias + ciclo_reposicao_dias)
                                                                     - estoque_vendavel - em_transito) AS INTEGER)) ELSE 0 END AS qtd_sugerida,
               CASE WHEN taxa_diaria = 0 AND estoque_vendavel > 0 THEN 'sem_giro'
                    WHEN taxa_diaria = 0 THEN 'sem_demanda'
                    WHEN estoque_vendavel = 0 AND em_transito = 0 THEN 'ruptura'
                    WHEN (estoque_vendavel + em_transito) / taxa_diaria < limite_reposicao_dias THEN 'repor'
                    WHEN estoque_vendavel / taxa_diaria > excesso_dias THEN 'excesso'
                    ELSE 'ok' END AS situacao
        FROM calc""")

    # ── Mart: transferência (sobra numa loja ↔ falta noutra, MESMA cidade) ──
    # Casamento por faixas acumuladas: doadores e receptores ordenados por tamanho; a sobreposição
    # das faixas é a quantidade transferida. Cada unidade é usada uma única vez.
    con.execute("""CREATE TABLE gold.mart_transferencia AS
        WITH par AS (SELECT * FROM gold.parametros),
        doa AS (
            SELECT m.sk_loja, m.sk_produto, l.cidade,
                   CAST(floor(greatest(
                       m.estoque_vendavel - m.taxa_diaria * p.doador_alvo_dias,
                       coalesce((SELECT sum(r.sobra_na_validade) FROM gold.mart_risco_vencimento r
                                 WHERE r.sk_loja = m.sk_loja AND r.sk_produto = m.sk_produto
                                   AND r.dias_ate_vencer >= p.dias_minimos_transferencia), 0))) AS INTEGER) AS excedente,
                   m.dias_estoque
            FROM gold.mart_estoque_loja_produto m JOIN gold.dim_loja l USING (sk_loja), par p
            WHERE m.situacao IN ('excesso', 'sem_giro', 'ok')),
        doadores AS (SELECT * FROM doa WHERE excedente > 0),
        rec AS (
            SELECT m.sk_loja, m.sk_produto, l.cidade, m.qtd_sugerida AS necessidade, m.dias_estoque
            FROM gold.mart_estoque_loja_produto m JOIN gold.dim_loja l USING (sk_loja)
            WHERE m.situacao IN ('ruptura', 'repor') AND m.qtd_sugerida > 0),
        d2 AS (SELECT *, sum(excedente) OVER (PARTITION BY sk_produto, cidade ORDER BY excedente DESC, sk_loja) AS fim FROM doadores),
        r2 AS (SELECT *, sum(necessidade) OVER (PARTITION BY sk_produto, cidade ORDER BY necessidade DESC, sk_loja) AS fim FROM rec)
        SELECT d2.sk_produto, d2.cidade, d2.sk_loja AS sk_loja_origem, r2.sk_loja AS sk_loja_destino,
               CAST(least(d2.fim, r2.fim) - greatest(d2.fim - d2.excedente, r2.fim - r2.necessidade) AS INTEGER) AS quantidade,
               d2.dias_estoque AS dias_estoque_origem, r2.dias_estoque AS dias_estoque_destino
        FROM d2 JOIN r2 ON d2.sk_produto = r2.sk_produto AND d2.cidade = r2.cidade
        WHERE least(d2.fim, r2.fim) - greatest(d2.fim - d2.excedente, r2.fim - r2.necessidade) > 0""")

    # ── Mart: receita retida (dado de saúde) — só agregado, com supressão de células pequenas ──
    con.execute("""CREATE TABLE gold.mart_receita_retida AS
        WITH agg AS (
            SELECT e.loja_id, p.categoria, CAST(date_trunc('month', e.data) AS DATE) AS mes,
                   count(*) AS eventos, sum(e.quantidade) AS unidades
            FROM silver_eventos e JOIN gold.dim_produto p ON p.produto_id = e.produto_id
            WHERE e.tipo = 'receita_retida' GROUP BY ALL)
        SELECT l.sk_loja, a.categoria, a.mes,
               CASE WHEN a.eventos >= k.k_minimo_receita THEN a.eventos END AS eventos,
               CASE WHEN a.eventos >= k.k_minimo_receita THEN a.unidades END AS unidades,
               a.eventos < k.k_minimo_receita AS suprimido
        FROM agg a JOIN gold.dim_loja l USING (loja_id), gold.parametros k""")

    resumo = {t: con.execute(f"SELECT count(*) FROM gold.{t}").fetchone()[0]
              for (t,) in con.execute("SELECT table_name FROM information_schema.tables "
                                      "WHERE table_schema = 'gold' ORDER BY 1").fetchall()}
    con.close()
    os.replace(temporario, WAREHOUSE)   # o warehouse antigo só é trocado se TUDO deu certo
    return resumo


def main() -> None:
    print(json.dumps(construir(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
