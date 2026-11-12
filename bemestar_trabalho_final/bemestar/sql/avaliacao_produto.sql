-- O produto acertou? Listas de 30/09 (data/produtos/antes_do_lote/) contra o que aconteceu em 01-14/10 (Etapa 10.6).
-- ATENÇÃO: o lote é SIMULADO (tools/simular_lote.py) - a contagem de 12/10 e as vendas de outubro foram geradas por ele.
-- Evento "ruptura confirmada" = a contagem física de 12/10 não encontrou nenhuma unidade vendável do produto na loja.
WITH repor AS (
    SELECT l.loja_id, p.produto_id, r.situacao FROM read_csv('data/produtos/antes_do_lote/repor_2026-09-30.csv') r
    JOIN gold.dim_loja l ON l.nome = r.loja JOIN gold.dim_produto p ON p.nome = r.produto),
base AS (  -- todas as combinações loja x produto que venderam em setembro e NÃO estavam na lista
    SELECT DISTINCT f.sk_loja, f.sk_produto FROM gold.fato_venda_item f JOIN gold.dim_data d USING (sk_data)
    WHERE d.data BETWEEN DATE '2026-09-02' AND DATE '2026-09-30'),
base_ids AS (SELECT l.loja_id, p.produto_id FROM base b JOIN gold.dim_loja l USING (sk_loja) JOIN gold.dim_produto p USING (sk_produto)),
estoque_12 AS (
    SELECT loja_id, produto_id, sum(quantidade) AS qtd FROM 'data/lake/silver/inventario/**/*.parquet'
    WHERE data_contagem = DATE '2026-10-12' AND validade >= DATE '2026-10-12' GROUP BY ALL),
grupo AS (
    SELECT 'apontados (ruptura/repor)' AS grupo, loja_id, produto_id FROM repor
    UNION ALL SELECT 'não apontados', b.loja_id, b.produto_id FROM base_ids b ANTI JOIN repor r USING (loja_id, produto_id))
SELECT g.grupo, count(*) AS combinacoes,
       count(*) FILTER (WHERE coalesce(e.qtd, 0) = 0) AS ruptura_confirmada_em_12_10,
       round(100.0 * count(*) FILTER (WHERE coalesce(e.qtd, 0) = 0) / count(*), 1) AS pct
FROM grupo g LEFT JOIN estoque_12 e USING (loja_id, produto_id) GROUP BY g.grupo ORDER BY 1;

-- Risco de vencimento: dos lotes apontados em 30/09 que venceriam até 14/10, quantos ainda tinham unidades na última contagem antes de vencer?
WITH risco AS (
    SELECT l.loja_id, p.produto_id, r.lote_codigo, r.validade, r.sobra_na_validade
    FROM read_csv('data/produtos/antes_do_lote/risco_2026-09-30.csv') r
    JOIN gold.dim_loja l ON l.nome = r.loja JOIN gold.dim_produto p ON p.nome = r.produto
    WHERE r.sobra_na_validade > 0 AND r.validade BETWEEN DATE '2026-10-06' AND DATE '2026-10-14'),
cont AS (SELECT loja_id, produto_id, lote_codigo, data_contagem, quantidade FROM 'data/lake/silver/inventario/**/*.parquet'
         WHERE data_contagem IN (DATE '2026-10-05', DATE '2026-10-12'))
SELECT count(*) AS lotes_apontados_vencendo_6_a_14_10,
       count(*) FILTER (WHERE c.quantidade > 0) AS ainda_com_unidades_na_ultima_contagem,
       round(100.0 * count(*) FILTER (WHERE c.quantidade > 0) / count(*), 1) AS pct,
       sum(r.sobra_na_validade) AS unidades_previstas_de_sobra, coalesce(sum(c.quantidade), 0) AS unidades_vistas_na_contagem
FROM risco r LEFT JOIN cont c ON c.loja_id = r.loja_id AND c.produto_id = r.produto_id AND c.lote_codigo = r.lote_codigo
 AND c.data_contagem = CASE WHEN r.validade >= DATE '2026-10-12' THEN DATE '2026-10-12' ELSE DATE '2026-10-05' END;

-- Cobertura (recall): perdas por vencimento que a Gold registra em outubro e que já estavam na lista de risco de 30/09
WITH perdas AS (
    SELECT l.loja_id, p.produto_id, m.lote_codigo, m.unidades_perdidas
    FROM gold.mart_perda_vencimento m JOIN gold.dim_loja l USING (sk_loja) JOIN gold.dim_produto p USING (sk_produto)
    WHERE m.validade >= DATE '2026-10-01'),
risco AS (
    SELECT l.loja_id, p.produto_id, r.lote_codigo FROM read_csv('data/produtos/antes_do_lote/risco_2026-09-30.csv') r
    JOIN gold.dim_loja l ON l.nome = r.loja JOIN gold.dim_produto p ON p.nome = r.produto WHERE r.sobra_na_validade > 0)
SELECT count(*) AS lotes_perdidos_em_outubro, count(r.lote_codigo) AS ja_estavam_na_lista_de_risco,
       round(100.0 * count(r.lote_codigo) / nullif(count(*), 0), 1) AS pct_cobertura
FROM perdas p LEFT JOIN risco r USING (loja_id, produto_id, lote_codigo);
