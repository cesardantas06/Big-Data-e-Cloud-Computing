-- As 5 perguntas da diretoria (ENUNCIADO.md §1), respondidas só com a Gold.
-- Uso:  python -m pipeline.consulta sql/perguntas_negocio.sql
-- "Hoje" = gold.parametros.data_ref (último dia de vendas nos dados).

-- P1a. Quantos dias de estoque cada loja tem de cada produto, no ritmo atual? Os 15 casos mais urgentes (cobertura menor que o prazo do distribuidor)
SELECT l.nome AS loja, p.nome AS produto, m.estoque_vendavel AS estoque, m.em_transito, m.taxa_diaria AS vendas_dia,
       m.dias_estoque, m.limite_reposicao_dias AS limite_dias, m.situacao
FROM gold.mart_estoque_loja_produto m JOIN gold.dim_loja l USING (sk_loja) JOIN gold.dim_produto p USING (sk_produto)
WHERE m.situacao IN ('ruptura', 'repor')
ORDER BY m.dias_posicao NULLS FIRST, l.nome LIMIT 15;

-- P1b. Resumo da rede: quantas combinações loja x produto em cada situação, e a cobertura mediana
SELECT situacao, count(*) AS combinacoes, round(quantile_cont(dias_estoque, 0.5), 1) AS cobertura_mediana_dias
FROM gold.mart_estoque_loja_produto GROUP BY situacao ORDER BY combinacoes DESC;

-- P2. Quais lotes vencem antes de serem vendidos no ritmo atual? (sobra prevista na validade, próximos 90 dias)
SELECT l.nome AS loja, p.nome AS produto, r.lote_codigo, r.validade, r.dias_ate_vencer AS dias, r.estoque_estimado AS estoque_lote,
       r.taxa_diaria AS vendas_dia, r.sobra_na_validade AS sobra, round(r.valor_em_risco_centavos / 100.0, 2) AS reais_em_risco
FROM gold.mart_risco_vencimento r JOIN gold.dim_loja l USING (sk_loja) JOIN gold.dim_produto p USING (sk_produto)
WHERE r.sobra_na_validade > 0 AND r.dias_ate_vencer <= 90
ORDER BY r.validade, r.valor_em_risco_centavos DESC LIMIT 20;

-- P3. Quais distribuidores entregam fora do prazo combinado? (pior primeiro)
SELECT nome AS distribuidor, prazo_cadastro_dias AS prazo_combinado, pedidos_entregues AS entregas, pct_no_prazo,
       atraso_medio_dos_atrasados_dias AS atraso_medio_dos_atrasados, prazo_real_medio_dias AS prazo_real_medio,
       prazo_real_p90_dias AS prazo_real_p90, em_transito, em_transito_atrasados
FROM gold.mart_pontualidade_fornecedor ORDER BY pct_no_prazo;

-- P4a. Quanto a rede perdeu com produtos vencidos no período? Total
SELECT count(*) AS lotes, sum(unidades_perdidas) AS unidades, round(sum(valor_perdido_centavos) / 100.0, 2) AS reais_perdidos,
       min(validade) AS primeiro_vencimento, max(validade) AS ultimo_vencimento
FROM gold.mart_perda_vencimento;

-- P4b. Perda por mês de vencimento e por categoria
SELECT strftime(m.mes_vencimento, '%Y-%m') AS mes, p.categoria, sum(m.unidades_perdidas) AS unidades,
       round(sum(m.valor_perdido_centavos) / 100.0, 2) AS reais
FROM gold.mart_perda_vencimento m JOIN gold.dim_produto p USING (sk_produto)
GROUP BY ALL ORDER BY mes, reais DESC;

-- P5. Existe produto sobrando numa loja e faltando em outra? Oportunidades de transferência (mesma cidade)
SELECT p.nome AS produto, lo.nome AS de_loja, ld.nome AS para_loja, t.cidade, t.quantidade,
       t.dias_estoque_origem, t.dias_estoque_destino
FROM gold.mart_transferencia t JOIN gold.dim_produto p USING (sk_produto)
JOIN gold.dim_loja lo ON lo.sk_loja = t.sk_loja_origem JOIN gold.dim_loja ld ON ld.sk_loja = t.sk_loja_destino
ORDER BY t.quantidade DESC, p.nome LIMIT 20;
