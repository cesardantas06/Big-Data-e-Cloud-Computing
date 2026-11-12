-- Contagens por camada (Etapa 10.1). Rode ANTES e DEPOIS do lote:
--   python -m pipeline.consulta sql/contagens.sql > docs/contagens_antes_do_lote.txt
-- Contagens por camada
SELECT 'bronze_sqlite_produtos' AS item, count(*) AS linhas FROM 'data/lake/bronze/sqlite/produtos/*.parquet'
UNION ALL SELECT 'bronze_sqlite_clientes', count(*) FROM 'data/lake/bronze/sqlite/clientes/*.parquet'
UNION ALL SELECT 'bronze_sqlite_vendas', count(*) FROM 'data/lake/bronze/sqlite/vendas/*.parquet'
UNION ALL SELECT 'bronze_sqlite_itens_venda', count(*) FROM 'data/lake/bronze/sqlite/itens_venda/*.parquet'
UNION ALL SELECT 'bronze_csv_inventario', count(*) FROM 'data/lake/bronze/csv/**/*.parquet'
UNION ALL SELECT 'bronze_events', count(*) FROM 'data/lake/bronze/events/*.parquet'
UNION ALL SELECT 'bronze_api_pedidos (todas as versões)', count(*) FROM 'data/lake/bronze/api/**/*.parquet'
UNION ALL SELECT 'silver_produtos', count(*) FROM 'data/lake/silver/produtos/*.parquet'
UNION ALL SELECT 'silver_vendas', count(*) FROM 'data/lake/silver/vendas/*.parquet'
UNION ALL SELECT 'silver_itens_venda', count(*) FROM 'data/lake/silver/itens_venda/*.parquet'
UNION ALL SELECT 'silver_inventario', count(*) FROM 'data/lake/silver/inventario/**/*.parquet'
UNION ALL SELECT 'silver_eventos', count(*) FROM 'data/lake/silver/eventos/**/*.parquet'
UNION ALL SELECT 'silver_pedidos_reposicao (1 por pedido)', count(*) FROM 'data/lake/silver/pedidos_reposicao/*.parquet'
UNION ALL SELECT 'quarentena_total', (SELECT count(*) FROM 'data/lake/quarentena/**/*.parquet')
UNION ALL SELECT 'gold_dim_produto', count(*) FROM gold.dim_produto
UNION ALL SELECT 'gold_fato_venda_item', count(*) FROM gold.fato_venda_item
UNION ALL SELECT 'gold_fato_estoque_lote', count(*) FROM gold.fato_estoque_lote
UNION ALL SELECT 'gold_fato_reposicao', count(*) FROM gold.fato_reposicao
UNION ALL SELECT 'gold_pedidos_em_transito', count(*) FROM gold.fato_reposicao WHERE status = 'em_transito'
UNION ALL SELECT 'gold_pedidos_entregues', count(*) FROM gold.fato_reposicao WHERE status = 'entregue'
ORDER BY 1;
