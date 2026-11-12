# Dicionário de dados — Drogaria Bem-Estar

**Período da base:** 01/06/2026 a 30/09/2026
**Fuso:** horários em `-03:00` (Brasília), salvo quando indicado de outra forma no próprio valor
**Lote incremental:** 01/10 a 14/10/2026, entregue pelo professor depois do checkpoint parcial

> Os dados vêm de sistemas reais da empresa (simulados) e **não foram limpos**. Este dicionário descreve o que cada campo **deveria** conter. Descobrir onde os dados fogem disso faz parte do trabalho.

```
fontes/
├── sqlite/bemestar.db                          ← sistema de vendas
├── csv/inventario/AAAA-MM-DD/loja_NN.csv       ← contagem de estoque de toda segunda-feira, por loja
├── json/fornecedores.json                      ← exportação do sistema de compras
├── events/pdv_AAAA-MM-DD.jsonl                 ← eventos dos caixas, um arquivo por dia de chegada
└── api/pedidos_reposicao.jsonl                 ← dados servidos pela API (não leia direto: use a API)
api/app.py                                      ← API do distribuidor
```

---

## 1. SQLite — `sqlite/bemestar.db`

### `lojas`

| Coluna | Descrição |
| --- | --- |
| `loja_id` | PK |
| `nome`, `cidade`, `bairro` | Identificação |
| `inaugurada_em` | Data de inauguração |

### `produtos`

| Coluna | Descrição |
| --- | --- |
| `produto_id` | PK |
| `ean` | Código de barras |
| `nome` | Nome comercial |
| `principio_ativo` | Princípio ativo (medicamentos) |
| `categoria` | `uso_continuo`, `antibiotico`, `analgesico`, `higiene`, `dermocosmetico`, `vitaminas` |
| `controlado` | 1 se a venda exige retenção de receita |
| `preco_centavos` | Preço de venda atual |
| `fornecedor_id` | Distribuidor que abastece o produto → `fornecedores.json` |
| `atualizado_em` | Última alteração (ex.: reajuste de preço) |

### `clientes` (programa de fidelidade)

| Coluna | Descrição |
| --- | --- |
| `cliente_id` | PK |
| `nome`, `cpf`, `email`, `telefone`, `data_nascimento` | Cadastro. O CPF deveria estar no formato `000.000.000-00` |
| `criado_em`, `atualizado_em` | Cadastro e última alteração |

### `vendas`

| Coluna | Descrição |
| --- | --- |
| `venda_id` | PK |
| `loja_id` | FK → `lojas` |
| `cliente_id` | Cliente identificado no caixa (nulo se não identificado) |
| `vendido_em` | Momento da venda |
| `valor_total_centavos` | Total |
| `status` | `concluida` ou `cancelada` |
| `atualizado_em` | Última alteração |

### `itens_venda`

| Coluna | Descrição |
| --- | --- |
| `item_id` | PK |
| `venda_id` | FK → `vendas` |
| `produto_id` | FK → `produtos` |
| `lote_codigo` | Lote do qual a unidade saiu → mesmo código do inventário |
| `quantidade` | Unidades |
| `preco_unitario_centavos` | Preço praticado |

---

## 2. CSV — `csv/inventario/AAAA-MM-DD/loja_NN.csv`

Toda segunda-feira, antes de abrir, cada loja conta o estoque **por lote**. A pasta tem a data da contagem. Lotes vencidos aparecem na contagem e são descartados logo depois.

| Coluna | Descrição |
| --- | --- |
| `loja_id`, `produto_id` | Referências |
| `lote_codigo` | Lote |
| `validade` | Data de validade do lote |
| `quantidade` | Unidades contadas |
| `data_contagem` | Data da contagem |

> O formato da validade depende do sistema da loja.

---

## 3. JSON — `json/fornecedores.json`

| Campo | Descrição |
| --- | --- |
| `fornecedor_id` | Identificador (deveria ser único) |
| `nome`, `cnpj`, `contato_email` | Identificação |
| `prazo_entrega_dias` | Prazo **combinado** de entrega, em dias |
| `pedido_minimo_reais` | Valor mínimo por pedido |

---

## 4. JSONL — `events/pdv_AAAA-MM-DD.jsonl`

Eventos enviados em tempo real pelos caixas (PDV). O arquivo de um dia contém os eventos que **chegaram** naquele dia. O SQLite é o registro consolidado das mesmas vendas.

| Campo | Descrição |
| --- | --- |
| `evento_id` | Identificador único |
| `loja_id`, `venda_id`, `produto_id` | Referências (`produto_id` nulo no cancelamento da venda inteira) |
| `tipo` | `venda` (um evento por item), `receita_retida`, `devolucao`, `cancelamento` |
| `quantidade` | Unidades do item |
| `ocorrido_em` | Momento do evento (ISO 8601) |
| `operador_id` | Operador de caixa |

---

## 5. API — Distribuidor (pedidos de reposição)

Início, na pasta do cenário, com o ambiente Python ativado:

```bash
python -m uvicorn api.app:app --host 127.0.0.1 --port 8001
```

Documentação interativa: <http://127.0.0.1:8001/docs>

| Endpoint | Descrição |
| --- | --- |
| `GET /health` | Verificação de disponibilidade (use no sensor do Airflow) |
| `GET /pedidos-reposicao?after_sequence=N&limit=500&page=1` | Registros com `sequencia > N`, paginados. Repita com `page+1` enquanto `has_more = true` |
| `GET /pedidos-reposicao/{pedido_reposicao_id}` | Versão mais recente de um pedido |

| Campo | Descrição |
| --- | --- |
| `pedido_reposicao_id` | Identificador do pedido |
| `sequencia` | Número crescente, usado para captura incremental |
| `fornecedor_id`, `loja_id`, `produto_id` | Referências |
| `quantidade` | Unidades pedidas |
| `lote_codigo`, `validade_lote` | Lote que será entregue e sua validade |
| `solicitado_em` | Momento do pedido |
| `previsto_para` | Data prevista pelo prazo combinado |
| `entregue_em` | Momento da entrega (nulo enquanto em trânsito) |
| `status` | `em_transito` ou `entregue` |
| `atualizado_em` | Última atualização |

> **Um pedido pode aparecer mais de uma vez:** quando o status muda, a API publica o registro de novo, com uma sequência maior. O registro mais recente é o que vale.
