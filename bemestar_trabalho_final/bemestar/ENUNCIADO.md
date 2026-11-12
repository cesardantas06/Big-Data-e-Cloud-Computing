# Cenário 3 — Rede de Farmácias

## Empresa: **Drogaria Bem-Estar**

A Drogaria Bem-Estar tem vinte lojas. Ela sofre com dois problemas ao mesmo tempo, e eles parecem contraditórios: em algumas lojas, **falta** remédio de uso contínuo e o cliente vai para o concorrente; em outras, o mesmo produto **vence na prateleira** e vira prejuízo.

O estoque existe — só está no lugar errado, ou foi pedido tarde demais.

---

## 1. O problema de negócio

> "Quais produtos vão faltar e quais vão vencer, em cada loja, nos próximos dias?"

Perguntas que a diretoria quer responder:

- Quantos dias de estoque cada loja tem de cada produto, no ritmo atual de vendas?
- Quais lotes vencem antes de serem vendidos no ritmo atual?
- Quais fornecedores/distribuidores entregam fora do prazo combinado?
- Quanto a rede perdeu com produtos vencidos no último período?
- Existe produto sobrando numa loja e faltando em outra (oportunidade de transferência)?

---

## 2. As fontes

| Área | Dados | Formato | Observação |
| --- | --- | --- | --- |
| Comercial | Lojas, produtos, clientes do programa de fidelidade, vendas | **SQLite** | Sistema de vendas |
| Estoque | Inventário por loja e por lote, com data de validade | **CSV** | Cada loja exporta seu inventário |
| Compras | Fornecedores e condições comerciais (prazo, pedido mínimo) | **JSON** | Exportado do sistema de compras |
| Caixa | Eventos do PDV: venda, devolução, cancelamento, receita retida | **JSONL** | Um evento por linha |
| Distribuidor | Pedidos de reposição e prazos de entrega | **API REST** | Serviço do distribuidor |

> Os dados estão na pasta [`fontes/`](fontes/), e cada campo está descrito no [**Dicionário de Dados**](DICIONARIO_DE_DADOS.md). A API sobe com `python -m uvicorn api.app:app --port 8001`. O **lote incremental** (01 a 14/10) será entregue pelo professor depois do checkpoint parcial. **Espere encontrar problemas de qualidade**, como acontece com dados de verdade.

---

## 3. A solução na ponta

### Relatório diário de reposição

Todo dia, o comprador da rede precisa abrir **um arquivo** e saber o que fazer. O DAG deve gerar e publicar no Data Lake um relatório (CSV, Excel ou HTML) com três seções:

| Seção | Conteúdo |
| --- | --- |
| **Repor** | produto, loja, dias de estoque restantes, quantidade sugerida, distribuidor e prazo |
| **Risco de vencimento** | lote, loja, validade, quantidade que deve sobrar no ritmo atual |
| **Transferir** | produto sobrando na loja A que está faltando na loja B |

**Requisitos mínimos:**

- [ ] Uma regra de reposição explícita (ex.: repor quando dias de estoque < prazo do distribuidor + margem), com a margem justificada.
- [ ] O cálculo de "dias de estoque" usa a média de vendas recentes da Gold.
- [ ] A geração do relatório é uma **tarefa do DAG**, executada depois do gate de qualidade.
- [ ] O relatório é publicado no SeaweedFS com a data no nome, mantendo o histórico.

**Quem usa:** o comprador da rede, todo dia de manhã.

---

## 4. Ponto de atenção — LGPD

Venda de medicamento associada a cliente identificado revela **condição de saúde** — dado **sensível** (art. 11 da LGPD). Um cliente que compra insulina todo mês é, quase certamente, diabético.

- O relatório de reposição precisa saber **quem** comprou? Provavelmente não.
- Em que camada a ligação entre cliente e medicamento deixa de existir? Justifique no relatório.

---

## 5. Entregas específicas deste cenário

Além dos requisitos comuns do [README](../README.md):

- [ ] Mart Gold de estoque por loja/produto/lote com dias de estoque e validade.
- [ ] Mart Gold de pontualidade dos distribuidores.
- [ ] Relatório diário gerado pelo DAG e publicado no Data Lake.
- [ ] No relatório do trabalho: valor estimado de perda por vencimento no período.
