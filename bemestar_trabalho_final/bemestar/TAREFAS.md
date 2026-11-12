# Tarefas — Cenário 3: Drogaria Bem-Estar (Rede de Farmácias)

Este roteiro divide o trabalho em **etapas pequenas, na ordem em que devem ser feitas**. Cada tarefa tem um critério de **"pronto quando"**: só avance quando ele for verdadeiro.

Leia antes: [ENUNCIADO.md](ENUNCIADO.md) · [DICIONARIO_DE_DADOS.md](DICIONARIO_DE_DADOS.md) · [README geral](../README.md)

**Como fazer cada etapa, passo a passo, com o código:** [Guia do Aluno](../GUIA%20DO%20ALUNO/README.md). Cada etapa abaixo tem um arquivo de mesmo número no guia.

| Etapa | Tema | Aula de referência | Checkpoint |
| --- | --- | --- | --- |
| 0 | Ambiente | Aulas 4 e 7 | **Checkpoint de ambiente** |
| 1 | Conhecer os dados | Aulas 2 e 3 | — |
| 2 | Ingestão (ELs → Landing) | Aula 4 | — |
| 3 | Data Lake (SeaweedFS) | Aula 4 | — |
| 4 | Bronze | Aula 5 | **Checkpoint parcial** |
| 5 | Silver, qualidade e quarentena | Aula 5 | — |
| 6 | Gold no DuckDB | Aula 5 | — |
| 7 | Segurança e LGPD | Aula 6 | — |
| 8 | Produto de dados: relatório diário de reposição | Aula 7 | — |
| 9 | Orquestração no Airflow | Aula 7 | — |
| 10 | Lote incremental | Aulas 4 e 7 | — |
| 11 | Relatório e apresentação | — | **Entrega final** |

---

## Etapa 0 — Ambiente

- [ ] **0.1** Copiar a pasta `CENARIO 3 - Rede de Farmacias` para o seu computador e abri-la no VS Code.
  *Pronto quando:* você vê `fontes/`, `api/` e os três `.md` no explorador.
- [ ] **0.2** Criar o ambiente Python (`.venv`) e instalar o `requirements.txt` da Aula 7.
  *Pronto quando:* `python -c "import fastapi, duckdb, boto3, pandas"` não dá erro.
- [ ] **0.3** Subir a API do distribuidor: `python -m uvicorn api.app:app --host 127.0.0.1 --port 8001`.
  *Pronto quando:* <http://127.0.0.1:8001/health> responde `"status": "ok"` e <http://127.0.0.1:8001/docs> abre.
- [ ] **0.4** Subir o SeaweedFS (como na Aula 4) e criar um bucket para o projeto, por exemplo `bemestar-lake`.
  *Pronto quando:* você lista o bucket vazio pelo protocolo S3.
- [ ] **0.5** Instalar e iniciar o Airflow no WSL (como na Aula 7).
  *Pronto quando:* a interface do Airflow abre no navegador.
- [ ] **0.6** 📸 **Checkpoint de ambiente:** enviar ao professor um print com a API, o SeaweedFS e o Airflow abertos.

> Se alguma etapa falhar no seu computador (WSL, memória, permissão de administrador), **avise o professor agora**, não na semana da entrega.

---

## Etapa 1 — Conhecer os dados

Antes de escrever um EL, entenda o que você tem. Crie um arquivo `docs/diario_de_descobertas.md` e anote tudo o que achar estranho.

- [ ] **1.1** Abrir o `bemestar.db` e contar as linhas de cada tabela.
- [ ] **1.2** Desenhar (à mão ou em Mermaid) como as fontes se ligam: `vendas` ↔ `itens_venda` ↔ `produtos`, `itens_venda.lote_codigo` ↔ inventário ↔ pedidos de reposição (API), `produtos.fornecedor_id` ↔ `fornecedores.json`.
- [ ] **1.3** Abrir os inventários de **lojas diferentes** e compará-los.
- [ ] **1.4** Escolher **um produto em uma loja** e montar a "linha do tempo" do estoque: contagens de segunda, vendas da semana, pedidos de reposição e chegadas. Os números fecham?
- [ ] **1.5** Chamar a API com `after_sequence=0&limit=5` e depois com `page=2`. Procurar um `pedido_reposicao_id` que aparece **mais de uma vez**.
- [ ] **1.6** Responder no diário: **como eu calculo quantos dias de estoque uma loja tem de um produto?**

*Pronto quando:* o diário tem o diagrama das ligações, a linha do tempo do item 1.4 e pelo menos 5 suspeitas de problemas de qualidade.

---

## Etapa 2 — Ingestão: fontes → Landing

Um EL por fonte, gravando **sem transformar** na Landing (`data/lake/landing/...`). Use os ELs da Aula 4 como modelo.

- [ ] **2.1 EL SQLite.** Capturar as 5 tabelas. `produtos`, `clientes` e `vendas` de forma **incremental** por `atualizado_em`; `itens_venda` juntando pelas vendas novas; `lojas` pode ser carga completa. Guardar a marca d'água (*watermark*) em um arquivo de controle.
- [ ] **2.2 EL CSV.** Copiar os inventários **preservando a pasta da data de contagem**. Capturar só as pastas/arquivos ainda não capturados.
- [ ] **2.3 EL JSON.** Capturar o `fornecedores.json` **somente se ele mudou** (compare o *hash* do arquivo). Guardar cada versão com a data da captura.
- [ ] **2.4 EL JSONL.** Capturar só os arquivos de eventos do PDV novos.
- [ ] **2.5 EL API.** Ler `/pedidos-reposicao` com `after_sequence` = maior sequência já capturada, paginando até `has_more = false`. Guardar **todas** as versões que chegarem (a escolha da versão vigente é feita na Silver).
- [ ] **2.6 Teste de idempotência.** Rodar os 5 ELs duas vezes seguidas.

*Pronto quando:* a segunda execução não cria nenhum arquivo novo e não duplica nenhuma linha, e você consegue **provar** isso com um log ou uma contagem.

---

## Etapa 3 — Data Lake

- [ ] **3.1** Sincronizar a Landing com o bucket no SeaweedFS (S3), como na Aula 4.
- [ ] **3.2** Rodar a sincronização de novo sem mudar nada.

*Pronto quando:* o bucket tem a mesma estrutura da Landing e a segunda sincronização envia **zero** arquivos.

---

## Etapa 4 — Bronze

- [ ] **4.1** Converter cada fonte da Landing para **Parquet** na Bronze, ainda sem limpar nada, apenas com tipos e colunas de controle (`_fonte`, `_arquivo_origem`, `_capturado_em`).
- [ ] **4.2** A Bronze espelha a organização da Landing (eventos em um arquivo por dia de chegada, CSV em pastas por data). Conferir que dá para consultar só um pedaço (ex.: um dia). O particionamento pela data **do evento** (`data=AAAA-MM-DD`) é feito na Silver (Etapa 5).
- [ ] **4.3** Garantir que a Bronze pode ser reconstruída do zero a partir da Landing.
- [ ] **4.4** 📸 **Checkpoint parcial:** mostrar ao professor ELs + Landing + Bronze funcionando. Você recebe o **lote incremental** nesse momento (só vai usá-lo na Etapa 10).

*Pronto quando:* você consulta a Bronze com DuckDB e as contagens batem com a Landing.

---

## Etapa 5 — Silver, qualidade e quarentena

Para **cada fonte**, escreva regras de qualidade explícitas. Cada regra tem nome, descrição e ação: **corrigir**, **mandar para a quarentena** ou **aceitar com alerta**.

- [ ] **5.1** Criar pelo menos uma regra de cada tipo:
  - **unicidade** (registro repetido);
  - **completude** (campo obrigatório vazio);
  - **validade** (valor fora do domínio ou impossível);
  - **integridade** (referência para algo que não existe);
  - **consistência** (datas em ordem lógica, formatos diferentes para o mesmo dado).
- [ ] **5.2** Padronizar: datas de validade em `AAAA-MM-DD` (decida e documente o que fazer quando só há mês e ano), horários em um **único fuso**, textos de domínio normalizados.
- [ ] **5.3** **Versão vigente dos pedidos de reposição:** para cada `pedido_reposicao_id`, manter só o registro de maior `sequencia`.
- [ ] **5.4** Tratar eventos que chegaram em arquivo de outro dia (a data do evento é `ocorrido_em`, não o nome do arquivo).
- [ ] **5.5** Separar os registros reprovados em `quarentena/`, guardando **o motivo** de cada um.
- [ ] **5.6** Gerar o **relatório de qualidade**: por fonte e por regra, quantos registros entraram, quantos foram corrigidos e quantos foram para a quarentena.
- [ ] **5.7** Definir o **gate de qualidade**: um limite (ex.: "mais de 3% do inventário na quarentena bloqueia a Gold"). Justifique o número no relatório.

*Pronto quando:* toda linha da Bronze está **ou** na Silver **ou** na quarentena (a soma bate) e o relatório de qualidade é gerado automaticamente.

---

## Etapa 6 — Gold no DuckDB

- [ ] **6.1** Modelo dimensional sugerido (pode adaptar, justificando):

  | Tabela | Grão |
  | --- | --- |
  | `fato_venda_item` | 1 linha por item vendido: loja, produto, lote, quantidade, valor, data |
  | `fato_estoque_lote` | 1 linha por loja × produto × lote × data de contagem (*snapshot* periódico) |
  | `fato_reposicao` | 1 linha por pedido de reposição (versão vigente): prazo prometido, prazo real, dias de atraso |
  | `dim_loja`, `dim_produto`, `dim_fornecedor` | 1 linha por item |
  | `dim_data` | data, semana, mês |

- [ ] **6.2** Escrever no relatório o **grão** de cada fato e conferir que ele não se repete.
- [ ] **6.3** Mart **estoque por loja/produto/lote**: quantidade, validade, média de vendas recentes e **dias de estoque**.
- [ ] **6.4** Mart **pontualidade dos distribuidores**: % de entregas no prazo e atraso médio.
- [ ] **6.5** Mart **perda por vencimento**: unidades e valor dos lotes que venceram na prateleira.
- [ ] **6.6** Responder com SQL as 5 perguntas do enunciado e guardar as consultas em `sql/perguntas_negocio.sql`.

*Pronto quando:* cada pergunta do enunciado tem uma consulta e um número.

---

## Etapa 7 — Segurança e LGPD

- [ ] **7.1** Inventário: listar **todas** as colunas de **todas** as fontes que contêm dado pessoal e classificar cada uma (identificador direto, quase-identificador, **sensível**, não pessoal).
- [ ] **7.2** Pseudonimizar `cliente_id` e CPF antes da Gold, usando HMAC com chave fora do código.
- [ ] **7.3** Decidir em que camada a ligação **cliente ↔ medicamento** deixa de existir. O relatório de reposição precisa dela?
- [ ] **7.4** Tratar os eventos `receita_retida`: o que eles revelam e quem pode vê-los?

*Pronto quando:* nenhuma tabela da Gold permite descobrir qual medicamento uma pessoa identificável compra.

---

## Etapa 8 — Produto de dados: relatório diário de reposição

- [ ] **8.1** Definir a **regra de reposição** (ex.: repor quando dias de estoque < prazo **real** do distribuidor + margem). Justifique a margem com o mart de pontualidade.
- [ ] **8.2** Seção **Repor**: produto, loja, dias de estoque restantes, quantidade sugerida, distribuidor e prazo.
- [ ] **8.3** Seção **Risco de vencimento**: lote, loja, validade e quantidade que deve sobrar no ritmo atual de vendas.
- [ ] **8.4** Seção **Transferir**: produto sobrando na loja A e faltando na loja B.
- [ ] **8.5** Gerar o arquivo (CSV, Excel ou HTML) com a data no nome, por exemplo `reposicao_2026-09-30.html`, e publicá-lo no SeaweedFS **sem apagar os anteriores**.

*Pronto quando:* o comprador consegue abrir um único arquivo e decidir o que pedir, o que transferir e o que colocar em promoção.

---

## Etapa 9 — Orquestração no Airflow

- [ ] **9.1** Cadastrar as **Connections**: API do distribuidor (HTTP) e SeaweedFS (AWS/S3). Nenhuma senha ou URL fixa no código.
- [ ] **9.2** Escrever o DAG `bemestar_pipeline`, com tarefas nesta ordem:

  ```mermaid
  flowchart LR
      S[sensor: API no ar] --> EL[5 ELs em paralelo] --> SYNC[sync Lake] --> B[Bronze] --> SV[Silver + quarentena]
      SV --> Q[relatório de qualidade] --> G{gate}
      G -- aprovado --> GOLD[Gold] --> R[relatório de reposição] --> P[publica no Lake]
      G -- reprovado --> X[para e avisa]
  ```

- [ ] **9.3** Testar o **sensor**: derrubar a API e ver o DAG esperar. Depois subir a API e ver o DAG seguir.
- [ ] **9.4** Testar o **gate**: baixar o limite de propósito e ver que **nenhum relatório novo** é publicado.
- [ ] **9.5** Rodar o DAG duas vezes seguidas: a segunda não pode duplicar nada.

*Pronto quando:* um clique em "Trigger" leva das fontes ao relatório publicado no Lake.

---

## Etapa 10 — Lote incremental

- [ ] **10.1** Colocar a pasta `lote_incremental` recebida no checkpoint dentro da pasta do cenário e rodar `python lote_incremental/aplicar_lote.py`.
- [ ] **10.2** Acionar o DAG.
- [ ] **10.3** Conferir e registrar no relatório:
  - quantas vendas, contagens de inventário e eventos novos entraram;
  - que os **pedidos que estavam em trânsito** agora aparecem como **entregues**, sem duplicar o pedido;
  - que os **produtos novos** e os **reajustes de preço** aparecem na Gold (atualizados, não duplicados);
  - que um novo relatório de reposição foi publicado ao lado do anterior.
- [ ] **10.4** Comparar o relatório de 30/09 com o que aconteceu no lote: os produtos que ele mandou repor faltaram mesmo? Os lotes em risco venceram?

*Pronto quando:* a Gold reflete o lote sem duplicatas e o Lake tem os dois relatórios.

---

## Etapa 11 — Relatório e apresentação

- [ ] **11.1** Escrever o relatório (máximo 6 páginas), seguindo a estrutura do [README geral](../README.md).
- [ ] **11.2** Incluir, como pede o enunciado, o **valor estimado de perda por vencimento** no período.
- [ ] **11.3** Organizar o repositório: `README.md` explicando como rodar tudo do zero, sem senhas versionadas.
- [ ] **11.4** Ensaiar a apresentação de 15 minutos **no seu computador**, incluindo derrubar a API e rodar o DAG de novo, como o professor pode pedir.

*Pronto quando:* outra pessoa consegue rodar o seu projeto do zero só lendo o seu README.
