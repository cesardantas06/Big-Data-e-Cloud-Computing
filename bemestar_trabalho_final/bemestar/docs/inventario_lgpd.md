# Inventário LGPD — Drogaria Bem-Estar

Fontes: `bemestar.db` (SQLite), `inventario/*.csv`, `fornecedores.json`, `events/*.jsonl`, API `pedidos-reposicao`.
Classificação: **ID** identificador direto · **QI** quase-identificador · **S** dado sensível (saúde, art. 5º, II) · **NP** não pessoal.
Regra de necessidade (art. 6º, III): o produto de dados (reposição, vencimento, distribuidores) trabalha com **loja × produto × lote** e nunca precisa saber **quem** comprou. Por isso nenhuma coluna de pessoa chega à Gold.

## Colunas pessoais

| Fonte | Coluna | Class. | Gold precisa? | Tratamento | Deixa de existir em |
| --- | --- | --- | --- | --- | --- |
| clientes | `nome` | ID | não | descartada | **Silver** (permanece na Landing/Bronze, zona restrita) |
| clientes | `cpf` | ID | não | validado (dígitos) e guardado só como `cpf_pseudo` = HMAC-SHA256 dos 11 dígitos com chave secreta | Silver (como valor legível) |
| clientes | `cliente_id` | ID | não | `cliente_pseudo` = HMAC-SHA256 | Silver (como valor legível) |
| clientes | `email` | ID | não | só vira o indicador `email_valido` | Silver |
| clientes | `telefone` | ID | não | descartada (a qualidade só gera alerta `telefone_invalido`) | Silver |
| clientes | `data_nascimento` | QI | não | descartada | Silver |
| vendas | `cliente_id` | ID | não | substituída por `cliente_identificado` (booleano) | **Silver** |
| eventos | `operador_id` | ID (funcionário) | não | descartada | Silver |
| fornecedores | `contato_email` | QI (pessoa do distribuidor) | não | descartada | Silver |
| fornecedores | `cnpj` | NP (pessoa jurídica) | não | mantido na Silver, fora da Gold | Gold |
| eventos | `tipo = receita_retida` (9.769 eventos) | **S** | só agregado | ver abaixo | Gold detalhada |
| itens_venda/eventos | medicamento de uso controlado × venda | **S** (combinado com pessoa) | produto sim, pessoa não | a ligação é cortada na Silver | Silver |

Sem dado pessoal: `lojas`, `produtos`, `inventario`, `pedidos_reposicao`.

## Decisões específicas

1. **Ligação cliente ↔ medicamento (7.3).** É cortada na **Silver**: `vendas` perde `cliente_id` (fica só `cliente_identificado`) e a Gold nem lê `clientes`. O relatório de reposição precisa de *quanto* se vende de cada produto por loja, não de *quem* compra. Resultado: a Gold não permite descobrir o medicamento que uma pessoa identificável compra.
2. **Pseudonimização (7.2).** HMAC-SHA256 com chave em `PSEUDO_CHAVE` (no Airflow, Variable `pseudo_secret`; nunca no código nem no Git). SHA-256 puro seria refeito por quem tivesse uma lista de CPFs. Sem a chave não há como refazer a conta; com a mesma chave o mesmo CPF gera sempre o mesmo pseudônimo (dá para contar clientes distintos sem identificá-los). Trocar a chave invalida os pseudônimos antigos: guardar a chave em cofre.
3. **`receita_retida` (7.4).** Revela que uma pessoa levou um medicamento que exige retenção de receita, ou seja, indício de condição de saúde. Na Gold só existe `mart_receita_retida`: **loja × categoria × mês**, contagem sem cliente nem produto individual, com supressão de células com menos de k = 5 eventos (parâmetro em `gold.parametros`). Medido: 160 células, 0 abaixo de k. Quem vê: gerência/farmacêutico responsável; não vai para o relatório distribuído às lojas.
4. **Quarentena é zona restrita.** Guarda a linha reprovada com todas as colunas de negócio, mas as colunas pessoais são **mascaradas (NULL) ao entrar**: `nome, cpf, email, telefone, data_nascimento, cpf_dig` em clientes, `cliente_id` em vendas, `operador_id` em eventos. Mesmo assim o acesso é restrito ao time de dados.
5. **Landing e Bronze são zonas restritas.** Guardam o dado bruto (inclusive CPF e nome) para auditoria e reprocessamento. No laboratório o SeaweedFS tem uma credencial única (`admin`/`bigdata-secret`, local); numa empresa, buckets/prefixos separados com IAM: consumidores só leem `warehouse/` e `relatorios/`.
6. **Relatório de reposição** é feito só com a Gold (leitura): loja, produto, lote, fornecedor, quantidades. Sem coluna de pessoa.

## Verificações automáticas (`pipeline/transform/validar.py`)

- Nenhuma coluna da Gold com nome em `{cliente_id, cliente_pseudo, cpf, cpf_pseudo, email, telefone, data_nascimento, operador_id, cliente_identificado, contato_email, cnpj}`.
- `nome` só nas dimensões de loja, produto e fornecedor.
- `mart_receita_retida`: nenhuma célula não suprimida com menos de k eventos.
- Resultado da última execução: `data/qualidade/validacao_gold.json` (47/47).

## Limites conhecidos

- O pseudônimo de CPF (Silver) ainda é pessoal (LGPD art. 12, §2º) e por isso fica fora da Gold.
- Combinações raras (loja × produto de baixíssimo giro) não identificam pessoas, pois não há atributo de pessoa na Gold.
