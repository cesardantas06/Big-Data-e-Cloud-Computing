"""Bronze → Silver + Quarentena + relatório de qualidade.

Uso (na pasta do projeto, com PSEUDO_CHAVE definida):
    python -m pipeline.transform.silver

Cada conjunto de dados passa por 3 passos:
  1. ORIGEM  ler a Bronze (e, se a fonte manda versões, ficar com a mais recente)
  2. REGRAS  marcar cada linha com os MOTIVOS de reprovação ('' = aprovada)
  3. SAÍDA   aprovadas → Silver (limpas e tipadas) | reprovadas → Quarentena (com o motivo)

Cada regra tem NOME, TIPO (unicidade, completude, validade, integridade, consistência),
AÇÃO (quarentena, corrigir ou alerta) e DESCRIÇÃO. O relatório de qualidade lista todas,
com quantos registros cada uma afetou (inclusive as que afetaram zero).

A Silver é reconstruída inteira a cada execução, a partir da Bronze.

LGPD (ver docs/inventario_lgpd.md): identificadores pessoais são pseudonimizados aqui
(HMAC), nome/e-mail/telefone/nascimento/operador NÃO passam para a Silver, e a venda deixa
de apontar para o cliente (só fica o indicador `cliente_identificado`). Assim a ligação
cliente ↔ medicamento deixa de existir já na Silver.
"""
from __future__ import annotations

import json
import shutil
from datetime import date

import duckdb

from ..config import BRONZE, FUSO, QUALIDADE, QUARENTENA, SILVER
from ..lgpd.pseudo import registrar
from ..util import agora_utc, caminho_sql, gravar_json
from .validacoes import registrar_validacoes

# Janela dos dados do cenário (usada em regras de plausibilidade de datas)
INICIO_BASE = "DATE '2026-01-01'"          # nada anterior a isto é plausível para este sistema
FIM_VALIDADE = "DATE '2035-12-31'"         # validade de medicamento além disto é erro de digitação


def R(tipo: str, condicao: str, descricao: str) -> tuple[str, str, str]:
    """Uma regra: (tipo, condição SQL VERDADEIRA quando há PROBLEMA, descrição)."""
    return (tipo, condicao, descricao)


class Silver:
    def __init__(self) -> None:
        self.con = duckdb.connect()
        # Fuso da SESSÃO: CAST(timestamptz AS DATE), try_cast de datas sem fuso e a exibição usam este fuso.
        self.con.execute(f"SET TimeZone = '{FUSO}'")
        registrar(self.con)                 # pseudo(...)
        registrar_validacoes(self.con)      # cpf_digitos(...)
        self.relatorio: dict[str, dict] = {}
        self.origens: dict[str, dict] = {}  # stg → {fonte, bronze_linhas, lidas}
        self.catalogo: dict[str, list[dict]] = {}

    # ── 1. ORIGEM ───────────────────────────────────────────────────────
    def bronze(self, caminho: str) -> str:
        """Expressão SQL que lê todos os Parquet de bronze/<caminho>."""
        return f"read_parquet('{caminho_sql(BRONZE / caminho)}/**/*.parquet', union_by_name=true)"

    def _registrar_origem(self, stg: str, caminho: str, filtro: str) -> None:
        bronze_linhas = self.con.execute(
            f"SELECT count(*) FROM {self.bronze(caminho)} {('WHERE ' + filtro) if filtro else ''}").fetchone()[0]
        lidas = self.con.execute(f"SELECT count(*) FROM {stg}").fetchone()[0]
        self.origens[stg] = {"fonte_bronze": caminho, "filtro": filtro or None,
                             "bronze_linhas": bronze_linhas, "versoes_substituidas": bronze_linhas - lidas}

    def versao_mais_recente(self, stg: str, caminho: str, chave: str, ordem: str, filtro: str = "") -> None:
        """Uma linha por chave: a de maior <ordem> (ex.: atualizado_em, sequencia).
        Usado quando a MESMA linha chega mais de uma vez porque foi ATUALIZADA."""
        self.con.execute(f"""
            CREATE OR REPLACE TABLE {stg} AS
            SELECT * FROM {self.bronze(caminho)} {('WHERE ' + filtro) if filtro else ''}
            QUALIFY row_number() OVER (PARTITION BY {chave} ORDER BY {ordem} DESC NULLS LAST, _capturado_em DESC) = 1
        """)
        self._registrar_origem(stg, caminho, filtro)

    def snapshot_mais_recente(self, stg: str, caminho: str, filtro: str = "") -> None:
        """Para fontes que mandam o ARQUIVO INTEIRO de novo a cada mudança (ex.: um JSON
        exportado): fica só com as linhas da captura mais recente."""
        self.con.execute(f"""
            CREATE OR REPLACE TABLE {stg} AS
            SELECT * FROM {self.bronze(caminho)} {('WHERE ' + filtro) if filtro else ''}
            QUALIFY _capturado_em = max(_capturado_em) OVER ()
        """)
        self._registrar_origem(stg, caminho, filtro)

    def todas_as_linhas(self, stg: str, caminho: str, filtro: str = "") -> None:
        """Fonte sem versões (cada linha é um fato único): lê tudo."""
        self.con.execute(f"CREATE OR REPLACE TABLE {stg} AS SELECT * FROM {self.bronze(caminho)} "
                         f"{('WHERE ' + filtro) if filtro else ''}")
        self._registrar_origem(stg, caminho, filtro)

    # ── 2. REGRAS ───────────────────────────────────────────────────────
    def aplicar_regras(self, nome: str, origem: str, regras: dict[str, tuple], juncoes: str = "",
                       correcoes: dict[str, tuple] | None = None, alertas: dict[str, tuple] | None = None) -> None:
        """Cria chk_<nome> = origem + colunas `motivos`, `correcoes` e `alertas`.

        regras:    {"motivo": R(tipo, condição, descrição)} → REPROVA (quarentena)
        correcoes: {"correcao": R(...)} → a linha continua aprovada; a correção em si é feita
                   no SELECT de publicar() e aqui só é CONTADA, para aparecer no relatório
        alertas:   {"alerta": R(...)}   → aceita com alerta: a linha segue, a contagem vai ao relatório
        Na condição, a linha da origem se chama `o`. `juncoes` permite LEFT JOIN
        com tabelas já prontas (para verificar se uma referência existe).
        """
        correcoes, alertas = correcoes or {}, alertas or {}

        def lista(itens: dict[str, tuple]) -> str:
            if not itens:
                return "''"
            casos = ", ".join(f"CASE WHEN {cond} THEN '{rotulo}' END" for rotulo, (_, cond, _d) in itens.items())
            return f"concat_ws(', ', {casos})"

        self.con.execute(f"""
            CREATE OR REPLACE TABLE chk_{nome} AS
            SELECT o.*,
                   {lista(regras)} AS motivos,
                   {lista(correcoes)} AS correcoes,
                   {lista(alertas)} AS alertas
            FROM {origem} o
            {juncoes}
        """)
        self.catalogo[nome] = (
            [{"regra": r, "tipo": t, "acao": "quarentena", "descricao": d} for r, (t, _c, d) in regras.items()]
            + [{"regra": r, "tipo": t, "acao": "corrigir", "descricao": d} for r, (t, _c, d) in correcoes.items()]
            + [{"regra": r, "tipo": t, "acao": "alerta", "descricao": d} for r, (t, _c, d) in alertas.items()])
        self._origem_do[nome] = origem

    _origem_do: dict[str, str] = {}

    # ── 3. SAÍDA ────────────────────────────────────────────────────────
    def publicar(self, nome: str, colunas: str, stg: str, particao: str | None = None,
                 mascarar_quarentena: str = "") -> None:
        """Aprovadas → tabela <nome> (e silver/<nome>/); reprovadas → quarentena/<nome>/.

        colunas: o SELECT que limpa e tipa as colunas (a linha se chama `o`).
        particao: coluna para dividir a Silver em pastas (ex.: 'data').
        mascarar_quarentena: colunas pessoais que NÃO devem ir para a quarentena (viram NULL).
        """
        self.con.execute(f"CREATE OR REPLACE TABLE {nome} AS SELECT {colunas} FROM chk_{nome} o WHERE o.motivos = ''")
        if self.con.execute(f"SELECT count(*) FROM {nome}").fetchone()[0] == 0:
            print(f"ATENÇÃO: {nome} ficou sem nenhuma linha aprovada.")
        destino = SILVER / nome
        shutil.rmtree(destino, ignore_errors=True)
        destino.mkdir(parents=True)
        if particao:
            self.con.execute(f"COPY {nome} TO '{caminho_sql(destino)}' "
                             f"(FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY ({particao}))")
        else:
            self.con.execute(f"COPY {nome} TO '{caminho_sql(destino / (nome + '.parquet'))}' "
                             f"(FORMAT PARQUET, COMPRESSION ZSTD)")

        reprovadas = QUARENTENA / nome
        shutil.rmtree(reprovadas, ignore_errors=True)
        reprovadas.mkdir(parents=True)
        replace = ""
        if mascarar_quarentena:
            replace = " REPLACE (" + ", ".join(f"CAST(NULL AS VARCHAR) AS {c.strip()}"
                                               for c in mascarar_quarentena.split(",")) + ")"
        self.con.execute(f"""
            COPY (SELECT *{replace}, current_timestamp AS _quarentenado_em FROM chk_{nome} WHERE motivos <> '')
            TO '{caminho_sql(reprovadas / (nome + '.parquet'))}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        lidas, quarentena = self.con.execute(
            f"SELECT count(*), count(*) FILTER (WHERE motivos <> '') FROM chk_{nome}").fetchone()

        def contar(coluna: str, so_aprovadas: bool) -> dict[str, int]:
            filtro = "motivos = '' AND " if so_aprovadas else ""
            return dict(self.con.execute(f"""
                SELECT item, count(*) FROM (SELECT unnest(string_split({coluna}, ', ')) AS item
                                            FROM chk_{nome} WHERE {filtro}{coluna} <> '')
                GROUP BY item""").fetchall())

        por_motivo, por_correcao, por_alerta = contar("motivos", False), contar("correcoes", True), contar("alertas", True)
        regras = []
        for r in self.catalogo[nome]:
            fonte = {"quarentena": por_motivo, "corrigir": por_correcao, "alerta": por_alerta}[r["acao"]]
            regras.append({**r, "afetadas": int(fonte.get(r["regra"], 0))})
        origem = self.origens.get(stg, {})
        self.relatorio[nome] = {
            "fonte_bronze": origem.get("fonte_bronze"),
            "bronze_linhas": origem.get("bronze_linhas"),
            "versoes_substituidas": origem.get("versoes_substituidas"),
            "lidas": lidas,
            "aprovadas": lidas - quarentena,
            "quarentena": quarentena,
            "percentual_quarentena": round(100 * quarentena / lidas, 2) if lidas else 0.0,
            "por_motivo": dict(sorted(por_motivo.items(), key=lambda kv: -kv[1])),
            "corrigidas_por_tipo": por_correcao,
            "alertas": por_alerta,
            "regras": regras,
        }

    def concluir(self) -> None:
        relatorio = {"gerado_em": agora_utc().isoformat(), "conjuntos": self.relatorio}
        gravar_json(QUALIDADE / "relatorio_qualidade.json", relatorio)
        with (QUALIDADE / "relatorio_historico.jsonl").open("a", encoding="utf-8") as f:
            f.write(json.dumps({"gerado_em": relatorio["gerado_em"], "conjuntos": {
                k: {c: v[c] for c in ("bronze_linhas", "lidas", "aprovadas", "quarentena", "percentual_quarentena")}
                for k, v in self.relatorio.items()}}, ensure_ascii=False) + "\n")
        self.con.close()
        resumo = {k: {c: v[c] for c in ("lidas", "aprovadas", "quarentena", "percentual_quarentena", "por_motivo")}
                  for k, v in self.relatorio.items()}
        print(json.dumps(resumo, ensure_ascii=False, indent=2))


# ═══════════════════════════════════════════════════════════════════════════
# As regras de cada fonte. A ORDEM importa: primeiro as tabelas "mestras"
# (lojas, fornecedores, produtos), depois as que apontam para elas.
# ═══════════════════════════════════════════════════════════════════════════
def main() -> None:
    s = Silver()
    QUALIDADE.mkdir(parents=True, exist_ok=True)

    # ── LOJAS (SQLite, carga completa → só a captura mais recente vale) ─────
    s.snapshot_mais_recente("stg_lojas", "sqlite/lojas")
    s.aplicar_regras("lojas", """(SELECT *, row_number() OVER (PARTITION BY loja_id ORDER BY _arquivo_origem) AS ocorrencia
                                  FROM stg_lojas)""", {
        "loja_duplicada": R("unicidade", "o.ocorrencia > 1", "loja_id repetido na mesma captura"),
        "id_invalido": R("validade", "try_cast(o.loja_id AS INTEGER) IS NULL", "loja_id não numérico"),
        "nome_ausente": R("completude", "o.nome IS NULL OR trim(o.nome) = ''", "loja sem nome"),
        "inauguracao_invalida": R("validade", "try_cast(o.inaugurada_em AS DATE) IS NULL",
                                  "data de inauguração impossível"),
    })
    s.publicar("lojas", """
        CAST(o.loja_id AS INTEGER) AS loja_id, trim(o.nome) AS nome, trim(o.cidade) AS cidade,
        trim(o.bairro) AS bairro, CAST(o.inaugurada_em AS DATE) AS inaugurada_em,
        o._arquivo_origem, o._carregado_em""", "stg_lojas")

    # ── FORNECEDORES (JSON exportado inteiro a cada mudança → captura mais recente) ──
    s.snapshot_mais_recente("stg_fornecedores", "json", "_arquivo_origem LIKE 'json/fornecedores%'")
    # Prazo COMBINADO observado na API (previsto_para − solicitado_em): usado para decidir qual
    # versão de um fornecedor duplicado é a verdadeira (a que concorda com o que a API pratica).
    s.con.execute(f"""
        CREATE OR REPLACE TABLE prazo_api AS
        SELECT try_cast(fornecedor_id AS INTEGER) AS fornecedor_id,
               mode(date_diff('day', CAST(try_cast(solicitado_em AS TIMESTAMPTZ) AS DATE),
                              try_cast(previsto_para AS DATE))) AS prazo_api
        FROM {s.bronze('api/pedidos-reposicao')}
        WHERE try_cast(solicitado_em AS TIMESTAMPTZ) IS NOT NULL AND try_cast(previsto_para AS DATE) IS NOT NULL
        GROUP BY 1""")
    forn_origem = """(
        SELECT f.*, p.prazo_api,
               row_number() OVER (PARTITION BY f.fornecedor_id
                                  ORDER BY (f.prazo_num IS NOT DISTINCT FROM p.prazo_api) DESC, f._ordem) AS ocorrencia
        FROM (SELECT *, row_number() OVER () AS _ordem,
                     coalesce(try_cast(prazo_entrega_dias AS INTEGER),
                              try_cast(regexp_extract(prazo_entrega_dias, '^\\s*(\\d+)\\s*dias?\\s*$', 1) AS INTEGER)) AS prazo_num
              FROM stg_fornecedores) f
        LEFT JOIN prazo_api p ON p.fornecedor_id = try_cast(f.fornecedor_id AS INTEGER))"""
    s.aplicar_regras("fornecedores", forn_origem, {
        "fornecedor_duplicado_divergente": R(
            "unicidade", "o.ocorrencia > 1",
            "mesmo fornecedor_id com dados diferentes; fica a versão cujo prazo concorda com o prazo combinado "
            "praticado na API, e a outra vai para a quarentena"),
        "id_invalido": R("validade", "try_cast(o.fornecedor_id AS INTEGER) IS NULL", "fornecedor_id não numérico"),
        "nome_ausente": R("completude", "o.nome IS NULL OR trim(o.nome) = ''", "fornecedor sem nome"),
        "prazo_invalido": R("validade", "o.prazo_num IS NULL OR o.prazo_num NOT BETWEEN 1 AND 60",
                            "prazo de entrega ausente ou fora de 1–60 dias"),
        "pedido_minimo_invalido": R("validade", "try_cast(o.pedido_minimo_reais AS DOUBLE) IS NULL",
                                    "pedido mínimo ausente ou não numérico"),
    }, correcoes={
        "prazo_texto_convertido": R("consistência", "try_cast(o.prazo_entrega_dias AS INTEGER) IS NULL AND o.prazo_num IS NOT NULL",
                                    "prazo escrito como texto ('2 dias') convertido para número"),
        "pedido_minimo_sinal_invertido": R("validade", "try_cast(o.pedido_minimo_reais AS DOUBLE) < 0",
                                           "pedido mínimo negativo: sinal invertido, vira valor absoluto "
                                           "(hipótese documentada: todos os outros ficam entre 300 e 1000)"),
    }, alertas={
        "cnpj_ausente": R("completude", "o.cnpj IS NULL OR trim(o.cnpj) = ''", "fornecedor sem CNPJ (não usado na Gold)"),
        "cnpj_formato_invalido": R("validade", "o.cnpj IS NOT NULL AND NOT regexp_matches(o.cnpj, '^\\d{2}\\.\\d{3}\\.\\d{3}/\\d{4}-\\d{2}$')",
                                   "CNPJ fora do formato 00.000.000/0000-00"),
        "prazo_diverge_da_api": R("consistência", "o.prazo_api IS NOT NULL AND o.prazo_num IS NOT NULL AND o.prazo_num <> o.prazo_api",
                                  "prazo do cadastro diferente do prazo previsto nos pedidos da API"),
    })
    s.publicar("fornecedores", """
        CAST(o.fornecedor_id AS INTEGER) AS fornecedor_id, trim(o.nome) AS nome, o.cnpj,
        CAST(o.prazo_num AS INTEGER) AS prazo_entrega_dias,
        abs(CAST(o.pedido_minimo_reais AS DOUBLE)) AS pedido_minimo_reais,
        o._arquivo_origem, o._carregado_em""", "stg_fornecedores")

    # ── PRODUTOS (SQLite com atualizações: reajuste de preço → versão mais recente) ──
    s.versao_mais_recente("stg_produtos", "sqlite/produtos", "produto_id", "try_cast(atualizado_em AS TIMESTAMPTZ)")
    s.aplicar_regras("produtos", """(SELECT *, count(*) OVER (PARTITION BY ean) AS ean_repetido FROM stg_produtos)""", {
        "id_invalido": R("validade", "try_cast(o.produto_id AS INTEGER) IS NULL", "produto_id não numérico"),
        "nome_ausente": R("completude", "o.nome IS NULL OR trim(o.nome) = ''", "produto sem nome"),
        "preco_invalido": R("validade", "try_cast(o.preco_centavos AS BIGINT) IS NULL OR try_cast(o.preco_centavos AS BIGINT) <= 0",
                            "preço ausente, zero ou negativo"),
        "categoria_fora_do_dominio": R("validade", "lower(trim(o.categoria)) NOT IN ('uso_continuo','antibiotico','analgesico','higiene','dermocosmetico','vitaminas')",
                                       "categoria fora das seis categorias do dicionário"),
        "controlado_invalido": R("validade", "try_cast(o.controlado AS INTEGER) IS NULL OR try_cast(o.controlado AS INTEGER) NOT IN (0, 1)",
                                 "flag controlado diferente de 0/1"),
        "fornecedor_inexistente": R("integridade", "f.fornecedor_id IS NULL",
                                    "fornecedor_id não existe em fornecedores (aprovados)"),
        "atualizado_em_invalido": R("validade", "try_cast(o.atualizado_em AS TIMESTAMPTZ) IS NULL", "data de atualização inválida"),
    }, juncoes="LEFT JOIN fornecedores f ON f.fornecedor_id = try_cast(o.fornecedor_id AS INTEGER)",
        correcoes={
            "texto_padronizado": R("consistência", "o.nome <> trim(o.nome) OR o.categoria <> lower(trim(o.categoria))",
                                   "espaços e maiúsculas padronizados em nome/categoria"),
        }, alertas={
            "ean_invalido": R("validade", "NOT regexp_matches(coalesce(o.ean, ''), '^\\d{13}$')", "EAN fora de 13 dígitos"),
            "ean_repetido": R("unicidade", "o.ean_repetido > 1", "mesmo EAN em mais de um produto"),
            "principio_ativo_ausente": R("completude", "lower(trim(o.categoria)) IN ('uso_continuo','antibiotico','analgesico') AND (o.principio_ativo IS NULL OR trim(o.principio_ativo) = '')",
                                         "medicamento sem princípio ativo"),
        })
    s.publicar("produtos", """
        CAST(o.produto_id AS INTEGER) AS produto_id, o.ean, trim(o.nome) AS nome, o.principio_ativo,
        lower(trim(o.categoria)) AS categoria, CAST(o.controlado AS INTEGER) = 1 AS controlado,
        CAST(o.preco_centavos AS BIGINT) AS preco_centavos, CAST(o.fornecedor_id AS INTEGER) AS fornecedor_id,
        CAST(o.atualizado_em AS TIMESTAMPTZ) AS atualizado_em, o._arquivo_origem, o._carregado_em""", "stg_produtos")

    # ── CLIENTES (LGPD: só pseudônimos e indicadores de qualidade passam para a Silver) ──
    s.versao_mais_recente("stg_clientes", "sqlite/clientes", "cliente_id", "try_cast(atualizado_em AS TIMESTAMPTZ)")
    s.aplicar_regras("clientes", """(SELECT *, cpf_digitos(cpf) AS cpf_dig,
                                            count(*) OVER (PARTITION BY cpf_digitos(cpf)) AS cpf_repetido
                                     FROM stg_clientes)""", {
        "id_invalido": R("validade", "try_cast(o.cliente_id AS BIGINT) IS NULL", "cliente_id não numérico"),
        "criado_em_invalido": R("validade", "try_cast(o.criado_em AS TIMESTAMPTZ) IS NULL", "data de cadastro inválida"),
        "atualizado_antes_de_criado": R("consistência", "try_cast(o.atualizado_em AS TIMESTAMPTZ) < try_cast(o.criado_em AS TIMESTAMPTZ)",
                                        "atualização anterior ao cadastro"),
    }, correcoes={
        "cpf_sem_mascara": R("consistência", "o.cpf_dig IS NOT NULL AND regexp_matches(o.cpf, '^\\d{11}$')",
                             "CPF só com números e dígitos válidos: aceito (a Silver guarda só o HMAC dos 11 dígitos)"),
    }, alertas={
        "cpf_invalido": R("validade", "o.cpf_dig IS NULL", "CPF fora do formato, com dígito verificador errado ou repetido (111.111.111-11): o cadastro segue, sem CPF"),
        "cpf_compartilhado": R("unicidade", "o.cpf_dig IS NOT NULL AND o.cpf_repetido > 1", "mesmo CPF válido em mais de um cadastro"),
        "email_invalido": R("validade", "NOT regexp_matches(coalesce(o.email, ''), '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$')", "e-mail vazio ou fora do formato"),
        "telefone_invalido": R("validade", "NOT (length(regexp_replace(coalesce(o.telefone, ''), '\\D', '', 'g')) = 11 AND substr(regexp_replace(o.telefone, '\\D', '', 'g'), 3, 1) = '9' AND length(replace(substr(regexp_replace(o.telefone, '\\D', '', 'g'), 3), substr(regexp_replace(o.telefone, '\\D', '', 'g'), 3, 1), '')) > 0)",
                               "telefone curto, zerado ou placeholder (21999999999)"),
        "nascimento_implausivel": R("validade", "try_cast(o.data_nascimento AS DATE) IS NULL OR try_cast(o.data_nascimento AS DATE) NOT BETWEEN DATE '1900-01-01' AND current_date",
                                    "data de nascimento ausente, futura ou anterior a 1900"),
    })
    s.publicar("clientes", """
        pseudo(CAST(o.cliente_id AS VARCHAR)) AS cliente_pseudo,
        pseudo(o.cpf_dig) AS cpf_pseudo,
        o.cpf_dig IS NOT NULL AS cpf_valido,
        regexp_matches(coalesce(o.email, ''), '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$') AS email_valido,
        CAST(o.criado_em AS TIMESTAMPTZ) AS criado_em, CAST(o.atualizado_em AS TIMESTAMPTZ) AS atualizado_em,
        o._arquivo_origem, o._carregado_em""", "stg_clientes",
        mascarar_quarentena="nome, cpf, email, telefone, data_nascimento, cpf_dig")

    # ── ITENS (carregados antes das vendas: o total da venda é conferido contra eles) ──
    s.versao_mais_recente("stg_itens_venda", "sqlite/itens_venda", "item_id", "_capturado_em")
    s.con.execute("""CREATE OR REPLACE TABLE soma_itens AS
        SELECT try_cast(venda_id AS BIGINT) AS venda_id,
               sum(try_cast(quantidade AS BIGINT) * try_cast(preco_unitario_centavos AS BIGINT)) AS total
        FROM stg_itens_venda GROUP BY 1""")

    # ── VENDAS ───────────────────────────────────────────────────────────
    s.versao_mais_recente("stg_vendas", "sqlite/vendas", "venda_id", "try_cast(atualizado_em AS TIMESTAMPTZ)")
    s.aplicar_regras("vendas", "stg_vendas", {
        "id_invalido": R("validade", "try_cast(o.venda_id AS BIGINT) IS NULL", "venda_id não numérico"),
        "loja_inexistente": R("integridade", "l.loja_id IS NULL", "loja_id não existe em lojas"),
        "data_invalida": R("validade", "try_cast(o.vendido_em AS TIMESTAMPTZ) IS NULL OR try_cast(o.vendido_em AS TIMESTAMPTZ) > current_timestamp "
                                      "OR try_cast(o.vendido_em AS TIMESTAMPTZ) < TIMESTAMPTZ '2026-01-01 00:00:00-03'",
                           "data da venda inválida, futura ou fora da janela da base"),
        "status_invalido": R("validade", "lower(trim(o.status)) NOT IN ('concluida', 'cancelada')", "status fora de concluida/cancelada"),
        "valor_invalido": R("validade", "try_cast(o.valor_total_centavos AS BIGINT) IS NULL OR try_cast(o.valor_total_centavos AS BIGINT) = 0",
                            "valor total ausente ou zero"),
        "atualizado_antes_da_venda": R("consistência", "try_cast(o.atualizado_em AS TIMESTAMPTZ) < try_cast(o.vendido_em AS TIMESTAMPTZ)",
                                       "atualização anterior à venda"),
    }, juncoes="LEFT JOIN lojas l ON l.loja_id = try_cast(o.loja_id AS INTEGER) "
               "LEFT JOIN soma_itens si ON si.venda_id = try_cast(o.venda_id AS BIGINT) "
               "LEFT JOIN stg_clientes c ON c.cliente_id = o.cliente_id",
        correcoes={
            "valor_total_sinal_invertido": R("validade", "try_cast(o.valor_total_centavos AS BIGINT) < 0",
                                             "valor total negativo com itens positivos: sinal invertido, vira valor absoluto "
                                             "(conferido: em todos os casos |valor| = soma dos itens)"),
        }, alertas={
            "cliente_inexistente": R("integridade", "o.cliente_id IS NOT NULL AND c.cliente_id IS NULL", "cliente_id não existe em clientes"),
            "total_diverge_dos_itens": R("consistência", "si.total IS NOT NULL AND abs(try_cast(o.valor_total_centavos AS BIGINT)) <> si.total",
                                         "valor total diferente da soma dos itens (mesmo após corrigir o sinal)"),
            "venda_sem_itens": R("completude", "si.total IS NULL", "venda sem nenhum item"),
        })
    s.publicar("vendas", """
        CAST(o.venda_id AS BIGINT) AS venda_id, CAST(o.loja_id AS INTEGER) AS loja_id,
        o.cliente_id IS NOT NULL AND trim(o.cliente_id) <> '' AS cliente_identificado,
        CAST(o.vendido_em AS TIMESTAMPTZ) AS vendido_em,
        CAST(CAST(o.vendido_em AS TIMESTAMPTZ) AS DATE) AS data,
        abs(CAST(o.valor_total_centavos AS BIGINT)) AS valor_total_centavos,
        lower(trim(o.status)) AS status,
        CAST(o.atualizado_em AS TIMESTAMPTZ) AS atualizado_em, o._arquivo_origem, o._carregado_em""", "stg_vendas",
        mascarar_quarentena="cliente_id")

    # ── ITENS DE VENDA: regras ───────────────────────────────────────────
    s.aplicar_regras("itens_venda", "stg_itens_venda", {
        "id_invalido": R("validade", "try_cast(o.item_id AS BIGINT) IS NULL", "item_id não numérico"),
        "venda_inexistente_ou_reprovada": R("integridade", "v.venda_id IS NULL",
                                            "a venda do item não existe ou foi para a quarentena (efeito cascata intencional)"),
        "produto_inexistente": R("integridade", "p.produto_id IS NULL", "produto_id não existe em produtos (ex.: 9999)"),
        "quantidade_invalida": R("validade", "try_cast(o.quantidade AS INTEGER) IS NULL OR try_cast(o.quantidade AS INTEGER) <= 0",
                                 "quantidade ausente, zero ou negativa"),
        "preco_invalido": R("validade", "try_cast(o.preco_unitario_centavos AS BIGINT) IS NULL OR try_cast(o.preco_unitario_centavos AS BIGINT) <= 0",
                            "preço unitário ausente, zero ou negativo"),
        "lote_incompativel_com_produto": R("consistência", "regexp_matches(coalesce(o.lote_codigo, ''), '^L\\d{3}-\\d{4}$') AND "
                                           "try_cast(substr(o.lote_codigo, 2, 3) AS INTEGER) <> try_cast(o.produto_id AS INTEGER)",
                                           "o código do lote (Lxxx-) pertence a outro produto"),
    }, juncoes="LEFT JOIN vendas v ON v.venda_id = try_cast(o.venda_id AS BIGINT) "
               "LEFT JOIN produtos p ON p.produto_id = try_cast(o.produto_id AS INTEGER)",
        alertas={
            "lote_ausente": R("completude", "o.lote_codigo IS NULL OR trim(o.lote_codigo) = ''",
                              "item sem lote: entra nas vendas, mas não baixa nenhum lote do estoque"),
            "lote_formato_invalido": R("validade", "o.lote_codigo IS NOT NULL AND trim(o.lote_codigo) <> '' AND NOT regexp_matches(o.lote_codigo, '^L\\d{3}-\\d{4}$')",
                                       "lote fora do formato Lnnn-nnnn"),
        })
    s.publicar("itens_venda", """
        CAST(o.item_id AS BIGINT) AS item_id, CAST(o.venda_id AS BIGINT) AS venda_id,
        CAST(o.produto_id AS INTEGER) AS produto_id,
        CASE WHEN regexp_matches(coalesce(o.lote_codigo, ''), '^L\\d{3}-\\d{4}$') THEN o.lote_codigo END AS lote_codigo,
        CAST(o.quantidade AS INTEGER) AS quantidade, CAST(o.preco_unitario_centavos AS BIGINT) AS preco_unitario_centavos,
        o._arquivo_origem, o._carregado_em""", "stg_itens_venda")

    # ── INVENTÁRIO (CSV: contagem de segunda-feira por loja e lote) ─────────
    s.todas_as_linhas("stg_inventario", "csv")
    # Validade: lojas com outro sistema mandam só MM/AAAA. A API informa a validade EXATA dos lotes
    # que ela entregou; usamos essa data quando existe (é mais precisa) e, senão, o ÚLTIMO dia do mês
    # (convenção de embalagem: "VAL 01/2028" vale até o fim de janeiro).
    s.con.execute(f"""
        CREATE OR REPLACE TABLE validade_api AS
        SELECT try_cast(loja_id AS INTEGER) AS loja_id, try_cast(produto_id AS INTEGER) AS produto_id, lote_codigo,
               min(try_cast(validade_lote AS DATE)) AS validade_api
        FROM {s.bronze('api/pedidos-reposicao')} WHERE try_cast(validade_lote AS DATE) IS NOT NULL GROUP BY ALL""")
    inv_origem = """(
        SELECT b.*,
               row_number() OVER (PARTITION BY b.loja_id, b.produto_id, b.lote_codigo, b.data_contagem
                                  ORDER BY b._arquivo_origem) AS ocorrencia,
               regexp_extract(b._arquivo_origem, 'inventario/(\\d{4}-\\d{2}-\\d{2})/', 1) AS data_pasta,
               regexp_extract(b._arquivo_origem, 'loja_(\\d+)', 1) AS loja_arquivo,
               CASE WHEN regexp_matches(b.validade, '^\\d{4}-\\d{2}-\\d{2}$') THEN try_cast(b.validade AS DATE)
                    WHEN regexp_matches(b.validade, '^\\d{2}/\\d{4}$')
                         THEN coalesce(va.validade_api, last_day(try_strptime(b.validade, '%m/%Y'))) END AS validade_final,
               CASE WHEN regexp_matches(b.validade, '^\\d{4}-\\d{2}-\\d{2}$') THEN 'iso'
                    WHEN regexp_matches(b.validade, '^\\d{2}/\\d{4}$') AND va.validade_api IS NOT NULL THEN 'api'
                    WHEN regexp_matches(b.validade, '^\\d{2}/\\d{4}$') THEN 'ultimo_dia_do_mes' END AS validade_metodo
        FROM stg_inventario b
        LEFT JOIN validade_api va ON va.loja_id = try_cast(b.loja_id AS INTEGER)
             AND va.produto_id = try_cast(b.produto_id AS INTEGER) AND va.lote_codigo = b.lote_codigo)"""
    s.aplicar_regras("inventario", inv_origem, {
        "linha_duplicada": R("unicidade", "o.ocorrencia > 1", "mesma loja × produto × lote × data de contagem repetida no arquivo"),
        "loja_inexistente": R("integridade", "l.loja_id IS NULL", "loja_id não existe em lojas"),
        "produto_inexistente": R("integridade", "p.produto_id IS NULL", "produto_id não existe em produtos (ex.: 9999)"),
        "quantidade_invalida": R("validade", "try_cast(o.quantidade AS INTEGER) IS NULL", "quantidade ausente ou não numérica"),
        "quantidade_negativa": R("validade", "try_cast(o.quantidade AS INTEGER) < 0", "contagem negativa é impossível"),
        "validade_invalida": R("validade", f"o.validade_final IS NULL OR o.validade_final NOT BETWEEN {INICIO_BASE} AND {FIM_VALIDADE}",
                               "validade ilegível ou fora de 2026–2035"),
        "data_contagem_invalida": R("validade", "try_cast(o.data_contagem AS DATE) IS NULL", "data da contagem inválida"),
        "contagem_fora_da_pasta": R("consistência", "o.data_contagem <> o.data_pasta OR o.loja_id <> ltrim(o.loja_arquivo, '0')",
                                    "data/loja da linha diferente da pasta/arquivo de onde veio"),
        "lote_incompativel_com_produto": R("consistência", "NOT regexp_matches(coalesce(o.lote_codigo, ''), '^L\\d{3}-\\d{4}$') OR "
                                           "try_cast(substr(o.lote_codigo, 2, 3) AS INTEGER) <> try_cast(o.produto_id AS INTEGER)",
                                           "lote ausente/mal formado ou do código de outro produto"),
    }, juncoes="LEFT JOIN lojas l ON l.loja_id = try_cast(o.loja_id AS INTEGER) "
               "LEFT JOIN produtos p ON p.produto_id = try_cast(o.produto_id AS INTEGER)",
        correcoes={
            "validade_mes_ano_resolvida_pela_api": R("consistência", "o.validade_metodo = 'api'",
                                                     "validade MM/AAAA trocada pela data exata que a API informou para o mesmo lote"),
            "validade_mes_ano_ultimo_dia": R("consistência", "o.validade_metodo = 'ultimo_dia_do_mes'",
                                             "validade MM/AAAA sem outra fonte: assume o último dia do mês"),
        }, alertas={
            "quantidade_atipica": R("validade", "try_cast(o.quantidade AS INTEGER) >= 100", "contagem de 100 ou mais unidades de um lote (confira)"),
            "contagem_fora_da_segunda": R("consistência", "dayofweek(try_cast(o.data_contagem AS DATE)) <> 1", "contagem que não caiu numa segunda-feira"),
            "lote_vencido_na_contagem": R("consistência", "o.validade_final < try_cast(o.data_contagem AS DATE)",
                                          "lote já vencido na hora da contagem (fato de negócio: base da perda por vencimento)"),
        })
    s.publicar("inventario", """
        CAST(o.loja_id AS INTEGER) AS loja_id, CAST(o.produto_id AS INTEGER) AS produto_id, o.lote_codigo,
        o.validade_final AS validade, CAST(o.quantidade AS INTEGER) AS quantidade,
        CAST(o.data_contagem AS DATE) AS data_contagem, o._arquivo_origem, o._carregado_em""", "stg_inventario",
        particao="data_contagem")

    # ── EVENTOS DO PDV (JSONL) ───────────────────────────────────────────
    s.todas_as_linhas("stg_eventos", "events")
    # Fuso: ISO com offset (-03:00) ou Z já são convertidos corretamente por TIMESTAMPTZ; sem offset
    # assumimos Brasília (a sessão está em America/Sao_Paulo). A data do evento é a de `ocorrido_em`, não a do arquivo.
    ev_origem = """(
        SELECT b.*,
               row_number() OVER (PARTITION BY b.evento_id ORDER BY b._arquivo_origem, b.ocorrido_em, b.tipo) AS ocorrencia,
               try_cast(b.ocorrido_em AS TIMESTAMPTZ) AS ts,
               try_cast(regexp_extract(b._arquivo_origem, 'pdv_(\\d{4}-\\d{2}-\\d{2})', 1) AS DATE) AS data_arquivo
        FROM stg_eventos b)"""
    s.aplicar_regras("eventos", ev_origem, {
        "evento_duplicado": R("unicidade", "o.ocorrencia > 1", "evento_id repetido (reenvio do PDV); fica a primeira ocorrência"),
        "tipo_ausente": R("completude", "o.tipo IS NULL OR trim(o.tipo) = ''", "evento sem tipo"),
        "tipo_fora_do_dominio": R("validade", "o.tipo IS NOT NULL AND trim(o.tipo) <> '' AND lower(trim(o.tipo)) NOT IN ('venda','receita_retida','devolucao','cancelamento')",
                                  "tipo fora de venda/receita_retida/devolucao/cancelamento"),
        "data_invalida": R("validade", "o.ts IS NULL OR o.ts > current_timestamp OR o.ts < TIMESTAMPTZ '2026-01-01 00:00:00-03'",
                           "ocorrido_em vazio, ilegível ('2026-13-45...'), futuro ou fora da janela"),
        "venda_inexistente": R("integridade", "v.venda_id IS NULL", "venda_id não existe em vendas (aprovadas)"),
        "loja_diverge_da_venda": R("consistência", "v.venda_id IS NOT NULL AND v.loja_id <> try_cast(o.loja_id AS INTEGER)",
                                   "a loja do evento é diferente da loja da venda"),
        "produto_ausente": R("completude", "lower(trim(o.tipo)) IN ('venda','devolucao','receita_retida') AND (o.produto_id IS NULL OR trim(o.produto_id) = '')",
                             "evento de item sem produto_id (só o cancelamento da venda inteira pode vir sem)"),
        "produto_inexistente": R("integridade", "o.produto_id IS NOT NULL AND trim(o.produto_id) <> '' AND p.produto_id IS NULL",
                                 "produto_id não existe em produtos"),
        "quantidade_invalida": R("validade", "lower(trim(o.tipo)) IN ('venda','devolucao') AND (try_cast(o.quantidade AS INTEGER) IS NULL OR try_cast(o.quantidade AS INTEGER) <= 0)",
                                 "quantidade ausente, zero ou negativa num evento de item"),
    }, juncoes="LEFT JOIN vendas v ON v.venda_id = try_cast(o.venda_id AS BIGINT) "
               "LEFT JOIN produtos p ON p.produto_id = try_cast(o.produto_id AS INTEGER)",
        correcoes={
            "fuso_utc_convertido": R("consistência", "o.ts IS NOT NULL AND o.ocorrido_em LIKE '%Z'",
                                     "horário em UTC ('Z') convertido para o mesmo instante em Brasília"),
            "fuso_ausente_assumido_brasilia": R("consistência", "o.ts IS NOT NULL AND NOT regexp_matches(o.ocorrido_em, '(Z|[+-]\\d{2}:\\d{2})$')",
                                                "horário sem fuso: assume Brasília"),
            "tipo_padronizado": R("consistência", "o.tipo IS NOT NULL AND o.tipo <> lower(trim(o.tipo))", "tipo com espaços/maiúsculas padronizado"),
        }, alertas={
            "evento_atrasado": R("consistência", "o.ts IS NOT NULL AND CAST(o.ts AS DATE) <> o.data_arquivo",
                                 "evento chegou em arquivo de outro dia (a data vale pela hora do evento, não pelo arquivo)"),
        })
    s.publicar("eventos", """
        o.evento_id, CAST(o.loja_id AS INTEGER) AS loja_id, CAST(o.venda_id AS BIGINT) AS venda_id,
        lower(trim(o.tipo)) AS tipo, CAST(o.produto_id AS INTEGER) AS produto_id,
        CAST(o.quantidade AS INTEGER) AS quantidade,
        o.ts AS ocorrido_em, CAST(o.ts AS DATE) AS data,
        CAST(o.ts AS DATE) <> o.data_arquivo AS evento_atrasado,
        o._arquivo_origem, o._carregado_em""", "stg_eventos", particao="data", mascarar_quarentena="operador_id")

    # ── PEDIDOS DE REPOSIÇÃO (API: o mesmo pedido volta com sequência maior quando muda) ──
    s.versao_mais_recente("stg_pedidos", "api/pedidos-reposicao", "pedido_reposicao_id", "try_cast(sequencia AS BIGINT)")
    st = "replace(lower(trim(o.status)), ' ', '_')"
    s.aplicar_regras("pedidos_reposicao", "stg_pedidos", {
        "id_invalido": R("validade", "NOT regexp_matches(coalesce(o.pedido_reposicao_id, ''), '^REP-\\d+$')", "pedido_reposicao_id fora do formato REP-nnnnnn"),
        "sequencia_invalida": R("validade", "try_cast(o.sequencia AS BIGINT) IS NULL", "sequência não numérica"),
        "status_invalido": R("validade", f"{st} NOT IN ('em_transito', 'entregue')", "status fora de em_transito/entregue (após padronizar)"),
        "quantidade_invalida": R("validade", "try_cast(o.quantidade AS INTEGER) IS NULL OR try_cast(o.quantidade AS INTEGER) <= 0",
                                 "quantidade pedida ausente, zero ou negativa"),
        "produto_inexistente": R("integridade", "p.produto_id IS NULL", "produto_id não existe em produtos (ex.: 9999)"),
        "loja_inexistente": R("integridade", "l.loja_id IS NULL", "loja_id não existe em lojas"),
        "fornecedor_inexistente": R("integridade", "f.fornecedor_id IS NULL", "fornecedor_id não existe em fornecedores (aprovados)"),
        "fornecedor_diverge_do_produto": R("consistência", "p.produto_id IS NOT NULL AND p.fornecedor_id <> try_cast(o.fornecedor_id AS INTEGER)",
                                           "o distribuidor do pedido não é o do cadastro do produto"),
        "lote_incompativel_com_produto": R("consistência", "NOT regexp_matches(coalesce(o.lote_codigo, ''), '^L\\d{3}-\\d{4}$') OR "
                                           "try_cast(substr(o.lote_codigo, 2, 3) AS INTEGER) <> try_cast(o.produto_id AS INTEGER)",
                                           "lote ausente/mal formado ou do código de outro produto"),
        "datas_invalidas": R("validade", "try_cast(o.solicitado_em AS TIMESTAMPTZ) IS NULL OR try_cast(o.previsto_para AS DATE) IS NULL "
                                         "OR try_cast(o.validade_lote AS DATE) IS NULL "
                                         "OR (o.entregue_em IS NOT NULL AND try_cast(o.entregue_em AS TIMESTAMPTZ) IS NULL)",
                             "solicitado_em, previsto_para, validade_lote ou entregue_em ilegíveis"),
        "entregue_antes_de_solicitado": R("consistência", "try_cast(o.entregue_em AS TIMESTAMPTZ) < try_cast(o.solicitado_em AS TIMESTAMPTZ)",
                                          "entrega anterior ao pedido (data impossível)"),
        "previsto_antes_de_solicitado": R("consistência", "try_cast(o.previsto_para AS DATE) < CAST(try_cast(o.solicitado_em AS TIMESTAMPTZ) AS DATE)",
                                          "previsão de entrega anterior ao pedido"),
        "status_incoerente_com_entrega": R("consistência", f"({st} = 'entregue' AND o.entregue_em IS NULL) OR ({st} = 'em_transito' AND o.entregue_em IS NOT NULL)",
                                           "status 'em trânsito' com data de entrega (ou 'entregue' sem data): ambíguo, não corrigimos"),
    }, juncoes="LEFT JOIN produtos p ON p.produto_id = try_cast(o.produto_id AS INTEGER) "
               "LEFT JOIN lojas l ON l.loja_id = try_cast(o.loja_id AS INTEGER) "
               "LEFT JOIN fornecedores f ON f.fornecedor_id = try_cast(o.fornecedor_id AS INTEGER)",
        correcoes={
            "status_padronizado": R("consistência", "o.status <> " + st, "'ENTREGUE', 'entregue ' e 'em transito' padronizados para entregue/em_transito"),
        }, alertas={
            "prazo_previsto_diverge_do_fornecedor": R(
                "consistência", "f.prazo_entrega_dias IS NOT NULL AND date_diff('day', CAST(try_cast(o.solicitado_em AS TIMESTAMPTZ) AS DATE), "
                                "try_cast(o.previsto_para AS DATE)) <> f.prazo_entrega_dias",
                "previsto_para − solicitado_em diferente do prazo combinado do fornecedor"),
        })
    s.publicar("pedidos_reposicao", f"""
        o.pedido_reposicao_id, CAST(o.sequencia AS BIGINT) AS sequencia,
        CAST(o.fornecedor_id AS INTEGER) AS fornecedor_id, CAST(o.loja_id AS INTEGER) AS loja_id,
        CAST(o.produto_id AS INTEGER) AS produto_id, CAST(o.quantidade AS INTEGER) AS quantidade,
        o.lote_codigo, CAST(o.validade_lote AS DATE) AS validade_lote,
        CAST(o.solicitado_em AS TIMESTAMPTZ) AS solicitado_em, CAST(o.previsto_para AS DATE) AS previsto_para,
        CAST(o.entregue_em AS TIMESTAMPTZ) AS entregue_em, {st} AS status,
        CAST(o.atualizado_em AS TIMESTAMPTZ) AS atualizado_em, o._arquivo_origem, o._carregado_em""", "stg_pedidos")

    s.concluir()


if __name__ == "__main__":
    main()
