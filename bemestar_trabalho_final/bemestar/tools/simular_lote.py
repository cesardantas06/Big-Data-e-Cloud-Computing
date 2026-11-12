"""SIMULA o lote incremental da Etapa 10 (01/10 a 14/10/2026) sobre `fontes/`.

O `lote_incremental/aplicar_lote.py` do professor NÃO veio no material recebido; este script faz o mesmo
tipo de mudança (inserções, atualizações, arquivo substituído) com semente fixa, para testar o pipeline.
Os números do relatório que vierem deste lote são, portanto, de um lote SIMULADO — e estão rotulados assim.

Uso:  python tools/simular_lote.py            (idempotente: uma segunda execução não faz nada)
"""
from __future__ import annotations

import csv
import json
import random
import sqlite3
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
F = RAIZ / "fontes"
MARCA = F / ".lote_simulado_aplicado"
TZ = "-03:00"
rng = random.Random(20261001)


def ts(d: date, h: int | None = None) -> str:
    h = rng.randint(8, 21) if h is None else h
    return f"{d.isoformat()}T{h:02d}:{rng.randint(0, 59):02d}:{rng.randint(0, 59):02d}{TZ}"


def main() -> None:
    if MARCA.exists():
        print("Lote já aplicado (fontes/.lote_simulado_aplicado). Nada a fazer.")
        return
    db = sqlite3.connect(F / "sqlite" / "bemestar.db")
    dias = [date(2026, 10, 1) + timedelta(days=i) for i in range(14)]

    # ── clientes: 30 atualizados (contato mudou) + 40 novos ─────────────────────────────────────
    ids = [r[0] for r in db.execute("SELECT cliente_id FROM clientes")]
    for cid in rng.sample(ids, 30):
        db.execute("UPDATE clientes SET telefone = ?, email = ?, atualizado_em = ? WHERE cliente_id = ?",
                   (f"(21) 9{rng.randint(1000, 9999)}-{rng.randint(1000, 9999)}", f"novo{cid}@exemplo.com.br", ts(rng.choice(dias)), cid))
    prox_cli = max(ids) + 1
    for i in range(40):
        d = ts(rng.choice(dias))
        db.execute("INSERT INTO clientes VALUES (?,?,?,?,?,?,?,?)",
                   (prox_cli + i, f"Cliente Simulado {prox_cli + i}", f"{rng.randint(100, 999)}.{rng.randint(100, 999)}.{rng.randint(100, 999)}-{rng.randint(10, 99)}",
                    f"cli{prox_cli + i}@exemplo.com.br", f"(21) 9{rng.randint(1000, 9999)}-{rng.randint(1000, 9999)}", "1985-03-10", d, d))

    # ── produtos: 12 reajustes de preço + 3 produtos novos ──────────────────────────────────────
    prods = db.execute("SELECT produto_id, preco_centavos, fornecedor_id, categoria FROM produtos").fetchall()
    for pid, preco, _, _ in rng.sample(prods, 12):
        db.execute("UPDATE produtos SET preco_centavos = ?, atualizado_em = ? WHERE produto_id = ?",
                   (round(preco * rng.uniform(1.04, 1.12)), ts(date(2026, 10, 1), 0), pid))
    maxp = max(p[0] for p in prods)
    novos_prod = []
    for i, nome in enumerate(["Vitamina D 2000UI", "Protetor Solar FPS60", "Soro Fisiológico 500ml"]):
        pid = maxp + 1 + i
        _, _, forn, cat = rng.choice(prods)
        db.execute("INSERT INTO produtos VALUES (?,?,?,?,?,?,?,?,?)",
                   (pid, f"789{rng.randint(10**9, 10**10 - 1)}", nome, None, cat, 0, rng.randint(1500, 9000), forn, ts(date(2026, 10, 2), 9)))
        novos_prod.append((pid, forn))

    # ── estoque de partida: última contagem (28/09) ─────────────────────────────────────────────
    est: dict[tuple, int] = {}
    validade: dict[tuple, str] = {}
    for arq in sorted((F / "csv" / "inventario" / "2026-09-28").glob("*.csv")):
        for r in csv.DictReader(arq.open(encoding="utf-8")):
            k = (int(r["loja_id"]), int(r["produto_id"]), r["lote_codigo"])
            est[k] = est.get(k, 0) + int(r["quantidade"]); validade[k] = r["validade"]
    # vendas e entregas posteriores a 28/09 (para o estado de 30/09)
    for lote, loja, pid, q in db.execute("""SELECT i.lote_codigo, v.loja_id, i.produto_id, i.quantidade FROM itens_venda i
            JOIN vendas v USING (venda_id) WHERE v.status='concluida' AND date(v.vendido_em) > '2026-09-28'"""):
        k = (loja, pid, lote)
        if k in est: est[k] -= q
    api_path = F / "api" / "pedidos_reposicao.jsonl"
    pedidos = [json.loads(l) for l in api_path.open(encoding="utf-8")]
    ultimo: dict[str, dict] = {}
    for p in pedidos:
        if p["sequencia"] > 0: ultimo[p["pedido_reposicao_id"]] = p
    for p in ultimo.values():
        if p["status"].strip().lower().startswith("entregue") and p["entregue_em"] and p["entregue_em"][:10] > "2026-09-28":
            k = (p["loja_id"], p["produto_id"], p["lote_codigo"]); est[k] = est.get(k, 0) + p["quantidade"]; validade[k] = p["validade_lote"]

    lojas = [r[0] for r in db.execute("SELECT loja_id FROM lojas")]
    # ── pedidos de reposição: entregas de pedidos em trânsito + pedidos novos (API, sequências novas) ──
    seq = max(p["sequencia"] for p in pedidos); novos = []; entregas: list = []
    em_transito = [p for p in ultimo.values() if p["status"].strip().lower().replace(" ", "_") == "em_transito"]
    for p in rng.sample(em_transito, min(150, len(em_transito))):
        base = date.fromisoformat(p["previsto_para"]); atraso = rng.choice([-1, 0, 0, 1, 2, 4])
        ent = max(date(2026, 10, 1), base + timedelta(days=atraso)); ent = min(ent, dias[-1])
        seq += 1; novos.append({**p, "sequencia": seq, "entregue_em": ts(ent), "status": "entregue", "atualizado_em": ts(ent)})
        entregas.append((ent, (p["loja_id"], p["produto_id"], p["lote_codigo"]), p["quantidade"], p["validade_lote"]))
    num = max(int(p["pedido_reposicao_id"][4:]) for p in pedidos)
    forn_de = {p: f for p, _, f, _ in prods}
    for pid, forn in novos_prod: forn_de[pid] = forn
    lotes_n = Counter()
    for d in dias:
        for _ in range(rng.randint(45, 70)):
            num += 1; pid = rng.choice(list(forn_de)); loja = rng.choice(lojas); prazo = rng.choice([2, 3, 4])
            lotes_n[pid] += 1; prev = d + timedelta(days=prazo)
            entregue = rng.random() < 0.7 and prev + timedelta(days=1) <= dias[-1]
            ent = min(dias[-1], prev + timedelta(days=rng.choice([0, 0, 1, 3]))) if entregue else None
            seq += 1
            novos.append({"pedido_reposicao_id": f"REP-{num:06d}", "sequencia": seq, "fornecedor_id": forn_de[pid], "loja_id": loja, "produto_id": pid,
                          "quantidade": rng.choice([5, 6, 8, 10, 12]), "lote_codigo": f"L{pid:03d}-{9000 + lotes_n[pid]:04d}",
                          "validade_lote": (d + timedelta(days=rng.randint(300, 900))).isoformat(), "solicitado_em": ts(d), "previsto_para": prev.isoformat(),
                          "entregue_em": ts(ent) if ent else None, "status": "entregue" if ent else "em_transito", "atualizado_em": ts(ent or d)})
    with api_path.open("a", encoding="utf-8") as f:
        for p in novos: f.write(json.dumps(p, ensure_ascii=False) + "\n")
    for p in novos:
        if p["status"] == "entregue" and p["pedido_reposicao_id"] not in ultimo:
            entregas.append((date.fromisoformat(p["entregue_em"][:10]), (p["loja_id"], p["produto_id"], p["lote_codigo"]), p["quantidade"], p["validade_lote"]))
    entregas.sort(key=lambda e: e[0])
    # ── vendas 01–14/10 (FEFO sobre o estoque simulado) + eventos do PDV ───────────────────────
    freq = Counter(p for (p,) in db.execute("SELECT produto_id FROM itens_venda JOIN vendas USING (venda_id) WHERE date(vendido_em) >= '2026-09-01'"))
    prod_preco = {p: preco for p, preco, *_ in prods}
    for r in db.execute("SELECT produto_id, preco_centavos FROM produtos"): prod_preco[r[0]] = r[1]
    clientes_ids = ids + list(range(prox_cli, prox_cli + 40))
    venda_id = db.execute("SELECT max(venda_id) FROM vendas").fetchone()[0]
    item_id = db.execute("SELECT max(item_id) FROM itens_venda").fetchone()[0]
    ev_n = max(int(json.loads(l)["evento_id"][3:]) for f in (F / "events").glob("*.jsonl") for l in f.open())
    populares = [p for p in freq if p in prod_preco]; pesos = [freq[p] for p in populares]   # ignora ids órfãos (ex.: 9999)
    lotes_por: dict[tuple, list] = defaultdict(list)
    for k, q in est.items(): lotes_por[(k[0], k[1])].append(k)
    snapshots: dict[date, dict] = {}
    for d in dias:
        while entregas and entregas[0][0] <= d:                          # entregas já recebidas até a manhã de d
            _, k, q, val = entregas.pop(0); est[k] = est.get(k, 0) + q; validade[k] = val
            lotes_por[(k[0], k[1])].append(k) if k not in lotes_por[(k[0], k[1])] else None
        if d in (date(2026, 10, 5), date(2026, 10, 12)):                 # contagem física da segunda de manhã
            snapshots[d] = {k: q for k, q in est.items() if q > 0}
        eventos = []
        for _ in range(rng.randint(260, 340)):
            loja = rng.choice(lojas); itens = []
            for _ in range(rng.choice([1, 1, 2, 3])):
                pid = rng.choices(populares, pesos)[0]
                cand = sorted((k for k in lotes_por.get((loja, pid), []) if est[k] > 0 and validade[k] >= d.isoformat()), key=lambda k: validade[k])
                if not cand: continue                                            # sem estoque: a venda do item não acontece
                k = cand[0]; q = min(est[k], rng.choice([1, 1, 1, 2, 3])); est[k] -= q
                itens.append((pid, k[2], q))
            if not itens: continue
            venda_id += 1; quando = ts(d)
            total = sum(q * prod_preco[p] for p, _, q in itens)
            cli = rng.choice(clientes_ids) if rng.random() < 0.6 else None
            db.execute("INSERT INTO vendas VALUES (?,?,?,?,?,?,?)", (venda_id, loja, cli, quando, total, "concluida", quando))
            for pid, lote, q in itens:
                item_id += 1; ev_n += 1
                db.execute("INSERT INTO itens_venda VALUES (?,?,?,?,?,?)", (item_id, venda_id, pid, lote, q, prod_preco[pid]))
                eventos.append({"evento_id": f"PDV{ev_n:09d}", "loja_id": loja, "venda_id": venda_id, "tipo": "venda", "produto_id": pid,
                                "quantidade": q, "ocorrido_em": quando, "operador_id": f"OP{rng.randint(1, 120):03d}"})
        with (F / "events" / f"pdv_{d.isoformat()}.jsonl").open("w", encoding="utf-8") as f:
            for e in eventos: f.write(json.dumps(e, ensure_ascii=False) + "\n")
    # 12 vendas de fim de setembro canceladas depois (atualização de status)
    for (vid,) in db.execute("SELECT venda_id FROM vendas WHERE status='concluida' AND date(vendido_em) >= '2026-09-24' ORDER BY random() LIMIT 12").fetchall():
        db.execute("UPDATE vendas SET status='cancelada', atualizado_em=? WHERE venda_id=?", (ts(rng.choice(dias)), vid))
    db.commit(); db.close()

    # ── inventário: contagens das segundas 05/10 e 12/10 (estado simulado da manhã de cada dia) ──
    for seg, snap in snapshots.items():
        pasta = F / "csv" / "inventario" / seg.isoformat(); pasta.mkdir(parents=True, exist_ok=True)
        por_loja: dict[int, list] = defaultdict(list)
        for (loja, pid, lote), q in snap.items():
            if validade[(loja, pid, lote)] >= seg.isoformat(): por_loja[loja].append((pid, lote, validade[(loja, pid, lote)], q))
        for loja in lojas:
            with (pasta / f"loja_{loja:02d}.csv").open("w", encoding="utf-8", newline="") as f:
                w = csv.writer(f); w.writerow(["loja_id", "produto_id", "lote_codigo", "validade", "quantidade", "data_contagem"])
                for pid, lote, val, q in sorted(por_loja[loja]): w.writerow([loja, pid, lote, val, q, seg.isoformat()])

    # ── fornecedores.json: arquivo substituído (nova versão) ────────────────────────────────────
    jf = F / "json" / "fornecedores.json"; forn = json.loads(jf.read_text(encoding="utf-8"))
    for x in forn:
        if x["fornecedor_id"] == 1: x["pedido_minimo_reais"] = 900
        if x["fornecedor_id"] == 3: x["prazo_entrega_dias"] = 3
    jf.write_text(json.dumps(forn, ensure_ascii=False, indent=2), encoding="utf-8")
    MARCA.write_text(datetime.now().isoformat())
    print(f"Lote simulado aplicado: {venda_id} vendas no total, {len(novos)} versões de pedido novas, 2 contagens de inventário.")


if __name__ == "__main__":
    main()
