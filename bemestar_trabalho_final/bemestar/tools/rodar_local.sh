#!/usr/bin/env bash
# Executa o pipeline inteiro sem Airflow (mesma ordem do DAG). Pré-requisito: API (:8001) e SeaweedFS (:8333) no ar.
# Uso:  bash tools/rodar_local.sh [--sem-lake]
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
export PYTHONPATH=. PYTHONUTF8=1
export PSEUDO_CHAVE="${PSEUDO_CHAVE:?defina PSEUDO_CHAVE (segredo do HMAC)}"
LAKE=1; [ "${1:-}" = "--sem-lake" ] && LAKE=0
py=python
passo() { echo; echo "=== $* ==="; }

passo EL; $py -m pipeline.el.sqlite; for pasta in csv json events; do $py -m pipeline.el.arquivos --pasta $pasta; done
$py -m pipeline.el.api --api-url "${API_URL:-http://127.0.0.1:8001}"
[ $LAKE = 1 ] && { passo "Landing no Lake"; $py -m pipeline.lake.sync --criar-bucket; $py -m pipeline.lake.sync --pasta data/lake/landing --prefixo landing; }
passo Bronze; $py -m pipeline.transform.bronze
passo Silver; $py -m pipeline.transform.silver
passo Gate;   $py -m pipeline.transform.gate --max-quarentena-pct "${MAX_QUARENTENA_PCT:-3}"
passo Gold;   $py -m pipeline.transform.gold
passo Validar; $py -m pipeline.transform.validar
if [ $LAKE = 1 ]; then
  passo "Publicar no Lake"
  for c in bronze silver quarentena; do $py -m pipeline.lake.sync --pasta data/lake/$c --prefixo $c --espelhar; done
  $py -m pipeline.lake.sync --pasta data/qualidade --prefixo qualidade --espelhar
  $py -m pipeline.lake.sync --pasta data/warehouse --prefixo warehouse --espelhar
fi
passo "Produto de dados"; if [ $LAKE = 1 ]; then $py -m pipeline.produto.executar; else $py -m pipeline.produto.executar --sem-publicar; fi
echo; echo "Pipeline concluído."
