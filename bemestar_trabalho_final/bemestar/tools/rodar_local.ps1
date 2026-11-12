# Executa o pipeline inteiro sem Airflow (mesma ordem do DAG). Pré-requisito: API (:8001) e SeaweedFS (:8333) no ar,
# venv ativado e $env:PSEUDO_CHAVE definida.
param([switch]$SemLake)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
$env:PYTHONPATH = "."; $env:PYTHONUTF8 = "1"
if (-not $env:PSEUDO_CHAVE) { throw "Defina `$env:PSEUDO_CHAVE (segredo do HMAC)." }
if (-not $env:AWS_ACCESS_KEY_ID) { $env:AWS_ACCESS_KEY_ID = "admin"; $env:AWS_SECRET_ACCESS_KEY = "bigdata-secret" }  # lab local
function Passo($nome, [scriptblock]$cmd) { Write-Host "`n=== $nome ==="; & $cmd; if ($LASTEXITCODE -ne 0) { throw "Falhou: $nome" } }
$max = if ($env:MAX_QUARENTENA_PCT) { $env:MAX_QUARENTENA_PCT } else { "3" }
Passo "EL sqlite"  { python -m pipeline.el.sqlite }
foreach ($p in "csv", "json", "events") { Passo "EL $p" { python -m pipeline.el.arquivos --pasta $p } }
Passo "EL api"     { python -m pipeline.el.api }
if (-not $SemLake) { Passo "Landing no Lake" { python -m pipeline.lake.sync --criar-bucket; python -m pipeline.lake.sync --pasta data/lake/landing --prefixo landing } }
Passo "Bronze"     { python -m pipeline.transform.bronze }
Passo "Silver"     { python -m pipeline.transform.silver }
Passo "Gate"       { python -m pipeline.transform.gate --max-quarentena-pct $max }
Passo "Gold"       { python -m pipeline.transform.gold }
Passo "Validar"    { python -m pipeline.transform.validar }
if (-not $SemLake) {
  foreach ($c in "bronze", "silver", "quarentena") { Passo "Publicar $c" { python -m pipeline.lake.sync --pasta "data/lake/$c" --prefixo $c --espelhar } }
  Passo "Publicar qualidade" { python -m pipeline.lake.sync --pasta data/qualidade --prefixo qualidade --espelhar }
  Passo "Publicar warehouse" { python -m pipeline.lake.sync --pasta data/warehouse --prefixo warehouse --espelhar }
}
Passo "Produto de dados" { if ($SemLake) { python -m pipeline.produto.executar --sem-publicar } else { python -m pipeline.produto.executar } }
Write-Host "`nPipeline concluído."
