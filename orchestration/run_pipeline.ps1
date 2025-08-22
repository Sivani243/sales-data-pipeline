# Stop on errors
$ErrorActionPreference = "Stop"

# Load .env into the process env (optional)
if (Test-Path ".env") {
  Get-Content ".env" |
    Where-Object { $_ -notmatch '^\s*#' -and $_ -match '=' } |
    ForEach-Object {
      $name, $value = $_ -split '=', 2
      [Environment]::SetEnvironmentVariable($name.Trim(), $value.Trim())
    }
}

# Defaults from env or fallbacks
$pgUser  = if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { "analytics" }
$pgDb    = if ($env:POSTGRES_DB)   { $env:POSTGRES_DB }   else { "warehouse" }
$dbtBase = if ($env:DBT_SCHEMA)    { $env:DBT_SCHEMA }    else { "analytics" }

# Optional toggles
# - FULL_REFRESH: set to "1" to force full rebuild (otherwise incremental)
$full = if ($env:FULL_REFRESH -and $env:FULL_REFRESH -ne "0") { "--full-refresh" } else { "" }

Write-Output "[1/7] Building images..."
docker compose build

Write-Output "[2/7] Starting Postgres..."
docker compose up -d postgres

# Wait for healthcheck
Write-Output "Waiting for Postgres to be healthy"
$maxTries = 60
for ($i = 0; $i -lt $maxTries; $i++) {
  $status = docker inspect -f '{{.State.Health.Status}}' sales_pg 2>$null
  if ($status -eq "healthy") { Write-Output " - ready"; break }
  Start-Sleep -Seconds 2
  if ($i -eq ($maxTries - 1)) { throw "Postgres did not become healthy in time" }
}

Write-Output "[3/7] Running Python ETL..."
docker compose run --rm python-app python -m etl.main

Write-Output "[4/7] Running dbt (deps -> snapshots -> run -> test)..."
# Build the dbt command as a single string for bash -lc
$dbtCmd = "dbt deps && dbt run --select staging intermediate $full && dbt snapshot && dbt run --select marts $full && dbt test"


docker compose run --rm dbt bash -lc $dbtCmd

Write-Output "[5/7] Generating dbt docs..."
docker compose run --rm dbt bash -lc "dbt docs generate"
Write-Output "Docs generated at .\dbt\target\index.html"

# Compute schema names produced by dbt (base_schema + suffix)
$martsSchema = "${dbtBase}_marts"

Write-Output "[6/7] Sample results ($martsSchema.fct_daily_sales):"
$cmd = "SELECT sales_date, orders, units_sold, gross_revenue FROM ${martsSchema}.fct_daily_sales ORDER BY sales_date DESC LIMIT 10;"
docker compose exec -T postgres psql -U $pgUser -d $pgDb -c $cmd

Write-Output "[7/7] Done"