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

$pgUser  = if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { "analytics" }
$pgDb    = if ($env:POSTGRES_DB)   { $env:POSTGRES_DB }   else { "warehouse" }

Write-Output "[1/6] Building images..."
docker compose build

Write-Output "[2/6] Starting Postgres..."
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

Write-Output "[3/6] Running Python ETL..."
docker compose run --rm python-app python -m etl.main

Write-Output "[4/6] Running dbt models and tests..."
docker compose run --rm dbt bash -lc 'dbt deps && dbt run --full-refresh && dbt test'

# Compute schema names produced by dbt (base_schema + suffix)
$dbtBase     = if ($env:DBT_SCHEMA) { $env:DBT_SCHEMA } else { "analytics" }
$martsSchema = "${dbtBase}_marts"

Write-Output "[5/6] Sample results ($martsSchema.fct_daily_sales):"
$cmd = "SELECT sales_date, orders, units_sold, gross_revenue FROM ${martsSchema}.fct_daily_sales ORDER BY sales_date DESC LIMIT 10;"
docker compose exec -T postgres psql -U $pgUser -d $pgDb -c $cmd

Write-Output "[6/6] Done"