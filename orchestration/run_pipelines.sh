#!/usr/bin/env bash
set -euo pipefail

# Load .env if present
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

echo "[1/7] Building images…"
docker compose build --no-cache

echo "[2/7] Starting Postgres…"
docker compose up -d postgres

# Wait for healthcheck
printf "Waiting for Postgres to be healthy"
for i in {1..60}; do
  if [ "$(docker inspect -f '{{.State.Health.Status}}' sales_pg)" == "healthy" ]; then
    echo " — ready"; break
  fi
  printf "."; sleep 2
  if [ "$i" -eq 60 ]; then echo "\nPostgres did not become healthy in time"; exit 1; fi
done

echo "[3/7] Running Python ETL…"
docker compose run --rm python-app python -m etl.main

echo "[4/7] Running dbt models…"
docker compose run --rm dbt bash -lc "dbt deps && dbt run && dbt test"

echo "[5/7] Sample results:"
docker compose exec -T postgres psql -U ${POSTGRES_USER:-analytics} -d ${POSTGRES_DB:-warehouse} -c "\
  SELECT * FROM marts.fct_daily_sales ORDER BY sales_date DESC LIMIT 10;\
"
echo "[6/7] Validating post-dbt…"
docker compose run --rm \
  -e VALIDATE_RECENT_DAYS=2 \
  -e VALIDATE_RECON_DAYS=7 \
  -e VALIDATE_REVENUE_EPS=0.01 \
  python-app python -m etl.validate_post_dbt

echo "[7/7] Done"
