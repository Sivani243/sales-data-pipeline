# python/etl/validate_post_dbt.py
from __future__ import annotations
import os, sys
import psycopg2
import psycopg2.extras

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "warehouse")
DB_USER = os.getenv("DB_USER", "analytics")
DB_PASSWORD = os.getenv("DB_PASSWORD", "analytics_pw")

BASE_SCHEMA = os.getenv("DBT_SCHEMA", "analytics")
STG_SCHEMA = f"{BASE_SCHEMA}_staging"
INT_SCHEMA = f"{BASE_SCHEMA}_intermediate"
MRT_SCHEMA = f"{BASE_SCHEMA}_marts"

RECENT_DAYS = int(os.getenv("VALIDATE_RECENT_DAYS", "2"))       # freshness window
RECON_DAYS  = int(os.getenv("VALIDATE_RECON_DAYS", "7"))        # reconciliation window
REVENUE_EPS = float(os.getenv("VALIDATE_REVENUE_EPS", "0.01"))  # float tolerance

def q(conn, sql, params=None, one=False):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params or {})
        rows = cur.fetchall()
        return (rows[0] if rows else None) if one else rows

def fail(msg):
    print(f" {msg}")
    sys.exit(1)

def ok(msg):
    print(f" {msg}")

def main():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )
    try:
        # 0) Existence
        exists = q(conn, """
            select to_regclass(%s) is not null as exists
        """, (f"{MRT_SCHEMA}.fct_daily_sales",), one=True)["exists"]
        if not exists:
            fail(f"Table {MRT_SCHEMA}.fct_daily_sales does not exist")

        # 1) Row count > 0
        rc = q(conn, f"select count(*) as c from {MRT_SCHEMA}.fct_daily_sales", one=True)["c"]
        if rc == 0:
            fail("fct_daily_sales is empty")
        ok(f"Row count > 0 ({rc:,})")

        # 2) Uniqueness of sales_date
        uniq = q(conn, f"""
            select (count(*) = count(distinct sales_date)) as ok
            from {MRT_SCHEMA}.fct_daily_sales
        """, one=True)["ok"]
        if not uniq:
            fail("sales_date is not unique in fct_daily_sales")
        ok("sales_date is unique")

        # 3) Freshness (max date within RECENT_DAYS)
        fresh = q(conn, f"""
            select (max(sales_date) >= current_date - interval '{RECENT_DAYS} day') as fresh,
                   max(sales_date) as max_date
            from {MRT_SCHEMA}.fct_daily_sales
        """, one=True)
        if not fresh["fresh"]:
            fail(f"Data not fresh: max(sales_date) = {fresh['max_date']}")
        ok(f"Freshness OK (max sales_date = {fresh['max_date']})")

        # 4) Non-negative metrics
        negatives = q(conn, f"""
            select
              sum(case when orders < 0 then 1 else 0 end) as bad_orders,
              sum(case when units_sold < 0 then 1 else 0 end) as bad_units,
              sum(case when gross_revenue < 0 then 1 else 0 end) as bad_rev
            from {MRT_SCHEMA}.fct_daily_sales
        """, one=True)
        if any(v and v > 0 for v in negatives.values()):
            fail(f"Negative metrics detected: {negatives}")
        ok("No negative metrics")

        # 5) Null checks on required columns (defense-in-depth)
        nulls = q(conn, f"""
            select
              sum(case when sales_date is null then 1 else 0 end) as n_date,
              sum(case when orders     is null then 1 else 0 end) as n_orders,
              sum(case when units_sold is null then 1 else 0 end) as n_units,
              sum(case when gross_revenue is null then 1 else 0 end) as n_rev
            from {MRT_SCHEMA}.fct_daily_sales
        """, one=True)
        if any(v and v > 0 for v in nulls.values()):
            fail(f"Nulls detected in mart: {nulls}")
        ok("Null checks passed")

        print(" Post-dbt validation PASSED")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
