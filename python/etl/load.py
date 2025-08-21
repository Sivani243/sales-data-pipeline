from __future__ import annotations
import io
import psycopg2
import pandas as pd
from .config import SETTINGS
from .logger import logger

DDL = """
CREATE TABLE IF NOT EXISTS raw.orders (
  order_id        BIGINT,
  order_line_id   BIGINT,
  order_ts        TIMESTAMP,
  customer_id     BIGINT,
  product_id      BIGINT,
  quantity        INTEGER,
  unit_price      NUMERIC(12,2),
  currency        VARCHAR(3),
  country         VARCHAR(64),
  status          VARCHAR(16),
  updated_at      TIMESTAMP DEFAULT NOW()
);
"""

def _connect():
    return psycopg2.connect(
        host=SETTINGS.db_host,
        port=SETTINGS.db_port,
        dbname=SETTINGS.db_name,
        user=SETTINGS.db_user,
        password=SETTINGS.db_password,
    )

def ensure_table():
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        cur.execute(DDL)
        conn.commit()
    logger.info("Ensured raw.orders exists\n")

def copy_dataframe(df: pd.DataFrame):
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    with _connect() as conn, conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_orders (LIKE raw.orders INCLUDING ALL);")
        cur.copy_expert("""
            COPY tmp_orders (
              order_id, order_line_id, order_ts, customer_id, product_id, quantity, unit_price,
              currency, country, status, updated_at
            ) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
        """, buf)
        cur.execute("INSERT INTO raw.orders SELECT * FROM tmp_orders;")
        conn.commit()
    logger.info(f"Loaded {len(df):,} rows into raw.orders\n")
