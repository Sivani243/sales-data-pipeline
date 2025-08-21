-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- Raw table
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

CREATE INDEX IF NOT EXISTS ix_raw_orders_updated_at ON raw.orders(updated_at);
