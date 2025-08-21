from __future__ import annotations
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

STATUSES = ["completed", "cancelled", "pending"]
COUNTRIES = ["US", "CA", "GB", "DE", "FR", "IN", "AU"]
CURRENCIES = {"US": "USD", "CA": "CAD", "GB": "GBP", "DE": "EUR", "FR": "EUR", "IN": "INR", "AU": "AUD"}
rng = np.random.default_rng(42)

def synthetic_orders(n_rows: int = 5000, days: int = 60) -> pd.DataFrame:
    end = datetime.utcnow()
    start = end - timedelta(days=days)

    order_ids   = rng.integers(10_000_000, 99_999_999, size=n_rows)
    line_ids    = rng.integers(1, 5, size=n_rows)
    order_ts    = pd.to_datetime(rng.integers(int(start.timestamp()), int(end.timestamp()), size=n_rows), unit="s")
    customer_id = rng.integers(1_000, 9_999, size=n_rows)
    product_id  = rng.integers(100, 999, size=n_rows)
    quantity    = rng.integers(1, 8, size=n_rows)
    unit_price  = (rng.normal(40, 15, size=n_rows).clip(1, None)).round(2)

    countries = rng.choice(COUNTRIES, size=n_rows, replace=True)
    currencies = [CURRENCIES[c] for c in countries]
    statuses = rng.choice(STATUSES, size=n_rows, p=[0.85, 0.05, 0.10])

    df = pd.DataFrame({
        "order_id": order_ids,
        "order_line_id": line_ids,
        "order_ts": order_ts,
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": quantity,
        "unit_price": unit_price,
        "currency": currencies,
        "country": countries,
        "status": statuses,
        "updated_at": pd.Timestamp.utcnow(),
    })

    dup_sample = df.sample(frac=0.01, random_state=7)
    return pd.concat([df, dup_sample], ignore_index=True)
