# python/etl/validate.py
from __future__ import annotations
import pandas as pd
from datetime import datetime
from typing import List
from pydantic import BaseModel, Field, ValidationError, ConfigDict

class OrderLine(BaseModel):
    # allow pandas/other types if any sneak in
    model_config = ConfigDict(arbitrary_types_allowed=True)

    order_id: int
    order_line_id: int
    order_ts: datetime
    customer_id: int
    product_id: int
    quantity: int = Field(gt=0)
    unit_price: float = Field(gt=0)
    currency: str
    country: str
    status: str
    updated_at: datetime

ALLOWED_STATUSES = {"completed", "cancelled", "pending"}

def validate_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    expected = {
        "order_id","order_line_id","order_ts","customer_id","product_id",
        "quantity","unit_price","currency","country","status","updated_at"
    }
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    # basic filters
    df = df[df["status"].isin(ALLOWED_STATUSES)].copy()
    df["order_ts"]   = pd.to_datetime(df["order_ts"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
    df = df[(df["quantity"] > 0) & (df["unit_price"] > 0)]
    df = df.drop_duplicates()
    df = (
        df.sort_values("updated_at")
          .dropna(subset=["order_ts","updated_at"])
          .drop_duplicates(subset=["order_id","order_line_id","order_ts"], keep="last")
    )

    # IMPORTANT: convert to python datetime so Pydantic v2 is happy
    df["order_ts"]   = df["order_ts"].dt.to_pydatetime()
    df["updated_at"] = df["updated_at"].dt.to_pydatetime()

    # lightweight row validation on a small sample
    sample = df.head(200)
    errors: List[str] = []
    for _, row in sample.iterrows():
        try:
            OrderLine(**row.to_dict())
        except ValidationError as ve:
            errors.append(str(ve))
    if errors:
        raise ValueError("Validation errors: " + " | ".join(errors[:5]))

    return df
