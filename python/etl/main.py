from __future__ import annotations
import os
import sys
import pandas as pd
from .config import SETTINGS
from .logger import logger
from .generate_data import synthetic_orders
from .validate import validate_dataframe
from .load import ensure_table, copy_dataframe

def read_source(path: str) -> pd.DataFrame:
    if os.path.exists(path):
        logger.info(f"Reading CSV from {path} …\n")
        return pd.read_csv(path)
    logger.info("CSV not found — generating synthetic data …\n")
    return synthetic_orders(SETTINGS.rows)

def main():
    try:
        df = read_source(SETTINGS.data_source_path)
        logger.info(f"Source rows: {len(df):,}\n")
        df = validate_dataframe(df)
        logger.info(f"Validated rows: {len(df):,}\n")
        ensure_table()
        copy_dataframe(df)
        logger.info("ETL completed successfully ✅\n")
    except Exception as e:
        logger.exception(f"ETL failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
