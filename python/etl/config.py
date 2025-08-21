from dataclasses import dataclass
import os

@dataclass(frozen=True)
class Settings:
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", 5432))
    db_name: str = os.getenv("DB_NAME", "warehouse")
    db_user: str = os.getenv("DB_USER", "analytics")
    db_password: str = os.getenv("DB_PASSWORD", "analytics_pw")

    data_source_path: str = os.getenv("DATA_SOURCE_PATH", "/app/data/example_sales.csv")
    rows: int = int(os.getenv("ROWS", 5000))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

SETTINGS = Settings()
