# Sales Data Pipeline

This project demonstrates an end-to-end data pipeline using Python, PostgreSQL, and dbt, orchestrated with Docker Compose. It ingests sales data, validates it, loads it into Postgres, and transforms it into analytics-ready models.

## Features

- **Postgres Warehouse** – Centralized data storage with layered schemas (`raw`, `staging`, `intermediate`, `marts`)
- **Python ETL** – Extraction from CSV or synthetic generation, validation, and bulk loading into `raw.orders`
- **dbt Models** – Transformations in three layers: `staging` → `intermediate` → `marts`
- **Automated Orchestration** – Bash/PowerShell scripts to run the full pipeline
- **Testing & Documentation** – dbt tests (`not_null`, `unique`) and auto-generated lineage/docs

## Setup

### Prerequisites

- Docker and Docker Compose
- Git
- Basic knowledge of Python & SQL

### Clone the Repository

```bash
git clone https://github.com/Sivani243/sales-data-pipeline.git
cd sales-data-pipeline
```

### Environment Variables

Copy the example file and adjust if needed:

```bash
cp .env.example .env
```

Default `.env` includes:

```env
POSTGRES_USER=analytics
POSTGRES_PASSWORD=analytics_pw
POSTGRES_DB=warehouse
ROWS=5000
LOG_LEVEL=INFO
```

## Running the Pipeline

### Linux / macOS
```bash
./orchestration/run_pipeline.sh
```

### Windows (PowerShell)
```powershell


.\orchestration\run_pipeline.ps1

if you face Execution policy Error.

Copy-Item .env.example .env -ErrorAction SilentlyContinue                            
>> Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
>> .\orchestration\run_pipeline.ps1 -d
```

This will:

1. Build all Docker images
2. Start Postgres and wait for health check
3. Run the Python ETL (generate/validate/load data into `raw.orders`)
4. Run dbt models (`staging` → `intermediate` → `marts`) and tests
5. Generate dbt docs
6. Print sample results from `fct_daily_sales`

## Architecture

### Services

- **Postgres** – Warehouse database, initialized with schemas: `raw`, `staging`, `intermediate`, `marts`
- **Python ETL** – Loads data into `raw.orders` (reads CSV or generates synthetic sales data)
- **dbt** – Transforms raw data into analytics-ready tables, runs tests, generates lineage/docs

### Data Flow

1. **Extract** – Read CSV or generate synthetic sales orders
2. **Validate** – Enforce schema, datatypes, business rules, dedupe
3. **Load** – Bulk load into `raw.orders` in Postgres
4. **Transform (dbt)**:
   - **Staging**: Type casting, standardization
   - **Intermediate**: Enrich data (e.g., `line_amount`)
   - **Marts**: Aggregated facts (`fct_daily_sales`)
5. **Test & Document** – dbt validates constraints and generates documentation

## Example Output

After running the pipeline, you'll see sample results:

```sql
SELECT * FROM marts.fct_daily_sales ORDER BY sales_date DESC LIMIT 10;
```

| sales_date | orders | units_sold | gross_revenue |
|------------|--------|------------|---------------|
| 2025-08-21 | 59     | 231        | 9240.24       |
| 2025-08-20 | 82     | 330        | 13925.16      |
| ...        | ...    | ...        | ...           |

## Documentation

Generate dbt docs:

```bash
docker compose run --rm -p 8080:8080 dbt bash -lc "dbt docs generate && dbt docs serve --port 8080"
```

- **dbt tests** – `not_null`, `unique`, accepted values
- **Orchestration checks** – Waits for Postgres health before continuing

## Project Structure

```
sales-data-pipeline/
├── .env.example
├── docker-compose.yml
├── orchestration/
│   ├── run_pipeline.sh
│   └── run_pipeline.ps1
├── etl/
│   └── [Python ETL scripts]
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── dbt_project.yml
└── README.md
```

## Conclusion

This project demonstrates a modular, reproducible, and testable data pipeline. It highlights best practices across ETL, data warehousing, and dbt-based transformations—all containerized for simplicity.

---

**Contributing**

Feel free to submit issues and enhancement requests!
