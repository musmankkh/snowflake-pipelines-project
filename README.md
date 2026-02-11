# 🏔️ `Snowflake Pipeline` — dbt + Snowflake Data Pipeline

> A production-grade data engineering pipeline built with **dbt Core** and **Snowflake**.  
> Ingests Excel/CSV source files through a **Bronze → Silver → Gold** medallion architecture,  
> with automated CI/CD via GitHub Actions and scheduled orchestration via Snowflake Task DAGs.

---

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Local Setup](#local-setup)
- [Environment Configuration](#environment-configuration)
- [Running the Pipeline](#running-the-pipeline)
- [File Ingestion (Excel / CSV)](#file-ingestion-excel--csv)
- [CI/CD Pipeline](#cicd-pipeline)
- [Orchestration — Snowflake Task DAGs](#orchestration--snowflake-task-dags)
- [Monitoring & Logging](#monitoring--logging)
- [Contributing](#contributing)
- [Useful Commands](#useful-commands)

---

## Architecture Overview

```
Local Excel / CSV Files
        │
        ▼
  Snowflake Stage  (DEV_RAW_FILES_STAGE / PROD_RAW_FILES_STAGE)
        │
        ▼
  Snowflake Notebook  (Bronze Ingestion — Snowpark Python)
        │
        ▼
┌───────────────────────────────────────────────────────┐
│                  dbt Core Pipeline                    │
│                                                       │
│   BRONZE  ──▶  SILVER  ──▶  GOLD                     │
│   (raw)       (clean)     (aggregated / business)     │
└───────────────────────────────────────────────────────┘
        │
        ▼
  Snowflake Task DAG  (scheduled daily automation)
        │
        ▼
  GitHub Actions CI/CD  (test on PR / deploy on merge)
```

| Layer      | Schema            | Purpose                                      | Tooling                   |
|------------|-------------------|----------------------------------------------|---------------------------|
| **Bronze** | `BRONZE`          | Raw ingestion, no transformations             | Snowflake Notebook + Snowpark |
| **Silver** | `SILVER`          | Cleaned, standardized, type-cast             | dbt models                |
| **Gold**   | `GOLD`            | Business aggregations, reporting-ready        | dbt models                |

---

## Project Structure

```
your_project_name/
│
├── models/
│   ├── bronze/               # Raw source models — load from stage tables
│   │   └── *.sql
│   ├── silver/               # Cleaning and standardization models
│   │   └── *.sql
│   └── gold/                 # Aggregation and business logic models
│       └── *.sql
│
├── macros/
│   └── generate_schema_name.sql   # Routes models to correct schema (required)
│
├── tests/                    # Custom dbt data tests
│   └── *.sql
│
├── seeds/                    # Static reference data (if any)
│   └── *.csv
│
├── scripts/
│   ├── deploy_dag.py         # Deploys Snowflake Task DAG for orchestration
│   └── upload_files.sh       # Uploads local Excel/CSV files to Snowflake stage
│
├── .github/
│   └── workflows/
│       └── dbt_pipeline.yml  # GitHub Actions CI/CD workflow
│
├── dbt_project.yml           # Project config: schema routing, tags, materializations
├── packages.yml              # dbt package dependencies
├── .gitignore                # Excludes credentials, target/, Excel/CSV files
└── README.md                 # This file
```

> ⚠️ `profiles.yml` lives at `~/.dbt/profiles.yml` on your local machine — **never inside this repo**.

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.9+ | Recommended: 3.11 |
| dbt-snowflake | latest | `pip install dbt-snowflake` |
| Snowflake CLI | latest | `pip install snowflake-cli-labs` |
| Git | 2.x+ | Required for version control |
| GitHub account | — | Required for CI/CD |
| Snowflake account | — | ACCOUNTADMIN needed for initial setup |

---

## Local Setup

### 1. Clone the repository

```bash
git clone https://github.com/<your_username>/<your_repo_name>.git
cd <your_repo_name>

# Switch to dev branch for local development
git checkout dev
```

### 2. Create a virtual environment (recommended)

```bash
python -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows

pip install dbt-snowflake snowflake-cli-labs snowflake-snowpark-python openpyxl pandas
```

### 3. Install dbt packages

```bash
dbt deps
```

### 4. Verify connection

```bash
dbt debug --target dev
```

A successful output ends with `All checks passed!`

---

## Environment Configuration

### profiles.yml

Create `~/.dbt/profiles.yml` on your local machine (this file is **not** in the repo):

```yaml
your_project_name:
  target: dev
  outputs:

    dev:
      type: snowflake
      account: "<orgname-accountname>"   # e.g. myorg-xy12345
      user: "<your_snowflake_username>"
      password: "<your_snowflake_password>"
      role: DBT_ROLE
      warehouse: DBT_WH
      database: DBT_DEV_DB
      schema: BRONZE
      threads: 4

    prod:
      type: snowflake
      account: "<orgname-accountname>"
      user: "<your_snowflake_username>"
      password: "{{ env_var('DBT_SNOWFLAKE_PASSWORD') }}"
      role: DBT_ROLE
      warehouse: DBT_WH
      database: DBT_PROD_DB
      schema: BRONZE
      threads: 4
```

> 💡 Find your account name in Snowsight: click your username (bottom-left) → Account details.  
> Format is `orgname-accountname`, not the full `.snowflakecomputing.com` URL.

### Snowflake Environments

| Environment | Database      | Schemas                          | Used When              |
|-------------|---------------|----------------------------------|------------------------|
| `dev`       | `DBT_DEV_DB`  | `BRONZE`, `SILVER`, `GOLD`       | Local development      |
| `prod`      | `DBT_PROD_DB` | `BRONZE`, `SILVER`, `GOLD`       | Deployed via CI/CD     |

---

## Running the Pipeline

### Run all models (recommended)

```bash
# Runs models + tests against dev
dbt build --target dev
```

### Run by layer

```bash
# Bronze only
dbt run --select tag:bronze --target dev

# Silver only
dbt run --select tag:silver --target dev

# Gold only
dbt run --select tag:gold --target dev
```

### Run a specific model and all downstream dependencies

```bash
dbt run --select my_model+ --target dev
```

### Run tests only

```bash
dbt test --target dev
```

### Generate and view documentation

```bash
dbt docs generate
dbt docs serve
# Opens at http://localhost:8080
```

---

## File Ingestion (Excel / CSV)

Source Excel and CSV files are ingested through a two-step process:

### Step 1 — Upload files to Snowflake stage

Use the provided script to push files from your local machine to the Snowflake internal stage:

```bash
# Make the script executable (first time only)
chmod +x scripts/upload_files.sh

# Upload a single file
./scripts/upload_files.sh path/to/your_file.xlsx DEV

# Upload an entire folder
./scripts/upload_files.sh path/to/csv_folder/ DEV
```

Or upload manually with the Snowflake CLI:

```bash
snow stage copy /path/to/your_file.xlsx \
  @DBT_DEV_DB.BRONZE.DEV_RAW_FILES_STAGE \
  --temporary-connection \
  --account <account> --user <user> --password <password> \
  --role DBT_ROLE --warehouse DBT_WH
```

### Step 2 — Run the ingestion Notebook

In Snowsight, open `DEV_BRONZE_INGEST` (Projects → Notebooks) and click **Run all**.  
The Notebook reads files from the stage, standardizes column names, adds metadata columns (`_LOADED_AT`, `_SOURCE_FILE`), and loads them into bronze tables.

The Task DAG (see [Orchestration](#orchestration--snowflake-task-dags)) runs this automatically on a schedule.

---

## CI/CD Pipeline

Every code change flows through a controlled pipeline:

```
Local dev branch
      │
      ▼
  git push origin dev
      │
      ▼
  Open Pull Request (dev → main)
      │
      ▼
  GitHub Actions: dbt build --target dev   ← must pass ✅
      │
      ▼
  Merge to main
      │
      ▼
  GitHub Actions: dbt build --target prod  ← auto-deploys ✅
      │
      ▼
  Snowflake Git repo refreshed (FETCH)
```

### GitHub Actions Secrets Required

Navigate to your repo → **Settings → Secrets and variables → Actions** and add:

| Secret Name | Value |
|-------------|-------|
| `SNOWFLAKE_ACCOUNT` | Your account identifier |
| `SNOWFLAKE_USER` | Your Snowflake username |
| `SNOWFLAKE_PASSWORD` | Your Snowflake password |
| `SNOWFLAKE_ROLE` | `DBT_ROLE` |
| `SNOWFLAKE_WAREHOUSE` | `DBT_WH` |
| `SNOWFLAKE_DEV_DB` | `DBT_DEV_DB` |
| `SNOWFLAKE_PROD_DB` | `DBT_PROD_DB` |

### Workflow file

See [`.github/workflows/dbt_pipeline.yml`](.github/workflows/dbt_pipeline.yml)

- **On Pull Request → main**: runs `dbt build --target dev`
- **On Push to main**: runs `dbt build --target prod` + refreshes Snowflake Git repo

---

## Orchestration — Snowflake Task DAGs

The pipeline runs on a daily schedule via a Snowflake Task DAG:

```
DEV_BRONZE_TO_GOLD_DAG  (runs daily at midnight UTC)
│
├── Task 1: INGEST_FILES
│     └── Executes: DEV_BRONZE_INGEST Notebook
│
├── Task 2: DBT_SILVER   (runs after INGEST_FILES)
│     └── Calls: RUN_DBT_LAYER('silver', 'dev')
│
└── Task 3: DBT_GOLD     (runs after DBT_SILVER)
      └── Calls: RUN_DBT_LAYER('gold', 'dev')
```

### Deploy the DAG

```bash
export SNOWFLAKE_ACCOUNT="<your_account>"
export SNOWFLAKE_USER="<your_username>"
export SNOWFLAKE_PASSWORD="<your_password>"

# Deploy dev DAG
python scripts/deploy_dag.py DEV

# Deploy prod DAG
python scripts/deploy_dag.py PROD
```

### Manually trigger the DAG

In Snowsight: **Catalog → Database Explorer → DBT_DEV_DB → INTEGRATIONS → Tasks**  
→ Select `DEV_BRONZE_TO_GOLD_DAG` → **Graph** tab → click **▶ Run Task Graph**

---

## Monitoring & Logging

### View pipeline logs

```sql
-- Recent log messages (last 24 hours)
SELECT
    TIMESTAMP,
    RECORD:severity_text::STRING  AS SEVERITY,
    VALUE::STRING                 AS MESSAGE
FROM DBT_DEV_DB.INTEGRATIONS.PIPELINE_EVENTS
WHERE TIMESTAMP >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
ORDER BY TIMESTAMP DESC
LIMIT 100;
```

### Check Task run history

```sql
-- Task execution history (last 7 days)
SELECT
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    TIMESTAMPDIFF(SECOND, SCHEDULED_TIME, COMPLETED_TIME) AS DURATION_SEC,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('DAY', -7, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100
))
ORDER BY SCHEDULED_TIME DESC;
```

### Failed runs only

```sql
SELECT NAME, STATE, ERROR_MESSAGE, SCHEDULED_TIME
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE STATE = 'FAILED'
ORDER BY SCHEDULED_TIME DESC;
```

---

## Contributing

### Branching strategy

```
main       ← production, protected — no direct pushes
dev        ← integration branch — open PRs from feature branches
feature/*  ← your individual work
```

### Development workflow

```bash
# 1. Pull latest from dev
git checkout dev
git pull origin dev

# 2. Create a feature branch
git checkout -b feature/your-feature-name

# 3. Make changes, test locally
dbt build --target dev

# 4. Commit and push
git add .
git commit -m "feat: describe your change clearly"
git push origin feature/your-feature-name

# 5. Open a Pull Request into dev (not main)
```

### Commit message conventions

| Prefix | Use for |
|--------|---------|
| `feat:` | New models, new pipeline steps |
| `fix:` | Bug fixes in existing models |
| `refactor:` | Restructuring without behavior change |
| `test:` | Adding or updating dbt tests |
| `docs:` | README, comments, documentation only |
| `chore:` | Dependency updates, config changes |

### Adding a new model

1. Create your `.sql` file in the correct layer folder (`models/bronze/`, `models/silver/`, or `models/gold/`)
2. Add a `config()` block at the top with the correct `schema` and `tags`
3. Add tests in `schema.yml` inside the same folder
4. Run locally: `dbt build --select your_model_name --target dev`
5. Commit and open a PR

**Model config template:**

```sql
{{
    config(
        materialized = 'table',
        schema       = 'SILVER',
        tags         = ['silver']
    )
}}

SELECT ...
FROM {{ ref('your_upstream_model') }}
```

---

## Useful Commands

```bash
# ── dbt ─────────────────────────────────────────────────────────────────────
dbt debug                                       # Test Snowflake connection
dbt deps                                        # Install packages
dbt build --target dev                          # Run + test all models (dev)
dbt build --target prod                         # Run + test all models (prod)
dbt run --select tag:silver --target dev        # Run silver layer only
dbt run --select +my_model --target dev         # Run model + all its ancestors
dbt run --select my_model+ --target dev         # Run model + all its descendants
dbt test --target dev                           # Run all tests
dbt docs generate && dbt docs serve             # Build and view lineage docs

# ── Snowflake CLI ────────────────────────────────────────────────────────────
snow --version                                  # Verify CLI installation
snow sql -q "SELECT CURRENT_VERSION()"          # Quick connection test
snow stage copy file.xlsx @STAGE --temporary-connection ...  # Upload file

# ── Git ──────────────────────────────────────────────────────────────────────
git checkout dev && git pull origin dev         # Sync dev branch
git checkout -b feature/new-model               # Start a new feature
git push origin feature/new-model               # Push and open a PR
```

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `dbt debug` fails: account not found | Use `orgname-accountname` format — run `SELECT CURRENT_ACCOUNT()` in Snowflake to find it |
| Tables land in wrong schema | Confirm `macros/generate_schema_name.sql` exists in the `macros/` folder |
| GitHub Actions fails on `dbt build` | Add `dbt debug` before `dbt build` in the workflow to see the exact error |
| Stage upload: permission denied | Run `GRANT WRITE ON STAGE stage_name TO ROLE DBT_ROLE` in Snowflake |
| PAT rejected on Notebook commit | Token expired — regenerate at GitHub → Settings → Developer Settings → PAT |
| Task DAG not running on schedule | Run `ALTER TASK DEV_BRONZE_TO_GOLD_DAG RESUME` in a Snowflake worksheet |

---

*Built with [dbt Core](https://docs.getdbt.com) and [Snowflake](https://www.snowflake.com).*