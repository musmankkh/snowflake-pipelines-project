import os
import sys
from snowflake.snowpark import Session
from snowflake.core import Root
from snowflake.core.task.dagv1 import DAG, DAGTask


# ─────────────────────────────────────────────
# Create Snowflake session
# ─────────────────────────────────────────────
def get_session(env: str) -> Session:
    return Session.builder.configs({
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "role":      "DBT_ROLE",
        "warehouse": "DBT_WH",
        "database":  f"DBT_{env}_DB",
        "schema":    "INTEGRATIONS",
    }).create()


# ─────────────────────────────────────────────
# Deploy DAG
# ─────────────────────────────────────────────
def deploy(env: str = "DEV"):

    env = env.upper()
    db = f"DBT_{env}_DB"
    wh = "DBT_WH"
    dag_name = f"{env}_DAILY_DBT_PIPELINE_DAG"

    session = get_session(env)
    root = Root(session)

    task_collection = root.databases[db].schemas["INTEGRATIONS"].tasks

    with DAG(
        name=dag_name,
        schedule="USING CRON 0 2 * * * UTC",   # Daily at 02:00 UTC
        warehouse=wh,
    ) as dag:

        # ── Task 1: Run Bronze Ingestion Notebook ───────────
        ingest = DAGTask(
            name="INGEST_FILES",
            definition=f'''
                EXECUTE NOTEBOOK "{db}"."BRONZELAYER"."bronze_ingest"()
            ''',
            warehouse=wh,
        )

        # ── Task 2: Run Silver Layer ─────────────────────────
        silver = DAGTask(
            name="DBT_SILVER",
            definition=f'''
                CALL {db}.INTEGRATIONS.RUN_DBT_LAYER('silver', '{env.lower()}')
            ''',
            warehouse=wh,
        )

        # ── Task 3: Run Gold Layer ───────────────────────────
        gold = DAGTask(
            name="DBT_GOLD",
            definition=f'''
                CALL {db}.INTEGRATIONS.RUN_DBT_LAYER('gold', '{env.lower()}')
            ''',
            warehouse=wh,
        )

        # Execution order
        ingest >> silver >> gold

    # Deploy or replace existing DAG
    task_collection.deploy(dag, mode="orreplace")

    print(f"✅ DAG '{dag_name}' deployed successfully for {env}")


# ─────────────────────────────────────────────
if __name__ == "__main__":
    environment = sys.argv[1] if len(sys.argv) > 1 else "DEV"
    deploy(environment)
