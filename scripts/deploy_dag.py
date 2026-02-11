import os
import sys
from snowflake.snowpark import Session
from snowflake.core import Root
from snowflake.core.task.dagv1 import DAG, DAGTask


# ─────────────────────────────────────────────
# Create Snowflake Session
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
    warehouse = "DBT_WH"
    dag_name = f"{env}_DAILY_DBT_PIPELINE_DAG"

    session = get_session(env)
    root = Root(session)

    print(f"Deploying DAG for {env} environment...")

    with DAG(
        name=dag_name,
        schedule="USING CRON 0 2 * * * UTC",  # Runs daily at 02:00 UTC
        warehouse=warehouse,
    ) as dag:

        # ── Task 1: Bronze Ingestion Notebook ───────────────
        ingest_task = DAGTask(
            name="INGEST_FILES",
            definition=f"""
                EXECUTE NOTEBOOK "{db}"."BRONZELAYER"."bronze_ingest"()
            """,
            warehouse=warehouse,
        )

        # ── Task 2: Run dbt Silver Models ───────────────────
        silver_task = DAGTask(
            name="DBT_SILVER",
            definition=f"""
                CALL {db}.INTEGRATIONS.RUN_DBT_LAYER('silver', '{env.lower()}')
            """,
            warehouse=warehouse,
        )

        # ── Task 3: Run dbt Gold Models ─────────────────────
        gold_task = DAGTask(
            name="DBT_GOLD",
            definition=f"""
                CALL {db}.INTEGRATIONS.RUN_DBT_LAYER('gold', '{env.lower()}')
            """,
            warehouse=warehouse,
        )

        # Define dependency order
        ingest_task >> silver_task >> gold_task

    # ✅ Correct deployment method (new SDK)
    root.databases[db].schemas["INTEGRATIONS"].tasks.deploy(dag)

    print(f"✅ DAG '{dag_name}' deployed successfully for {env}.")


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────
if __name__ == "__main__":
    environment = sys.argv[1] if len(sys.argv) > 1 else "DEV"
    deploy(environment)
