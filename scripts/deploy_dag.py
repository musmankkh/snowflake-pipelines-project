import os
import sys
from snowflake.snowpark import Session
from snowflake.core import Root
from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation


# ─────────────────────────────────────────────
# Create Snowflake Session
# ─────────────────────────────────────────────
def get_session(env: str) -> Session:
    return Session.builder.configs({
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "role":      os.environ.get("SNOWFLAKE_ROLE", "DBT_ROLE"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "DBT_WH"),
        "database":  f"DBT_{env}_DB",
        "schema":    "INTEGRATIONS",
    }).create()


# ─────────────────────────────────────────────
# Deploy DAG
# ─────────────────────────────────────────────
def deploy(env: str = "DEV"):

    env = env.upper()
    db = f"DBT_{env}_DB"
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "DBT_WH")
    dag_name = f"{env}_DAILY_DBT_PIPELINE_DAG"

    session = get_session(env)

    print(f"🚀 Deploying DAG for {env} environment...")

    with DAG(
        name=dag_name,
        schedule="USING CRON 0 2 * * * UTC",   # Daily at 02:00 UTC
        warehouse=warehouse,
    ) as dag:

        # Task 1 — Bronze Notebook
        ingest_task = DAGTask(
            name="INGEST_FILES",
            definition=f'''
                EXECUTE NOTEBOOK "{db}"."BRONZELAYER"."bronze_ingest"()
            ''',
            warehouse=warehouse,
        )

        # Task 2 — dbt Silver
        silver_task = DAGTask(
            name="DBT_SILVER",
            definition=f'''
                CALL {db}.INTEGRATIONS.RUN_DBT_LAYER('silver', '{env.lower()}')
            ''',
            warehouse=warehouse,
        )

        # Task 3 — dbt Gold
        gold_task = DAGTask(
            name="DBT_GOLD",
            definition=f'''
                CALL {db}.INTEGRATIONS.RUN_DBT_LAYER('gold', '{env.lower()}')
            ''',
            warehouse=warehouse,
        )

        ingest_task >> silver_task >> gold_task

    # ✅ Deploy via DAGOperation (snowflake-core 1.x+)
    root = Root(session)
    task_collection = root.databases[db].schemas["INTEGRATIONS"].tasks
    dag_op = DAGOperation(task_collection)
    dag_op.deploy(dag, mode="orReplace")

    print(f"✅ DAG '{dag_name}' deployed successfully for {env}.")


# ─────────────────────────────────────────────
if __name__ == "__main__":
    environment = sys.argv[1] if len(sys.argv) > 1 else "DEV"
    deploy(environment)