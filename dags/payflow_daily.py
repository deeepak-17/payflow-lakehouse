"""Daily PayFlow batch: land -> bronze -> silver -> DQ gate -> gold -> DQ gate -> Postgres.

Each task shells out to `python -m payflow.cli <stage> --date {{ ds }}`, so every
stage is idempotent for its logical date and can be rerun or backfilled alone.
A failed critical DQ check raises, fails the task, and blocks the load.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PROJECT = os.environ.get("PAYFLOW_REPO", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PY = os.environ.get("PAYFLOW_PYTHON", "python3")
ENV = {
    "PYTHONPATH": f"{PROJECT}/src",
    "PAYFLOW_HOME": os.environ.get("PAYFLOW_HOME", PROJECT),
    "PAYFLOW_PG_DSN": os.environ.get("PAYFLOW_PG_DSN", "host=/tmp dbname=payflow user=postgres"),
    "PATH": os.environ.get("PATH", ""),
    "JAVA_HOME": os.environ.get("JAVA_HOME", ""),
}

with DAG(
    dag_id="payflow_daily",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,                      # silver rebuilds state from bronze history: run dates in order
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["payments", "lakehouse"],
) as dag:

    def stage(name: str, extra: str = "") -> BashOperator:
        return BashOperator(
            task_id=name,
            bash_command=f"{PY} -m payflow.cli {name} --date {{{{ ds }}}} {extra}",
            env=ENV,
            append_env=True,
        )

    # In production this is a sensor on the partner drop bucket; here the generator plays upstream.
    generate = stage("generate", "--payments ${PAYFLOW_PAYMENTS:-60000}")
    bronze = stage("bronze")
    silver = stage("silver")
    dq_silver = stage("dq_silver")
    gold = stage("gold")
    dq_gold = stage("dq_gold")
    load = stage("load")

    generate >> bronze >> silver >> dq_silver >> gold >> dq_gold >> load
