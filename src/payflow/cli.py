"""Single entry point used by the Airflow DAG and for local runs.

    python -m payflow.cli <stage> --date YYYY-MM-DD
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import date

from payflow import config as C

STAGES = ["generate", "bronze", "silver", "dq_silver", "gold", "dq_gold", "load", "evaluate"]


def run(stage: str, run_date: str, start: str, payments: int, eval_dates: list[str] | None = None) -> dict:
    t0 = time.perf_counter()
    if stage == "generate":
        from payflow.generate import generate_day
        d = date.fromisoformat(run_date)
        out = generate_day(d, (d - date.fromisoformat(start)).days, payments)
    else:
        from payflow.spark import get_spark
        spark = get_spark(f"payflow-{stage}")
        if stage == "bronze":
            from payflow.bronze import ingest
            out = ingest(spark, run_date)
        elif stage == "silver":
            from payflow.silver import build
            out = build(spark, run_date)
        elif stage == "dq_silver":
            from payflow.dq import check_silver
            out = {c.name: (c.observed, c.passed) for c in check_silver(spark, run_date)}
        elif stage == "gold":
            from payflow.gold import build
            out = build(spark, run_date)
        elif stage == "dq_gold":
            from payflow.dq import check_gold
            out = {c.name: (c.observed, c.passed) for c in check_gold(spark, run_date)}
        elif stage == "load":
            from payflow.load import load
            out = load(spark, run_date)
        elif stage == "evaluate":
            from payflow.evaluate import evaluate
            out = evaluate(spark, eval_dates or [run_date])
        else:
            raise SystemExit(f"unknown stage {stage}")
    out = {"stage": stage, "date": run_date, "seconds": round(time.perf_counter() - t0, 1), **out}
    log = C.DATA / "run_log.jsonl"
    log.parent.mkdir(parents=True, exist_ok=True)
    with open(log, "a") as f:
        f.write(json.dumps(out, default=str) + "\n")
    print(json.dumps(out, default=str))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=STAGES)
    ap.add_argument("--date", required=True)
    ap.add_argument("--start", default="2026-10-01")
    ap.add_argument("--payments", type=int, default=60_000)
    ap.add_argument("--eval-dates", nargs="*")
    a = ap.parse_args()
    run(a.stage, a.date, a.start, a.payments, a.eval_dates)


if __name__ == "__main__":
    main()
