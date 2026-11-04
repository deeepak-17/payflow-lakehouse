"""Data-quality gates. A failed `critical` check raises, which fails the Airflow task
and stops bad data from reaching the warehouse; `warn` checks are recorded only."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from payflow import config as C


class DQFailure(RuntimeError):
    pass


@dataclass
class Check:
    layer: str
    name: str
    severity: str  # critical | warn
    observed: float
    threshold: str
    passed: bool


def _finish(checks: list[Check], layer: str, run_date: str) -> list[Check]:
    out = C.DATA / "dq" / f"layer={layer}"
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{run_date}.json").write_text(json.dumps([asdict(c) for c in checks], indent=1))
    failed = [c for c in checks if not c.passed and c.severity == "critical"]
    if failed:
        raise DQFailure("; ".join(f"{c.name} observed={c.observed} expected {c.threshold}" for c in failed))
    return checks


def check_silver(spark: SparkSession, run_date: str) -> list[Check]:
    p = spark.read.parquet(str(C.SILVER / "payments")).cache()
    q = spark.read.parquet(str(C.QUARANTINE / "events"))
    b = spark.read.parquet(str(C.BRONZE / "events")).filter(F.col("ingest_date") <= F.lit(run_date))
    n, nq, nb = p.count(), q.count(), b.count()
    dup_ids = n - p.select("payment_id").distinct().count()
    nulls = p.filter(F.col("amount").isNull() | F.col("currency").isNull() | F.col("created_ts").isNull()).count()
    bad_ccy = p.filter(~F.col("currency").isin(C.CURRENCIES)).count()
    no_fx = p.filter(F.col("amount_usd").isNull()).count()
    q_rate = round(nq / nb, 5) if nb else 0.0
    checks = [
        Check("silver", "payments_not_empty", "critical", n, "> 0", n > 0),
        Check("silver", "payment_id_unique", "critical", dup_ids, "== 0", dup_ids == 0),
        Check("silver", "required_fields_not_null", "critical", nulls, "== 0", nulls == 0),
        Check("silver", "currency_in_reference_set", "critical", bad_ccy, "== 0", bad_ccy == 0),
        Check("silver", "fx_coverage", "critical", no_fx, "== 0", no_fx == 0),
        Check("silver", "quarantine_rate", "warn", q_rate, "<= 0.02", q_rate <= 0.02),
        Check("silver", "fx_fallback_rows", "warn", p.filter("fx_fallback").count(), "informational", True),
    ]
    p.unpersist()
    return _finish(checks, "silver", run_date)


def check_gold(spark: SparkSession, run_date: str) -> list[Check]:
    p = spark.read.parquet(str(C.SILVER / "payments"))
    f = spark.read.parquet(str(C.GOLD / "fact_payments"))
    m = spark.read.parquet(str(C.GOLD / "mart_corridor_daily"))
    n_p, n_f = p.count(), f.count()
    vol_f = f.agg(F.sum("amount_usd")).first()[0] or 0
    vol_m = m.agg(F.sum("volume_usd")).first()[0] or 0
    cnt_m = m.agg(F.sum("payments")).first()[0] or 0
    checks = [
        Check("gold", "fact_rowcount_matches_silver", "critical", n_f - n_p, "== 0", n_f == n_p),
        Check("gold", "mart_count_matches_fact", "critical", cnt_m - n_f, "== 0", cnt_m == n_f),
        Check("gold", "mart_volume_matches_fact", "critical", float(abs(vol_f - vol_m)), "<= 0.01",
              abs(vol_f - vol_m) <= 0.01),
    ]
    recon_dir = C.GOLD / "recon"
    if recon_dir.exists():
        r = spark.read.parquet(str(recon_dir)).filter(F.col("recon_date") == F.date_sub(F.lit(run_date), 1))
        total = r.count()
        breaks = r.filter(~F.col("recon_status").isin("MATCHED", "LEDGER_QUARANTINED")).count()
        rate = round(breaks / total, 5) if total else 0.0
        checks.append(Check("gold", "recon_break_rate", "warn", rate, "<= 0.01", rate <= 0.01))
    return _finish(checks, "gold", run_date)
