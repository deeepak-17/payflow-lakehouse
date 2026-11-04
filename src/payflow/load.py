"""Load gold tables into PostgreSQL in one transaction.

Fact rows are bulk-copied into a temp staging table and merged with
INSERT ... ON CONFLICT DO UPDATE, so a rerun of the same date is idempotent
and a PENDING payment upgrades to SETTLED/FAILED when its outcome arrives.
"""
from __future__ import annotations

import io
import json
from pathlib import Path

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from payflow import config as C

SCHEMA_SQL = Path(__file__).resolve().parents[2] / "sql" / "schema.sql"

FACT_COLS = ["payment_id", "created_date", "corridor_key", "currency", "target_currency", "rail", "payer_fi",
             "payee_fi", "status", "amount", "amount_usd", "fx_rate_date", "fx_fallback", "created_ts",
             "outcome_ts", "settlement_seconds"]


def _copy(cur, table: str, cols: list[str], pdf) -> None:
    buf = io.StringIO()
    pdf[cols].to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    cur.copy_expert(f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)


def load(spark: SparkSession, run_date: str) -> dict:
    fact = spark.read.parquet(str(C.GOLD / "fact_payments")).select(*FACT_COLS).toPandas()
    fact["settlement_seconds"] = fact["settlement_seconds"].astype("Int64")
    corridors = spark.read.parquet(str(C.GOLD / "dim_corridor")).toPandas()
    currencies = spark.read.parquet(str(C.GOLD / "dim_currency")).toPandas()
    dates = (spark.read.parquet(str(C.GOLD / "fact_payments")).select(F.col("created_date").alias("date_key"))
             .distinct().withColumn("day_of_week", F.dayofweek("date_key"))
             .withColumn("is_weekend", F.dayofweek("date_key").isin(1, 7)).toPandas())
    recon_date = None
    recon = None
    if (C.GOLD / "recon").exists():
        r = spark.read.parquet(str(C.GOLD / "recon")).filter(F.col("recon_date") == F.date_sub(F.lit(run_date), 1))
        recon = r.select("recon_date", "payment_id", "recon_status", "ledger_amount", "settled_amount",
                         "amount_diff").toPandas()
        recon_date = str(recon["recon_date"].iloc[0]) if len(recon) else None

    with psycopg2.connect(C.PG_DSN) as conn, conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute(SCHEMA_SQL.read_text())
        cur.execute("SET search_path TO payflow")
        cur.execute("CREATE TEMP TABLE s_dim (LIKE dim_corridor) ON COMMIT DROP")
        _copy(cur, "s_dim", ["corridor_key", "src_country", "dst_country"], corridors)
        cur.execute("INSERT INTO dim_corridor SELECT * FROM s_dim ON CONFLICT DO NOTHING")
        cur.execute("CREATE TEMP TABLE s_ccy (LIKE dim_currency) ON COMMIT DROP")
        _copy(cur, "s_ccy", ["currency"], currencies)
        cur.execute("INSERT INTO dim_currency SELECT * FROM s_ccy ON CONFLICT DO NOTHING")
        cur.execute("CREATE TEMP TABLE s_date (LIKE dim_date) ON COMMIT DROP")
        _copy(cur, "s_date", ["date_key", "day_of_week", "is_weekend"], dates)
        cur.execute("INSERT INTO dim_date SELECT * FROM s_date ON CONFLICT DO NOTHING")

        cur.execute("CREATE TEMP TABLE s_fact (LIKE fact_payments) ON COMMIT DROP")
        _copy(cur, "s_fact", FACT_COLS, fact)
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in FACT_COLS if c != "payment_id")
        cur.execute(f"INSERT INTO fact_payments SELECT * FROM s_fact "
                    f"ON CONFLICT (payment_id) DO UPDATE SET {updates}")
        upserted = cur.rowcount

        if recon is not None and recon_date:
            cur.execute("DELETE FROM recon_breaks WHERE recon_date = %s", (recon_date,))
            breaks = recon[recon["recon_status"] != "MATCHED"]
            _copy(cur, "recon_breaks", list(breaks.columns), breaks)

        for layer in ("silver", "gold"):
            f = C.DATA / "dq" / f"layer={layer}" / f"{run_date}.json"
            if f.exists():
                for c in json.loads(f.read_text()):
                    cur.execute(
                        "INSERT INTO dq_results VALUES (%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT (run_date, layer, check_name) DO UPDATE SET observed = EXCLUDED.observed, "
                        "passed = EXCLUDED.passed",
                        (run_date, c["layer"], c["name"], c["severity"], c["observed"], c["threshold"], c["passed"]))
        cur.execute("SELECT COUNT(*) FROM fact_payments")
        total = cur.fetchone()[0]
    return {"fact_upserted": upserted, "fact_total": total, "recon_date": recon_date}
