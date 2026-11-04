"""Gold: star-schema tables, a corridor daily mart, and T+1 settlement reconciliation."""
from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from payflow import config as C
from payflow.spark import SETTLEMENT_SCHEMA


def corridor_daily(payments: DataFrame) -> DataFrame:
    return (
        payments.groupBy("created_date", "src_country", "dst_country")
        .agg(
            F.count("*").alias("payments"),
            F.sum(F.when(F.col("status") == "SETTLED", 1).otherwise(0)).alias("settled"),
            F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed"),
            F.sum("amount_usd").cast("decimal(20,2)").alias("volume_usd"),
            F.percentile_approx(F.when(F.col("status") == "SETTLED", F.col("settlement_seconds")), 0.5)
             .alias("p50_settlement_s"),
            F.percentile_approx(F.when(F.col("status") == "SETTLED", F.col("settlement_seconds")), 0.95)
             .alias("p95_settlement_s"),
        )
    )


def reconcile(ledger: DataFrame, settlement: DataFrame, quarantined_ids: DataFrame) -> DataFrame:
    """Full outer join of our SETTLED ledger vs the partner settlement report for one date."""
    l = ledger.select("payment_id", F.col("amount").alias("ledger_amount"), F.col("currency").alias("ledger_ccy"))
    s = settlement.select("payment_id", "settled_amount", F.col("currency").alias("settle_ccy"))
    q = quarantined_ids.select("payment_id").distinct().withColumn("_quarantined", F.lit(True))
    j = l.join(s, "payment_id", "full_outer").join(q, "payment_id", "left")
    diff = F.abs(F.col("ledger_amount") - F.col("settled_amount"))
    return j.withColumn(
        "recon_status",
        F.when(F.col("ledger_amount").isNull() & F.col("_quarantined").isNotNull(), "LEDGER_QUARANTINED")
         .when(F.col("ledger_amount").isNull(), "MISSING_IN_LEDGER")
         .when(F.col("settled_amount").isNull(), "MISSING_IN_SETTLEMENT")
         .when(F.col("ledger_ccy") != F.col("settle_ccy"), "CURRENCY_MISMATCH")
         .when(diff > C.RECON_TOLERANCE, "AMOUNT_MISMATCH")
         .otherwise("MATCHED"),
    ).withColumn("amount_diff", diff).drop("_quarantined")


def build(spark: SparkSession, run_date: str) -> dict:
    payments = spark.read.parquet(str(C.SILVER / "payments")).cache()

    # dimensions + fact
    dim_currency = payments.select("currency").union(payments.select("target_currency")).distinct()
    dim_corridor = (payments.select("src_country", "dst_country").distinct()
                    .withColumn("corridor_key", F.concat_ws("-", "src_country", "dst_country")))
    fact = payments.select(
        "payment_id", "created_date", F.concat_ws("-", "src_country", "dst_country").alias("corridor_key"),
        "rail", "currency", "target_currency", "payer_fi", "payee_fi", "status",
        "amount", "amount_usd", "fx_rate_date", "fx_fallback", "created_ts", "outcome_ts", "settlement_seconds",
    )
    dim_currency.write.mode("overwrite").parquet(str(C.GOLD / "dim_currency"))
    dim_corridor.write.mode("overwrite").parquet(str(C.GOLD / "dim_corridor"))
    fact.write.mode("overwrite").partitionBy("created_date").parquet(str(C.GOLD / "fact_payments"))
    mart = corridor_daily(payments)
    mart.write.mode("overwrite").partitionBy("created_date").parquet(str(C.GOLD / "mart_corridor_daily"))

    # T+1 reconciliation: on run date D we reconcile settlements dated D-1, after D's
    # late-arriving events have landed.
    recon_date = (date.fromisoformat(run_date) - timedelta(days=1)).isoformat()
    sfile = C.SETTLEMENT / f"date={recon_date}" / "settlement.csv"
    recon_stats = {}
    if sfile.exists():
        settlement = spark.read.schema(SETTLEMENT_SCHEMA).option("header", True).csv(str(sfile))
        ledger = payments.filter((F.col("status") == "SETTLED") & (F.col("outcome_date") == F.lit(recon_date)))
        quarantined = spark.read.parquet(str(C.QUARANTINE / "events"))
        recon = reconcile(ledger, settlement, quarantined).withColumn("recon_date", F.lit(recon_date).cast("date"))
        recon.write.mode("overwrite").partitionBy("recon_date").parquet(str(C.GOLD / "recon"))
        recon_stats = {r["recon_status"]: r["count"] for r in recon.groupBy("recon_status").count().collect()}

    stats = {"fact_rows": fact.count(), "corridors": dim_corridor.count(), "recon_date": recon_date,
             "recon": recon_stats}
    payments.unpersist()
    return stats
