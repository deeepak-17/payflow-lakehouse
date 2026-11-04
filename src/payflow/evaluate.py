"""Score the pipeline's data-quality handling against the generator's fault labels.

For each injected fault type we know what the correct outcome is (quarantined
with a specific reason, recovered into silver, routed to the right date,
flagged by reconciliation). Recall = labelled faults handled correctly;
precision = flags raised that correspond to a real labelled fault.
"""
from __future__ import annotations

import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from payflow import config as C

QUARANTINE_EXPECT = {
    "missing_amount": "MISSING_AMOUNT",
    "invalid_currency": "INVALID_CURRENCY",
    "nonpositive_amount": "NONPOSITIVE_AMOUNT",
    "orphan_status": "ORPHAN_STATUS",
}
RECON_EXPECT = {
    "missing_in_settlement": "MISSING_IN_SETTLEMENT",
    "amount_mismatch": "AMOUNT_MISMATCH",
    "missing_in_ledger": "MISSING_IN_LEDGER",
}


def evaluate(spark: SparkSession, dates: list[str]) -> dict:
    gt_all = (spark.read.option("header", True).csv(str(C.GROUND_TRUTH / "*" / "faults.csv"))
              .withColumn("gt_date", F.regexp_extract(F.col("_metadata.file_path"), r"date=([0-9-]+)", 1)))
    # event faults are scored only for dates whose late/orphan grace window has fully closed
    gt = gt_all.filter(F.col("gt_date").isin(dates))
    q = spark.read.parquet(str(C.QUARANTINE / "events"))
    silver = spark.read.parquet(str(C.SILVER / "payments"))
    bronze = spark.read.parquet(str(C.BRONZE / "events"))
    recon = spark.read.parquet(str(C.GOLD / "recon"))
    res: dict = {}

    # quarantine rules
    q_x = q.select("event_id", F.explode("dq_reasons").alias("reason"))
    for fault, reason in QUARANTINE_EXPECT.items():
        labelled = gt.filter(F.col("fault_type") == fault).select("event_id").distinct()
        hit = labelled.join(q_x.filter(F.col("reason") == reason), "event_id", "left_semi").count()
        res[fault] = {"injected": labelled.count(), "caught": hit}
    # precision: every quarantined event (any date) must match some injected label (any date)
    flagged = q.filter(~F.array_contains("dq_reasons", "PARENT_QUARANTINED")).select("event_id").distinct()
    all_q_labels = gt_all.filter(F.col("fault_type").isin(*QUARANTINE_EXPECT)).select("event_id")
    tp = flagged.join(all_q_labels, "event_id", "left_semi").count()
    res["quarantine_precision"] = {"flagged": flagged.count(), "true_faults": tp}

    # recovered drift: payment must be in silver with a valid currency and positive amount
    for fault in ("amount_as_string", "currency_case"):
        labelled = gt.filter(F.col("fault_type") == fault).select("payment_id").distinct()
        ok = labelled.join(silver.filter((F.col("amount") > 0) & F.col("currency").isin(C.CURRENCIES)),
                           "payment_id", "left_semi").count()
        res[fault] = {"injected": labelled.count(), "recovered": ok}

    # late events: payment lands under its original business date, not the arrival date
    late = gt.filter(F.col("fault_type") == "late_event").select("payment_id", "gt_date")
    ok = (late.join(silver, "payment_id")
          .filter(F.col("created_date") == F.col("gt_date").cast("date")).count())
    res["late_event"] = {"injected": late.count(), "routed_to_correct_date": ok}

    # duplicates: each replayed event_id collapses to one
    dup = gt.filter(F.col("fault_type") == "duplicate_event").select("event_id").distinct()
    bronze_dup = (bronze.groupBy("event_id").count().filter("count > 1").select("event_id"))
    res["duplicate_event"] = {"injected": dup.count(),
                              "removed": dup.join(bronze_dup, "event_id", "left_semi").count(),
                              "payment_id_unique": silver.count() == silver.select("payment_id").distinct().count()}

    # reconciliation (only dates that were reconciled)
    # settlement faults: gt_date is the settlement date; score only dates that were reconciled (T+1).
    # Payments whose own CREATED event was quarantined cannot be reconciled and are reported apart.
    recon_dates = [r[0].isoformat() for r in recon.select("recon_date").distinct().collect()]
    quarantined_pids = q.select("payment_id").distinct()
    gt_r_all = gt_all.filter(F.col("gt_date").isin(recon_dates) & F.col("fault_type").isin(*RECON_EXPECT))
    gt_r = gt_r_all.join(quarantined_pids, "payment_id", "left_anti")
    res["recon_excluded_quarantined_payment"] = gt_r_all.count() - gt_r.count()
    for fault, status in RECON_EXPECT.items():
        labelled = gt_r.filter(F.col("fault_type") == fault).select("payment_id").distinct()
        hit = labelled.join(recon.filter(F.col("recon_status") == status), "payment_id", "left_semi").count()
        res[fault] = {"injected": labelled.count(), "caught": hit}
    breaks = recon.filter(~F.col("recon_status").isin("MATCHED", "LEDGER_QUARANTINED")).select("payment_id")
    tp = breaks.join(gt_r_all.select("payment_id"), "payment_id", "left_semi").count()
    res["recon_precision"] = {"breaks_flagged": breaks.count(), "true_breaks": tp,
                              "recon_dates": sorted(recon_dates)}

    # FX fallback
    fb = silver.filter("fx_fallback").groupBy("currency", "created_date").count().collect()
    res["fx_fallback"] = [{"currency": r["currency"], "date": str(r["created_date"]), "rows": r["count"]} for r in fb]

    out = C.DATA / "evaluation.json"
    out.write_text(json.dumps(res, indent=1, default=str))
    return res
