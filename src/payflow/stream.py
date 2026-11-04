"""Near-real-time path: Spark Structured Streaming over an event drop folder.

A producer thread writes a JSON file of payment events every second (with
replays and bad records mixed in). The stream applies the same normalise /
validate rules as the batch silver layer, de-duplicates replays with a
watermark (bounded state), and writes valid and quarantined events as Parquet
per micro-batch. Per-batch stats record end-to-end latency from the moment an
event was produced to the moment its micro-batch was committed.
"""
from __future__ import annotations

import argparse
import json
import random
import shutil
import threading
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from payflow import config as C
from payflow.silver import normalise, validate
from payflow.spark import EVENT_SCHEMA, get_spark


def producer(n_files: int, per_file: int, interval: float, seed: int = 11) -> None:
    rng = random.Random(seed)
    C.STREAM_IN.mkdir(parents=True, exist_ok=True)
    prev: list[dict] = []
    for k in range(n_files):
        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for i in range(per_file):
            ccy = rng.choice(C.CURRENCIES)
            pid = f"s{k:04d}_{i:05d}"
            e = {"event_id": f"evt_{pid}", "payment_id": pid, "event_type": "PAYMENT_CREATED",
                 "event_ts": now, "produced_at": now, "rail": rng.choice(C.RAILS),
                 "payer": {"institution_id": "FI101", "country": C.CURRENCY_COUNTRY[ccy]},
                 "payee": {"institution_id": "FI202", "country": "IN"},
                 "amount": {"value": round(rng.uniform(10, 50_000), 2), "currency": ccy},
                 "target_currency": "INR"}
            if rng.random() < 0.01:
                e["amount"]["currency"] = "XXX"            # invalid record
            rows.append(e)
        replays = rng.sample(prev, min(len(prev), per_file // 100))  # ~1% replays of the previous drop
        tmp = C.STREAM_IN / f".part{k:04d}.tmp"
        with open(tmp, "w") as f:
            for e in rows + replays:
                f.write(json.dumps(e) + "\n")
        tmp.rename(C.STREAM_IN / f"part{k:04d}.json")          # atomic publish so Spark never reads half a file
        prev = rows
        time.sleep(interval)


def run(n_files: int = 40, per_file: int = 2_000, interval: float = 1.0, trigger: str = "2 seconds") -> dict:
    for p in (C.STREAM_IN, C.STREAM_OUT, C.CHECKPOINTS / "stream"):
        shutil.rmtree(p, ignore_errors=True)
    C.STREAM_IN.mkdir(parents=True, exist_ok=True)
    spark = get_spark("payflow-stream")

    src = spark.readStream.schema(EVENT_SCHEMA).option("maxFilesPerTrigger", 10).json(str(C.STREAM_IN))
    events = (validate(normalise(src))
              .withColumn("produced_at", F.to_timestamp("produced_at"))
              .withWatermark("event_ts", "10 minutes")
              .dropDuplicatesWithinWatermark(["event_id"]))

    batches: list[dict] = []

    def sink(df: DataFrame, batch_id: int) -> None:
        df = df.cache()
        n = df.count()
        if n == 0:
            df.unpersist()
            return
        valid = df.filter(F.size("dq_reasons") == 0).drop("amount_recovered", "currency_recovered")
        valid.write.mode("append").parquet(str(C.STREAM_OUT / "valid"))
        df.filter(F.size("dq_reasons") > 0).write.mode("append").parquet(str(C.STREAM_OUT / "quarantine"))
        committed = time.time()
        lat = (df.select((F.lit(committed) - F.unix_micros("produced_at") / 1e6).alias("lat"))
               .agg(F.percentile_approx("lat", [0.5, 0.95], 1000).alias("p"), F.max("lat").alias("mx")).first())
        batches.append({"batch": batch_id, "rows": n, "p50_s": round(lat["p"][0], 2),
                        "p95_s": round(lat["p"][1], 2), "max_s": round(lat["mx"], 2)})
        df.unpersist()

    q = (events.writeStream.foreachBatch(sink).trigger(processingTime=trigger)
         .option("checkpointLocation", str(C.CHECKPOINTS / "stream")).start())

    t0 = time.time()
    prod = threading.Thread(target=producer, args=(n_files, per_file, interval))
    prod.start()
    prod.join()
    time.sleep(8)                      # let the last files drain
    q.processAllAvailable()
    q.stop()
    wall = time.time() - t0

    valid = spark.read.parquet(str(C.STREAM_OUT / "valid"))
    quar = spark.read.parquet(str(C.STREAM_OUT / "quarantine"))
    produced = n_files * per_file
    lat_all = [b["p95_s"] for b in batches]
    out = {
        "files": n_files, "events_produced_unique": produced,
        "events_with_replays": produced + (n_files - 1) * (per_file // 100),
        "valid_written": valid.count(), "distinct_valid_ids": valid.select("event_id").distinct().count(),
        "quarantined": quar.count(), "micro_batches": len(batches),
        "median_batch_p50_latency_s": sorted(b["p50_s"] for b in batches)[len(batches) // 2],
        "worst_batch_p95_latency_s": max(lat_all), "wall_seconds": round(wall, 1),
        "input_rate_events_per_s": round(per_file / interval, 1),
    }
    (C.DATA / "stream_stats.json").write_text(json.dumps({"summary": out, "batches": batches}, indent=1))
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", type=int, default=40)
    ap.add_argument("--per-file", type=int, default=2000)
    a = ap.parse_args()
    print(json.dumps(run(a.files, a.per_file)))
