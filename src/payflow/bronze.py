"""Bronze: land raw JSON events as Parquet, untouched except for lineage columns.

Idempotent: rerunning a date overwrites only that ingest_date partition.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from payflow import config as C
from payflow.spark import EVENT_SCHEMA


def ingest(spark: SparkSession, run_date: str) -> dict:
    src = str(C.LANDING / f"date={run_date}" / "*.json")
    df = (
        spark.read.schema(EVENT_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(src)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("ingest_date", F.lit(run_date).cast("date"))
    )
    df.cache()
    stats = {"rows": df.count(), "corrupt": df.filter(F.col("_corrupt_record").isNotNull()).count()}
    df.write.mode("overwrite").partitionBy("ingest_date").parquet(str(C.BRONZE / "events"))
    df.unpersist()
    return stats
