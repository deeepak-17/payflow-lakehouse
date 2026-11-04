"""SparkSession factory and the explicit schemas every stage reads with."""
from pyspark.sql import SparkSession
from pyspark.sql import types as T


def get_spark(app: str = "payflow") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # idempotent partition reruns
        .config("spark.driver.memory", "3g")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


PARTY = T.StructType([
    T.StructField("institution_id", T.StringType()),
    T.StructField("country", T.StringType()),
])

# amount.value is read as STRING on purpose: upstream sometimes sends "12,345.67"
# (schema drift). Silver parses it; a numeric schema would silently null those rows.
EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("payment_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("event_ts", T.StringType()),
    T.StructField("rail", T.StringType()),
    T.StructField("payer", PARTY),
    T.StructField("payee", PARTY),
    T.StructField("amount", T.StructType([
        T.StructField("value", T.StringType()),
        T.StructField("currency", T.StringType()),
    ])),
    T.StructField("target_currency", T.StringType()),
    T.StructField("produced_at", T.StringType()),
    T.StructField("_corrupt_record", T.StringType()),
])

FX_SCHEMA = T.StructType([
    T.StructField("rate_date", T.DateType()),
    T.StructField("currency", T.StringType()),
    T.StructField("units_per_usd", T.DoubleType()),
])

SETTLEMENT_SCHEMA = T.StructType([
    T.StructField("payment_id", T.StringType()),
    T.StructField("settled_amount", T.DecimalType(18, 2)),
    T.StructField("currency", T.StringType()),
    T.StructField("settlement_date", T.DateType()),
])
