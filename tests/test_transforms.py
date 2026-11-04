from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import Row

from payflow.gold import corridor_daily, reconcile
from payflow.silver import build_state, dedupe, normalise, to_usd, validate
from payflow.spark import EVENT_SCHEMA, FX_SCHEMA, get_spark


@pytest.fixture(scope="session")
def spark():
    s = get_spark("payflow-tests")
    yield s
    s.stop()


def ev(eid, pid, etype="PAYMENT_CREATED", ts="2026-10-01T10:00:00+00:00", value="100.00", ccy="USD"):
    return (eid, pid, etype, ts, "SWIFT", ("FI1", "US"), ("FI2", "IN"), (value, ccy), "INR", None, None)


def frame(spark, rows, ingested="2026-10-01 12:00:00"):
    df = spark.createDataFrame(rows, EVENT_SCHEMA)
    from pyspark.sql import functions as F
    return (df.withColumn("_ingested_at", F.lit(ingested).cast("timestamp"))
            .withColumn("_source_file", F.lit("f.json")).withColumn("ingest_date", F.lit("2026-10-01").cast("date")))


def reasons(df, eid):
    return df.filter(f"event_id = '{eid}'").first()["dq_reasons"]


def test_validation_rules(spark):
    rows = [ev("ok", "p1"), ev("nullamt", "p2", value=None), ev("neg", "p3", value="-5"),
            ev("badccy", "p4", ccy="XXX"), ev("junk", "p5", value="abc")]
    out = validate(normalise(frame(spark, rows)))
    assert reasons(out, "ok") == []
    assert reasons(out, "nullamt") == ["MISSING_AMOUNT"]
    assert reasons(out, "neg") == ["NONPOSITIVE_AMOUNT"]
    assert reasons(out, "badccy") == ["INVALID_CURRENCY"]
    assert reasons(out, "junk") == ["UNPARSEABLE_AMOUNT"]


def test_schema_drift_is_recovered(spark):
    out = validate(normalise(frame(spark, [ev("s", "p1", value="12,345.67", ccy=" eur ")])))
    r = out.first()
    assert r["amount"] == Decimal("12345.67") and r["currency"] == "EUR"
    assert r["dq_reasons"] == [] and r["amount_recovered"] and r["currency_recovered"]


def test_dedupe_keeps_first_arrival(spark):
    a = frame(spark, [ev("e1", "p1")], ingested="2026-10-01 01:00:00")
    b = frame(spark, [ev("e1", "p1"), ev("e2", "p2")], ingested="2026-10-01 05:00:00")
    out = dedupe(a.unionByName(b))
    assert out.count() == 2
    assert str(out.filter("event_id = 'e1'").first()["_ingested_at"]) == "2026-10-01 01:00:00"


def test_state_orphans_and_late_outcomes(spark):
    rows = [
        ev("c1", "p1"), ev("o1", "p1", "PAYMENT_SETTLED", ts="2026-10-01T11:00:00+00:00"),
        ev("o_old", "ghost", "PAYMENT_SETTLED", ts="2026-09-28T09:00:00+00:00"),   # orphan past grace
        ev("o_new", "early", "PAYMENT_SETTLED", ts="2026-10-01T09:00:00+00:00"),   # created not landed yet
    ]
    ev_df = validate(normalise(frame(spark, rows)))
    valid = ev_df.filter("size(dq_reasons) = 0")
    invalid = ev_df.filter("size(dq_reasons) > 0")
    pay, quarantined, pending = build_state(valid, invalid, date(2026, 10, 1))
    p1 = pay.filter("payment_id = 'p1'").first()
    assert p1["status"] == "SETTLED" and p1["settlement_seconds"] == 3600
    assert [r["event_id"] for r in quarantined.collect()] == ["o_old"]
    assert [r["event_id"] for r in pending.collect()] == ["o_new"]


def test_fx_asof_fallback(spark):
    pay = spark.createDataFrame(
        [Row(payment_id="p1", currency="JPY", amount=Decimal("15100.00"), created_date=date(2026, 10, 3))])
    rates = spark.createDataFrame([(date(2026, 10, 1), "JPY", 150.0), (date(2026, 10, 2), "JPY", 151.0)],
                                  FX_SCHEMA)
    r = to_usd(pay, rates).first()
    assert r["fx_rate_date"] == date(2026, 10, 2) and r["fx_fallback"] is True
    assert r["amount_usd"] == Decimal("100.00")


def test_reconciliation_classes(spark):
    ledger = spark.createDataFrame([
        Row(payment_id="m", amount=Decimal("10.00"), currency="USD"),
        Row(payment_id="diff", amount=Decimal("10.00"), currency="USD"),
        Row(payment_id="nosettle", amount=Decimal("10.00"), currency="USD"),
    ])
    settle = spark.createDataFrame([
        Row(payment_id="m", settled_amount=Decimal("10.00"), currency="USD"),
        Row(payment_id="diff", settled_amount=Decimal("11.00"), currency="USD"),
        Row(payment_id="noledger", settled_amount=Decimal("5.00"), currency="USD"),
        Row(payment_id="quar", settled_amount=Decimal("5.00"), currency="USD"),
    ])
    q = spark.createDataFrame([Row(payment_id="quar")])
    got = {r["payment_id"]: r["recon_status"] for r in reconcile(ledger, settle, q).collect()}
    assert got == {"m": "MATCHED", "diff": "AMOUNT_MISMATCH", "nosettle": "MISSING_IN_SETTLEMENT",
                   "noledger": "MISSING_IN_LEDGER", "quar": "LEDGER_QUARANTINED"}


def test_corridor_mart_totals(spark):
    pay = spark.createDataFrame([
        Row(created_date=date(2026, 10, 1), src_country="US", dst_country="IN", status="SETTLED",
            amount_usd=Decimal("10.00"), settlement_seconds=60),
        Row(created_date=date(2026, 10, 1), src_country="US", dst_country="IN", status="FAILED",
            amount_usd=Decimal("5.00"), settlement_seconds=30),
    ])
    r = corridor_daily(pay).first()
    assert (r["payments"], r["settled"], r["failed"], r["volume_usd"]) == (2, 1, 1, Decimal("15.00"))
