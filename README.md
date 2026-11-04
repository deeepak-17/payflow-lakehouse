# PayFlow — Cross-Border Payments Lakehouse

A batch + near-real-time data pipeline for cross-border payment events, built with
**PySpark**, orchestrated by **Apache Airflow**, and served from a **PostgreSQL** star
schema. Its core idea: every bad record the upstream feed can send is injected on purpose
and **labelled**, so the data-quality layer is scored against ground truth instead of
being assumed to work.

```
 upstream JSON drops ─┐                         ┌─► quarantine/ (reason codes)
 FX rate files ───────┤   bronze      silver    │
 partner settlement ──┤ ─► Parquet ─► validate ─┼─► payments (1 row / payment, USD) ─► gold ─► PostgreSQL
                      │   (raw +      dedupe     │                                    star schema,
                      │   lineage)    state      └─ DQ gate (fails task on critical)  corridor mart,
                      │              as-of FX                                          T+1 recon
 stream_in/ files ────┴─► Structured Streaming: validate + watermark de-dup ─► stream_out/
```

## Results (8 daily runs, 60,000 payments/day)

All numbers below come from the run outputs committed in `results/` (`run_log.jsonl`,
`evaluation.json`, `stream_stats.json`, `dq/`).

| | |
|---|---|
| Airflow DAG runs | 8 of 8 succeeded (`airflow dags test`, 7 tasks each) |
| Raw events ingested | 964,300 → 476,360 payments in `fact_payments` |
| Replayed events removed | 9,175 (0 duplicate payment IDs after silver) |
| Wall time per daily run | 96 s (day 1) → 148 s (day 8) on a 2-core laptop-class VM |
| Unit tests | 7 passing (`pytest`) |

**Fault detection vs ground truth** (event faults scored on days 1–6, whose late/orphan
grace windows have closed; settlement faults scored on the 7 reconciled dates):

| Injected fault | Expected handling | Injected | Handled correctly |
|---|---|---|---|
| Missing amount | quarantine `MISSING_AMOUNT` | 1,080 | 1,080 |
| Unknown currency | quarantine `INVALID_CURRENCY` | 714 | 714 |
| Zero / negative amount | quarantine `NONPOSITIVE_AMOUNT` | 714 | 714 |
| Outcome with no payment | quarantine `ORPHAN_STATUS` after 1-day grace | 360 | 360 |
| Amount sent as `"12,345.67"` | parsed and kept | 1,068 | 1,068 |
| Currency as `" eur "` | normalised and kept | 1,074 | 1,074 |
| Event arrives next day | routed to its original business date | 1,776 | 1,776 |
| Replayed event | dropped | 6,882 | 6,882 |
| Missing from partner settlement | `MISSING_IN_SETTLEMENT` | 777 | 777 |
| Settled amount differs | `AMOUNT_MISMATCH` | 388 | 388 |
| Settlement with no ledger entry | `MISSING_IN_LEDGER` | 194 | 194 |

Precision: all 3,704 quarantined events and all 1,359 reconciliation breaks map to an
injected fault (zero false flags). 11 settlement faults landed on payments already
quarantined for bad data; those are reported as `LEDGER_QUARANTINED`, not scored.
The day-3 feed has no JPY rate; 3,092 JPY payments fell back to the previous day's rate
via the as-of join and are flagged `fx_fallback`.

**Streaming** (60 one-second drops of 2,000 events, ~1% replays, ~1% invalid):
120,000 unique events in → 118,815 valid + 1,185 quarantined, all 1,180 replays dropped
by watermark de-duplication, 0 duplicate IDs. Median end-to-end latency (event produced →
micro-batch committed) was 1.85 s; per-batch p95 stayed at 2–3 s after JVM warm-up
(first two batches peaked at 9.7 s).

## Design decisions

- **Amount read as a string in bronze.** A numeric schema silently nulls `"12,345.67"`;
  reading strings and parsing in silver turns schema drift into a recoverable case.
- **`try_cast` / `try_to_timestamp`.** Spark 4 runs in ANSI mode, where one malformed
  value raises and kills the batch. The unit tests caught this.
- **Outcome-before-create is legal for one day.** Late `CREATED` events are common, so an
  unmatched `SETTLED` event is held as pending for one day before it is quarantined as an
  orphan.
- **T+1 reconciliation.** Run date D reconciles settlements dated D-1, after D's
  late-arriving events have landed; reconciling same-day produced false breaks.
- **Idempotent everywhere.** Bronze overwrites only its `ingest_date` partition; the
  Postgres load stages into a temp table and merges with `INSERT ... ON CONFLICT DO UPDATE`
  in one transaction. Rerunning a date leaves row counts and USD totals unchanged
  (verified: 476,360 rows / $1,211,673,002 before and after).
- **DQ gates block the load.** Critical checks (unique IDs, required fields, currency
  reference set, 100% FX coverage, fact = silver row count, mart = fact volume) raise and
  fail the Airflow task. Removing INR rates makes `fx_coverage` fail on 1,409 rows and the
  task exit 1.

## Known limits

- Silver rebuilds payment state from the full bronze history each run, so run time grows
  with history (96 s → 148 s over 8 days). The next step is an incremental `MERGE` on a
  table format such as Delta Lake or Iceberg.
- Runs locally (`local[*]` Spark, Postgres on the same machine). Nothing here has been
  deployed to a cluster or cloud.
- Data is synthetic; the generator is in `src/payflow/generate.py`.

## Run it

```bash
pip install -r requirements.txt          # Java 17+ required for Spark
createdb payflow
export PYTHONPATH=src PAYFLOW_HOME=$PWD
for s in generate bronze silver dq_silver gold dq_gold load; do
  python -m payflow.cli $s --date 2026-10-01
done
python -m pytest -q tests
python -m payflow.stream --files 60          # streaming benchmark

# Airflow
pip install "apache-airflow==3.1.*"
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags
airflow db migrate && airflow dags test payflow_daily 2026-10-01
```

## Layout

```
dags/payflow_daily.py      Airflow DAG: generate >> bronze >> silver >> dq_silver >> gold >> dq_gold >> load
src/payflow/generate.py    synthetic feed + labelled fault injection
src/payflow/bronze.py      raw JSON -> Parquet with lineage columns
src/payflow/silver.py      normalise, validate, dedupe, payment state, as-of FX
src/payflow/gold.py        star schema, corridor mart, T+1 reconciliation
src/payflow/dq.py          data-quality gates
src/payflow/load.py        transactional upsert into PostgreSQL
src/payflow/stream.py      Structured Streaming path
src/payflow/evaluate.py    scores DQ handling against ground truth
sql/schema.sql             warehouse DDL (PK/FK, CHECK constraints, indexes)
sql/analytics.sql          window-function analytics over the warehouse
tests/                     pytest suite for the transforms
```
