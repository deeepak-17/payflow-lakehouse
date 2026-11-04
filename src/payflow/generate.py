"""Synthetic cross-border payment event generator.

Produces, for one business date:
  * landing/date=D/events_partK.json  - nested JSON payment lifecycle events
  * fx/date=D/rates.csv               - daily FX rates to USD
  * settlement/date=D/settlement.csv  - partner-bank settlement report
  * ground_truth/date=D/faults.csv    - labels for every injected fault

Faults are injected on purpose so the pipeline's data-quality layer can be
scored against ground truth (see evaluate.py). The pipeline never reads
ground_truth/.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from payflow import config as C

BASE_RATE = {  # units of currency per 1 USD
    "USD": 1.0, "EUR": 0.92, "GBP": 0.79, "INR": 83.5, "SGD": 1.35,
    "AED": 3.67, "JPY": 151.0, "MXN": 17.2, "PHP": 56.4, "AUD": 1.52,
}
# currency that has no published rate on a given day-offset (tests as-of fallback)
MISSING_RATE = {2: "JPY"}

FAULT_RATES = {
    "duplicate_event": 0.010,
    "missing_amount": 0.003,
    "invalid_currency": 0.002,
    "currency_case": 0.003,
    "nonpositive_amount": 0.002,
    "amount_as_string": 0.003,
    "late_event": 0.005,
    "orphan_status": 0.001,
}
SETTLEMENT_FAULTS = {"missing_in_settlement": 0.002, "amount_mismatch": 0.001, "missing_in_ledger": 0.0005}


def fx_for_day(day_index: int, rng: np.random.Generator) -> dict[str, float]:
    rates = {}
    for ccy, base in BASE_RATE.items():
        drift = rng.normal(0, 0.004, size=day_index + 1).sum()
        rates[ccy] = 1.0 if ccy == "USD" else round(base * float(np.exp(drift)), 6)
    return rates


def generate_day(run_date: date, day_index: int, n_payments: int, seed: int = 7) -> dict:
    rng = np.random.default_rng(seed * 1000 + day_index)
    prng = random.Random(seed * 1000 + day_index)
    d = run_date.isoformat()
    next_d = (run_date + timedelta(days=1)).isoformat()

    # ---------- FX ----------
    rates = fx_for_day(day_index, np.random.default_rng(seed))
    fx_dir = C.FX / f"date={d}"
    fx_dir.mkdir(parents=True, exist_ok=True)
    with open(fx_dir / "rates.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["rate_date", "currency", "units_per_usd"])
        for ccy, r in rates.items():
            if MISSING_RATE.get(day_index) == ccy:
                continue
            w.writerow([d, ccy, r])

    faults: list[tuple[str, str, str]] = []  # (event_id, payment_id, fault_type)
    if day_index in MISSING_RATE:
        faults.append(("", "", f"missing_fx_rate:{MISSING_RATE[day_index]}"))

    # ---------- payments ----------
    start = datetime.combine(run_date, datetime.min.time(), tzinfo=timezone.utc)
    weights = np.array([30, 15, 10, 14, 6, 6, 5, 6, 4, 4], dtype=float)
    weights /= weights.sum()
    src = rng.choice(C.CURRENCIES, size=n_payments, p=weights)
    dst = rng.choice(C.CURRENCIES, size=n_payments, p=weights)
    clash = src == dst
    dst[clash] = np.where(src[clash] == "USD", "INR", "USD")
    usd_amount = np.round(np.exp(rng.normal(7.0, 1.3, size=n_payments)), 2)  # ~1.1k USD median
    created_offset = np.sort(rng.uniform(0, 86_399, size=n_payments))
    settle_delay = rng.exponential(2 * 3600, size=n_payments)
    outcome_failed = rng.random(n_payments) < 0.05
    rails = rng.choice(C.RAILS, size=n_payments)

    events: list[dict] = []
    settled_rows: list[dict] = []
    next_day_events: list[dict] = []
    next_day_settle: list[dict] = []
    settled_rows.extend(_read_carry(run_date, kind="settle"))
    for i in range(n_payments):
        pid = f"pay_{run_date:%Y%m%d}_{i:07d}"
        ccy = str(src[i])
        amt = round(float(usd_amount[i]) * rates[ccy], 2)
        cts = start + timedelta(seconds=float(created_offset[i]))
        base = {
            "payment_id": pid,
            "rail": str(rails[i]),
            "payer": {"institution_id": f"FI{rng.integers(100, 400)}", "country": C.CURRENCY_COUNTRY[ccy]},
            "payee": {"institution_id": f"FI{rng.integers(100, 400)}", "country": C.CURRENCY_COUNTRY[str(dst[i])]},
            "amount": {"value": amt, "currency": ccy},
            "target_currency": str(dst[i]),
        }
        events.append({**base, "event_id": f"evt_{pid}_c", "event_type": "PAYMENT_CREATED",
                       "event_ts": cts.isoformat()})
        ots = cts + timedelta(seconds=float(settle_delay[i]))
        etype = "PAYMENT_FAILED" if outcome_failed[i] else "PAYMENT_SETTLED"
        out = {**base, "event_id": f"evt_{pid}_o", "event_type": etype, "event_ts": ots.isoformat()}
        srow = {"payment_id": pid, "settled_amount": amt, "currency": ccy, "settlement_date": ots.date().isoformat()}
        if ots.date() == run_date:
            events.append(out)
            if etype == "PAYMENT_SETTLED":
                settled_rows.append(srow)
        else:
            next_day_events.append(out)  # crosses midnight: lands in next day's feed
            if etype == "PAYMENT_SETTLED":
                next_day_settle.append(srow)

    # ---------- inject event faults ----------
    created = [e for e in events if e["event_type"] == "PAYMENT_CREATED"]

    def pick(pool, rate):
        k = max(1, int(len(pool) * rate))
        return prng.sample(pool, k)

    used: set[str] = set()

    def fresh(pool, rate):
        chosen = [e for e in pick([e for e in pool if e["event_id"] not in used], rate)]
        used.update(e["event_id"] for e in chosen)
        return chosen

    for e in fresh(created, FAULT_RATES["missing_amount"]):
        e["amount"] = {"value": None, "currency": e["amount"]["currency"]}
        faults.append((e["event_id"], e["payment_id"], "missing_amount"))
    for e in fresh(created, FAULT_RATES["invalid_currency"]):
        e["amount"] = {**e["amount"], "currency": prng.choice(["XXX", "ZZZ", "US$", ""])}
        faults.append((e["event_id"], e["payment_id"], "invalid_currency"))
    for e in fresh(created, FAULT_RATES["currency_case"]):
        e["amount"] = {**e["amount"], "currency": f" {e['amount']['currency'].lower()} "}
        faults.append((e["event_id"], e["payment_id"], "currency_case"))
    for e in fresh(created, FAULT_RATES["nonpositive_amount"]):
        e["amount"] = {**e["amount"], "value": prng.choice([0.0, -abs(e["amount"]["value"])])}
        faults.append((e["event_id"], e["payment_id"], "nonpositive_amount"))
    for e in fresh(created, FAULT_RATES["amount_as_string"]):
        e["amount"] = {**e["amount"], "value": f"{e['amount']['value']:,.2f}"}  # schema drift: "12,345.67"
        faults.append((e["event_id"], e["payment_id"], "amount_as_string"))

    late = fresh(created, FAULT_RATES["late_event"])
    late_ids = {e["event_id"] for e in late}
    for e in late:
        faults.append((e["event_id"], e["payment_id"], "late_event"))
    events = [e for e in events if e["event_id"] not in late_ids]

    orphans = []
    for k in range(max(1, int(n_payments * FAULT_RATES["orphan_status"]))):
        pid = f"pay_{run_date:%Y%m%d}_X{k:05d}"
        ts = start + timedelta(seconds=prng.uniform(0, 86_399))
        orphans.append({"payment_id": pid, "event_id": f"evt_{pid}_o", "event_type": "PAYMENT_SETTLED",
                        "event_ts": ts.isoformat(), "rail": "SWIFT",
                        "payer": {"institution_id": "FI999", "country": "US"},
                        "payee": {"institution_id": "FI998", "country": "IN"},
                        "amount": {"value": 100.0, "currency": "USD"}, "target_currency": "INR"})
        faults.append((f"evt_{pid}_o", pid, "orphan_status"))
    events.extend(orphans)
    events.sort(key=lambda e: e["event_ts"])

    dups = pick(events, FAULT_RATES["duplicate_event"])
    for e in dups:
        faults.append((e["event_id"], e["payment_id"], "duplicate_event"))

    # ---------- write landing files (6 drops per day) ----------
    land = C.LANDING / f"date={d}"
    land.mkdir(parents=True, exist_ok=True)
    parts = np.array_split(np.arange(len(events)), 6)
    carry_in = _read_carry(run_date)
    for k, idx in enumerate(parts):
        rows = [events[j] for j in idx]
        if k == 5:
            rows += [json.loads(json.dumps(e)) for e in dups]   # replays arrive in the last drop
            rows += carry_in                                     # previous day's late/cross-midnight events
        with open(land / f"events_part{k}.json", "w") as f:
            for e in rows:
                f.write(json.dumps(e) + "\n")
    _write_carry(run_date + timedelta(days=1), next_day_events + late)
    _write_carry(run_date + timedelta(days=1), next_day_settle, kind="settle")

    # ---------- settlement report ----------
    s_rng = random.Random(seed * 77 + day_index)
    sfaults = []
    k_drop = max(1, int(len(settled_rows) * SETTLEMENT_FAULTS["missing_in_settlement"]))
    drop = set(s_rng.sample(range(len(settled_rows)), k_drop))
    rest = [i for i in range(len(settled_rows)) if i not in drop]
    k_mis = max(1, int(len(settled_rows) * SETTLEMENT_FAULTS["amount_mismatch"]))
    mism = set(s_rng.sample(rest, k_mis))
    out_rows = []
    for i, r in enumerate(settled_rows):
        if i in drop:
            sfaults.append(("", r["payment_id"], "missing_in_settlement"))
            continue
        if i in mism:
            r = {**r, "settled_amount": round(r["settled_amount"] * s_rng.choice([0.9, 1.1, 0.99]), 2)}
            sfaults.append(("", r["payment_id"], "amount_mismatch"))
        out_rows.append(r)
    for k in range(max(1, int(len(settled_rows) * SETTLEMENT_FAULTS["missing_in_ledger"]))):
        pid = f"pay_{run_date:%Y%m%d}_S{k:05d}"
        out_rows.append({"payment_id": pid, "settled_amount": 250.0, "currency": "EUR", "settlement_date": d})
        sfaults.append(("", pid, "missing_in_ledger"))
    sdir = C.SETTLEMENT / f"date={d}"
    sdir.mkdir(parents=True, exist_ok=True)
    with open(sdir / "settlement.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["payment_id", "settled_amount", "currency", "settlement_date"])
        w.writeheader()
        w.writerows(out_rows)

    # ---------- ground truth ----------
    gdir = C.GROUND_TRUTH / f"date={d}"
    gdir.mkdir(parents=True, exist_ok=True)
    with open(gdir / "faults.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["event_id", "payment_id", "fault_type"])
        w.writerows(faults + sfaults)

    return {"date": d, "payments": n_payments, "events_written": len(events) + len(dups) + len(carry_in),
            "faults": len(faults) + len(sfaults)}


def _carry_path(day: date, kind: str) -> Path:
    return C.DATA / "_carry" / f"{kind}_{day.isoformat()}.json"


def _write_carry(day: date, rows: list[dict], kind: str = "events") -> None:
    p = _carry_path(day, kind)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(rows))


def _read_carry(day: date, kind: str = "events") -> list[dict]:
    p = _carry_path(day, kind)
    return json.loads(p.read_text()) if p.exists() else []


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="business date YYYY-MM-DD")
    ap.add_argument("--start", required=True, help="first date of the series (for FX drift)")
    ap.add_argument("--payments", type=int, default=60_000)
    a = ap.parse_args()
    d, s = date.fromisoformat(a.date), date.fromisoformat(a.start)
    print(generate_day(d, (d - s).days, a.payments))


if __name__ == "__main__":
    main()
