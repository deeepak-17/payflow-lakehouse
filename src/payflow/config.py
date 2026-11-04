"""Paths and constants shared by every pipeline stage."""
import os
from pathlib import Path

ROOT = Path(os.environ.get("PAYFLOW_HOME", Path(__file__).resolve().parents[2]))
DATA = ROOT / "data"

LANDING = DATA / "landing"          # raw JSON event files dropped by "upstream"
SETTLEMENT = DATA / "settlement"    # partner-bank settlement CSVs
FX = DATA / "fx"                    # daily FX rate CSVs
BRONZE = DATA / "bronze"
SILVER = DATA / "silver"
QUARANTINE = DATA / "quarantine"
GOLD = DATA / "gold"
GROUND_TRUTH = DATA / "ground_truth"  # labels of injected faults, never read by the pipeline
STREAM_IN = DATA / "stream_in"
STREAM_OUT = DATA / "stream_out"
CHECKPOINTS = DATA / "_checkpoints"

PG_DSN = os.environ.get("PAYFLOW_PG_DSN", "host=/tmp dbname=payflow user=postgres")

CURRENCIES = ["USD", "EUR", "GBP", "INR", "SGD", "AED", "JPY", "MXN", "PHP", "AUD"]
CURRENCY_COUNTRY = {
    "USD": "US", "EUR": "DE", "GBP": "GB", "INR": "IN", "SGD": "SG",
    "AED": "AE", "JPY": "JP", "MXN": "MX", "PHP": "PH", "AUD": "AU",
}
RAILS = ["SWIFT", "SEPA", "ACH", "FPS", "UPI", "SPEI"]
EVENT_TYPES = ["PAYMENT_CREATED", "PAYMENT_SETTLED", "PAYMENT_FAILED"]

# amount tolerance (in source currency) when reconciling ledger vs settlement
RECON_TOLERANCE = 0.01
