"""
ingest.py
Reads the four raw CSV files from data/raw/, normalizes them into a single
unified schema, and writes data/processed/client_health_records.csv.
Run with: python src/ingest.py
"""

import uuid
from pathlib import Path

import pandas as pd

try:
    from src.utils import clean_text, to_datetime_safe
except ModuleNotFoundError:
    from utils import clean_text, to_datetime_safe

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = PROCESSED_DIR / "client_health_records.csv"

# Canonical column order for the unified schema
UNIFIED_COLUMNS = [
    "record_id",
    "date",
    "source",
    "client_name",
    "contact_role",
    "product_area",
    "raw_text",
    "status",
    "priority",
    "owner",
    "original_channel",
]


def _new_id() -> str:
    return str(uuid.uuid4())[:8].upper()


# ---------------------------------------------------------------------------
# Source-specific normalizers
# ---------------------------------------------------------------------------

def ingest_support_tickets(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    out = pd.DataFrame()
    out["record_id"] = [f"TKT-{_new_id()}" for _ in range(len(df))]
    out["date"] = to_datetime_safe(df["created_date"])
    out["source"] = "support_ticket"
    out["client_name"] = df["client_name"]
    out["contact_role"] = df["contact_role"]
    out["product_area"] = df["product_area"]
    out["raw_text"] = clean_text(df["issue_description"])
    out["status"] = df["ticket_status"]
    out["priority"] = df["priority"]
    out["owner"] = df["assigned_to"]
    out["original_channel"] = "Support Portal"
    return out


def ingest_account_emails(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    out = pd.DataFrame()
    out["record_id"] = [f"EM-{_new_id()}" for _ in range(len(df))]
    out["date"] = to_datetime_safe(df["date_sent"])
    out["source"] = "account_email"
    out["client_name"] = df["client"]
    out["contact_role"] = df["from_role"]
    out["product_area"] = df["subject_product_area"]
    out["raw_text"] = clean_text(df["email_body"])
    out["status"] = df["thread_status"]
    # Map email urgency → priority label so all sources share the same vocabulary
    urgency_map = {"High": "High", "Medium": "Medium", "Low": "Low"}
    out["priority"] = df["urgency"].map(urgency_map).fillna("Medium")
    out["owner"] = df["account_owner"]
    out["original_channel"] = "Email"
    return out


def ingest_qbr_notes(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    out = pd.DataFrame()
    out["record_id"] = [f"QBR-{_new_id()}" for _ in range(len(df))]
    out["date"] = to_datetime_safe(df["meeting_date"])
    out["source"] = "qbr_notes"
    out["client_name"] = df["client_name"]
    out["contact_role"] = df["attendee_role"]
    out["product_area"] = df["topic_area"]
    out["raw_text"] = clean_text(df["meeting_notes"])
    out["status"] = df["follow_up_status"]
    # QBR notes have no native priority field; default to Medium
    out["priority"] = "Medium"
    out["owner"] = df["nymbus_owner"]
    out["original_channel"] = "QBR Meeting"
    return out


def ingest_product_feedback(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    out = pd.DataFrame()
    out["record_id"] = [f"FB-{_new_id()}" for _ in range(len(df))]
    out["date"] = to_datetime_safe(df["submitted_date"])
    out["source"] = "product_feedback"
    out["client_name"] = df["organization"]
    out["contact_role"] = df["submitter_role"]
    out["product_area"] = df["module"]
    out["raw_text"] = clean_text(df["feedback_text"])
    # Feedback has no status in the traditional sense; use reviewed flag
    out["status"] = df["reviewed"].map({"Yes": "Reviewed", "No": "Pending Review"})
    # Derive priority from sentiment: Negative → High, Neutral → Medium, Positive → Low
    sentiment_priority = {"Negative": "High", "Neutral": "Medium", "Positive": "Low"}
    out["priority"] = df["sentiment"].map(sentiment_priority).fillna("Medium")
    out["owner"] = ""  # product feedback has no assigned owner in raw data
    out["original_channel"] = "Feedback Form"
    return out


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_ingestion() -> pd.DataFrame:
    ingested = []

    sources = [
        (RAW_DIR / "support_tickets.csv", ingest_support_tickets),
        (RAW_DIR / "account_emails.csv", ingest_account_emails),
        (RAW_DIR / "qbr_notes.csv", ingest_qbr_notes),
        (RAW_DIR / "product_feedback.csv", ingest_product_feedback),
    ]

    for path, normalizer in sources:
        if not path.exists():
            print(f"  [WARN] {path} not found — skipping.")
            continue
        df = normalizer(path)
        ingested.append(df)
        print(f"  Loaded {len(df):>3} rows from {path.name}")

    combined = pd.concat(ingested, ignore_index=True)

    # Enforce column order
    combined = combined[UNIFIED_COLUMNS]

    # Sort by date ascending so the output is chronologically ordered
    combined = combined.sort_values("date").reset_index(drop=True)

    combined.to_csv(OUTPUT_PATH, index=False)
    return combined


def main():
    print("Starting ingestion pipeline...\n")
    result = run_ingestion()
    print(f"\nOutput saved to: {OUTPUT_PATH.resolve()}")
    print(f"Shape: {result.shape[0]} rows × {result.shape[1]} columns")
    print(f"Columns: {list(result.columns)}")
    print(f"\nSource breakdown:")
    print(result["source"].value_counts().to_string())
    print(f"\nDate range: {result['date'].min().date()} → {result['date'].max().date()}")


if __name__ == "__main__":
    main()
