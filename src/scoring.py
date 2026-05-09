"""
scoring.py
Computes a 0–100 renewal risk score for each classified record, then
aggregates summary metrics by theme and product area.
Reads:  data/processed/client_health_classified.csv
Writes: data/processed/client_health_scored.csv
        outputs/client_health_summary.csv
Run with: python src/scoring.py
"""

from pathlib import Path

import pandas as pd

INPUT_PATH = Path("data/processed/client_health_classified.csv")
SCORED_PATH = Path("data/processed/client_health_scored.csv")
SUMMARY_PATH = Path("outputs/client_health_summary.csv")

SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Scoring weights
# Each weight adds to a raw score that is then capped at 100.
# Keeping the weights explicit here makes them easy to tune later.
# ---------------------------------------------------------------------------

WEIGHTS = {
    "sentiment_negative": 15,
    "sentiment_neutral": 5,
    "severity_high": 20,
    "severity_medium": 10,
    "business_impact_high": 15,
    "business_impact_medium": 8,
    "is_open_or_unresolved": 10,
    "is_repeated_issue": 10,
    "renewal_risk_high": 20,
    "renewal_risk_medium": 10,
    # Extra weight when a strategic channel (QBR, email) surfaces a problem
    "strategic_channel_signal": 10,
    # Priority uplift
    "priority_critical": 10,
    "priority_high": 5,
}

STRATEGIC_SOURCES = {"qbr_notes", "account_email"}


def compute_risk_score(row: pd.Series) -> int:
    score = 0

    # Sentiment
    if row["sentiment"] == "Negative":
        score += WEIGHTS["sentiment_negative"]
    elif row["sentiment"] == "Neutral":
        score += WEIGHTS["sentiment_neutral"]

    # Severity
    if row["severity"] == "High":
        score += WEIGHTS["severity_high"]
    elif row["severity"] == "Medium":
        score += WEIGHTS["severity_medium"]

    # Business impact
    if row["business_impact"] == "High":
        score += WEIGHTS["business_impact_high"]
    elif row["business_impact"] == "Medium":
        score += WEIGHTS["business_impact_medium"]

    # Status
    if row["is_open_or_unresolved"]:
        score += WEIGHTS["is_open_or_unresolved"]

    # Repeated issue
    if row["is_repeated_issue"]:
        score += WEIGHTS["is_repeated_issue"]

    # Renewal risk signal (adds on top as an aggregate signal)
    if row["renewal_risk_signal"] == "High":
        score += WEIGHTS["renewal_risk_high"]
    elif row["renewal_risk_signal"] == "Medium":
        score += WEIGHTS["renewal_risk_medium"]

    # Strategic channel bonus: QBR or email surfaces a negative or high-severity issue
    if row["source"] in STRATEGIC_SOURCES and (
        row["sentiment"] == "Negative" or row["severity"] == "High"
    ):
        score += WEIGHTS["strategic_channel_signal"]

    # Priority uplift
    if row["priority"] == "Critical":
        score += WEIGHTS["priority_critical"]
    elif row["priority"] == "High":
        score += WEIGHTS["priority_high"]

    return min(score, 100)


def assign_risk_level(score: int) -> str:
    if score >= 70:
        return "High"
    if score >= 40:
        return "Medium"
    return "Low"


# ---------------------------------------------------------------------------
# Aggregated summary
# ---------------------------------------------------------------------------

def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby(["theme", "product_area"])
        .agg(
            record_count=("record_id", "count"),
            avg_risk_score=("risk_score", "mean"),
            high_risk_count=("risk_level", lambda x: (x == "High").sum()),
            unresolved_count=("is_open_or_unresolved", "sum"),
            repeated_issue_count=("is_repeated_issue", "sum"),
            negative_sentiment_count=("sentiment", lambda x: (x == "Negative").sum()),
        )
        .reset_index()
    )
    summary["avg_risk_score"] = summary["avg_risk_score"].round(1)
    summary = summary.sort_values("avg_risk_score", ascending=False).reset_index(drop=True)
    return summary


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_scoring() -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_csv(INPUT_PATH, parse_dates=["date"])
    print(f"Loaded {len(df)} classified records from {INPUT_PATH}")

    df["risk_score"] = df.apply(compute_risk_score, axis=1)
    df["risk_level"] = df["risk_score"].apply(assign_risk_level)

    df.to_csv(SCORED_PATH, index=False)

    summary = build_summary(df)
    summary.to_csv(SUMMARY_PATH, index=False)

    return df, summary


def main():
    df, summary = run_scoring()

    print(f"\nScored output saved to:  {SCORED_PATH.resolve()}")
    print(f"Summary output saved to: {SUMMARY_PATH.resolve()}")

    print(f"\nScored shape: {df.shape[0]} rows × {df.shape[1]} columns")
    print(f"Columns: {list(df.columns)}")

    print(f"\nRisk level distribution:")
    print(df["risk_level"].value_counts().to_string())

    print(f"\nRisk score — min: {df['risk_score'].min()}, "
          f"max: {df['risk_score'].max()}, "
          f"mean: {df['risk_score'].mean():.1f}")

    print(f"\n--- Top 5 highest-risk records ---")
    top5 = df.nlargest(5, "risk_score")[
        ["record_id", "date", "source", "theme", "product_area",
         "risk_score", "risk_level", "renewal_risk_signal", "summary"]
    ]
    for _, r in top5.iterrows():
        print(f"\n  [{r['risk_score']:>3}] {r['record_id']}  |  {r['source']}  |  {r['theme']} / {r['product_area']}")
        print(f"       {r['summary'][:110]}")

    print(f"\n--- Top 5 highest-risk theme × product area combinations ---")
    top5_summary = summary.head(5)[
        ["theme", "product_area", "record_count", "avg_risk_score",
         "high_risk_count", "unresolved_count", "negative_sentiment_count"]
    ]
    print(top5_summary.to_string(index=False))


if __name__ == "__main__":
    main()
