"""
classify.py
Rule-based classifier for client health records.
Reads:  data/processed/client_health_records.csv
Writes: data/processed/client_health_classified.csv
Run with: python src/classify.py
"""

from pathlib import Path

import pandas as pd

INPUT_PATH = Path("data/processed/client_health_records.csv")
OUTPUT_PATH = Path("data/processed/client_health_classified.csv")

# Statuses that mean the issue is still open / needs attention
OPEN_STATUSES = {
    "open", "in progress", "awaiting response", "overdue", "escalated", "pending review"
}

# Minimum occurrences of (theme, product_area) to flag as a repeated issue
REPEATED_ISSUE_THRESHOLD = 3


# ---------------------------------------------------------------------------
# Theme classification
# Rules are evaluated in priority order; first match wins.
# Each rule is (theme_label, list_of_keywords_to_search_in_text_and_product_area).
# ---------------------------------------------------------------------------

THEME_RULES = [
    ("Compliance Reporting", [
        "compliance", "bsa", "hmda", "cra", "reg e", "reg cc", "regulatory",
        "audit", "exam", "regulation", "examiner",
    ]),
    ("Payment Reconciliation", [
        "payment", "reconcil", "ach", "batch", "discrepancy", "disbursement",
        "rounding", "interest calculation",
    ]),
    ("Lending Workflow", [
        "loan", "lending", "approval", "amortization", "loan id", "loan pipeline",
    ]),
    ("Manual Workaround", [
        "manual", "workaround", "spreadsheet", "re-key", "re-keying", "manually",
        "3 hours", "every monday",
    ]),
    ("Follow-up / Ownership", [
        "follow up", "follow-up", "no response", "delayed follow", "delayed",
        "escalat", "overdue", "no action", "unanswered", "open commitment",
        "unresolved commitment",
    ]),
    ("Feature Request", [
        "feature request", "feature", "wish", "would love", "add ", "capability",
        "could add", "would be great", "missing capability",
    ]),
    ("Admin Permissions", [
        "admin", "permission", "access", "locked out", "propagat", "user management",
        "role", "onboarding", "sub-module",
    ]),
    ("Data Export", [
        "export", "download", "extract", "row limit", "bulk export", "10,000 rows",
        "10000 rows", "file generation",
    ]),
    ("Dashboard Limitations", [
        "dashboard", "refresh", "stale", "cache", "display", "not reflecting",
        "live data", "layout",
    ]),
    ("Reporting", [
        "report", "reporting", "gl reconciliation", "call report", "hmda",
        "scheduled report", "historical", "column", "metric",
    ]),
    ("Data Access", [
        "data access", "api", "endpoint", "integration", "feed", "sync",
        "silent failure", "silently", "outdated documentation",
    ]),
    ("General Support", []),  # catch-all — always matches
]


def classify_theme(text: str, product_area: str) -> str:
    combined = (text + " " + product_area).lower()
    for theme, keywords in THEME_RULES:
        if not keywords:  # catch-all
            return theme
        if any(kw in combined for kw in keywords):
            return theme
    return "General Support"


# ---------------------------------------------------------------------------
# Sentiment classification
# ---------------------------------------------------------------------------

POSITIVE_WORDS = [
    "appreciate", "appreciated", "improved", "improvement", "happy", "satisfied",
    "great", "thank", "keep it up", "better", "pleased", "positive", "excellent",
    "responsive", "love", "good",
]

NEGATIVE_WORDS = [
    "frustrat", "failing", "failed", "broken", "not working", "error", "stuck",
    "concern", "issue", "problem", "escalat", "cost us", "discrepancy",
    "incomplete", "intermittent", "missing", "blocker", "blocking", "persist",
    "friction", "painful", "slow", "too slow", "silently", "unreliable",
    "missed", "no response", "wasted", "overdue", "cannot", "can't", "failure",
    "limit", "restrictive", "outdated", "stale", "confusion", "confusing",
]


def classify_sentiment(text: str) -> str:
    lower = text.lower()
    pos = sum(1 for w in POSITIVE_WORDS if w in lower)
    neg = sum(1 for w in NEGATIVE_WORDS if w in lower)
    if neg > pos:
        return "Negative"
    if pos > neg:
        return "Positive"
    return "Neutral"


# ---------------------------------------------------------------------------
# Severity classification
# Combines priority field with sentiment and theme signals.
# ---------------------------------------------------------------------------

HIGH_IMPACT_THEMES = {
    "Compliance Reporting", "Payment Reconciliation", "Lending Workflow", "Follow-up / Ownership"
}


def classify_severity(priority: str, sentiment: str, theme: str, status: str) -> str:
    if priority == "Critical":
        return "High"
    if status.lower() in ("escalated", "overdue"):
        return "High"
    if priority == "High" and sentiment == "Negative":
        return "High"
    if priority == "High":
        return "Medium"
    if sentiment == "Negative" and theme in HIGH_IMPACT_THEMES:
        return "Medium"
    if priority == "Low" or sentiment == "Positive":
        return "Low"
    return "Medium"


# ---------------------------------------------------------------------------
# Business impact classification
# ---------------------------------------------------------------------------

HIGH_IMPACT_THEMES_BIZ = {
    "Compliance Reporting", "Payment Reconciliation", "Lending Workflow"
}
MEDIUM_IMPACT_THEMES_BIZ = {
    "Reporting", "Data Export", "Dashboard Limitations", "Manual Workaround",
    "Follow-up / Ownership", "Admin Permissions", "Data Access"
}


def classify_business_impact(theme: str, severity: str) -> str:
    if theme in HIGH_IMPACT_THEMES_BIZ:
        return "High"
    if severity == "High" and theme not in ("Feature Request", "General Support"):
        return "High"
    if theme in MEDIUM_IMPACT_THEMES_BIZ:
        return "Medium"
    return "Low"


# ---------------------------------------------------------------------------
# Renewal risk signal
# ---------------------------------------------------------------------------

def classify_renewal_risk(severity: str, business_impact: str, sentiment: str,
                           is_open: bool, is_repeated: bool) -> str:
    score = 0
    if severity == "High":
        score += 3
    elif severity == "Medium":
        score += 1
    if business_impact == "High":
        score += 3
    elif business_impact == "Medium":
        score += 1
    if sentiment == "Negative":
        score += 2
    if is_open:
        score += 1
    if is_repeated:
        score += 2

    if score >= 6:
        return "High"
    if score >= 3:
        return "Medium"
    return "Low"


# ---------------------------------------------------------------------------
# Recommended action
# ---------------------------------------------------------------------------

ACTION_MAP = {
    ("Compliance Reporting", "High"):   "Escalate compliance gap to product and confirm resolution timeline with client",
    ("Compliance Reporting", "Medium"): "Review compliance reporting capabilities before renewal call",
    ("Payment Reconciliation", "High"): "Conduct post-mortem on payment discrepancy and share fix timeline",
    ("Payment Reconciliation", "Medium"): "Confirm payment reconciliation status with account team",
    ("Lending Workflow", "High"):       "Escalate repeated lending workflow issue to product as renewal blocker",
    ("Lending Workflow", "Medium"):     "Review lending workflow open items before next QBR",
    ("Manual Workaround", "High"):      "Prioritize automation fix — client is absorbing ongoing operational cost",
    ("Manual Workaround", "Medium"):    "Prepare roadmap response on manual workaround reduction",
    ("Follow-up / Ownership", "High"):  "Assign owner immediately and send client acknowledgment within 24 hours",
    ("Follow-up / Ownership", "Medium"): "Confirm owner and next step with client before renewal",
    ("Feature Request", "High"):        "Validate feature commitment status and include in renewal talking points",
    ("Feature Request", "Medium"):      "Log feature request and acknowledge with client timeline",
    ("Admin Permissions", "High"):      "Review admin permission issues and provide resolution path",
    ("Admin Permissions", "Medium"):    "Confirm permission workflow improvements with IT contact",
    ("Data Export", "High"):            "Prepare response on export limitations and any upcoming increases",
    ("Data Export", "Medium"):          "Include data export roadmap in renewal readiness materials",
    ("Dashboard Limitations", "High"):  "Escalate repeated dashboard issue and confirm fix timeline",
    ("Dashboard Limitations", "Medium"): "Add dashboard reliability to renewal prep discussion",
    ("Reporting", "High"):              "Review unresolved reporting requests before renewal call",
    ("Reporting", "Medium"):            "Prepare reporting capability summary for renewal readiness brief",
    ("Data Access", "High"):            "Investigate integration feed reliability and escalate if systemic",
    ("Data Access", "Medium"):          "Review API/integration documentation and update client",
    ("General Support", "High"):        "Triage high-severity support item and confirm resolution path",
    ("General Support", "Medium"):      "Include in account review and assign owner",
}

DEFAULT_ACTION = "Review record and include in account health summary"


def get_recommended_action(theme: str, severity: str, is_open: bool) -> str:
    action = ACTION_MAP.get((theme, severity))
    if action:
        return action
    # Low severity — only flag if open
    if is_open:
        return "Confirm owner and next step with client"
    return DEFAULT_ACTION


# ---------------------------------------------------------------------------
# Summary (truncated raw_text for now; LLM layer can replace this later)
# ---------------------------------------------------------------------------

def make_summary(text: str, max_len: int = 130) -> str:
    text = text.strip()
    if len(text) <= max_len:
        return text
    truncated = text[:max_len].rsplit(" ", 1)[0]
    return truncated + "…"


# ---------------------------------------------------------------------------
# Repeated issue detection
# ---------------------------------------------------------------------------

def flag_repeated_issues(df: pd.DataFrame, threshold: int = REPEATED_ISSUE_THRESHOLD) -> pd.Series:
    counts = df.groupby(["client_name", "theme", "product_area"])["record_id"].transform("count")
    return counts >= threshold


# ---------------------------------------------------------------------------
# Open/unresolved detection
# ---------------------------------------------------------------------------

def flag_open(status_series: pd.Series) -> pd.Series:
    return status_series.str.lower().isin(OPEN_STATUSES)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_classification() -> pd.DataFrame:
    df = pd.read_csv(INPUT_PATH, parse_dates=["date"])

    print(f"Loaded {len(df)} records from {INPUT_PATH}")

    df["theme"] = df.apply(
        lambda r: classify_theme(r["raw_text"], r["product_area"]), axis=1
    )
    df["sentiment"] = df["raw_text"].apply(classify_sentiment)
    df["is_open_or_unresolved"] = flag_open(df["status"])

    # Repeated issue needs the theme column to already exist
    df["is_repeated_issue"] = flag_repeated_issues(df)

    df["severity"] = df.apply(
        lambda r: classify_severity(r["priority"], r["sentiment"], r["theme"], r["status"]),
        axis=1,
    )
    df["business_impact"] = df.apply(
        lambda r: classify_business_impact(r["theme"], r["severity"]), axis=1
    )
    df["renewal_risk_signal"] = df.apply(
        lambda r: classify_renewal_risk(
            r["severity"], r["business_impact"], r["sentiment"],
            r["is_open_or_unresolved"], r["is_repeated_issue"]
        ),
        axis=1,
    )
    df["recommended_action"] = df.apply(
        lambda r: get_recommended_action(r["theme"], r["severity"], r["is_open_or_unresolved"]),
        axis=1,
    )
    df["summary"] = df["raw_text"].apply(make_summary)

    df.to_csv(OUTPUT_PATH, index=False)
    return df


def main():
    result = run_classification()

    print(f"\nOutput saved to: {OUTPUT_PATH.resolve()}")
    print(f"Shape: {result.shape[0]} rows × {result.shape[1]} columns")
    print(f"Columns: {list(result.columns)}")

    print(f"\nTheme distribution:")
    print(result["theme"].value_counts().to_string())

    print(f"\nSentiment distribution:")
    print(result["sentiment"].value_counts().to_string())

    print(f"\nRenewal risk signal distribution:")
    print(result["renewal_risk_signal"].value_counts().to_string())

    print(f"\nRepeated issues flagged: {result['is_repeated_issue'].sum()}")
    print(f"Open/unresolved flagged:  {result['is_open_or_unresolved'].sum()}")


if __name__ == "__main__":
    main()
