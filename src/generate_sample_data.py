"""
generate_sample_data.py
Generates realistic sample CSV files for Harborview Community Bank across four
communication channels: support tickets, account emails, QBR notes, and product feedback.
Run with: python src/generate_sample_data.py
"""

import random
from pathlib import Path

import numpy as np
import pandas as pd

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

CLIENT = "Harborview Community Bank"

# Contacts at the bank
CONTACTS = [
    ("Lisa Nguyen", "VP of Operations"),
    ("Mark Delgado", "IT Director"),
    ("Patricia Owens", "Compliance Officer"),
    ("James Hartley", "CFO"),
    ("Sandra Chen", "Branch Manager"),
    ("Kevin Torres", "Loan Operations Manager"),
    ("Debra Mills", "Core Banking Admin"),
]

PRODUCT_AREAS = [
    "Reporting & Analytics",
    "Data Exports",
    "Dashboard",
    "Admin & Permissions",
    "Payment Reconciliation",
    "Lending Workflow",
    "Compliance Reporting",
    "Core Banking",
    "User Management",
    "Integrations",
]

OWNERS = ["Alex Rivera", "Jordan Kim", "Taylor Brooks", "Morgan Lee", "Casey Patel"]

# Date range: roughly 3 years of activity, renewal in 4 months from today
DATE_START = pd.Timestamp("2023-01-01")
DATE_END = pd.Timestamp("2026-05-01")


def random_dates(n: int) -> list[pd.Timestamp]:
    span = (DATE_END - DATE_START).days
    offsets = np.random.randint(0, span, size=n)
    return [DATE_START + pd.Timedelta(days=int(d)) for d in offsets]


def random_contact() -> tuple[str, str]:
    return random.choice(CONTACTS)


# ---------------------------------------------------------------------------
# Support tickets
# ---------------------------------------------------------------------------

TICKET_TEMPLATES = [
    "User unable to run {report} report — system times out after 30 seconds. This is blocking month-end close.",
    "Data export for {area} is returning incomplete rows. Missing records from {period}.",
    "Dashboard not refreshing correctly for {area}. Stale data shown despite cache clear.",
    "Admin permission change for new user {user} not propagating to sub-modules.",
    "Payment reconciliation discrepancy flagged by auditor — {amount} off on {period} batch.",
    "Lending workflow stuck at approval stage for loan ID {loan_id}. Blocking disbursement.",
    "Compliance report for {regulation} cannot be generated — field mapping broken.",
    "Manual workaround needed: {area} export requires re-keying into spreadsheet every week.",
    "Delayed follow-up on ticket #{ref} — no response in 10 business days.",
    "Feature request: add {feature} to {area} module to reduce manual steps.",
    "ACH file generation fails intermittently — no error shown to end user.",
    "User locked out of {area} after permission update pushed by IT.",
    "Interest calculation rounding error in loan amortization schedule.",
    "Core banking sync with {area} delayed by up to 4 hours during peak load.",
    "Regulatory exam prep — need historical {report} report for past 24 months.",
    "Export limit of 10,000 rows too restrictive for full GL reconciliation.",
    "Scheduled report not emailing to distribution list since last update.",
    "Two-factor authentication failing for remote branch staff.",
    "Vendor integration feed for {area} dropping records silently.",
    "Audit trail missing for permission changes in admin console.",
]

REPORTS = ["GL reconciliation", "call report", "HMDA", "BSA/AML", "loan pipeline", "ACH batch"]
PERIODS = ["March 2025", "Q4 2024", "January 2026", "Q1 2025", "February 2026"]
REGULATIONS = ["Reg E", "BSA", "HMDA", "CRA", "Reg CC"]
FEATURES = ["bulk export", "date range filter", "role-based access", "scheduled snapshots", "drill-down"]


def _ticket_text() -> str:
    template = random.choice(TICKET_TEMPLATES)
    return template.format(
        report=random.choice(REPORTS),
        area=random.choice(PRODUCT_AREAS),
        period=random.choice(PERIODS),
        user="user" + str(random.randint(100, 999)),
        amount=f"${random.randint(500, 50000):,}",
        loan_id="LN" + str(random.randint(10000, 99999)),
        regulation=random.choice(REGULATIONS),
        ref=str(random.randint(1000, 9999)),
        feature=random.choice(FEATURES),
    )


def generate_support_tickets(n: int = 35) -> pd.DataFrame:
    dates = random_dates(n)
    records = []
    for i, date in enumerate(dates):
        contact, role = random_contact()
        records.append(
            {
                "ticket_id": f"TKT-{1000 + i}",
                "created_date": date.strftime("%Y-%m-%d"),
                "client_name": CLIENT,
                "contact_name": contact,
                "contact_role": role,
                "product_area": random.choice(PRODUCT_AREAS),
                "issue_description": _ticket_text(),
                "ticket_status": random.choices(
                    ["Open", "In Progress", "Resolved", "Escalated"],
                    weights=[20, 30, 40, 10],
                )[0],
                "priority": random.choices(
                    ["Critical", "High", "Medium", "Low"],
                    weights=[10, 25, 45, 20],
                )[0],
                "assigned_to": random.choice(OWNERS),
                "days_open": random.randint(1, 120),
            }
        )
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Account emails
# ---------------------------------------------------------------------------

EMAIL_TEMPLATES = [
    "Hi {owner}, just following up on the {area} issue we discussed last week — still no resolution and my team is using manual workarounds daily.",
    "We need to revisit the {feature} commitment from our last QBR. {period} is approaching and we haven't seen any movement.",
    "The compliance team is asking about {regulation} reporting capabilities. Can you confirm the timeline for the fix?",
    "Quick note: we've had three separate users report problems with {area} this month alone.",
    "I wanted to flag that we are tracking unresolved items from our {period} review. We need a status update before our board meeting.",
    "Our auditors have flagged the missing audit trail in {area}. This is now a regulatory risk for us.",
    "Can you schedule a call this week? We want to discuss our renewal options and have some concerns about platform direction.",
    "We appreciate the responsiveness on {area} — wanted to acknowledge that the last update improved things for our lending team.",
    "We're evaluating alternatives ahead of our renewal. Happy to discuss what would make us confident in renewing.",
    "Data export limitations are causing significant friction. My team spends 3 hours every Monday on manual reconciliation.",
    "The {area} dashboard hasn't been reflecting live data — we've escalated twice and the issue persists.",
    "I'm looping in Patricia from Compliance — she has specific requirements for the {regulation} report.",
    "Our branch managers are frustrated with the permission workflow. Every admin change requires an IT ticket.",
    "We want to put together a list of our top 5 pain points for the next QBR. Can we get time on the agenda?",
    "The ACH file failures from last quarter cost us real operational time. We need a post-mortem and a fix timeline.",
]


def _email_text() -> str:
    template = random.choice(EMAIL_TEMPLATES)
    return template.format(
        owner=random.choice(OWNERS).split()[0],
        area=random.choice(PRODUCT_AREAS),
        feature=random.choice(FEATURES),
        period=random.choice(PERIODS),
        regulation=random.choice(REGULATIONS),
    )


def generate_account_emails(n: int = 30) -> pd.DataFrame:
    dates = random_dates(n)
    records = []
    for i, date in enumerate(dates):
        contact, role = random_contact()
        records.append(
            {
                "email_id": f"EM-{2000 + i}",
                "date_sent": date.strftime("%Y-%m-%d"),
                "from_name": contact,
                "from_role": role,
                "client": CLIENT,
                "subject_product_area": random.choice(PRODUCT_AREAS),
                "email_body": _email_text(),
                "thread_status": random.choices(
                    ["Awaiting Response", "Resolved", "Escalated", "Closed"],
                    weights=[35, 30, 15, 20],
                )[0],
                "urgency": random.choices(
                    ["High", "Medium", "Low"],
                    weights=[30, 50, 20],
                )[0],
                "account_owner": random.choice(OWNERS),
            }
        )
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# QBR notes
# ---------------------------------------------------------------------------

QBR_THEMES = [
    "Client raised unresolved {area} issues from prior quarter. Action item assigned to {owner}.",
    "Discussed {feature} roadmap. Client expects delivery by {period}. Noted as committed.",
    "{regulation} compliance gap identified — engineering team to provide timeline within 2 weeks.",
    "Client satisfaction score self-reported as {score}/10. Main concern: {area} reliability.",
    "Lending team reported {area} workflow improvements have reduced approval time by 20%.",
    "Outstanding commitment: {feature} for {area} module — originally promised in Q3 2024, still open.",
    "Client asked about platform roadmap and whether {feature} is on the 12-month plan.",
    "Migration from legacy {area} system discussed — client wants phased approach.",
    "Client expressed intent to expand usage to 2 additional branches if {area} issues are resolved.",
    "Escalation review: 3 critical tickets unresolved for more than 60 days — VP flagged to executive sponsor.",
    "Renewal discussion initiated. Client wants ROI summary and SLA performance data before committing.",
    "Client satisfied with recent {area} improvements but concerned about support response times.",
    "Feature gap: no bulk {feature} capability — client using spreadsheet workaround.",
    "Data integrity concern raised: {area} export results inconsistent across runs.",
    "Client requested dedicated CSM contact for compliance-related questions.",
]


def _qbr_text() -> str:
    template = random.choice(QBR_THEMES)
    return template.format(
        area=random.choice(PRODUCT_AREAS),
        owner=random.choice(OWNERS).split()[0],
        feature=random.choice(FEATURES),
        period=random.choice(PERIODS),
        regulation=random.choice(REGULATIONS),
        score=random.randint(5, 9),
    )


# QBRs happen roughly quarterly — generate ~4 per year over 3 years = ~12 meetings
QBR_DATES = [
    "2023-03-15", "2023-06-20", "2023-09-18", "2023-12-12",
    "2024-03-19", "2024-06-17", "2024-09-16", "2024-12-10",
    "2025-03-18", "2025-06-16", "2025-09-15", "2025-12-09",
    "2026-03-17",
]


def generate_qbr_notes(notes_per_meeting: int = 2) -> pd.DataFrame:
    records = []
    note_id = 3000
    for qbr_date in QBR_DATES:
        attendees = random.sample(CONTACTS, k=min(3, len(CONTACTS)))
        for _ in range(notes_per_meeting):
            contact, role = random.choice(attendees)
            records.append(
                {
                    "note_id": f"QBR-{note_id}",
                    "meeting_date": qbr_date,
                    "client_name": CLIENT,
                    "attendee_name": contact,
                    "attendee_role": role,
                    "topic_area": random.choice(PRODUCT_AREAS),
                    "meeting_notes": _qbr_text(),
                    "follow_up_status": random.choices(
                        ["Open", "Completed", "Overdue", "No Action Required"],
                        weights=[30, 35, 20, 15],
                    )[0],
                    "nymbus_owner": random.choice(OWNERS),
                }
            )
            note_id += 1
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Product feedback
# ---------------------------------------------------------------------------

FEEDBACK_TEMPLATES = [
    "The {area} module is too slow for day-to-day use. Export takes 5+ minutes.",
    "I wish {area} had a {feature} option — it would eliminate our biggest manual step.",
    "Really appreciated the recent update to {area} — the new layout is much cleaner.",
    "The {area} dashboard is missing key metrics our compliance team needs for {regulation}.",
    "Permissions in {area} are confusing. New staff struggle with onboarding.",
    "{area} integration with our core system still feels brittle — we get silent failures weekly.",
    "Would love to see {feature} added — other platforms we've used had this out of the box.",
    "The reporting is good but we can't customize column order, which is frustrating during audits.",
    "Support response for {area} tickets has improved — keep it up.",
    "We submitted this {area} feedback six months ago and haven't seen it addressed.",
    "Mobile access for {area} is essential for our branch managers — this is a recurring ask.",
    "The bulk export limit is a real problem. We need more than 10,000 rows for our GL runs.",
    "Scheduled reports are unreliable — missed two weeks in a row without any notification.",
    "{area} API documentation is outdated — our IT team wasted 3 days on a stale endpoint.",
    "Overall happy with the platform direction but {area} needs more investment.",
]


def _feedback_text() -> str:
    template = random.choice(FEEDBACK_TEMPLATES)
    return template.format(
        area=random.choice(PRODUCT_AREAS),
        feature=random.choice(FEATURES),
        regulation=random.choice(REGULATIONS),
    )


def generate_product_feedback(n: int = 25) -> pd.DataFrame:
    dates = random_dates(n)
    records = []
    for i, date in enumerate(dates):
        contact, role = random_contact()
        records.append(
            {
                "feedback_id": f"FB-{4000 + i}",
                "submitted_date": date.strftime("%Y-%m-%d"),
                "submitter_name": contact,
                "submitter_role": role,
                "organization": CLIENT,
                "module": random.choice(PRODUCT_AREAS),
                "feedback_text": _feedback_text(),
                "sentiment": random.choices(
                    ["Positive", "Neutral", "Negative"],
                    weights=[20, 30, 50],
                )[0],
                "reviewed": random.choice(["Yes", "No"]),
            }
        )
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    tickets = generate_support_tickets(35)
    emails = generate_account_emails(30)
    qbr = generate_qbr_notes(notes_per_meeting=2)
    feedback = generate_product_feedback(25)

    tickets.to_csv(RAW_DIR / "support_tickets.csv", index=False)
    emails.to_csv(RAW_DIR / "account_emails.csv", index=False)
    qbr.to_csv(RAW_DIR / "qbr_notes.csv", index=False)
    feedback.to_csv(RAW_DIR / "product_feedback.csv", index=False)

    total = len(tickets) + len(emails) + len(qbr) + len(feedback)
    print(f"support_tickets.csv   — {len(tickets):>3} rows, cols: {list(tickets.columns)}")
    print(f"account_emails.csv    — {len(emails):>3} rows, cols: {list(emails.columns)}")
    print(f"qbr_notes.csv         — {len(qbr):>3} rows, cols: {list(qbr.columns)}")
    print(f"product_feedback.csv  — {len(feedback):>3} rows, cols: {list(feedback.columns)}")
    print(f"\nTotal records generated: {total}")
    print(f"Files saved to: {RAW_DIR.resolve()}")


if __name__ == "__main__":
    main()
