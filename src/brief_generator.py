"""
brief_generator.py
Generates a senior-ready renewal readiness brief for Harborview Community Bank.
Uses the Claude API to write the narrative from structured scored data.
Falls back to a rule-based brief if the API key is missing or the call fails.

Run with: python src/brief_generator.py
"""

import json
import os
import textwrap
from datetime import datetime
from pathlib import Path

import anthropic
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

SCORED_PATH = Path("data/processed/client_health_scored.csv")
SUMMARY_PATH = Path("outputs/client_health_summary.csv")
BRIEF_PATH = Path("outputs/renewal_readiness_brief.md")
PACKAGE_PATH = Path("outputs/briefing_package.json")

OUTPUTS_DIR = Path("outputs")
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

MODEL = "claude-sonnet-4-5-20250929"
CLIENT_NAME = "Harborview Community Bank"
RENEWAL_MONTHS = 4


# ---------------------------------------------------------------------------
# 1. Load data
# ---------------------------------------------------------------------------

def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_csv(SCORED_PATH, parse_dates=["date"])
    summary = pd.read_csv(SUMMARY_PATH)
    return df, summary


# ---------------------------------------------------------------------------
# 2. Prepare compact briefing package
# ---------------------------------------------------------------------------

def prepare_briefing_package(df: pd.DataFrame, summary: pd.DataFrame) -> dict:
    total = len(df)
    avg_score = round(df["risk_score"].mean(), 1)
    high_risk_n = int((df["risk_level"] == "High").sum())
    open_n = int(df["is_open_or_unresolved"].sum())
    neg_n = int((df["sentiment"] == "Negative").sum())
    repeated_n = int(df["is_repeated_issue"].sum())

    # Derive an overall risk level from the average score
    if avg_score >= 70:
        overall_risk = "High"
    elif avg_score >= 40:
        overall_risk = "Medium"
    else:
        overall_risk = "Low"

    # Top themes by average risk score
    top_themes = (
        df.groupby("theme")["risk_score"]
        .mean()
        .round(1)
        .sort_values(ascending=False)
        .head(5)
        .reset_index()
        .rename(columns={"theme": "theme", "risk_score": "avg_risk_score"})
        .to_dict(orient="records")
    )

    # Top product areas by average risk score
    top_areas = (
        df.groupby("product_area")["risk_score"]
        .mean()
        .round(1)
        .sort_values(ascending=False)
        .head(5)
        .reset_index()
        .rename(columns={"product_area": "product_area", "risk_score": "avg_risk_score"})
        .to_dict(orient="records")
    )

    # Top 5 highest-risk records
    top_records = (
        df.nlargest(5, "risk_score")[
            ["date", "source", "theme", "product_area", "risk_score",
             "risk_level", "summary", "recommended_action"]
        ]
        .assign(date=lambda x: x["date"].dt.strftime("%Y-%m-%d"))
        .to_dict(orient="records")
    )

    # Top recommended actions (de-duplicated, from high-risk records)
    top_actions = (
        df[df["risk_level"] == "High"]["recommended_action"]
        .value_counts()
        .head(5)
        .index.tolist()
    )

    # Repeated unresolved themes
    repeated_themes = (
        df[df["is_repeated_issue"] & df["is_open_or_unresolved"]]
        .groupby("theme")["record_id"]
        .count()
        .sort_values(ascending=False)
        .head(5)
        .reset_index()
        .rename(columns={"record_id": "open_repeated_count"})
        .to_dict(orient="records")
    )

    return {
        "client_name": CLIENT_NAME,
        "renewal_in_months": RENEWAL_MONTHS,
        "generated_on": datetime.today().strftime("%Y-%m-%d"),
        "total_communications": total,
        "avg_risk_score": avg_score,
        "overall_risk_level": overall_risk,
        "high_risk_records": high_risk_n,
        "open_or_unresolved_records": open_n,
        "negative_sentiment_records": neg_n,
        "repeated_issue_records": repeated_n,
        "top_themes_by_risk": top_themes,
        "top_product_areas_by_risk": top_areas,
        "top_5_highest_risk_records": top_records,
        "top_recommended_actions": top_actions,
        "repeated_unresolved_themes": repeated_themes,
    }


# ---------------------------------------------------------------------------
# 3. Build the Claude prompt
# ---------------------------------------------------------------------------

def build_prompt(package: dict) -> str:
    pkg_json = json.dumps(package, indent=2)
    return textwrap.dedent(f"""
        You are a senior customer success strategist preparing an internal renewal readiness brief.

        Below is a structured data package derived from analyzing {package['total_communications']}
        client communications (support tickets, account emails, QBR notes, and product feedback)
        for {package['client_name']}, who is up for renewal in {package['renewal_in_months']} months.

        DATA PACKAGE:
        {pkg_json}

        Write a concise, professional renewal readiness brief in markdown.
        Address it to the account team. Use the following section structure exactly:

        # Renewal Readiness Brief: {package['client_name']}

        ## Executive Summary
        ## Account Health Snapshot
        ## Top Renewal Risks
        ## High-Risk Evidence
        ## Suggested Talking Points
        ## Recommended Next Actions
        ## Limitations

        Style guidelines:
        - Write for a senior account team — assume they are busy and experienced
        - Be concise and direct; avoid filler phrases and generic AI language
        - Tie every claim to the data (cite risk scores, record counts, themes)
        - Do not overstate certainty — this is AI-assisted synthesis from rule-based scoring
        - Aim for 1 to 2 pages total
        - The Limitations section should note this is based on synthetic sample data and
          that in production, CSM judgment and live source-system data are required
        - Format dates as Month YYYY where shown
    """).strip()


# ---------------------------------------------------------------------------
# 4. Call the Claude API
# ---------------------------------------------------------------------------

def call_claude(prompt: str) -> str:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set")

    client = anthropic.Anthropic(api_key=api_key)

    message = client.messages.create(
        model=MODEL,
        max_tokens=2048,
        system=[
            {
                "type": "text",
                "text": (
                    "You are a senior customer success strategist. "
                    "You write clear, evidence-based internal briefs for account teams. "
                    "You are concise, direct, and never use filler language."
                ),
                # Cache the system prompt — it won't change between runs
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": prompt}],
    )

    return message.content[0].text


# ---------------------------------------------------------------------------
# 5. Fallback rule-based brief
# ---------------------------------------------------------------------------

def build_fallback_brief(package: dict) -> str:
    today = package["generated_on"]
    top_theme = package["top_themes_by_risk"][0]["theme"] if package["top_themes_by_risk"] else "N/A"
    top_area = package["top_product_areas_by_risk"][0]["product_area"] if package["top_product_areas_by_risk"] else "N/A"

    top_records_md = ""
    for r in package["top_5_highest_risk_records"]:
        top_records_md += (
            f"- **[{r['risk_score']}]** {r['date']} · {r['source']} · "
            f"{r['theme']} / {r['product_area']}\n"
            f"  _{r['summary']}_\n"
        )

    actions_md = "\n".join(f"- {a}" for a in package["top_recommended_actions"])

    repeated_md = ""
    for t in package["repeated_unresolved_themes"]:
        repeated_md += f"- {t['theme']}: {t['open_repeated_count']} open repeated records\n"
    if not repeated_md:
        repeated_md = "_No repeated unresolved themes detected._"

    risk_level = package["overall_risk_level"]
    top_risks = "\n".join(
        f"{i+1}. **{t['theme']}** — avg risk score {t['avg_risk_score']}"
        for i, t in enumerate(package["top_themes_by_risk"][:4])
    )

    sections = [
        f"# Renewal Readiness Brief: {package['client_name']}",
        f"_Generated: {today} · Rule-based fallback (Claude API unavailable)_",
        "---",
        "## Executive Summary",
        (
            f"Analysis of {package['total_communications']} client communications over the past "
            f"3 years indicates a **{risk_level}** renewal risk for {package['client_name']}, "
            f"with an average risk score of **{package['avg_risk_score']}/100**. "
            f"{package['high_risk_records']} of {package['total_communications']} records are "
            f"classified as high-risk, and {package['open_or_unresolved_records']} items remain "
            f"open or unresolved heading into renewal. The account team should prioritize "
            f"addressing persistent issues before the renewal conversation."
        ),
        "---",
        "## Account Health Snapshot",
        "| Metric | Value |",
        "|---|---|",
        f"| Total communications analyzed | {package['total_communications']} |",
        f"| Average risk score | {package['avg_risk_score']} / 100 |",
        f"| Overall risk level | **{risk_level}** |",
        f"| High-risk records | {package['high_risk_records']} |",
        f"| Open / unresolved records | {package['open_or_unresolved_records']} |",
        f"| Negative sentiment records | {package['negative_sentiment_records']} |",
        f"| Repeated issue records | {package['repeated_issue_records']} |",
        f"| Top risk theme | {top_theme} |",
        f"| Top risk product area | {top_area} |",
        "---",
        "## Top Renewal Risks",
        top_risks,
        "---",
        "## High-Risk Evidence",
        top_records_md,
        "---",
        "## Suggested Talking Points",
        "- Acknowledge outstanding items and provide a written resolution timeline before the renewal call.",
        f"- Address persistent issues in {top_theme} and {top_area} directly — the client has raised these multiple times.",
        "- Prepare a clear post-mortem on any compliance or payment-related issues.",
        "- Come with concrete evidence of platform improvements made since the last QBR.",
        "- If any QBR commitments remain open, own the gap proactively rather than waiting for the client to raise it.",
        "---",
        "## Recommended Next Actions",
        actions_md,
        "---",
        "## Limitations",
        (
            "This brief is generated from synthetic sample data for prototype demonstration purposes. "
            "Classification and scoring are rule-based. In production, this output must be validated "
            "against live source-system data, actual CSM notes, and account team judgment before use."
        ),
    ]
    return "\n\n".join(sections)


# ---------------------------------------------------------------------------
# 6. Write output
# ---------------------------------------------------------------------------

def write_brief(content: str, path: Path) -> None:
    path.write_text(content, encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Loading data...")
    df, summary = load_data()

    print("Preparing briefing package...")
    package = prepare_briefing_package(df, summary)

    # Write the input package so reviewers can inspect what went to the LLM
    PACKAGE_PATH.write_text(json.dumps(package, indent=2), encoding="utf-8")
    print(f"Briefing package saved to: {PACKAGE_PATH}")

    # Try Claude API first; fall back gracefully on any failure
    api_used = False
    try:
        print(f"Calling Claude API ({MODEL})...")
        prompt = build_prompt(package)
        brief_content = call_claude(prompt)
        # Prepend a generation note
        header = (
            f"_Generated: {package['generated_on']} · "
            f"AI-assisted synthesis via Claude ({MODEL}) · "
            f"Input: {package['total_communications']} scored records_\n\n---\n\n"
        )
        brief_content = header + brief_content
        api_used = True
        print("Claude API call successful.")
    except Exception as e:
        print(f"Claude API call failed ({e}). Using rule-based fallback.")
        brief_content = build_fallback_brief(package)

    write_brief(brief_content, BRIEF_PATH)

    print(f"\nBrief saved to: {BRIEF_PATH.resolve()}")
    print(f"API used: {'Yes — Claude ' + MODEL if api_used else 'No — rule-based fallback'}")
    print("\n--- Executive Summary preview ---")

    # Print the executive summary section from the generated brief
    lines = brief_content.splitlines()
    in_exec = False
    preview_lines = []
    for line in lines:
        if "## Executive Summary" in line:
            in_exec = True
            continue
        if in_exec:
            if line.startswith("## ") or line.startswith("---"):
                break
            preview_lines.append(line)
    print("\n".join(preview_lines).strip())


if __name__ == "__main__":
    main()
