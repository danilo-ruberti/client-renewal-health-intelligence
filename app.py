"""
app.py — Client Renewal Health Dashboard
Streamlit dashboard for visualizing renewal risk signals for Harborview Community Bank.
Run with: streamlit run app.py
"""

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Client Renewal Health Dashboard",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

SCORED_PATH = Path("data/processed/client_health_scored.csv")
SUMMARY_PATH = Path("outputs/client_health_summary.csv")

RISK_COLOR_MAP = {"Low": "#2ecc71", "Medium": "#f39c12", "High": "#e74c3c"}
SENTIMENT_COLOR_MAP = {"Positive": "#2ecc71", "Neutral": "#95a5a6", "Negative": "#e74c3c"}
SOURCE_LABELS = {
    "support_ticket": "Support Ticket",
    "account_email": "Account Email",
    "qbr_notes": "QBR Notes",
    "product_feedback": "Product Feedback",
}


@st.cache_data
def load_data():
    df = pd.read_csv(SCORED_PATH, parse_dates=["date"])
    df["source_label"] = df["source"].map(SOURCE_LABELS).fillna(df["source"])
    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()
    summary = pd.read_csv(SUMMARY_PATH)
    return df, summary


df, summary = load_data()

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

st.sidebar.header("Filters")

sources = ["All"] + sorted(df["source_label"].unique().tolist())
sel_source = st.sidebar.selectbox("Source", sources)

product_areas = ["All"] + sorted(df["product_area"].unique().tolist())
sel_product_area = st.sidebar.selectbox("Product Area", product_areas)

themes = ["All"] + sorted(df["theme"].unique().tolist())
sel_theme = st.sidebar.selectbox("Theme", themes)

risk_levels = ["All"] + ["High", "Medium", "Low"]
sel_risk = st.sidebar.selectbox("Risk Level", risk_levels)

sentiments = ["All"] + ["Negative", "Neutral", "Positive"]
sel_sentiment = st.sidebar.selectbox("Sentiment", sentiments)

sel_open_only = st.sidebar.checkbox("Open / Unresolved only", value=False)

# Apply filters
filt = df.copy()
if sel_source != "All":
    filt = filt[filt["source_label"] == sel_source]
if sel_product_area != "All":
    filt = filt[filt["product_area"] == sel_product_area]
if sel_theme != "All":
    filt = filt[filt["theme"] == sel_theme]
if sel_risk != "All":
    filt = filt[filt["risk_level"] == sel_risk]
if sel_sentiment != "All":
    filt = filt[filt["sentiment"] == sel_sentiment]
if sel_open_only:
    filt = filt[filt["is_open_or_unresolved"]]

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("Client Renewal Health Dashboard")
st.caption(
    "Prototype for analyzing renewal risk from support tickets, account emails, "
    "QBR notes, and product feedback."
)
st.markdown(
    "**Client:** Harborview Community Bank &nbsp;|&nbsp; "
    "**Tenure:** 3 years &nbsp;|&nbsp; "
    "**Renewal in:** ~4 months"
)
st.divider()

# ---------------------------------------------------------------------------
# Section 1 — Executive Summary
# ---------------------------------------------------------------------------

st.header("Executive Summary")

top_risk_theme = (
    filt.groupby("theme")["risk_score"].mean().idxmax()
    if not filt.empty else "—"
)

c1, c2, c3 = st.columns(3)
c1.metric("Communications Analyzed", len(filt))
c2.metric("Avg Risk Score", f"{filt['risk_score'].mean():.1f}" if not filt.empty else "—")
c3.metric("High-Risk Records", int((filt["risk_level"] == "High").sum()))

c4, c5, c6 = st.columns(3)
c4.metric("Open / Unresolved", int(filt["is_open_or_unresolved"].sum()))
c5.metric("Negative Sentiment", int((filt["sentiment"] == "Negative").sum()))
c6.metric("Top Risk Theme", top_risk_theme)

st.divider()

# ---------------------------------------------------------------------------
# Section 2 — Renewal Risk Overview
# ---------------------------------------------------------------------------

st.header("Renewal Risk Overview")

col_a, col_b, col_c = st.columns(3)

with col_a:
    st.subheader("Risk Level Distribution")
    risk_counts = (
        filt["risk_level"]
        .value_counts()
        .reindex(["High", "Medium", "Low"])
        .reset_index()
    )
    risk_counts.columns = ["Risk Level", "Count"]
    fig_risk = px.bar(
        risk_counts,
        x="Risk Level",
        y="Count",
        color="Risk Level",
        color_discrete_map=RISK_COLOR_MAP,
        text="Count",
    )
    fig_risk.update_layout(showlegend=False, margin=dict(t=10, b=10))
    st.plotly_chart(fig_risk, use_container_width=True)

with col_b:
    st.subheader("Avg Risk Score by Source")
    by_source = (
        filt.groupby("source_label")["risk_score"]
        .mean()
        .round(1)
        .reset_index()
        .sort_values("risk_score", ascending=True)
    )
    by_source.columns = ["Source", "Avg Risk Score"]
    fig_src = px.bar(
        by_source,
        x="Avg Risk Score",
        y="Source",
        orientation="h",
        color="Avg Risk Score",
        color_continuous_scale=["#2ecc71", "#f39c12", "#e74c3c"],
        range_color=[0, 100],
        text="Avg Risk Score",
    )
    fig_src.update_layout(showlegend=False, margin=dict(t=10, b=10), coloraxis_showscale=False)
    st.plotly_chart(fig_src, use_container_width=True)

with col_c:
    st.subheader("Avg Risk Score by Product Area")
    by_area = (
        filt.groupby("product_area")["risk_score"]
        .mean()
        .round(1)
        .reset_index()
        .sort_values("risk_score", ascending=True)
    )
    by_area.columns = ["Product Area", "Avg Risk Score"]
    fig_area = px.bar(
        by_area,
        x="Avg Risk Score",
        y="Product Area",
        orientation="h",
        color="Avg Risk Score",
        color_continuous_scale=["#2ecc71", "#f39c12", "#e74c3c"],
        range_color=[0, 100],
        text="Avg Risk Score",
    )
    fig_area.update_layout(showlegend=False, margin=dict(t=10, b=10), coloraxis_showscale=False)
    st.plotly_chart(fig_area, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Section 3 — Trends Over Time
# ---------------------------------------------------------------------------

st.header("Trends Over Time")

if filt.empty:
    st.info("No records match current filters.")
else:
    monthly = (
        filt.groupby("month")
        .agg(
            volume=("record_id", "count"),
            avg_risk=("risk_score", "mean"),
            neg_sentiment=("sentiment", lambda x: (x == "Negative").sum()),
        )
        .reset_index()
    )
    monthly["avg_risk"] = monthly["avg_risk"].round(1)

    col_t1, col_t2, col_t3 = st.columns(3)

    with col_t1:
        st.subheader("Monthly Communication Volume")
        fig_vol = px.line(
            monthly, x="month", y="volume", markers=True,
            labels={"month": "Month", "volume": "Record Count"},
        )
        fig_vol.update_traces(line_color="#3498db")
        fig_vol.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig_vol, use_container_width=True)

    with col_t2:
        st.subheader("Avg Monthly Risk Score")
        fig_risk_trend = px.line(
            monthly, x="month", y="avg_risk", markers=True,
            labels={"month": "Month", "avg_risk": "Avg Risk Score"},
        )
        fig_risk_trend.update_traces(line_color="#e74c3c")
        fig_risk_trend.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig_risk_trend, use_container_width=True)

    with col_t3:
        st.subheader("Monthly Negative Sentiment Count")
        fig_neg = px.bar(
            monthly, x="month", y="neg_sentiment",
            labels={"month": "Month", "neg_sentiment": "Negative Records"},
            color_discrete_sequence=["#e74c3c"],
        )
        fig_neg.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig_neg, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Section 4 — Theme and Product Area Analysis
# ---------------------------------------------------------------------------

st.header("Theme and Product Area Analysis")

col_th1, col_th2 = st.columns(2)

with col_th1:
    st.subheader("Top Themes by Record Count")
    theme_vol = (
        filt["theme"].value_counts().reset_index()
    )
    theme_vol.columns = ["Theme", "Count"]
    fig_tv = px.bar(
        theme_vol.sort_values("Count"),
        x="Count",
        y="Theme",
        orientation="h",
        text="Count",
        color_discrete_sequence=["#3498db"],
    )
    fig_tv.update_layout(showlegend=False, margin=dict(t=10, b=10))
    st.plotly_chart(fig_tv, use_container_width=True)

with col_th2:
    st.subheader("Top Themes by Avg Risk Score")
    theme_risk = (
        filt.groupby("theme")["risk_score"]
        .mean()
        .round(1)
        .reset_index()
        .sort_values("risk_score")
    )
    theme_risk.columns = ["Theme", "Avg Risk Score"]
    fig_tr = px.bar(
        theme_risk,
        x="Avg Risk Score",
        y="Theme",
        orientation="h",
        text="Avg Risk Score",
        color="Avg Risk Score",
        color_continuous_scale=["#2ecc71", "#f39c12", "#e74c3c"],
        range_color=[0, 100],
    )
    fig_tr.update_layout(showlegend=False, margin=dict(t=10, b=10), coloraxis_showscale=False)
    st.plotly_chart(fig_tr, use_container_width=True)

with st.expander("Theme × Product Area Summary Table"):
    st.dataframe(
        summary.sort_values("avg_risk_score", ascending=False),
        use_container_width=True,
        hide_index=True,
    )

st.divider()

# ---------------------------------------------------------------------------
# Section 5 — High-Risk Evidence Table
# ---------------------------------------------------------------------------

st.header("High-Risk Evidence Table")

EVIDENCE_COLS = [
    "date", "source_label", "contact_role", "product_area", "theme",
    "sentiment", "severity", "business_impact", "status",
    "risk_score", "risk_level", "summary", "recommended_action",
]

evidence = filt[EVIDENCE_COLS].copy().rename(columns={"source_label": "source"})
evidence["date"] = evidence["date"].dt.strftime("%Y-%m-%d")
evidence = evidence.sort_values("risk_score", ascending=False)

st.caption(f"Showing {len(evidence)} records matching current filters.")

# Color-code risk_score column via a styled display
def highlight_risk(val):
    if val == "High":
        return "background-color: #fdecea; color: #c0392b; font-weight: bold"
    if val == "Medium":
        return "background-color: #fef9e7; color: #d35400"
    return "background-color: #eafaf1; color: #1e8449"

styled = evidence.style.map(highlight_risk, subset=["risk_level"])
st.dataframe(styled, use_container_width=True, hide_index=True, height=400)

st.divider()

# ---------------------------------------------------------------------------
# Section 6 — Account Team Talking Points
# ---------------------------------------------------------------------------

st.header("Account Team Talking Points")
st.caption("Auto-generated from highest-risk records and recurring patterns. Rule-based — no LLM.")

def generate_talking_points(data: pd.DataFrame) -> list[str]:
    points = []

    high = data[data["risk_level"] == "High"]
    open_records = data[data["is_open_or_unresolved"]]
    repeated = data[data["is_repeated_issue"]]

    # Unresolved high-severity issues
    open_high = open_records[open_records["severity"] == "High"]
    if not open_high.empty:
        areas = open_high["product_area"].value_counts().head(2).index.tolist()
        points.append(
            f"Address {len(open_high)} open high-severity issues — "
            f"particularly in {' and '.join(areas)} — before the renewal call."
        )

    # Repeated issues
    if not repeated.empty:
        rep_themes = repeated["theme"].value_counts().head(2).index.tolist()
        points.append(
            f"Acknowledge recurring problems in {' and '.join(rep_themes)}. "
            "The client has raised these multiple times — a clear resolution timeline is needed."
        )

    # Compliance / regulatory exposure
    compliance = data[data["theme"] == "Compliance Reporting"]
    if not compliance.empty and (compliance["risk_level"] == "High").any():
        points.append(
            "Confirm compliance reporting capabilities meet regulatory requirements. "
            "This is a non-negotiable for a community bank and a potential renewal blocker."
        )

    # Payment / reconciliation
    payment = data[data["theme"] == "Payment Reconciliation"]
    if not payment.empty and (payment["risk_level"] == "High").any():
        points.append(
            "Prepare a clear post-mortem and fix timeline for payment reconciliation issues. "
            "Month-end close friction has direct operational cost to the client."
        )

    # Manual workarounds
    manual = data[data["theme"] == "Manual Workaround"]
    if not manual.empty:
        points.append(
            "Quantify the manual workaround burden the client is absorbing. "
            "Use this in the renewal conversation to show platform improvement ROI."
        )

    # Follow-up / ownership gaps
    followup = data[data["theme"] == "Follow-up / Ownership"]
    if not followup.empty:
        points.append(
            "Audit open commitments and unanswered follow-ups before the renewal meeting. "
            "Missed follow-through is a leading indicator of churn risk."
        )

    # Negative sentiment from strategic channels (QBR / email)
    strategic_neg = data[
        (data["source"].isin(["qbr_notes", "account_email"])) &
        (data["sentiment"] == "Negative")
    ]
    if not strategic_neg.empty:
        points.append(
            f"{len(strategic_neg)} negative signals surfaced directly through QBR meetings or "
            "account emails — the client has communicated dissatisfaction through formal channels."
        )

    # Positive signals to acknowledge
    positive = data[data["sentiment"] == "Positive"]
    if not positive.empty:
        pos_areas = positive["product_area"].value_counts().head(1).index.tolist()
        if pos_areas:
            points.append(
                f"Highlight recent wins in {pos_areas[0]} — "
                "the client has acknowledged improvements there. Use this as a renewal anchor."
            )

    if not points:
        points.append("No significant risk patterns detected under current filters.")

    return points

talking_points = generate_talking_points(filt)
for i, point in enumerate(talking_points, 1):
    st.markdown(f"**{i}.** {point}")

st.divider()

# ---------------------------------------------------------------------------
# Section 7 — Recommended Account Actions
# ---------------------------------------------------------------------------

st.header("Recommended Account Actions")
st.caption("Derived from recommended_action field across highest-risk records.")

top_actions = (
    filt[filt["risk_level"] == "High"]["recommended_action"]
    .value_counts()
    .head(8)
    .index.tolist()
)

# Fall back to all records if no high-risk match current filters
if not top_actions:
    top_actions = (
        filt["recommended_action"]
        .value_counts()
        .head(8)
        .index.tolist()
    )

if top_actions:
    for action in top_actions:
        st.markdown(f"- {action}")
else:
    st.info("No recommended actions for current filter selection.")

st.divider()
st.caption(
    "Client Renewal Health Intelligence · Prototype · "
    "Data is synthetic and generated for demonstration purposes only."
)
