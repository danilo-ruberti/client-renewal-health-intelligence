# Process Note: Client Renewal Health Intelligence

_Prepared for: Applied AI / AI Product Analyst assessment — Nymbus_
_Date: May 2026_

---

## What This Prototype Does

This prototype demonstrates an end-to-end AI-assisted workflow for surfacing client renewal risk from unstructured communications. It ingests four types of client communication data, classifies and scores each record for renewal risk, visualizes the results in an interactive dashboard, and generates a senior-ready renewal readiness brief using the Claude API.

The fictional client — Harborview Community Bank — has been on the platform for three years and is up for renewal in four months. All data is synthetic but designed to reflect realistic patterns in a community banking context.

---

## Pipeline Architecture

The pipeline is intentionally linear and additive. Each step reads the previous step's output, so no stage has side effects on upstream data.

```
generate_sample_data.py  →  data/raw/*.csv
ingest.py                →  data/processed/client_health_records.csv
classify.py              →  data/processed/client_health_classified.csv
scoring.py               →  data/processed/client_health_scored.csv
                             outputs/client_health_summary.csv
brief_generator.py       →  outputs/renewal_readiness_brief.md
app.py                   →  Streamlit dashboard (reads scored CSV + summary)
```

This separation keeps each module independently testable and makes it straightforward to swap out the rule-based classifier for an LLM-based one without touching the ingestion or scoring logic.

---

## Key Design Decisions

**Rule-based classification over LLM for scoring.**
The classifier (`classify.py`) and scorer (`scoring.py`) are entirely rule-based — keyword matching, conditional logic, and weighted arithmetic. This was a deliberate choice. Rule-based logic is transparent, auditable, and runs without an API key or internet connection. In a production CSM tool, account teams need to trust and explain the outputs; a black-box classifier creates friction. The scoring weights are explicitly declared as named constants, making them easy to tune as the model is validated against real outcomes.

**LLM used only for narrative generation.**
Claude is invoked in exactly one place — `brief_generator.py` — to turn structured, pre-computed metrics into polished prose. The input to Claude is a compact JSON package, not the raw dataset. This keeps token usage low, makes the prompt auditable, and means the brief's factual claims are grounded in deterministic outputs rather than LLM inference. The briefing package is also written to `outputs/briefing_package.json` so reviewers can inspect exactly what was sent.

**Graceful fallback.**
If the API key is missing or the Claude call fails, `brief_generator.py` falls back to a rule-based markdown brief. The project runs end-to-end without any external dependency.

**Prompt caching.**
The Claude system prompt uses `cache_control: ephemeral` to take advantage of Anthropic's prompt caching. On repeated runs, this reduces latency and cost for the static portion of the prompt.

**Schema normalization before analysis.**
The four raw sources have meaningfully different column structures — support tickets have `ticket_status` and `days_open`; product feedback has `sentiment` and `reviewed`. The ingestion layer normalizes everything into an 11-column unified schema before any analysis runs. This mirrors what a production data pipeline would need to do when pulling from a CRM, ticketing system, and email archive simultaneously.

---

## What Would Change in Production

The main gaps between this prototype and a production system are data sourcing, model validation, and access controls.

**Data sourcing.** In production, the ingestion layer would pull from live systems — Salesforce or Gainsight for account data, Zendesk or Jira for support tickets, and an email API for account communications. The normalization logic in `ingest.py` would need to handle schema drift, deduplication, and incremental updates rather than a one-time batch load.

**Classifier validation.** The rule-based classifier was designed for interpretability, not precision. Themes like "Compliance Reporting" and "Payment Reconciliation" are assigned based on keyword presence, which will produce false positives on records that mention those terms in passing. In production, this would need validation against CSM-labeled ground truth and likely a hybrid approach — rules for high-confidence cases, an LLM layer for ambiguous ones.

**Scoring calibration.** The 0–100 risk score is additive and uses fixed weights. The weights reflect reasonable intuitions (unresolved high-severity issues in QBR channels matter more than low-priority feedback form submissions), but they have not been validated against actual churn outcomes. A production model would use historical renewal data to calibrate weights or train a lightweight classifier.

**Brief quality.** Claude produces a coherent brief from the structured input, but the output has not been reviewed against actual CSM judgment. In production, the brief would serve as a first draft — a starting point for the account team, not a finished document.

---

## Tools Used

| Layer | Tool |
|---|---|
| Data generation & pipeline | Python, pandas, numpy, pathlib |
| Classification & scoring | Rule-based Python (no external dependencies) |
| Brief generation | Anthropic Claude API (`claude-sonnet-4-5-20250929`) |
| Dashboard | Streamlit, Plotly |
| Environment management | python-dotenv |
