"""
utils.py
Shared helper functions for the ingestion pipeline.
"""

import re

import pandas as pd


def clean_text(series: pd.Series) -> pd.Series:
    """Strip leading/trailing whitespace and normalize internal whitespace."""
    return (
        series.astype(str)
        .str.strip()
        .str.replace(r"[\r\n]+", " ", regex=True)
        .str.replace(r" {2,}", " ", regex=True)
    )


def to_datetime_safe(series: pd.Series, fmt: str = "%Y-%m-%d") -> pd.Series:
    """Parse a date column to datetime, coercing unparseable values to NaT."""
    return pd.to_datetime(series, format=fmt, errors="coerce")
