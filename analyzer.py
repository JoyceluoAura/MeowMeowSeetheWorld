"""Data processing utilities for trade analysis."""

from __future__ import annotations

import pandas as pd


def clean_trade_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize raw trade data columns for analysis."""
    cleaned = df.copy()

    cleaned["year"] = pd.to_numeric(cleaned["year"], errors="coerce").fillna(0).astype(int)
    cleaned["trade_value_usd"] = pd.to_numeric(cleaned["trade_value_usd"], errors="coerce").fillna(0.0)
    cleaned["quantity"] = pd.to_numeric(cleaned["quantity"], errors="coerce").fillna(0.0)

    cleaned["flow"] = cleaned["flow"].replace(
        {
            "Imports": "Import",
            "Exports": "Export",
            "Import": "Import",
            "Export": "Export",
        }
    )

    # Remove invalid/empty key fields to avoid charting issues.
    cleaned = cleaned[cleaned["year"] > 0]
    cleaned = cleaned[cleaned["partner"].notna()]

    return cleaned


def aggregate_yearly_values(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate import/export values by year for line chart."""
    grouped = (
        df.groupby(["year", "flow"], as_index=False)["trade_value_usd"]
        .sum()
        .sort_values(["year", "flow"])
    )
    return grouped


def top_partner_countries(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Rank top partner countries by total trade value."""
    ranked = (
        df.groupby("partner", as_index=False)["trade_value_usd"]
        .sum()
        .sort_values("trade_value_usd", ascending=False)
        .head(top_n)
    )
    return ranked
