"""Streamlit MVP: Equipment Trade Intelligence Tool (Phase 1)."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

from analyzer import aggregate_yearly_values, clean_trade_data, top_partner_countries
from hs_mapper import normalize_equipment_name, suggest_hs_codes
from trade_fetcher import default_year_range, load_trade_data_with_fallback


# -------------------------
# App configuration & title
# -------------------------
st.set_page_config(page_title="Equipment Trade Intelligence Tool", layout="wide")
st.title("🔎 Equipment Trade Intelligence Tool")
st.caption("Phase 1 MVP — Trade insights for industrial and scientific equipment")


# -------------------------
# Sidebar user input section
# -------------------------
def_start, def_end = default_year_range(last_n_years=5)
current_year = datetime.utcnow().year

with st.sidebar:
    st.header("Search Input")

    equipment_name = st.text_input("Equipment Name *", placeholder="e.g., high-temperature kiln")
    brand = st.text_input("Brand (optional)", placeholder="e.g., Nabertherm")
    model = st.text_input("Model (optional)", placeholder="e.g., LHT 08/17")
    country = st.text_input(
        "Country (optional)",
        placeholder="Global by default. Optionally enter country or M49 code",
    )
    year_range = st.slider(
        "Year Range",
        min_value=2010,
        max_value=current_year,
        value=(def_start, def_end),
        step=1,
    )

    run = st.button("Run Analysis", type="primary", use_container_width=True)


# -------------------------
# Main analysis workflow
# -------------------------
if run:
    if not equipment_name.strip():
        st.error("Equipment Name is required. Please enter a value in the sidebar.")
        st.stop()

    start_year, end_year = year_range

    with st.spinner("Running trade intelligence analysis..."):
        # 1) Normalize equipment text.
        normalized_name = normalize_equipment_name(equipment_name)

        # 2) Suggest HS code candidates via lightweight mapper.
        hs_candidates = suggest_hs_codes(equipment_name=equipment_name, brand=brand, model=model)
        hs_codes = [candidate.hs_code for candidate in hs_candidates]

        # 3) Load trade dataset (live API first, mock fallback).
        raw_df, source_message = load_trade_data_with_fallback(
            equipment_name=equipment_name,
            hs_codes=hs_codes,
            country=country,
            start_year=start_year,
            end_year=end_year,
        )
        connection_attempts = raw_df.attrs.get("connection_attempts", [])

        # 4) Clean and aggregate data.
        clean_df = clean_trade_data(raw_df)
        yearly_df = aggregate_yearly_values(clean_df)
        partners_df = top_partner_countries(clean_df, top_n=10)

    # -------------------------
    # Display: Input summary + HS mapping
    # -------------------------
    st.subheader("Input & HS Mapping")
    col_a, col_b = st.columns([1, 2])

    with col_a:
        st.markdown("**Normalized Equipment Name**")
        st.code(normalized_name)

    with col_b:
        st.markdown("**HS Code Candidates**")
        hs_df = pd.DataFrame(
            [
                {
                    "HS Code": c.hs_code,
                    "Description": c.description,
                    "Confidence": c.confidence,
                    "Why Suggested": c.reason,
                }
                for c in hs_candidates
            ]
        )
        st.dataframe(hs_df, use_container_width=True, hide_index=True)

    st.info(source_message)

    with st.expander("Connection diagnostics"):
        for line in connection_attempts:
            st.write(line)

    # -------------------------
    # Display: Charts
    # -------------------------
    st.subheader("Trade Insights")

    # Line chart for import/export trend over time.
    line_fig = px.line(
        yearly_df,
        x="year",
        y="trade_value_usd",
        color="flow",
        markers=True,
        title="Import / Export Value Over Time",
        labels={"trade_value_usd": "Trade Value (USD)", "year": "Year", "flow": "Flow"},
    )
    st.plotly_chart(line_fig, use_container_width=True)

    # Bar chart for top partners.
    bar_fig = px.bar(
        partners_df,
        x="partner",
        y="trade_value_usd",
        title="Top Partner Countries by Trade Value",
        labels={"partner": "Partner Country", "trade_value_usd": "Total Trade Value (USD)"},
    )
    st.plotly_chart(bar_fig, use_container_width=True)

    # -------------------------
    # Display: Raw data table
    # -------------------------
    st.subheader("Raw Trade Data")
    st.dataframe(clean_df.sort_values(["year", "flow", "partner"]), use_container_width=True)
else:
    st.write("Use the sidebar to enter an equipment name and click **Run Analysis**.")
