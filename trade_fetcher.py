"""Trade data retrieval layer.

Uses multiple real-data connectors first, then deterministic mock fallback:
1) UN Comtrade (World Bank-hosted endpoint)
2) UN Comtrade (legacy UN endpoint)
3) World Bank macro trade indicators (country-level import/export totals)
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, Optional

import pandas as pd
import requests

UN_COMTRADE_WB_BASE_URL = "https://comtradeapi.worldbank.org/data/v1/get/C/A/HS"
UN_COMTRADE_UN_BASE_URL = "https://api.uncomtrade.org/data/v1/get/C/A/HS"
WORLD_BANK_COUNTRY_BASE_URL = "https://api.worldbank.org/v2/country"

# Lightweight mapping to improve reporter selection without extra dependencies.
COUNTRY_TO_M49 = {
    "united states": "842",
    "usa": "842",
    "us": "842",
    "china": "156",
    "germany": "276",
    "japan": "392",
    "south korea": "410",
    "korea": "410",
    "india": "356",
    "italy": "380",
    "france": "250",
    "united kingdom": "826",
    "uk": "826",
}

COUNTRY_TO_ISO3 = {
    "united states": "USA",
    "usa": "USA",
    "us": "USA",
    "china": "CHN",
    "germany": "DEU",
    "japan": "JPN",
    "south korea": "KOR",
    "korea": "KOR",
    "india": "IND",
    "italy": "ITA",
    "france": "FRA",
    "united kingdom": "GBR",
    "uk": "GBR",
}


def _year_list(start_year: int, end_year: int) -> str:
    """Return comma-separated year list for API usage."""
    return ",".join(str(y) for y in range(start_year, end_year + 1))


def _normalize_country_text(country: Optional[str]) -> str:
    return (country or "").strip().lower()


def _country_to_reporter_m49(country: Optional[str]) -> str:
    """Resolve country input to reporter code for Comtrade, or 'all' if unknown."""
    cleaned = _normalize_country_text(country)
    if not cleaned:
        return "all"
    if cleaned.isdigit():
        return cleaned
    return COUNTRY_TO_M49.get(cleaned, "all")


def _country_to_iso3(country: Optional[str]) -> str:
    """Resolve country input to ISO3 for World Bank API, or WLD for global."""
    cleaned = _normalize_country_text(country)
    if not cleaned:
        return "WLD"
    if len(cleaned) == 3 and cleaned.isalpha():
        return cleaned.upper()
    return COUNTRY_TO_ISO3.get(cleaned, "WLD")


def _parse_comtrade_rows(rows: list[dict], default_hs_code: str) -> pd.DataFrame:
    """Convert Comtrade response rows into normalized dataframe schema."""
    records = []
    for row in rows:
        records.append(
            {
                "year": int(row.get("period", 0)),
                "flow": row.get("flowDesc", "Unknown"),
                "reporter": row.get("reporterDesc", "Unknown"),
                "partner": row.get("partnerDesc", "Unknown"),
                "hs_code": str(row.get("cmdCode", default_hs_code)),
                "trade_value_usd": float(row.get("primaryValue", 0) or 0),
                "quantity": float(row.get("qty", 0) or 0),
                "quantity_unit": row.get("qtyUnitAbbr", ""),
            }
        )
    return pd.DataFrame(records)


def fetch_trade_data_un_comtrade(
    base_url: str,
    hs_codes: Iterable[str],
    country: Optional[str],
    start_year: int,
    end_year: int,
    timeout_sec: int = 30,
) -> pd.DataFrame:
    """Fetch trade data from a Comtrade-compatible endpoint."""
    primary_hs_code = list(hs_codes)[0] if hs_codes else "847989"
    reporter = _country_to_reporter_m49(country)

    params = {
        "reporter": reporter,
        "partner": "all",
        "cmdCode": primary_hs_code,
        "flow": "M,X",
        "period": _year_list(start_year, end_year),
        "format": "json",
    }

    response = requests.get(
        base_url,
        params=params,
        timeout=timeout_sec,
        headers={"User-Agent": "equipment-trade-intel-mvp/1.0"},
    )
    response.raise_for_status()

    payload = response.json()
    rows = payload.get("data", [])
    if not rows:
        raise ValueError("Comtrade returned no rows for selected query.")

    df = _parse_comtrade_rows(rows, default_hs_code=primary_hs_code)
    if df.empty:
        raise ValueError("Comtrade parsing created an empty DataFrame.")

    # Optional post-filter by country text when reporter could not be resolved.
    if country and reporter == "all":
        mask = df["reporter"].str.contains(country, case=False, na=False)
        filtered_df = df[mask]
        if not filtered_df.empty:
            df = filtered_df

    return df


def fetch_trade_data_world_bank_macro(
    country: Optional[str],
    start_year: int,
    end_year: int,
    timeout_sec: int = 30,
) -> pd.DataFrame:
    """Fetch real import/export totals from World Bank indicators.

    This is a macro-level fallback (not HS/partner granular), but still real data.
    """
    iso3 = _country_to_iso3(country)
    indicators = {
        "Import": "NE.IMP.GNFS.CD",  # Imports of goods and services (current US$)
        "Export": "NE.EXP.GNFS.CD",  # Exports of goods and services (current US$)
    }

    records = []
    for flow, indicator in indicators.items():
        url = f"{WORLD_BANK_COUNTRY_BASE_URL}/{iso3}/indicator/{indicator}"
        params = {
            "format": "json",
            "date": f"{start_year}:{end_year}",
            "per_page": 200,
        }
        response = requests.get(
            url,
            params=params,
            timeout=timeout_sec,
            headers={"User-Agent": "equipment-trade-intel-mvp/1.0"},
        )
        response.raise_for_status()

        payload = response.json()
        if not isinstance(payload, list) or len(payload) < 2 or not payload[1]:
            raise ValueError(f"World Bank returned no rows for indicator {indicator}.")

        for row in payload[1]:
            year = int(row.get("date", 0) or 0)
            value = float(row.get("value", 0) or 0)
            if year <= 0:
                continue
            records.append(
                {
                    "year": year,
                    "flow": flow,
                    "reporter": row.get("country", {}).get("value", iso3),
                    "partner": "All partners (macro)",
                    "hs_code": "N/A",
                    "trade_value_usd": value,
                    "quantity": 0.0,
                    "quantity_unit": "",
                }
            )

    df = pd.DataFrame(records)
    if df.empty:
        raise ValueError("World Bank macro parser created an empty DataFrame.")

    return df


def mock_trade_data(
    equipment_name: str,
    hs_code: str,
    start_year: int,
    end_year: int,
    country: Optional[str] = None,
) -> pd.DataFrame:
    """Generate deterministic mock trade data for local demos and API fallback."""
    reporter = country.strip() if country and country.strip() else "Global"
    base = abs(hash(f"{equipment_name}-{hs_code}")) % 2_000_000 + 500_000

    partners = ["China", "Germany", "Japan", "United States", "South Korea", "Italy"]
    flows = ["Import", "Export"]

    records = []
    for y in range(start_year, end_year + 1):
        for flow in flows:
            flow_multiplier = 1.2 if flow == "Import" else 1.0
            for idx, partner in enumerate(partners):
                value = (base * flow_multiplier) * (1 + 0.07 * (y - start_year)) * (1 - idx * 0.08)
                qty = max(10, value / 25000)
                records.append(
                    {
                        "year": y,
                        "flow": flow,
                        "reporter": reporter,
                        "partner": partner,
                        "hs_code": hs_code,
                        "trade_value_usd": round(value, 2),
                        "quantity": round(qty, 2),
                        "quantity_unit": "units",
                    }
                )

    return pd.DataFrame(records)


def load_trade_data_with_fallback(
    equipment_name: str,
    hs_codes: Iterable[str],
    country: Optional[str],
    start_year: int,
    end_year: int,
) -> tuple[pd.DataFrame, str, list[str]]:
    """Try multiple live APIs first, then fallback to mock dataset.

    Returns:
        (dataframe, chosen_source_message, connection_attempt_logs)
    """
    hs_codes = list(hs_codes)
    if not hs_codes:
        hs_codes = ["847989"]

    attempts: list[str] = []

    connectors = [
        (
            "UN Comtrade (World Bank endpoint)",
            lambda: fetch_trade_data_un_comtrade(
                base_url=UN_COMTRADE_WB_BASE_URL,
                hs_codes=hs_codes,
                country=country,
                start_year=start_year,
                end_year=end_year,
            ),
        ),
        (
            "UN Comtrade (legacy UN endpoint)",
            lambda: fetch_trade_data_un_comtrade(
                base_url=UN_COMTRADE_UN_BASE_URL,
                hs_codes=hs_codes,
                country=country,
                start_year=start_year,
                end_year=end_year,
            ),
        ),
        (
            "World Bank macro trade indicators",
            lambda: fetch_trade_data_world_bank_macro(
                country=country,
                start_year=start_year,
                end_year=end_year,
            ),
        ),
    ]

    for name, loader in connectors:
        try:
            df = loader()
            attempts.append(f"✅ {name}: connected")
            return df, f"Live data source: {name}", attempts
        except Exception as exc:
            attempts.append(f"❌ {name}: {exc.__class__.__name__}")

    # If all live endpoints fail, fallback to deterministic mock data.
    df = mock_trade_data(
        equipment_name=equipment_name,
        hs_code=hs_codes[0],
        start_year=start_year,
        end_year=end_year,
        country=country,
    )
    attempts.append("⚠️ Mock data fallback activated")
    return df, "Fallback source: Mock data (all live endpoints failed)", attempts


def default_year_range(last_n_years: int = 5) -> tuple[int, int]:
    """Return default inclusive year range covering the last N years."""
    current_year = datetime.utcnow().year
    return current_year - last_n_years + 1, current_year
