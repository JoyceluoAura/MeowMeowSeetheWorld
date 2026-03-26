"""Trade data retrieval layer.

Uses UN Comtrade public endpoint when possible and falls back to mock data for MVP reliability.
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, Optional

import pandas as pd
import requests

UN_COMTRADE_BASE_URL = "https://comtradeapi.worldbank.org/data/v1/get/C/A/HS"


def _year_list(start_year: int, end_year: int) -> str:
    """Return comma-separated year list for API usage."""
    return ",".join(str(y) for y in range(start_year, end_year + 1))


def fetch_trade_data(
    hs_codes: Iterable[str],
    country: Optional[str],
    start_year: int,
    end_year: int,
    timeout_sec: int = 20,
) -> pd.DataFrame:
    """Fetch trade data from UN Comtrade; raise exception when request fails.

    Notes:
    - This function is intentionally simple for Phase 1.
    - Uses a reporter string (M49 code) if country is numeric; otherwise tries all reporters.
    """
    primary_hs_code = list(hs_codes)[0] if hs_codes else "847989"

    # If user enters a numeric country code, pass it through; otherwise query all reporters.
    reporter = country.strip() if country and country.strip().isdigit() else "all"

    params = {
        "reporter": reporter,
        "partner": "all",
        "cmdCode": primary_hs_code,
        "flow": "M,X",
        "period": _year_list(start_year, end_year),
        "format": "json",
    }

    response = requests.get(UN_COMTRADE_BASE_URL, params=params, timeout=timeout_sec)
    response.raise_for_status()
    payload = response.json()

    # The response often stores data rows in a list under "data".
    rows = payload.get("data", [])
    if not rows:
        raise ValueError("UN Comtrade returned no rows for selected query.")

    records = []
    for row in rows:
        records.append(
            {
                "year": int(row.get("period", 0)),
                "flow": row.get("flowDesc", "Unknown"),
                "reporter": row.get("reporterDesc", "Unknown"),
                "partner": row.get("partnerDesc", "Unknown"),
                "hs_code": str(row.get("cmdCode", primary_hs_code)),
                "trade_value_usd": float(row.get("primaryValue", 0) or 0),
                "quantity": float(row.get("qty", 0) or 0),
                "quantity_unit": row.get("qtyUnitAbbr", ""),
            }
        )

    df = pd.DataFrame(records)
    if df.empty:
        raise ValueError("UN Comtrade parsing created an empty DataFrame.")

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
                # Simple trend + partner weighting for realistic-looking demo numbers.
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
) -> tuple[pd.DataFrame, str]:
    """Try live API first; fallback to mock dataset if anything fails."""
    hs_codes = list(hs_codes)
    if not hs_codes:
        hs_codes = ["847989"]

    try:
        df = fetch_trade_data(hs_codes=hs_codes, country=country, start_year=start_year, end_year=end_year)
        source_msg = "Live data source: UN Comtrade API"
    except Exception as exc:  # Fallback is intentional in MVP.
        df = mock_trade_data(
            equipment_name=equipment_name,
            hs_code=hs_codes[0],
            start_year=start_year,
            end_year=end_year,
            country=country,
        )
        source_msg = f"Fallback source: Mock data (reason: {exc.__class__.__name__})"

    return df, source_msg


def default_year_range(last_n_years: int = 5) -> tuple[int, int]:
    """Return default inclusive year range covering the last N years."""
    current_year = datetime.utcnow().year
    return current_year - last_n_years + 1, current_year
