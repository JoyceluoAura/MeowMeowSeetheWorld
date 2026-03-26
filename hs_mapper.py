"""Simple HS code mapper for industrial/scientific equipment.

This module keeps Phase-1 logic intentionally lightweight:
- normalize free-text equipment names
- suggest HS code candidates from keyword heuristics
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List


@dataclass
class HSCandidate:
    """Represents a possible HS code match with reasoning."""

    hs_code: str
    description: str
    confidence: str
    reason: str


# Lightweight keyword-based mapping for MVP use.
HS_KEYWORD_MAP = {
    "kiln": [
        ("841780", "Industrial/laboratory furnaces and ovens (non-electric)", "high"),
        ("851410", "Industrial/laboratory electric furnaces and ovens", "high"),
    ],
    "furnace": [
        ("851410", "Industrial/laboratory electric furnaces and ovens", "high"),
        ("841780", "Industrial/laboratory furnaces and ovens (non-electric)", "medium"),
    ],
    "sintering": [("851410", "Electric furnaces for sintering/heat treatment", "high")],
    "press": [
        ("847480", "Machinery for agglomerating/shaping mineral substances", "medium"),
        ("846291", "Hydraulic presses for working metal/materials", "medium"),
    ],
    "milling": [
        ("847420", "Crushing or grinding machines", "medium"),
        ("847982", "Mixing/kneading/crushing/grinding machines", "medium"),
    ],
    "ball mill": [("847420", "Crushing or grinding machines", "high")],
    "spray dryer": [("841939", "Dryers for materials by process involving temperature change", "high")],
    "microscope": [("901180", "Optical microscopes", "high")],
    "xrd": [("902219", "X-ray apparatus for industrial/scientific use", "high")],
    "xrf": [("902219", "X-ray apparatus for industrial/scientific use", "high")],
    "spectrometer": [("902730", "Spectrometers/spectrophotometers", "high")],
    "particle size analyzer": [("902780", "Physical/chemical analysis instruments", "high")],
    "testing machine": [("902480", "Machines for testing hardness/strength of materials", "high")],
    "vacuum pump": [("841410", "Vacuum pumps", "high")],
}


def normalize_equipment_name(name: str) -> str:
    """Normalize user-provided equipment name into plain standardized English text."""
    text = (name or "").strip().lower()
    text = re.sub(r"[^a-z0-9\s\-]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text


def suggest_hs_codes(equipment_name: str, brand: str = "", model: str = "") -> List[HSCandidate]:
    """Return ranked HS code suggestions using simple keyword rules.

    For MVP behavior:
    - match any mapped keyword found in normalized text
    - de-duplicate by HS code
    - fallback to generic scientific/industrial categories when no match found
    """
    normalized = normalize_equipment_name(" ".join([equipment_name, brand, model]))

    matched: List[HSCandidate] = []
    seen_codes = set()

    # Check longer keywords first to improve relevance.
    for keyword in sorted(HS_KEYWORD_MAP.keys(), key=len, reverse=True):
        if keyword in normalized:
            for hs_code, description, confidence in HS_KEYWORD_MAP[keyword]:
                if hs_code in seen_codes:
                    continue
                matched.append(
                    HSCandidate(
                        hs_code=hs_code,
                        description=description,
                        confidence=confidence,
                        reason=f"Keyword match: '{keyword}' found in normalized equipment text.",
                    )
                )
                seen_codes.add(hs_code)

    # Fallback to broad categories if no specific keyword matched.
    if not matched:
        matched = [
            HSCandidate(
                hs_code="847989",
                description="Other machines and mechanical appliances (industrial)",
                confidence="low",
                reason="No direct keyword matched; broad industrial machinery fallback.",
            ),
            HSCandidate(
                hs_code="902780",
                description="Other instruments for physical/chemical analysis",
                confidence="low",
                reason="No direct keyword matched; broad scientific instrument fallback.",
            ),
        ]

    return matched
