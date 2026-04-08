from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from app.models import MappingFamily


class FamilyDecision(BaseModel):
    family_id: str | None = None
    confidence: float = 0.0
    matched_mapping_id: str | None = None
    reason: str = ""
    matched_source_fields: list[str] = Field(default_factory=list)


def _family_score(row: dict[str, Any], family: MappingFamily) -> tuple[float, list[str]]:
    matched: list[str] = []
    expected_fields = []
    for mapping in family.mappings:
        field = mapping.source_field
        if field not in expected_fields:
            expected_fields.append(field)
    if not expected_fields:
        return 0.0, matched
    for field in expected_fields:
        value = row.get(field)
        if value is None:
            continue
        if str(value).strip() == "":
            continue
        matched.append(field)
    return len(matched) / len(expected_fields), matched


def choose_family(row: dict[str, Any], families: list[MappingFamily]) -> FamilyDecision:
    if not families:
        return FamilyDecision(reason="no_families_configured")

    scored = []
    for family in families:
        score, matched = _family_score(row, family)
        scored.append((score, len(matched), family, matched))

    scored.sort(key=lambda item: (item[0], item[1], item[2].mapping_id or item[2].family_id), reverse=True)
    best_score, _, best_family, best_matched = scored[0]
    if best_score < best_family.min_match_ratio:
        return FamilyDecision(
            family_id=None,
            confidence=best_score,
            matched_mapping_id=None,
            reason="below_min_match_ratio",
            matched_source_fields=best_matched,
        )
    if len(scored) > 1:
        second_score = scored[1][0]
        if best_score == second_score and best_score > 0:
            return FamilyDecision(
                family_id=None,
                confidence=best_score,
                matched_mapping_id=None,
                reason="ambiguous_family_match",
                matched_source_fields=best_matched,
            )
    return FamilyDecision(
        family_id=best_family.family_id,
        confidence=best_score,
        matched_mapping_id=best_family.mapping_id,
        reason="matched",
        matched_source_fields=best_matched,
    )
