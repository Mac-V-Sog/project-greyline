from __future__ import annotations

import logging
from datetime import datetime
import re

from app.models import MappingCandidate, MappingProposal, RecordProfile, ValidationResult
from app.patterns import PHONE_RE, CELL_ID_RE, looks_like_phone, looks_like_datetime

logger = logging.getLogger("greyline.validator")

# --- Confidence thresholds ---
# Duplicate target field: clearly wrong, force below review threshold
CONF_DUPLICATE_TARGET = 0.45
# No sample values available: cannot verify, force below review threshold
CONF_NO_SAMPLES = 0.4
# Phone-like field with inconsistent values: likely wrong mapping
CONF_PHONE_INCONSISTENT = 0.49
# Timestamp without explicit timezone: probably correct but needs human check
CONF_TIMESTAMP_NO_TZ = 0.79
# Timestamp values that don't parse: likely wrong mapping
CONF_TIMESTAMP_UNPARSEABLE = 0.39
# Duration not integer-like: likely wrong mapping
CONF_DURATION_NOT_INT = 0.35
# Coordinate out of range or not numeric: likely wrong mapping
CONF_COORD_INVALID = 0.3
# Cell site ID doesn't match pattern: uncertain, needs review
CONF_CELL_ID_INCONSISTENT = 0.5
# Minimum confidence for automatic acceptance of unmapped target types
CONF_ACCEPT_THRESHOLD = 0.8


def _field_samples(profile: RecordProfile, name: str) -> list[str]:
    for field in profile.fields:
        if field.name == name:
            return [s.strip("'\"") for s in field.sample_values]
    return []


def _looks_like_int(value: str) -> bool:
    return bool(re.fullmatch(r"-?\d+", value))


def _looks_like_float(value: str) -> bool:
    return bool(re.fullmatch(r"-?\d+(\.\d+)?", value))


def validate_proposal(profile: RecordProfile, proposal: MappingProposal) -> ValidationResult:
    logger.info("validating proposal record_type=%s mappings=%d", proposal.record_type, len(proposal.mappings))
    accepted: list[MappingCandidate] = []
    needs_review: list[MappingCandidate] = []
    rejected: list[MappingCandidate] = []
    notes: list[str] = []
    used_targets: set[str] = set()
    for mapping in proposal.mappings:
        samples = _field_samples(profile, mapping.source_field)
        adjusted = mapping.model_copy(deep=True)
        if adjusted.target_field in used_targets:
            adjusted.warnings.append("Duplicate target field proposed")
            adjusted.confidence = min(adjusted.confidence, CONF_DUPLICATE_TARGET)
            rejected.append(adjusted)
            notes.append(f"Rejected duplicate target mapping for {adjusted.target_field}")
            continue
        if not samples:
            adjusted.warnings.append("No sample values available for source field")
            adjusted.confidence = min(adjusted.confidence, CONF_NO_SAMPLES)
            needs_review.append(adjusted)
            continue
        target = adjusted.target_field
        if target.endswith("msisdn"):
            if all(looks_like_phone(v) for v in samples[:3]):
                accepted.append(adjusted)
            else:
                adjusted.warnings.append("Values do not consistently look like phone numbers")
                adjusted.confidence = min(adjusted.confidence, CONF_PHONE_INCONSISTENT)
                needs_review.append(adjusted)
        elif target in {"start_time", "end_time", "sent_time", "captured_time"}:
            if all(looks_like_datetime(v) for v in samples[:3]):
                if not any("Z" in v or "+" in v for v in samples[:3]):
                    adjusted.warnings.append("Timezone not explicit in sample values")
                    adjusted.confidence = min(adjusted.confidence, CONF_TIMESTAMP_NO_TZ)
                    needs_review.append(adjusted)
                else:
                    accepted.append(adjusted)
            else:
                adjusted.warnings.append("Values do not parse cleanly as timestamps")
                adjusted.confidence = min(adjusted.confidence, CONF_TIMESTAMP_UNPARSEABLE)
                rejected.append(adjusted)
        elif target == "duration_sec":
            if all(_looks_like_int(v) for v in samples[:3]):
                accepted.append(adjusted)
            else:
                adjusted.warnings.append("Duration samples are not consistently integer-like")
                adjusted.confidence = min(adjusted.confidence, CONF_DURATION_NOT_INT)
                rejected.append(adjusted)
        elif target in {"location.lat", "cell_site.lat", "location.lon", "cell_site.lon"}:
            bounds = (-90, 90) if target.endswith("lat") else (-180, 180)
            ok = True
            for v in samples[:3]:
                if not _looks_like_float(v):
                    ok = False
                    break
                fv = float(v)
                if not (bounds[0] <= fv <= bounds[1]):
                    ok = False
                    break
            if ok:
                accepted.append(adjusted)
            else:
                adjusted.warnings.append("Coordinate samples are out of range or not numeric")
                adjusted.confidence = min(adjusted.confidence, CONF_COORD_INVALID)
                rejected.append(adjusted)
        elif target == "cell_site.id":
            if all(CELL_ID_RE.match(v) for v in samples[:3]):
                accepted.append(adjusted)
            else:
                adjusted.warnings.append("Cell site values do not match the basic identifier pattern")
                adjusted.confidence = min(adjusted.confidence, CONF_CELL_ID_INCONSISTENT)
                needs_review.append(adjusted)
        else:
            if adjusted.confidence >= CONF_ACCEPT_THRESHOLD:
                accepted.append(adjusted)
            else:
                needs_review.append(adjusted)
        if adjusted not in rejected:
            used_targets.add(adjusted.target_field)
    logger.info("validation result accepted=%d needs_review=%d rejected=%d", len(accepted), len(needs_review), len(rejected))
    return ValidationResult(
        accepted=accepted,
        needs_review=needs_review,
        rejected=rejected,
        validation_notes=notes + proposal.global_warnings,
    )
