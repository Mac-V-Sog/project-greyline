from __future__ import annotations

from datetime import datetime
import re

from app.models import MappingCandidate, MappingProposal, RecordProfile, ValidationResult

PHONE_RE = re.compile(r"^\+?\d{7,15}$")
CELL_ID_RE = re.compile(r"^[0-9A-Za-z_-]{4,32}$")


def _field_samples(profile: RecordProfile, name: str) -> list[str]:
    for field in profile.fields:
        if field.name == name:
            return [s.strip("'\"") for s in field.sample_values]
    return []


def _looks_like_phone(value: str) -> bool:
    compact = re.sub(r"[\s()-]", "", value)
    return bool(PHONE_RE.match(compact))


def _looks_like_datetime(value: str) -> bool:
    candidates = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y/%m/%d %H:%M:%S",
    ]
    for fmt in candidates:
        try:
            datetime.strptime(value, fmt)
            return True
        except ValueError:
            continue
    return value.isdigit() and len(value) >= 10


def _looks_like_int(value: str) -> bool:
    return bool(re.fullmatch(r"-?\d+", value))


def _looks_like_float(value: str) -> bool:
    return bool(re.fullmatch(r"-?\d+(\.\d+)?", value))


def validate_proposal(profile: RecordProfile, proposal: MappingProposal) -> ValidationResult:
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
            adjusted.confidence = min(adjusted.confidence, 0.45)
            rejected.append(adjusted)
            notes.append(f"Rejected duplicate target mapping for {adjusted.target_field}")
            continue
        if not samples:
            adjusted.warnings.append("No sample values available for source field")
            adjusted.confidence = min(adjusted.confidence, 0.4)
            needs_review.append(adjusted)
            continue
        target = adjusted.target_field
        if target.endswith("msisdn"):
            if all(_looks_like_phone(v) for v in samples[:3]):
                accepted.append(adjusted)
            else:
                adjusted.warnings.append("Values do not consistently look like phone numbers")
                adjusted.confidence = min(adjusted.confidence, 0.49)
                needs_review.append(adjusted)
        elif target in {"start_time", "end_time", "sent_time", "captured_time"}:
            if all(_looks_like_datetime(v) for v in samples[:3]):
                if not any("Z" in v or "+" in v for v in samples[:3]):
                    adjusted.warnings.append("Timezone not explicit in sample values")
                    adjusted.confidence = min(adjusted.confidence, 0.79)
                    needs_review.append(adjusted)
                else:
                    accepted.append(adjusted)
            else:
                adjusted.warnings.append("Values do not parse cleanly as timestamps")
                adjusted.confidence = min(adjusted.confidence, 0.39)
                rejected.append(adjusted)
        elif target == "duration_sec":
            if all(_looks_like_int(v) for v in samples[:3]):
                accepted.append(adjusted)
            else:
                adjusted.warnings.append("Duration samples are not consistently integer-like")
                adjusted.confidence = min(adjusted.confidence, 0.35)
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
                adjusted.confidence = min(adjusted.confidence, 0.3)
                rejected.append(adjusted)
        elif target == "cell_site.id":
            if all(CELL_ID_RE.match(v) for v in samples[:3]):
                accepted.append(adjusted)
            else:
                adjusted.warnings.append("Cell site values do not match the basic identifier pattern")
                adjusted.confidence = min(adjusted.confidence, 0.5)
                needs_review.append(adjusted)
        else:
            if adjusted.confidence >= 0.8:
                accepted.append(adjusted)
            else:
                needs_review.append(adjusted)
        if adjusted not in rejected:
            used_targets.add(adjusted.target_field)
    return ValidationResult(
        accepted=accepted,
        needs_review=needs_review,
        rejected=rejected,
        validation_notes=notes + proposal.global_warnings,
    )
