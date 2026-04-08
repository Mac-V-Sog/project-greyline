from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class QuarantineReason(str, Enum):
    UNKNOWN_FIELD_SEMANTICS = "unknown_field_semantics"
    UNKNOWN_FAMILY = "unknown_family"
    TIMESTAMP_PARSE_FAILED = "timestamp_parse_failed"
    IDENTIFIER_INVALID = "identifier_invalid"
    UNEXPECTED_VALUE_CLASS = "unexpected_value_class"
    ROW_CORRUPT = "row_corrupt"
    MAPPING_MISSING_REQUIRED_FIELD = "mapping_missing_required_field"
    VALIDATOR_CONFLICT = "validator_conflict"
    MANUAL_HOLD = "manual_hold"


class QuarantineRecord(BaseModel):
    row_number: int
    source_name: str
    source_type: str
    raw_row: dict[str, Any]
    failure_stage: str = "ingest"
    failure_reasons: list[str] = Field(default_factory=list)
    details: list[dict[str, Any]] = Field(default_factory=list)
    record_type: str | None = None
