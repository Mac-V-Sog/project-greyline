from __future__ import annotations

from typing import Any, Literal
from pydantic import BaseModel, Field


class FieldProfile(BaseModel):
    name: str
    sample_values: list[str] = Field(default_factory=list)
    primitive_types: list[str] = Field(default_factory=list)
    semantic_hints: list[str] = Field(default_factory=list)
    null_rate: float = 0.0
    unique_ratio: float = 0.0


class RecordProfile(BaseModel):
    source_name: str
    row_count_sampled: int
    file_format: str
    delimiter: str | None = None
    fields: list[FieldProfile]
    shape_fingerprint: str
    sample_rows: list[dict[str, Any]] = Field(default_factory=list)


class MappingCandidate(BaseModel):
    source_field: str
    target_field: str
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str
    relation: Literal["exact", "close", "broad", "narrow", "unknown"] = "exact"
    transforms: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class MappingProposal(BaseModel):
    record_type: str
    record_confidence: float = Field(ge=0.0, le=1.0)
    mappings: list[MappingCandidate]
    global_warnings: list[str] = Field(default_factory=list)


class ValidationResult(BaseModel):
    accepted: list[MappingCandidate] = Field(default_factory=list)
    needs_review: list[MappingCandidate] = Field(default_factory=list)
    rejected: list[MappingCandidate] = Field(default_factory=list)
    validation_notes: list[str] = Field(default_factory=list)


class MapRequest(BaseModel):
    profile: RecordProfile
    ontology: dict[str, Any]
    source_metadata: dict[str, str] = Field(default_factory=dict)
    prior_examples: list[dict[str, Any]] = Field(default_factory=list)


class MappingFamily(BaseModel):
    family_id: str
    record_type: str
    mappings: list[MappingCandidate]
    min_match_ratio: float = Field(default=0.5, ge=0.0, le=1.0)
    notes: str = ""
    mapping_id: str | None = None


class IngestRequest(BaseModel):
    record_type: str
    mappings: list[MappingCandidate]
    source_name: str = "uploaded_source"
    output_path: str | None = None
    max_rows: int | None = None
    family_id: str = "default"
    mapping_id: str | None = None


class IngestBundleRequest(BaseModel):
    families: list[MappingFamily]
    source_name: str = "uploaded_source"
    output_path: str | None = None
    max_rows: int | None = None


class ApprovedMapping(BaseModel):
    shape_fingerprint: str
    source_type: str = ""
    provider: str = ""
    record_type: str
    approved_by: str = "system"
    notes: str = ""
    mappings: list[MappingCandidate]
