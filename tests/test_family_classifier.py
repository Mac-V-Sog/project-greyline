from __future__ import annotations

from app.family_classifier import choose_family, FamilyDecision
from app.models import MappingCandidate, MappingFamily


def _call_family(mappings=None, family_id="calls", record_type="call_event", min_match_ratio=0.5, mapping_id=None):
    return MappingFamily(
        family_id=family_id,
        record_type=record_type,
        mappings=mappings or [],
        min_match_ratio=min_match_ratio,
        mapping_id=mapping_id,
    )


def test_choose_family_no_families():
    result = choose_family({"A": "1"}, [])
    assert result.family_id is None
    assert result.reason == "no_families_configured"


def test_choose_family_single_match():
    families = [
        _call_family(
            mappings=[
                MappingCandidate(source_field="A_NO", target_field="party_a.msisdn", confidence=0.9, rationale="", relation="exact"),
                MappingCandidate(source_field="B_NO", target_field="party_b.msisdn", confidence=0.9, rationale="", relation="exact"),
            ],
        ),
    ]
    row = {"A_NO": "471", "B_NO": "472"}
    result = choose_family(row, families)
    assert result.family_id == "calls"
    assert result.reason == "matched"
    assert result.confidence == 1.0


def test_choose_family_partial_match_above_threshold():
    families = [
        _call_family(
            mappings=[
                MappingCandidate(source_field="A_NO", target_field="party_a.msisdn", confidence=0.9, rationale="", relation="exact"),
                MappingCandidate(source_field="B_NO", target_field="party_b.msisdn", confidence=0.9, rationale="", relation="exact"),
                MappingCandidate(source_field="START", target_field="start_time", confidence=0.9, rationale="", relation="exact"),
            ],
            min_match_ratio=0.5,
        ),
    ]
    row = {"A_NO": "471", "B_NO": "472"}  # 2 out of 3 fields present
    result = choose_family(row, families)
    assert result.family_id == "calls"
    assert result.confidence > 0.5


def test_choose_family_below_min_match_ratio():
    families = [
        _call_family(
            mappings=[
                MappingCandidate(source_field="A_NO", target_field="party_a.msisdn", confidence=0.9, rationale="", relation="exact"),
                MappingCandidate(source_field="B_NO", target_field="party_b.msisdn", confidence=0.9, rationale="", relation="exact"),
                MappingCandidate(source_field="START", target_field="start_time", confidence=0.9, rationale="", relation="exact"),
            ],
            min_match_ratio=0.9,
        ),
    ]
    row = {"A_NO": "471"}  # only 1 out of 3
    result = choose_family(row, families)
    assert result.family_id is None
    assert result.reason == "below_min_match_ratio"


def test_choose_family_ambiguous_tie():
    families = [
        _call_family(
            mappings=[MappingCandidate(source_field="X", target_field="field1", confidence=0.9, rationale="", relation="exact")],
            family_id="fam_a",
            min_match_ratio=0.0,
        ),
        _call_family(
            mappings=[MappingCandidate(source_field="X", target_field="field2", confidence=0.9, rationale="", relation="exact")],
            family_id="fam_b",
            min_match_ratio=0.0,
        ),
    ]
    row = {"X": "value"}
    result = choose_family(row, families)
    assert result.family_id is None
    assert result.reason == "ambiguous_family_match"


def test_choose_family_distinguishes_between_families():
    call_family = _call_family(
        mappings=[
            MappingCandidate(source_field="A_NO", target_field="party_a.msisdn", confidence=0.9, rationale="", relation="exact"),
            MappingCandidate(source_field="B_NO", target_field="party_b.msisdn", confidence=0.9, rationale="", relation="exact"),
            MappingCandidate(source_field="DUR", target_field="duration_sec", confidence=0.9, rationale="", relation="exact"),
        ],
        family_id="calls",
        record_type="call_event",
        min_match_ratio=0.5,
    )
    sms_family = _call_family(
        mappings=[
            MappingCandidate(source_field="SENDER", target_field="sender.msisdn", confidence=0.9, rationale="", relation="exact"),
            MappingCandidate(source_field="RECIPIENT", target_field="recipient.msisdn", confidence=0.9, rationale="", relation="exact"),
        ],
        family_id="sms",
        record_type="sms_event",
        min_match_ratio=0.5,
    )
    # Row with call fields populated
    call_row = {"A_NO": "471", "B_NO": "472", "DUR": "62", "SENDER": "", "RECIPIENT": ""}
    result = choose_family(call_row, [call_family, sms_family])
    assert result.family_id == "calls"

    # Row with SMS fields populated
    sms_row = {"A_NO": "", "B_NO": "", "DUR": "", "SENDER": "555", "RECIPIENT": "666"}
    result = choose_family(sms_row, [call_family, sms_family])
    assert result.family_id == "sms"


def test_choose_family_empty_values_dont_count():
    families = [
        _call_family(
            mappings=[
                MappingCandidate(source_field="A_NO", target_field="party_a.msisdn", confidence=0.9, rationale="", relation="exact"),
                MappingCandidate(source_field="B_NO", target_field="party_b.msisdn", confidence=0.9, rationale="", relation="exact"),
            ],
            min_match_ratio=0.5,
        ),
    ]
    row = {"A_NO": "", "B_NO": ""}  # fields exist but values are empty
    result = choose_family(row, families)
    assert result.family_id is None
    assert result.reason == "below_min_match_ratio"


def test_choose_family_none_values_dont_count():
    families = [
        _call_family(
            mappings=[
                MappingCandidate(source_field="A_NO", target_field="party_a.msisdn", confidence=0.9, rationale="", relation="exact"),
            ],
            min_match_ratio=0.5,
        ),
    ]
    row = {"A_NO": None}
    result = choose_family(row, families)
    assert result.family_id is None
    assert result.reason == "below_min_match_ratio"
