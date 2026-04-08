from __future__ import annotations

import json
from pathlib import Path

from app.ingest import OUTPUT_DIR, canonicalize_row, ingest_bundle_to_ndjson, ingest_to_ndjson
from app.models import MappingCandidate, MappingFamily


def test_canonicalize_row_applies_basic_transforms() -> None:
    row = {
        "A_NO": "+47 123 45 678",
        "B_NO": "4799999999",
        "START": "2026-04-02 14:03:11",
        "DUR": "62",
    }
    mappings = [
        MappingCandidate(source_field="A_NO", target_field="party_a.msisdn", confidence=0.9, rationale="", relation="exact"),
        MappingCandidate(source_field="B_NO", target_field="party_b.msisdn", confidence=0.9, rationale="", relation="exact"),
        MappingCandidate(source_field="START", target_field="start_time", confidence=0.9, rationale="", relation="exact"),
        MappingCandidate(source_field="DUR", target_field="duration_sec", confidence=0.9, rationale="", relation="exact"),
    ]
    result = canonicalize_row(
        row,
        mappings,
        record_type="call_event",
        source_name="test",
        row_number=1,
        family_id="calls",
        mapping_id="map_calls_v1",
    )
    assert result.quarantine is None
    assert result.doc is not None
    assert result.doc["family_id"] == "calls"
    assert result.doc["governance"]["mapping_id"] == "map_calls_v1"
    assert result.doc["canonical"]["party_a"]["msisdn"] == "+4712345678"
    assert result.doc["canonical"]["party_b"]["msisdn"] == "4799999999"
    assert result.doc["canonical"]["duration_sec"] == 62
    assert result.doc["canonical"]["start_time"].endswith("Z")
    assert len(result.doc["provenance"]["field_mappings"]) == 4
    assert result.coercions["party_a.msisdn"] == 1
    assert result.mapped_field_counts["duration_sec"] == 1
    assert sum(result.null_field_counts.values()) == 0
    assert result.total_targets == 4


def test_canonicalize_row_quarantines_bad_timestamp() -> None:
    row = {"A_NO": "471", "START": "banana"}
    mappings = [
        MappingCandidate(source_field="A_NO", target_field="party_a.msisdn", confidence=0.9, rationale="", relation="exact"),
        MappingCandidate(source_field="START", target_field="start_time", confidence=0.9, rationale="", relation="exact"),
    ]
    result = canonicalize_row(row, mappings, record_type="call_event", source_name="test", row_number=7)
    assert result.doc is None
    assert result.quarantine is not None
    assert "timestamp_parse_failed" in result.quarantine.failure_reasons


def test_ingest_to_ndjson_streams_csv_and_writes_output(tmp_path: Path) -> None:
    src = tmp_path / "telecom.csv"
    src.write_text(
        "A_NO,B_NO,START,DUR\n"
        "471,472,2026-04-02 14:03:11,62\n"
        "473,474,2026-04-02 14:04:11,5\n",
        encoding="utf-8",
    )
    mappings = [
        MappingCandidate(source_field="A_NO", target_field="party_a.msisdn", confidence=0.9, rationale="", relation="exact"),
        MappingCandidate(source_field="B_NO", target_field="party_b.msisdn", confidence=0.9, rationale="", relation="exact"),
        MappingCandidate(source_field="START", target_field="start_time", confidence=0.9, rationale="", relation="exact"),
        MappingCandidate(source_field="DUR", target_field="duration_sec", confidence=0.9, rationale="", relation="exact"),
    ]
    out = OUTPUT_DIR / "out.ndjson"
    with src.open("rb") as f:
        result = ingest_to_ndjson(
            f,
            src.name,
            mappings,
            record_type="call_event",
            source_name="telecom_x",
            output_path=str(out),
            family_id="calls",
            mapping_id="map_calls_v1",
        )
    assert result["rows_written"] == 2
    assert result["rows_quarantined"] == 0
    lines = out.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    payload = json.loads(lines[0])
    assert payload["record_type"] == "call_event"
    assert payload["family_id"] == "calls"
    assert payload["governance"]["mapping_id"] == "map_calls_v1"
    assert payload["canonical"]["duration_sec"] == 62


def test_ingest_to_ndjson_writes_quarantine_and_stats(tmp_path: Path) -> None:
    src = tmp_path / "telecom_bad.csv"
    src.write_text(
        "A_NO,START,DUR\n"
        "471,2026-04-02 14:03:11,62\n"
        "472,banana,5\n",
        encoding="utf-8",
    )
    mappings = [
        MappingCandidate(source_field="A_NO", target_field="party_a.msisdn", confidence=0.9, rationale="", relation="exact"),
        MappingCandidate(source_field="START", target_field="start_time", confidence=0.9, rationale="", relation="exact"),
        MappingCandidate(source_field="DUR", target_field="duration_sec", confidence=0.9, rationale="", relation="exact"),
    ]
    out = OUTPUT_DIR / "accepted.ndjson"
    with src.open("rb") as f:
        result = ingest_to_ndjson(f, src.name, mappings, record_type="call_event", source_name="telecom_x", output_path=str(out))
    quarantine_path = Path(result["quarantine_output_path"])
    stats_path = Path(result["stats_path"])
    assert result["rows_seen"] == 2
    assert result["rows_written"] == 1
    assert result["rows_quarantined"] == 1
    qlines = quarantine_path.read_text(encoding="utf-8").splitlines()
    assert len(qlines) == 1
    qpayload = json.loads(qlines[0])
    assert "timestamp_parse_failed" in qpayload["failure_reasons"]
    stats = json.loads(stats_path.read_text(encoding="utf-8"))
    assert stats["errors_by_reason"]["timestamp_parse_failed"] == 1


def test_ingest_bundle_routes_mixed_rows_to_family_outputs(tmp_path: Path) -> None:
    src = tmp_path / "mixed.csv"
    src.write_text(
        "A_NO,B_NO,START,DUR,SENDER,RECIPIENT,SENT\n"
        "471,472,2026-04-02 14:03:11,62,,,\n"
        ",,,,555,666,2026-04-02 14:05:11\n"
        "x,,,,,,\n",
        encoding="utf-8",
    )
    families = [
        MappingFamily(
            family_id="calls",
            record_type="call_event",
            mapping_id="map_calls_v1",
            min_match_ratio=0.5,
            mappings=[
                MappingCandidate(source_field="A_NO", target_field="party_a.msisdn", confidence=0.9, rationale="", relation="exact"),
                MappingCandidate(source_field="B_NO", target_field="party_b.msisdn", confidence=0.9, rationale="", relation="exact"),
                MappingCandidate(source_field="START", target_field="start_time", confidence=0.9, rationale="", relation="exact"),
                MappingCandidate(source_field="DUR", target_field="duration_sec", confidence=0.9, rationale="", relation="exact"),
            ],
        ),
        MappingFamily(
            family_id="sms",
            record_type="sms_event",
            mapping_id="map_sms_v1",
            min_match_ratio=0.5,
            mappings=[
                MappingCandidate(source_field="SENDER", target_field="sender.msisdn", confidence=0.9, rationale="", relation="exact"),
                MappingCandidate(source_field="RECIPIENT", target_field="recipient.msisdn", confidence=0.9, rationale="", relation="exact"),
                MappingCandidate(source_field="SENT", target_field="sent_time", confidence=0.9, rationale="", relation="exact"),
            ],
        ),
    ]
    out = OUTPUT_DIR / "mixed_out.ndjson"
    with src.open("rb") as f:
        result = ingest_bundle_to_ndjson(f, src.name, families=families, source_name="mixed_source", output_path=str(out))
    assert result["rows_seen"] == 3
    assert result["rows_written"] == 2
    assert result["rows_quarantined"] == 1
    assert result["family_counts"] == {"calls": 1, "sms": 1}
    assert set(result["family_output_paths"].keys()) == {"calls", "sms"}
    call_lines = Path(result["family_output_paths"]["calls"]).read_text(encoding="utf-8").splitlines()
    sms_lines = Path(result["family_output_paths"]["sms"]).read_text(encoding="utf-8").splitlines()
    assert len(call_lines) == 1
    assert len(sms_lines) == 1
    quarantine_lines = Path(result["quarantine_output_path"]).read_text(encoding="utf-8").splitlines()
    assert len(quarantine_lines) == 1
    qp = json.loads(quarantine_lines[0])
    assert qp["failure_reasons"] == ["unknown_family"]
