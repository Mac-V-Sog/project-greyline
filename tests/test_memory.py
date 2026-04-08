from __future__ import annotations

import sqlite3
from pathlib import Path

from app.memory import get_conn, init_db, promote_mapping, search_memory, grouped_examples
from app.models import ApprovedMapping, MappingCandidate


def _tmp_db(tmp_path: Path) -> str:
    return str(tmp_path / "test_greyline.db")


def test_init_db_creates_table(tmp_path: Path):
    db = _tmp_db(tmp_path)
    init_db(db)
    conn = get_conn(db)
    tables = [row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    conn.close()
    assert "mapping_memory" in tables


def test_init_db_idempotent(tmp_path: Path):
    db = _tmp_db(tmp_path)
    init_db(db)
    init_db(db)  # should not raise
    conn = get_conn(db)
    count = conn.execute("SELECT COUNT(*) FROM mapping_memory").fetchone()[0]
    conn.close()
    assert count == 0


def test_promote_mapping_inserts_rows(tmp_path: Path):
    db = _tmp_db(tmp_path)
    init_db(db)
    payload = ApprovedMapping(
        shape_fingerprint="abc123",
        source_type="telecom_cdr",
        provider="telecom_x",
        record_type="call_event",
        approved_by="analyst",
        notes="test mapping",
        mappings=[
            MappingCandidate(source_field="A_NO", target_field="party_a.msisdn", confidence=0.95, rationale="test", relation="exact"),
            MappingCandidate(source_field="B_NO", target_field="party_b.msisdn", confidence=0.90, rationale="test", relation="exact"),
        ],
    )
    count = promote_mapping(payload, db)
    assert count == 2
    # Verify data is actually in the DB
    conn = get_conn(db)
    rows = conn.execute("SELECT * FROM mapping_memory").fetchall()
    conn.close()
    assert len(rows) == 2
    assert rows[0]["shape_fingerprint"] == "abc123"
    assert rows[0]["source_field"] == "A_NO"


def test_search_memory_by_shape(tmp_path: Path):
    db = _tmp_db(tmp_path)
    init_db(db)
    payload1 = ApprovedMapping(
        shape_fingerprint="fp1",
        source_type="telecom_cdr",
        provider="telecom_x",
        record_type="call_event",
        mappings=[MappingCandidate(source_field="A", target_field="party_a.msisdn", confidence=0.9, rationale="", relation="exact")],
    )
    payload2 = ApprovedMapping(
        shape_fingerprint="fp2",
        source_type="sms",
        provider="telecom_y",
        record_type="sms_event",
        mappings=[MappingCandidate(source_field="FROM", target_field="sender.msisdn", confidence=0.9, rationale="", relation="exact")],
    )
    promote_mapping(payload1, db)
    promote_mapping(payload2, db)
    results = search_memory(shape_fingerprint="fp1", db_path=db)
    assert len(results) == 1
    assert results[0]["shape_fingerprint"] == "fp1"


def test_search_memory_by_provider(tmp_path: Path):
    db = _tmp_db(tmp_path)
    init_db(db)
    payload = ApprovedMapping(
        shape_fingerprint="fp1",
        source_type="telecom_cdr",
        provider="telecom_x",
        record_type="call_event",
        mappings=[MappingCandidate(source_field="A", target_field="party_a.msisdn", confidence=0.9, rationale="", relation="exact")],
    )
    promote_mapping(payload, db)
    results = search_memory(provider="telecom_x", db_path=db)
    assert len(results) == 1
    results_none = search_memory(provider="nonexistent", db_path=db)
    assert len(results_none) == 0


def test_search_memory_no_filters_returns_all(tmp_path: Path):
    db = _tmp_db(tmp_path)
    init_db(db)
    payload = ApprovedMapping(
        shape_fingerprint="fp1",
        source_type="telecom_cdr",
        provider="telecom_x",
        record_type="call_event",
        mappings=[MappingCandidate(source_field="A", target_field="party_a.msisdn", confidence=0.9, rationale="", relation="exact")],
    )
    promote_mapping(payload, db)
    results = search_memory(db_path=db)
    assert len(results) == 1


def test_grouped_examples_returns_grouped_by_record_type(tmp_path: Path):
    db = _tmp_db(tmp_path)
    init_db(db)
    payload = ApprovedMapping(
        shape_fingerprint="fp1",
        source_type="telecom_cdr",
        provider="telecom_x",
        record_type="call_event",
        mappings=[
            MappingCandidate(source_field="A", target_field="party_a.msisdn", confidence=0.9, rationale="", relation="exact"),
            MappingCandidate(source_field="B", target_field="party_b.msisdn", confidence=0.85, rationale="", relation="close"),
        ],
    )
    promote_mapping(payload, db)
    groups = grouped_examples(shape_fingerprint="fp1", db_path=db)
    assert len(groups) == 1
    assert groups[0]["record_type"] == "call_event"
    assert len(groups[0]["mappings"]) == 2
    assert groups[0]["match_basis"] == "shape"


def test_grouped_examples_falls_back_to_provider(tmp_path: Path):
    db = _tmp_db(tmp_path)
    init_db(db)
    payload = ApprovedMapping(
        shape_fingerprint="fp_other",
        source_type="telecom_cdr",
        provider="telecom_x",
        record_type="call_event",
        mappings=[MappingCandidate(source_field="A", target_field="party_a.msisdn", confidence=0.9, rationale="", relation="exact")],
    )
    promote_mapping(payload, db)
    # Search with a different fingerprint but same provider
    groups = grouped_examples(shape_fingerprint="fp_nonexistent", provider="telecom_x", db_path=db)
    assert len(groups) == 1
    assert groups[0]["match_basis"] == "provider"


def test_grouped_examples_falls_back_to_source_type(tmp_path: Path):
    db = _tmp_db(tmp_path)
    init_db(db)
    payload = ApprovedMapping(
        shape_fingerprint="fp_other",
        source_type="telecom_cdr",
        provider="telecom_x",
        record_type="call_event",
        mappings=[MappingCandidate(source_field="A", target_field="party_a.msisdn", confidence=0.9, rationale="", relation="exact")],
    )
    promote_mapping(payload, db)
    # Search with different fingerprint and provider, but same source_type
    groups = grouped_examples(shape_fingerprint="fp_nonexistent", source_type="telecom_cdr", provider="nonexistent", db_path=db)
    assert len(groups) == 1
    assert groups[0]["match_basis"] == "source_type"
