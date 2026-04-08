from __future__ import annotations

import io
import json
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from app.main import app
from tests.conftest import FakeClient


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def test_health_endpoint(client):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "ollama" in data


def test_ontology_endpoint(client):
    response = client.get("/ontology")
    assert response.status_code == 200
    data = response.json()
    assert "record_types" in data
    assert "fields" in data
    assert "call_event" in data["record_types"]


def test_profile_endpoint_csv(client):
    csv_data = b"A_NO,B_NO,START,DUR\n471,472,2026-04-02 14:03:11,62\n"
    response = client.post(
        "/profile",
        files={"file": ("test.csv", io.BytesIO(csv_data), "text/csv")},
        data={"source_name": "test_source", "sample_rows": "100"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["file_format"] == "csv"
    assert data["source_name"] == "test_source"
    assert len(data["fields"]) == 4


def test_profile_endpoint_jsonl(client):
    jsonl_data = b'{"device":"abc","lat":58.97,"lon":5.73}\n{"device":"def","lat":59.0,"lon":5.8}\n'
    response = client.post(
        "/profile",
        files={"file": ("test.jsonl", io.BytesIO(jsonl_data), "application/jsonl")},
        data={"source_name": "geo_source", "sample_rows": "100"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["file_format"] == "jsonl"


def test_profile_endpoint_empty_file(client):
    response = client.post(
        "/profile",
        files={"file": ("empty.csv", io.BytesIO(b""), "text/csv")},
        data={"source_name": "empty_source"},
    )
    assert response.status_code == 400


def test_validate_endpoint(client):
    profile_data = {
        "source_name": "test",
        "row_count_sampled": 3,
        "file_format": "csv",
        "delimiter": ",",
        "fields": [
            {"name": "A_NO", "sample_values": ["471"], "primitive_types": ["str"], "semantic_hints": [], "null_rate": 0.0, "unique_ratio": 1.0},
        ],
        "shape_fingerprint": "abc123",
        "sample_rows": [],
    }
    proposal_data = {
        "record_type": "call_event",
        "record_confidence": 0.9,
        "mappings": [
            {"source_field": "A_NO", "target_field": "party_a.msisdn", "confidence": 0.95, "rationale": "test", "relation": "exact"},
        ],
        "global_warnings": [],
    }
    response = client.post("/validate", json={"profile": profile_data, "proposal": proposal_data})
    assert response.status_code == 200
    data = response.json()
    assert "accepted" in data
    assert "needs_review" in data
    assert "rejected" in data


def test_validate_endpoint_missing_key(client):
    response = client.post("/validate", json={})
    assert response.status_code == 400


def test_promote_endpoint(client):
    payload = {
        "shape_fingerprint": "abc123",
        "source_type": "telecom_cdr",
        "provider": "telecom_x",
        "record_type": "call_event",
        "approved_by": "analyst",
        "notes": "test",
        "mappings": [
            {"source_field": "A_NO", "target_field": "party_a.msisdn", "confidence": 0.95, "rationale": "test", "relation": "exact"},
        ],
    }
    response = client.post("/promote", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["stored"] == 1


def test_memory_search_endpoint(client):
    response = client.get("/memory/search")
    assert response.status_code == 200
    data = response.json()
    assert "results" in data


def test_memory_search_with_params(client):
    response = client.get("/memory/search", params={"provider": "telecom_x"})
    assert response.status_code == 200


def test_map_file_endpoint(monkeypatch, client):
    monkeypatch.setattr(httpx, "Client", FakeClient)
    csv_data = b"A_NO,B_NO,START,DUR,CGI\n471,472,2026-04-02 14:03:11,62,234-15-90123\n"
    response = client.post(
        "/map-file",
        files={"file": ("test.csv", io.BytesIO(csv_data), "text/csv")},
        data={"source_name": "test_source", "provider": "telecom_x", "source_type": "telecom_cdr", "sample_rows": "100"},
    )
    assert response.status_code == 200
    data = response.json()
    assert "profile" in data
    assert "proposal" in data
    assert "validation" in data
