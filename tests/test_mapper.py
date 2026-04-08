from pathlib import Path

import httpx

from app.mapper import OllamaMapper
from app.models import MapRequest
from app.ontology import load_ontology
from app.profiler import profile_bytes
from tests.conftest import FakeClient


def test_ollama_mapper_parses_structured_response(monkeypatch):
    monkeypatch.setattr(httpx, "Client", FakeClient)
    data = Path("examples/telecom_cdr.csv").read_bytes()
    profile = profile_bytes(data, "telecom_cdr.csv", "telecom_x")
    req = MapRequest(profile=profile, ontology=load_ontology(), source_metadata={"source_type": "telecom_cdr"})
    proposal = OllamaMapper().map(req)
    assert proposal.record_type == "call_event"
    targets = {m.target_field for m in proposal.mappings}
    assert "party_a.msisdn" in targets
    assert "party_b.msisdn" in targets
    assert "duration_sec" in targets
    assert any(m.relation == 'close' for m in proposal.mappings)
