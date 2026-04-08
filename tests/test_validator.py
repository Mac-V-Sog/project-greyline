from pathlib import Path

import httpx

from app.mapper import OllamaMapper
from app.models import MapRequest
from app.ontology import load_ontology
from app.profiler import profile_bytes
from app.validator import validate_proposal
from tests.conftest import FakeClient


def test_validator_accepts_core_telecom_fields(monkeypatch):
    monkeypatch.setattr(httpx, "Client", FakeClient)
    data = Path("examples/telecom_cdr.csv").read_bytes()
    profile = profile_bytes(data, "telecom_cdr.csv", "telecom_x")
    proposal = OllamaMapper().map(MapRequest(profile=profile, ontology=load_ontology()))
    result = validate_proposal(profile, proposal)
    accepted_targets = {m.target_field for m in result.accepted}
    assert "party_a.msisdn" in accepted_targets
    assert "party_b.msisdn" in accepted_targets
    assert "duration_sec" in accepted_targets
    assert any(m.target_field == "start_time" for m in result.needs_review)
