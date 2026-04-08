from pathlib import Path

import httpx

from app.mapper import OllamaMapper
from app.models import MapRequest
from app.ontology import load_ontology
from app.profiler import profile_bytes
from app.validator import validate_proposal


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _FakeClient:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, url: str, json: dict):
        return _FakeResponse(
            {
                "message": {
                    "content": '{"record_type":"call_event","record_confidence":0.97,"mappings":[{"source_field":"A_NO","target_field":"party_a.msisdn","confidence":0.98,"rationale":"A-party number","relation":"exact"},{"source_field":"B_NO","target_field":"party_b.msisdn","confidence":0.98,"rationale":"B-party number","relation":"exact"},{"source_field":"START","target_field":"start_time","confidence":0.93,"rationale":"Timestamp-like values","relation":"close"},{"source_field":"DUR","target_field":"duration_sec","confidence":0.99,"rationale":"Duration values","relation":"exact"},{"source_field":"CGI","target_field":"cell_site.id","confidence":0.84,"rationale":"Cell identifier","relation":"close"}],"global_warnings":["Timezone not explicit"]}'
                }
            }
        )


def test_validator_accepts_core_telecom_fields(monkeypatch):
    monkeypatch.setattr(httpx, "Client", _FakeClient)
    data = Path("examples/telecom_cdr.csv").read_bytes()
    profile = profile_bytes(data, "telecom_cdr.csv", "telecom_x")
    proposal = OllamaMapper().map(MapRequest(profile=profile, ontology=load_ontology()))
    result = validate_proposal(profile, proposal)
    accepted_targets = {m.target_field for m in result.accepted}
    assert "party_a.msisdn" in accepted_targets
    assert "party_b.msisdn" in accepted_targets
    assert "duration_sec" in accepted_targets
    assert any(m.target_field == "start_time" for m in result.needs_review)
