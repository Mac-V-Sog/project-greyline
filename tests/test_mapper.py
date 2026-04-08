from pathlib import Path

import httpx

from app.mapper import OllamaMapper
from app.models import MapRequest
from app.ontology import load_ontology
from app.profiler import profile_bytes


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
        assert url.endswith('/api/chat')
        return _FakeResponse(
            {
                "message": {
                    "content": '{"record_type":"call_event","record_confidence":0.97,"mappings":[{"source_field":"A_NO","target_field":"party_a.msisdn","confidence":0.98,"rationale":"A-party number","relation":"exact"},{"source_field":"B_NO","target_field":"party_b.msisdn","confidence":0.98,"rationale":"B-party number","relation":"exact"},{"source_field":"START","target_field":"start_time","confidence":0.93,"rationale":"Timestamp-like values","relation":"close"},{"source_field":"DUR","target_field":"duration_sec","confidence":0.99,"rationale":"Duration values","relation":"exact"},{"source_field":"CGI","target_field":"cell_site.id","confidence":0.84,"rationale":"Cell identifier","relation":"close"}],"global_warnings":["Timezone not explicit"]}'
                }
            }
        )


def test_ollama_mapper_parses_structured_response(monkeypatch):
    monkeypatch.setattr(httpx, "Client", _FakeClient)
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
