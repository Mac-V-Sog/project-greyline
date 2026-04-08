import io
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class FakeClient:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, url: str, json: dict):
        assert url.endswith('/api/chat')
        return FakeResponse(
            {
                "message": {
                    "content": '{"record_type":"call_event","record_confidence":0.97,"mappings":[{"source_field":"A_NO","target_field":"party_a.msisdn","confidence":0.98,"rationale":"A-party number","relation":"exact"},{"source_field":"B_NO","target_field":"party_b.msisdn","confidence":0.98,"rationale":"B-party number","relation":"exact"},{"source_field":"START","target_field":"start_time","confidence":0.93,"rationale":"Timestamp-like values","relation":"close"},{"source_field":"DUR","target_field":"duration_sec","confidence":0.99,"rationale":"Duration values","relation":"exact"},{"source_field":"CGI","target_field":"cell_site.id","confidence":0.84,"rationale":"Cell identifier","relation":"close"}],"global_warnings":["Timezone not explicit"]}'
                }
            }
        )
