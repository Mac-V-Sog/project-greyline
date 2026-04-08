from __future__ import annotations

from pathlib import Path
import json
from typing import Any


DEFAULT_ONTOLOGY = {
    "record_types": [
        "call_event",
        "sms_event",
        "device_location",
        "subscriber_record",
        "cell_site_reference",
    ],
    "fields": {
        "call_event": [
            "party_a.msisdn",
            "party_b.msisdn",
            "start_time",
            "end_time",
            "duration_sec",
            "cell_site.id",
        ],
        "sms_event": [
            "sender.msisdn",
            "recipient.msisdn",
            "sent_time",
            "message_text",
        ],
        "device_location": [
            "device.id",
            "captured_time",
            "location.lat",
            "location.lon",
            "location.source",
        ],
        "subscriber_record": [
            "subscriber.msisdn",
            "subscriber.name",
            "subscriber.account_id",
            "subscriber.address",
        ],
        "cell_site_reference": [
            "cell_site.id",
            "cell_site.lat",
            "cell_site.lon",
            "cell_site.label",
        ],
    },
}


def load_ontology(path: str | None = None) -> dict[str, Any]:
    if not path:
        return DEFAULT_ONTOLOGY
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)
