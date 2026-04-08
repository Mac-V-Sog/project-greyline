from __future__ import annotations

import re
from datetime import datetime

PHONE_RE = re.compile(r"^\+?\d{7,15}$")
CELL_ID_RE = re.compile(r"^[0-9A-Za-z_-]{4,32}$")

DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%Y/%m/%d %H:%M:%S",
]


def looks_like_phone(value: str) -> bool:
    compact = re.sub(r"[\s()-]", "", value)
    return bool(PHONE_RE.match(compact))


def looks_like_datetime(value: str) -> bool:
    for fmt in DATETIME_FORMATS:
        try:
            datetime.strptime(value, fmt)
            return True
        except ValueError:
            continue
    return value.isdigit() and len(value) >= 10
