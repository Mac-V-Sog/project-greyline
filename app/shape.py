from __future__ import annotations

import hashlib
from typing import Iterable


def fingerprint_fields(items: Iterable[tuple[str, tuple[str, ...]]]) -> str:
    normalised = [f"{name}:{'|'.join(sorted(set(types)))}" for name, types in sorted(items)]
    joined = "\n".join(normalised)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:16]
