from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from app.models import ApprovedMapping

DEFAULT_DB_PATH = str(Path(__file__).resolve().parent.parent / "schema_sidecar.db")


def get_conn(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    conn = get_conn(db_path)
    with conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mapping_memory (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              shape_fingerprint TEXT,
              source_type TEXT,
              provider TEXT,
              record_type TEXT,
              source_field TEXT,
              target_field TEXT,
              confidence REAL,
              rationale TEXT,
              relation TEXT DEFAULT 'exact',
              approved_by TEXT,
              created_at TEXT DEFAULT CURRENT_TIMESTAMP,
              notes TEXT
            )
            """
        )
        columns = {row['name'] for row in conn.execute("PRAGMA table_info(mapping_memory)").fetchall()}
        if 'relation' not in columns:
            conn.execute("ALTER TABLE mapping_memory ADD COLUMN relation TEXT DEFAULT 'exact'")
    conn.close()


def promote_mapping(payload: ApprovedMapping, db_path: str = DEFAULT_DB_PATH) -> int:
    conn = get_conn(db_path)
    count = 0
    with conn:
        for mapping in payload.mappings:
            conn.execute(
                """
                INSERT INTO mapping_memory (
                    shape_fingerprint, source_type, provider, record_type,
                    source_field, target_field, confidence, rationale, relation,
                    approved_by, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.shape_fingerprint,
                    payload.source_type,
                    payload.provider,
                    payload.record_type,
                    mapping.source_field,
                    mapping.target_field,
                    mapping.confidence,
                    mapping.rationale,
                    mapping.relation,
                    payload.approved_by,
                    payload.notes,
                ),
            )
            count += 1
    conn.close()
    return count


def search_memory(shape_fingerprint: str | None = None, source_type: str | None = None, provider: str | None = None, db_path: str = DEFAULT_DB_PATH) -> list[dict[str, Any]]:
    conn = get_conn(db_path)
    where = []
    params: list[Any] = []
    if shape_fingerprint:
        where.append("shape_fingerprint = ?")
        params.append(shape_fingerprint)
    if source_type:
        where.append("source_type = ?")
        params.append(source_type)
    if provider:
        where.append("provider = ?")
        params.append(provider)
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    query = f"SELECT * FROM mapping_memory {clause} ORDER BY created_at DESC LIMIT 200"
    rows = [dict(r) for r in conn.execute(query, params).fetchall()]
    conn.close()
    return rows


def grouped_examples(shape_fingerprint: str | None = None, source_type: str | None = None, provider: str | None = None, db_path: str = DEFAULT_DB_PATH) -> list[dict[str, Any]]:
    exact = search_memory(shape_fingerprint=shape_fingerprint, source_type=source_type, provider=provider, db_path=db_path)
    provider_only = []
    type_only = []
    if not exact and provider:
        provider_only = search_memory(provider=provider, db_path=db_path)
    if not exact and not provider_only and source_type:
        type_only = search_memory(source_type=source_type, db_path=db_path)
    rows = exact or provider_only or type_only
    by_record: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = row["record_type"]
        group = by_record.setdefault(key, {"record_type": key, "mappings": [], "match_basis": "shape" if exact else ("provider" if provider_only else "source_type")})
        group["mappings"].append({
            "source_field": row["source_field"],
            "target_field": row["target_field"],
            "confidence": row["confidence"],
            "relation": row.get("relation", "exact"),
        })
    return list(by_record.values())[:8]
