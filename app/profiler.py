from __future__ import annotations

import csv
import io
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO

from app.models import FieldProfile, RecordProfile
from app.patterns import PHONE_RE, CELL_ID_RE, looks_like_phone, looks_like_datetime
from app.shape import fingerprint_fields

logger = logging.getLogger("greyline.profiler")

CSV_DELIMITERS = [",", ";", "\t", "|"]
DEFAULT_SAMPLE_ROWS = 500
DEFAULT_SNIFF_BYTES = 256 * 1024


def _bytes_to_text(data: bytes) -> str:
    return data.decode("utf-8", errors="replace")


def guess_format(filename: str, sample_bytes: bytes) -> str:
    lower = filename.lower()
    if lower.endswith(".jsonl") or lower.endswith(".ndjson"):
        result = "jsonl"
        logger.debug("detected format=%s for %s", result, filename)
        return result
    if lower.endswith(".json"):
        result = "json"
        logger.debug("detected format=%s for %s", result, filename)
        return result
    if lower.endswith(".csv") or lower.endswith(".tsv"):
        result = "csv"
        logger.debug("detected format=%s for %s", result, filename)
        return result
    stripped = _bytes_to_text(sample_bytes).lstrip()
    if stripped.startswith("{") or stripped.startswith("["):
        result = "json"
        logger.debug("detected format=%s for %s", result, filename)
        return result
    if "\n{" in stripped:
        result = "jsonl"
        logger.debug("detected format=%s for %s", result, filename)
        return result
    result = "csv"
    logger.debug("detected format=%s for %s", result, filename)
    return result


def _sniff_csv_delimiter(sample_text: str) -> str:
    lines = [line for line in sample_text.splitlines() if line.strip()][:10]
    if not lines:
        raise ValueError("CSV input appears empty")

    best_delim = ","
    best_score = -1
    for delim in CSV_DELIMITERS:
        counts = [line.count(delim) for line in lines]
        if not counts or max(counts) <= 0:
            continue
        non_zero = [c for c in counts if c > 0]
        score = min(non_zero) if non_zero else 0
        if score > best_score:
            best_score = score
            best_delim = delim
    return best_delim


def _read_csv_from_text(text: str, sample_rows: int = DEFAULT_SAMPLE_ROWS) -> tuple[list[dict[str, Any]], str]:
    delim = _sniff_csv_delimiter(text)
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)
    rows: list[dict[str, Any]] = []
    for idx, row in enumerate(reader):
        rows.append(row)
        if idx + 1 >= sample_rows:
            break
    if not reader.fieldnames:
        raise ValueError("Unable to parse CSV headers")
    return rows, delim


def _sample_csv_stream(
    fileobj: BinaryIO,
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
    sniff_bytes: int = DEFAULT_SNIFF_BYTES,
) -> tuple[list[dict[str, Any]], str]:
    fileobj.seek(0)
    sniff = fileobj.read(sniff_bytes)
    fileobj.seek(0)
    delim = _sniff_csv_delimiter(_bytes_to_text(sniff))

    wrapper = io.TextIOWrapper(fileobj, encoding="utf-8", errors="replace", newline="")
    try:
        reader = csv.DictReader(wrapper, delimiter=delim)
        rows: list[dict[str, Any]] = []
        for idx, row in enumerate(reader):
            rows.append(dict(row))
            if idx + 1 >= sample_rows:
                break
        if not reader.fieldnames:
            raise ValueError("Unable to parse CSV headers")
        return rows, delim
    finally:
        try:
            wrapper.detach()
        except Exception:
            pass
        fileobj.seek(0)


def _read_json(data: bytes, sample_rows: int = DEFAULT_SAMPLE_ROWS) -> list[dict[str, Any]]:
    text = _bytes_to_text(data).strip()
    obj = json.loads(text)
    if isinstance(obj, list):
        return obj[:sample_rows]
    if isinstance(obj, dict):
        return [obj]
    raise ValueError("Unsupported JSON shape")


def _sample_jsonl_stream(fileobj: BinaryIO, sample_rows: int = DEFAULT_SAMPLE_ROWS) -> list[dict[str, Any]]:
    fileobj.seek(0)
    wrapper = io.TextIOWrapper(fileobj, encoding="utf-8", errors="replace", newline="")
    rows: list[dict[str, Any]] = []
    try:
        for line in wrapper:
            if not line.strip():
                continue
            rows.append(json.loads(line))
            if len(rows) >= sample_rows:
                break
        return rows
    finally:
        try:
            wrapper.detach()
        except Exception:
            pass
        fileobj.seek(0)


def _read_jsonl(data: bytes, sample_rows: int = DEFAULT_SAMPLE_ROWS) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in _bytes_to_text(data).splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
        if len(rows) >= sample_rows:
            break
    return rows


def _primitive_types(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    for value in values:
        if value is None or value == "":
            seen.add("null")
        elif isinstance(value, bool):
            seen.add("bool")
        elif isinstance(value, int) and not isinstance(value, bool):
            seen.add("int")
        elif isinstance(value, float):
            seen.add("float")
        else:
            sval = str(value)
            if sval.isdigit() or (sval.startswith("-") and sval[1:].isdigit()):
                seen.add("str:int")
            else:
                try:
                    float(sval)
                    seen.add("str:float")
                except Exception:
                    seen.add("str")
    return sorted(seen)


def _collect_columns(rows: list[dict[str, Any]]) -> list[str]:
    seen = []
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.append(key)
    return seen


def _semantic_hints(values: list[Any], name: str) -> list[str]:
    hints: set[str] = set()
    samples = [str(v).strip("'\"") for v in values if v not in (None, "")][:5]
    lname = name.lower()
    if any(token in lname for token in ["time", "date", "start", "end", "sent", "captured", "ts"]):
        hints.add("name:time_like")
    if any(token in lname for token in ["lat", "latitude"]):
        hints.add("name:latitude_like")
    if any(token in lname for token in ["lon", "lng", "longitude"]):
        hints.add("name:longitude_like")
    if any(token in lname for token in ["cell", "cgi", "lac", "cid", "tower"]):
        hints.add("name:cell_like")
    if any(token in lname for token in ["phone", "msisdn", "number", "party", "a_no", "b_no", "from", "to"]):
        hints.add("name:party_like")

    if samples and all(looks_like_phone(v) for v in samples[:3]):
        hints.add("value:phone_like")
    if samples and all(looks_like_datetime(v) for v in samples[:3]):
        hints.add("value:datetime_like")
    if samples and all(CELL_ID_RE.match(v) for v in samples[:3]):
        hints.add("value:identifier_like")

    try:
        floats = [float(v) for v in samples[:3]]
        if floats and all(-90 <= v <= 90 for v in floats):
            hints.add("value:latitude_possible")
        if floats and all(-180 <= v <= 180 for v in floats):
            hints.add("value:longitude_possible")
    except Exception:
        pass

    return sorted(hints)


def _build_profile(rows: list[dict[str, Any]], *, source_name: str, file_format: str, delimiter: str | None) -> RecordProfile:
    if not rows:
        raise ValueError("No rows found in input")

    columns = _collect_columns(rows)
    fields: list[FieldProfile] = []
    fingerprint_items: list[tuple[str, tuple[str, ...]]] = []

    for col in columns:
        values = [row.get(col) for row in rows]
        non_null = [v for v in values if v not in (None, "")]
        primitive_types = _primitive_types(non_null if non_null else values)
        null_rate = (len(values) - len(non_null)) / max(len(values), 1)
        unique_ratio = len(set(map(repr, non_null))) / max(len(non_null), 1) if non_null else 0.0
        sample_values = [repr(v)[:120] for v in non_null[:5]]
        fp_types = tuple(primitive_types)
        fingerprint_items.append((col, fp_types))
        fields.append(
            FieldProfile(
                name=col,
                sample_values=sample_values,
                primitive_types=primitive_types,
                semantic_hints=_semantic_hints(non_null if non_null else values, col),
                null_rate=round(null_rate, 4),
                unique_ratio=round(unique_ratio, 4),
            )
        )

    shape_fingerprint = fingerprint_fields(fingerprint_items)
    return RecordProfile(
        source_name=source_name,
        row_count_sampled=len(rows),
        file_format=file_format,
        delimiter=delimiter,
        fields=fields,
        shape_fingerprint=shape_fingerprint,
        sample_rows=rows[:5],
    )


def profile_bytes(data: bytes, filename: str, source_name: str, sample_rows: int = DEFAULT_SAMPLE_ROWS) -> RecordProfile:
    logger.info("profiling bytes filename=%s source_name=%s", filename, source_name)
    file_format = guess_format(filename, data[:DEFAULT_SNIFF_BYTES])
    delimiter = None
    if file_format == "csv":
        rows, delimiter = _read_csv_from_text(_bytes_to_text(data), sample_rows=sample_rows)
    elif file_format == "json":
        rows = _read_json(data, sample_rows=sample_rows)
    else:
        rows = _read_jsonl(data, sample_rows=sample_rows)
    return _build_profile(rows, source_name=source_name, file_format=file_format, delimiter=delimiter)


def profile_fileobj(fileobj: BinaryIO, filename: str, source_name: str, sample_rows: int = DEFAULT_SAMPLE_ROWS) -> RecordProfile:
    logger.info("profiling file=%s source_name=%s sample_rows=%d", filename, source_name, sample_rows)
    fileobj.seek(0)
    sniff = fileobj.read(DEFAULT_SNIFF_BYTES)
    file_format = guess_format(filename, sniff)
    delimiter = None

    if file_format == "csv":
        rows, delimiter = _sample_csv_stream(fileobj, sample_rows=sample_rows)
    elif file_format == "jsonl":
        rows = _sample_jsonl_stream(fileobj, sample_rows=sample_rows)
    else:
        fileobj.seek(0)
        data = fileobj.read()
        rows = _read_json(data, sample_rows=sample_rows)
        fileobj.seek(0)

    return _build_profile(rows, source_name=source_name, file_format=file_format, delimiter=delimiter)


def profile_path(path: str | Path, source_name: str, sample_rows: int = DEFAULT_SAMPLE_ROWS) -> RecordProfile:
    with open(path, "rb") as handle:
        return profile_fileobj(handle, Path(path).name, source_name, sample_rows=sample_rows)
