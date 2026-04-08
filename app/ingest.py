from __future__ import annotations

import csv
import io
import json
import logging
import re
import tempfile
from collections import Counter
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, NamedTuple

from app.family_classifier import choose_family
from app.models import MappingCandidate, MappingFamily
from app.patterns import DATETIME_FORMATS
from app.profiler import DEFAULT_SNIFF_BYTES, _bytes_to_text, _sniff_csv_delimiter, guess_format
from app.quarantine import QuarantineReason, QuarantineRecord

logger = logging.getLogger("greyline.ingest")

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "outputs"


def _ensure_output_dir() -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR


def _validate_output_path(output_path: str) -> Path:
    """Validate that output_path resolves to a location within OUTPUT_DIR."""
    resolved = Path(output_path).resolve()
    output_dir_resolved = OUTPUT_DIR.resolve()
    try:
        resolved.relative_to(output_dir_resolved)
    except ValueError:
        raise ValueError(f"output_path must be within {output_dir_resolved}, got {resolved}")
    return resolved


class CanonicalizeResult(NamedTuple):
    doc: dict[str, Any] | None
    quarantine: QuarantineRecord | None
    coercions: Counter[str]
    mapped_field_counts: Counter[str]
    null_field_counts: Counter[str]
    total_targets: int


_PHONE_CLEAN_RE = re.compile(r"[\s()\-]")
TIME_FIELDS = {"start_time", "end_time", "sent_time", "captured_time"}
FLOAT_FIELDS = {"location.lat", "location.lon", "cell_site.lat", "cell_site.lon"}


def iter_source_rows(fileobj: BinaryIO, filename: str) -> tuple[str, Iterator[dict[str, Any]]]:
    fileobj.seek(0)
    sniff = fileobj.read(DEFAULT_SNIFF_BYTES)
    fileobj.seek(0)
    file_format = guess_format(filename, sniff)
    if file_format == "csv":
        delim = _sniff_csv_delimiter(_bytes_to_text(sniff))
        return file_format, _iter_csv_rows(fileobj, delim)
    if file_format == "jsonl":
        return file_format, _iter_jsonl_rows(fileobj)
    if file_format == "json":
        return file_format, _iter_json_rows(fileobj)
    raise ValueError(f"Unsupported file format: {file_format}")


def _iter_csv_rows(fileobj: BinaryIO, delimiter: str) -> Iterator[dict[str, Any]]:
    wrapper = io.TextIOWrapper(fileobj, encoding="utf-8", errors="replace", newline="")
    try:
        reader = csv.DictReader(wrapper, delimiter=delimiter)
        for row in reader:
            yield dict(row)
    finally:
        try:
            wrapper.detach()
        except Exception:
            pass
        fileobj.seek(0)


def _iter_jsonl_rows(fileobj: BinaryIO) -> Iterator[dict[str, Any]]:
    wrapper = io.TextIOWrapper(fileobj, encoding="utf-8", errors="replace", newline="")
    try:
        for line in wrapper:
            if not line.strip():
                continue
            obj = json.loads(line)
            if isinstance(obj, dict):
                yield obj
    finally:
        try:
            wrapper.detach()
        except Exception:
            pass
        fileobj.seek(0)


def _iter_json_rows(fileobj: BinaryIO) -> Iterator[dict[str, Any]]:
    text = io.TextIOWrapper(fileobj, encoding="utf-8", errors="replace", newline="")
    try:
        obj = json.load(text)
    finally:
        try:
            text.detach()
        except Exception:
            pass
        fileobj.seek(0)
    if isinstance(obj, dict):
        yield obj
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                yield item
    else:
        raise ValueError("Unsupported JSON shape for ingest")


def _set_nested(d: dict[str, Any], path: str, value: Any) -> None:
    parts = path.split('.')
    cur = d
    for part in parts[:-1]:
        nxt = cur.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[part] = nxt
        cur = nxt
    cur[parts[-1]] = value


def _compact_phone(value: str) -> str:
    return _PHONE_CLEAN_RE.sub('', value).strip()


def _parse_datetime(value: str) -> str | None:
    value = value.strip()
    for fmt in DATETIME_FORMATS:
        try:
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc).isoformat().replace('+00:00', 'Z')
            return dt.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
        except ValueError:
            continue
    if value.isdigit() and len(value) >= 10:
        try:
            dt = datetime.fromtimestamp(int(value), tz=timezone.utc)
            return dt.isoformat().replace('+00:00', 'Z')
        except Exception:
            return None
    return None


def _coerce_value(value: Any, mapping: MappingCandidate) -> tuple[Any, str | None, str | None]:
    if value is None:
        return None, None, None
    sval = str(value).strip()
    if sval == '':
        return None, None, None

    transforms = set(mapping.transforms)
    target = mapping.target_field
    coercion: str | None = None

    if 'strip' in transforms:
        stripped = sval.strip()
        if stripped != sval:
            coercion = 'strip'
        sval = stripped
    if 'compact_phone' in transforms or target.endswith('msisdn'):
        compact = _compact_phone(sval)
        if compact != sval:
            coercion = 'compact_phone'
        sval = compact
        if not sval or not re.fullmatch(r'\+?\d{3,20}', sval):
            return sval, QuarantineReason.IDENTIFIER_INVALID.value, coercion
    if 'parse_datetime' in transforms or target in TIME_FIELDS:
        parsed = _parse_datetime(sval)
        if parsed is None:
            return sval, QuarantineReason.TIMESTAMP_PARSE_FAILED.value, coercion
        if parsed != sval:
            coercion = 'parse_datetime'
        sval = parsed
    if 'to_int' in transforms or target == 'duration_sec':
        try:
            parsed = int(float(sval))
            if str(parsed) != sval:
                coercion = 'to_int'
            return parsed, None, coercion
        except Exception:
            return sval, QuarantineReason.UNEXPECTED_VALUE_CLASS.value, coercion
    if 'to_float' in transforms or target in FLOAT_FIELDS:
        try:
            parsed = float(sval)
            if str(parsed) != sval:
                coercion = 'to_float'
            return parsed, None, coercion
        except Exception:
            return sval, QuarantineReason.UNEXPECTED_VALUE_CLASS.value, coercion
    return sval, None, coercion


def canonicalize_row(
    row: dict[str, Any],
    mappings: list[MappingCandidate],
    record_type: str,
    source_name: str,
    row_number: int,
    family_id: str = 'default',
    mapping_id: str | None = None,
) -> CanonicalizeResult:
    logger.debug("canonicalizing row=%d source_name=%s family_id=%s", row_number, source_name, family_id)
    canonical: dict[str, Any] = {}
    field_provenance: list[dict[str, Any]] = []
    warnings: list[str] = []
    failures: list[dict[str, Any]] = []
    coercions: Counter[str] = Counter()
    mapped_field_counts: Counter[str] = Counter()
    null_field_counts: Counter[str] = Counter()
    total_targets = 0

    for mapping in mappings:
        total_targets += 1
        raw_value = row.get(mapping.source_field)
        if raw_value is None and mapping.source_field not in row:
            failures.append({
                'source_field': mapping.source_field,
                'target_field': mapping.target_field,
                'reason': QuarantineReason.MAPPING_MISSING_REQUIRED_FIELD.value,
            })
            continue
        value, error, coercion = _coerce_value(raw_value, mapping)
        if error:
            failures.append({
                'source_field': mapping.source_field,
                'target_field': mapping.target_field,
                'reason': error,
                'raw_value': raw_value,
            })
            continue
        if value is None:
            null_field_counts.update([mapping.target_field])
            continue
        _set_nested(canonical, mapping.target_field, value)
        mapped_field_counts.update([mapping.target_field])
        if coercion:
            coercions.update([mapping.target_field])
        field_provenance.append(
            {
                'source_field': mapping.source_field,
                'target_field': mapping.target_field,
                'relation': mapping.relation,
                'confidence': mapping.confidence,
                'raw_value': raw_value,
                'transforms': mapping.transforms,
                'mapping_id': mapping_id,
                'family_id': family_id,
                'coercion': coercion,
            }
        )
        warnings.extend(mapping.warnings)

    if failures:
        logger.warning("row=%d quarantined reasons=%s", row_number, sorted({f['reason'] for f in failures}))
        quarantine = QuarantineRecord(
            row_number=row_number,
            source_name=source_name,
            source_type='ingest_record',
            raw_row=row,
            failure_stage='ingest',
            failure_reasons=sorted({f['reason'] for f in failures}),
            details=failures,
            record_type=record_type,
        )
        return CanonicalizeResult(
            doc=None,
            quarantine=quarantine,
            coercions=coercions,
            mapped_field_counts=mapped_field_counts,
            null_field_counts=null_field_counts,
            total_targets=total_targets,
        )

    if not field_provenance:
        logger.warning("row=%d quarantined reasons=%s", row_number, [QuarantineReason.UNKNOWN_FIELD_SEMANTICS.value])
        quarantine = QuarantineRecord(
            row_number=row_number,
            source_name=source_name,
            source_type='ingest_record',
            raw_row=row,
            failure_stage='ingest',
            failure_reasons=[QuarantineReason.UNKNOWN_FIELD_SEMANTICS.value],
            details=[{'reason': QuarantineReason.UNKNOWN_FIELD_SEMANTICS.value}],
            record_type=record_type,
        )
        return CanonicalizeResult(
            doc=None,
            quarantine=quarantine,
            coercions=coercions,
            mapped_field_counts=mapped_field_counts,
            null_field_counts=null_field_counts,
            total_targets=total_targets,
        )

    return CanonicalizeResult(
        doc={
            'family_id': family_id,
            'record_type': record_type,
            'canonical': canonical,
            'source': {
                'source_name': source_name,
                'row_number': row_number,
            },
            'governance': {
                'mapping_id': mapping_id,
                'family_id': family_id,
            },
            'provenance': {
                'raw_row': row,
                'field_mappings': field_provenance,
            },
            'warnings': sorted({w for w in warnings if w}),
        },
        quarantine=None,
        coercions=coercions,
        mapped_field_counts=mapped_field_counts,
        null_field_counts=null_field_counts,
        total_targets=total_targets,
    )


def _derive_output_paths(output_path: str | None) -> tuple[Path, Path, Path, Path]:
    _ensure_output_dir()
    if output_path is None:
        handle = tempfile.NamedTemporaryFile(prefix='accepted_', suffix='.ndjson', delete=False, dir=OUTPUT_DIR)
        accepted_path = Path(handle.name)
        handle.close()
    else:
        accepted_path = _validate_output_path(output_path)
        accepted_path.parent.mkdir(parents=True, exist_ok=True)
    base = accepted_path.with_suffix('')
    quarantine_path = base.parent / f'{base.name}.quarantine.ndjson'
    stats_path = base.parent / f'{base.name}.stats.json'
    family_dir = base.parent / f'{base.name}.families'
    family_dir.mkdir(parents=True, exist_ok=True)
    return accepted_path, quarantine_path, stats_path, family_dir


def ingest_to_ndjson(
    fileobj: BinaryIO,
    filename: str,
    mappings: list[MappingCandidate],
    record_type: str,
    source_name: str,
    output_path: str | None = None,
    max_rows: int | None = None,
    family_id: str = 'default',
    mapping_id: str | None = None,
) -> dict[str, Any]:
    family = MappingFamily(
        family_id=family_id,
        record_type=record_type,
        mappings=mappings,
        mapping_id=mapping_id,
        min_match_ratio=0.0,
    )
    bundle = ingest_bundle_to_ndjson(
        fileobj,
        filename,
        families=[family],
        source_name=source_name,
        output_path=output_path,
        max_rows=max_rows,
    )
    bundle['record_type'] = record_type
    return bundle


def ingest_bundle_to_ndjson(
    fileobj: BinaryIO,
    filename: str,
    families: list[MappingFamily],
    source_name: str,
    output_path: str | None = None,
    max_rows: int | None = None,
) -> dict[str, Any]:
    logger.info("starting bundle ingest source_name=%s families=%d max_rows=%s", source_name, len(families), max_rows)
    if not families:
        raise ValueError('At least one family mapping is required')

    file_format, iterator = iter_source_rows(fileobj, filename)
    accepted_path, quarantine_path, stats_path, family_dir = _derive_output_paths(output_path)

    rows_seen = 0
    rows_written = 0
    rows_quarantined = 0
    error_counts: Counter[str] = Counter()
    coercions_by_field: Counter[str] = Counter()
    mapped_field_counts: Counter[str] = Counter()
    null_field_counts: Counter[str] = Counter()
    target_field_totals: Counter[str] = Counter()
    family_counts: Counter[str] = Counter()
    family_outputs: dict[str, str] = {}
    family_file_handles: dict[str, Any] = {}

    with accepted_path.open('w', encoding='utf-8') as accepted_f, quarantine_path.open('w', encoding='utf-8') as quarantine_f:
        try:
            for idx, row in enumerate(iterator, start=1):
                rows_seen += 1
                decision = choose_family(row, families)
                if not decision.family_id:
                    quarantine = QuarantineRecord(
                        row_number=idx,
                        source_name=source_name,
                        source_type='ingest_record',
                        raw_row=row,
                        failure_stage='family_classification',
                        failure_reasons=[QuarantineReason.UNKNOWN_FAMILY.value],
                        details=[{
                            'reason': QuarantineReason.UNKNOWN_FAMILY.value,
                            'classifier_reason': decision.reason,
                            'matched_source_fields': decision.matched_source_fields,
                            'classifier_confidence': decision.confidence,
                        }],
                        record_type=None,
                    )
                    quarantine_f.write(quarantine.model_dump_json() + '\n')
                    rows_quarantined += 1
                    error_counts.update(quarantine.failure_reasons)
                else:
                    family = next(f for f in families if f.family_id == decision.family_id)
                    result = canonicalize_row(
                        row,
                        family.mappings,
                        record_type=family.record_type,
                        source_name=source_name,
                        row_number=idx,
                        family_id=family.family_id,
                        mapping_id=family.mapping_id,
                    )
                    coercions_by_field.update(result.coercions)
                    mapped_field_counts.update(result.mapped_field_counts)
                    null_field_counts.update(result.null_field_counts)
                    for mapping in family.mappings:
                        target_field_totals.update([mapping.target_field])
                    if result.quarantine is not None:
                        quarantine_f.write(result.quarantine.model_dump_json() + '\n')
                        rows_quarantined += 1
                        error_counts.update(result.quarantine.failure_reasons)
                    elif result.doc is not None:
                        payload = json.dumps(result.doc, ensure_ascii=False)
                        accepted_f.write(payload + '\n')
                        if family.family_id not in family_file_handles:
                            fam_path = family_dir / f'{family.family_id}.ndjson'
                            family_outputs[family.family_id] = str(fam_path)
                            family_file_handles[family.family_id] = fam_path.open('w', encoding='utf-8')
                        family_file_handles[family.family_id].write(payload + '\n')
                        family_counts.update([family.family_id])
                        rows_written += 1
                if max_rows is not None and rows_seen >= max_rows:
                    break
        finally:
            for handle in family_file_handles.values():
                handle.close()

    field_null_rates = {}
    for field, total in target_field_totals.items():
        if total:
            field_null_rates[field] = null_field_counts.get(field, 0) / total

    logger.info("ingest complete source_name=%s rows_seen=%d rows_written=%d rows_quarantined=%d", source_name, rows_seen, rows_written, rows_quarantined)
    stats = {
        'status': 'ok',
        'file_format': file_format,
        'rows_seen': rows_seen,
        'rows_written': rows_written,
        'rows_quarantined': rows_quarantined,
        'errors_by_reason': dict(error_counts),
        'coercions_by_field': dict(coercions_by_field),
        'mapped_field_counts': dict(mapped_field_counts),
        'field_null_rates': field_null_rates,
        'family_counts': dict(family_counts),
        'family_output_paths': family_outputs,
        'accepted_output_path': str(accepted_path),
        'quarantine_output_path': str(quarantine_path),
        'stats_path': str(stats_path),
    }
    stats_path.write_text(json.dumps(stats, indent=2), encoding='utf-8')
    return stats
