from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from app.ingest import ingest_bundle_to_ndjson, ingest_to_ndjson
from app.memory import grouped_examples, init_db, promote_mapping, search_memory
from app.mapper import get_mapper
from app.models import ApprovedMapping, IngestBundleRequest, IngestRequest, MapRequest, MappingProposal, RecordProfile
from app.ontology import load_ontology
from app.profiler import DEFAULT_SAMPLE_ROWS, profile_fileobj
from app.validator import validate_proposal

router = APIRouter()
init_db()


@router.get('/health')
def health() -> dict[str, Any]:
    mapper = get_mapper()
    try:
        ollama = mapper.ping()
    except Exception as exc:
        return {'status': 'degraded', 'ollama': {'reachable': False, 'error': str(exc)}}
    status = 'ok' if ollama.get('model_installed') else 'degraded'
    return {'status': status, 'ollama': ollama}


@router.get('/ontology')
def ontology() -> dict[str, Any]:
    return load_ontology()


@router.post('/profile')
async def profile(
    file: UploadFile = File(...),
    source_name: str = Form('uploaded_source'),
    sample_rows: int = Form(DEFAULT_SAMPLE_ROWS),
) -> RecordProfile:
    try:
        return profile_fileobj(file.file, file.filename or 'uploaded', source_name, sample_rows=sample_rows)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/map')
def map_profile(req: MapRequest) -> MappingProposal:
    mapper = get_mapper()
    try:
        return mapper.map(req)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Mapper failed: {exc}') from exc


@router.post('/map-file')
async def map_file(
    file: UploadFile = File(...),
    source_name: str = Form('uploaded_source'),
    provider: str = Form(''),
    source_type: str = Form(''),
    ontology_json: str | None = Form(None),
    sample_rows: int = Form(DEFAULT_SAMPLE_ROWS),
) -> dict[str, Any]:
    try:
        profile = profile_fileobj(file.file, file.filename or 'uploaded', source_name, sample_rows=sample_rows)
        ontology = json.loads(ontology_json) if ontology_json else load_ontology()
        prior_examples = grouped_examples(
            shape_fingerprint=profile.shape_fingerprint,
            source_type=source_type or None,
            provider=provider or None,
        )
        req = MapRequest(
            profile=profile,
            ontology=ontology,
            source_metadata={
                'provider': provider,
                'source_type': source_type,
                'source_name': source_name,
            },
            prior_examples=prior_examples,
        )
        proposal = get_mapper().map(req)
        validation = validate_proposal(profile, proposal)
        return {
            'profile': profile.model_dump(),
            'proposal': proposal.model_dump(),
            'validation': validation.model_dump(),
            'prior_examples': prior_examples,
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/validate')
def validate(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        profile = RecordProfile.model_validate(payload['profile'])
        proposal = MappingProposal.model_validate(payload['proposal'])
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=f'Missing key: {exc}') from exc
    result = validate_proposal(profile, proposal)
    return result.model_dump()


@router.post('/promote')
def promote(payload: ApprovedMapping) -> dict[str, Any]:
    count = promote_mapping(payload)
    return {'stored': count}


@router.get('/memory/search')
def memory_search(shape_fingerprint: str | None = None, source_type: str | None = None, provider: str | None = None) -> dict[str, Any]:
    return {'results': search_memory(shape_fingerprint=shape_fingerprint, source_type=source_type, provider=provider)}


@router.post('/ingest-file')
async def ingest_file(
    file: UploadFile = File(...),
    mapping_json: str | None = Form(None),
    mapping_bundle_json: str | None = Form(None),
    source_name: str = Form('uploaded_source'),
    output_path: str | None = Form(None),
    max_rows: int | None = Form(None),
) -> dict[str, Any]:
    try:
        if mapping_bundle_json:
            payload = IngestBundleRequest.model_validate({
                **json.loads(mapping_bundle_json),
                'source_name': source_name,
                'output_path': output_path,
                'max_rows': max_rows,
            })
            return ingest_bundle_to_ndjson(
                file.file,
                file.filename or 'uploaded',
                families=payload.families,
                source_name=payload.source_name,
                output_path=payload.output_path,
                max_rows=payload.max_rows,
            )
        if mapping_json:
            payload = IngestRequest.model_validate({
                **json.loads(mapping_json),
                'source_name': source_name,
                'output_path': output_path,
                'max_rows': max_rows,
            })
            return ingest_to_ndjson(
                file.file,
                file.filename or 'uploaded',
                mappings=payload.mappings,
                record_type=payload.record_type,
                source_name=payload.source_name,
                output_path=payload.output_path,
                max_rows=payload.max_rows,
                family_id=payload.family_id,
                mapping_id=payload.mapping_id,
            )
        raise ValueError('Provide either mapping_json or mapping_bundle_json')
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
