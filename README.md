<div align="center">

# Greyline

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-3776AB?logo=python&logoColor=white)](#requirements)
[![FastAPI](https://img.shields.io/badge/FastAPI-API-009688?logo=fastapi&logoColor=white)](#quick-start)
[![Ollama](https://img.shields.io/badge/Ollama-local%20LLM-000000?logo=ollama&logoColor=white)](#model-options)
[![Tests](https://img.shields.io/badge/tests-passing-2ea44f)](#tests)
[![License](https://img.shields.io/badge/license-MIT-blue)](#license)

*Local, LLM-assisted schema discovery and ingest for ugly structured data.*

</div>

Greyline is built for the awkward space between raw exports and usable records: large CSVs, mixed evidence-style dumps, telecom tables, handset extraction artefacts, and other datasets that are technically structured but operationally messy.

The model is used once, in a bounded way, to help identify record semantics and propose mappings into a target ontology. After that, Greyline processes the source deterministically in a streaming pipeline, preserves provenance, and separates accepted output from quarantined rows.

## What it is for

Greyline is meant for situations where the hard part is not opening a CSV. The hard part is working out what the columns actually mean, deciding how they map into a sane target model, and then pushing large volumes of data through that mapping without losing traceability.

Typical use cases include:

- large AXIOM-style or other forensic exports
- CDRs with inconsistent field names and formats
- mixed exports containing more than one record family
- ingestion front ends for search or analytics platforms such as Elasticsearch
- lawful processing environments where provenance and repeatability matter

## Design principles

- use the model for bounded semantic interpretation, not bulk transformation
- keep raw source truth intact
- prefer explicit uncertainty over confident nonsense
- validate deterministically before promoting meaning
- stream large files instead of dragging them whole into memory
- preserve provenance so every canonical field can be traced back
- quarantine bad or ambiguous rows rather than silently forcing them through

## Current capabilities

- bounded profiling for large CSV and NDJSON files
- shape fingerprinting and field-level semantic hints
- ontology-driven mapping proposals via a local Ollama-served model
- deterministic validation of mapping proposals
- governed mapping versions stored in SQLite
- chunked canonical ingest to NDJSON
- family-aware ingest for mixed exports
- quarantine output for rows that cannot be trusted
- per-run ingest stats with row counts, errors, coercions, and family breakdowns
- health checks for Ollama reachability and model availability

## Requirements

- Python 3.11+
- Ollama running locally or on a reachable host
- one pulled chat model served by Ollama

## Model options

Greyline is model-agnostic at the application layer. In practice, you want a small instruct model that is reasonably good at structured output, field-name interpretation, and short bursts of semantic classification.

Reasonable starting points:

- **Qwen2.5 3B Instruct**  
  Hugging Face: `https://huggingface.co/Qwen/Qwen2.5-3B-Instruct`  
  Good default for local experiments. Small enough to be practical, capable enough to do useful correspondence work.

- **Phi-4 Mini Instruct**  
  Hugging Face: `https://huggingface.co/microsoft/Phi-4-mini-instruct`  
  Worth trying when you want a compact model with strong instruction-following behaviour.

- **Gemma 4 E2B / E4B**  
  Hugging Face: `https://huggingface.co/google/gemma-4-E2B`  
  Hugging Face: `https://huggingface.co/google/gemma-4-E4B`  
  Google model card: `https://ai.google.dev/gemma/docs/core/model_card_4`  
  Better current Google-family option than Gemma 3. Worth testing if you want a modern small model with more headroom and are happy to validate prompt fit on your data.

A practical rule of thumb:

- start with **Qwen2.5 3B**
- compare against **Phi-4 Mini** on your own schema-mapping fixtures
- keep whichever one is more reliable on strict JSON output and field correspondence quality

## Quick start

### 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

### 2. Pull a model in Ollama

```bash
ollama pull qwen2.5:3b
```

### 3. Configure the service

```bash
export OLLAMA_BASE_URL=http://127.0.0.1:11434
export OLLAMA_MODEL=qwen2.5:3b
```

### 4. Run the API

```bash
uvicorn app.main:app --reload
```

### 5. Check health

```bash
curl http://127.0.0.1:8000/health
```

## How Greyline works

### 1. Profile the source

Greyline samples the file in a bounded way and builds a profile of the record structure:

- field names
- sample values
- primitive type hints
- semantic hints
- shape fingerprint

For large CSV or NDJSON files this is done in streaming mode, so schema discovery does not require parsing the entire export into memory.

### 2. Ask the model for candidate semantics

Greyline sends the compact profile, source metadata, and ontology options to a local model through Ollama. The model is asked to return structured JSON only.

The model proposes:

- likely record type
- field correspondences
- confidence
- relation type such as `exact`, `close`, `broad`, or `unknown`
- warnings where ambiguity remains

### 3. Validate the proposal

The proposal is checked with deterministic rules. The model does not get the final word.

Typical checks include:

- phone-like identifiers
- timestamp parsing
- numeric coercion
- geographic range checks
- duplicate target collisions

### 4. Promote an approved mapping

Once reviewed, mappings can be promoted into governed storage with a mapping ID, approval metadata, and ontology version.

### 5. Run full ingest deterministically

After a mapping is approved, Greyline can stream the full source file and emit canonical NDJSON without re-running the model on every row.

## Example workflow

### Profile a file

```bash
curl -X POST "http://127.0.0.1:8000/profile" \
  -F "file=@examples/telecom_cdr.csv" \
  -F "source_name=telecom_x_cdr" \
  -F "sample_rows=500"
```

### Map the file against the ontology

```bash
curl -X POST "http://127.0.0.1:8000/map-file" \
  -F "file=@examples/telecom_cdr.csv" \
  -F "source_name=telecom_x_cdr" \
  -F "provider=telecom_x" \
  -F "source_type=telecom_cdr" \
  -F "sample_rows=500"
```

### Promote an approved mapping

```bash
curl -X POST "http://127.0.0.1:8000/promote" \
  -H "Content-Type: application/json" \
  -d @approved_mapping.json
```

### Ingest a file using a specific governed mapping

```bash
curl -X POST "http://127.0.0.1:8000/ingest-file" \
  -F "file=@examples/telecom_cdr.csv" \
  -F "mapping_id=map_telecom_x_v1"
```

## Large-file behaviour

Greyline treats schema discovery and bulk ingest as separate jobs.

For discovery:

- CSV files are streamed and only sampled rows are parsed
- NDJSON files are streamed line by line and only sampled rows are parsed
- the model sees only the compact profile, not the whole file

For ingest:

- the approved mapping is applied row by row
- output is written as NDJSON
- accepted and quarantined rows are kept separate
- stats are written for each run

This keeps the expensive semantic work bounded while still allowing large exports to be processed end to end.

## Outputs from ingest

A typical ingest run produces:

- `out.ndjson` for accepted canonical records
- `out.quarantine.ndjson` for rows that could not be trusted
- `out.stats.json` for run telemetry
- optional per-family output files under a `.families/` directory when mixed-family routing is used

Canonical output includes:

- `record_type`
- canonical mapped fields
- source metadata including row number
- provenance showing which source fields produced which canonical fields
- governance metadata including mapping version details

## Family-aware ingest

Some large exports contain more than one row family. Greyline can accept a mapping bundle and route rows to different outputs based on source-field coverage against each family.

Rows that do not match any family strongly enough are quarantined as `unknown_family` rather than being forced into a bad mapping.

## Boundaries

Greyline is not trying to be a full casework or analysis platform.

It does not do entity resolution, investigative reasoning, or final evidential interpretation. Its job is narrower and more useful than that:

- recognise likely schema meaning
- preserve uncertainty
- validate aggressively
- apply approved mappings at scale
- keep provenance intact

## Development notes

The design rationale behind the prompting and correspondence model lives in `docs/research-notes.md`.

## Repo polish

A tidy public repo usually needs a few basics besides code:

- a short, plain-language description at the top
- visible status badges
- clear model options, including current Gemma 4 links
- a direct quick start that works
- explicit boundaries so people know what the tool does not claim to do

This README aims to cover those without turning into marketing sludge.

## Tests

```bash
pytest -q
```

## License

MIT.
