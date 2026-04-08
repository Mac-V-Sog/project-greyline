from __future__ import annotations

import json
import logging
import os
from typing import Protocol

import httpx

from app.models import MapRequest, MappingProposal

logger = logging.getLogger("greyline.mapper")


class Mapper(Protocol):
    def map(self, req: MapRequest) -> MappingProposal:
        ...

    def ping(self) -> dict:
        ...


class OllamaMapper:
    def __init__(self, base_url: str | None = None, model: str | None = None) -> None:
        self.base_url = (base_url or os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")).rstrip("/")
        self.model = model or os.getenv("OLLAMA_MODEL", "qwen2.5:3b")

    def ping(self) -> dict:
        logger.debug("pinging ollama at %s", self.base_url)
        with httpx.Client(timeout=20.0) as client:
            tags = client.get(f"{self.base_url}/api/tags")
            tags.raise_for_status()
            models = tags.json().get("models", [])
        installed = {m.get("name", "") for m in models}
        return {
            "base_url": self.base_url,
            "model": self.model,
            "reachable": True,
            "model_installed": self.model in installed,
            "installed_models": sorted(installed),
        }

    def map(self, req: MapRequest) -> MappingProposal:
        logger.info("requesting mapping from ollama model=%s fields=%d", self.model, len(req.profile.fields))
        schema = MappingProposal.model_json_schema()
        payload = {
            "model": self.model,
            "messages": self._build_messages(req),
            "stream": False,
            "format": schema,
            "options": {"temperature": 0.0},
        }
        with httpx.Client(timeout=120.0) as client:
            response = client.post(f"{self.base_url}/api/chat", json=payload)
            response.raise_for_status()
            data = response.json()
        raw = data.get("message", {}).get("content", "")
        if not raw:
            logger.error("ollama returned empty response")
            raise RuntimeError("Ollama returned an empty response")
        parsed = json.loads(raw)
        proposal = MappingProposal.model_validate(parsed)
        if not proposal.mappings and proposal.record_confidence < 0.1:
            logger.error("ollama returned unusable proposal record_confidence=%.2f", proposal.record_confidence)
            raise RuntimeError("Ollama returned an unusable mapping proposal")
        logger.info("mapping complete record_type=%s confidence=%.2f mappings=%d", proposal.record_type, proposal.record_confidence, len(proposal.mappings))
        return proposal

    def _build_messages(self, req: MapRequest) -> list[dict[str, str]]:
        schema_json = json.dumps(MappingProposal.model_json_schema(), indent=2)
        ontology_summary = self._summarise_ontology(req.ontology)
        fields = [
            {
                "name": f.name,
                "sample_values": f.sample_values,
                "primitive_types": f.primitive_types,
                "semantic_hints": f.semantic_hints,
                "null_rate": f.null_rate,
                "unique_ratio": f.unique_ratio,
            }
            for f in req.profile.fields
        ]
        system = (
            "You map messy structured records into a narrow ontology. "
            "Be conservative. Do not guess when semantics are ambiguous. "
            "Return only JSON that matches the schema exactly."
        )
        user = f"""Task:
1. Choose the single best record_type from the ontology.
2. For each strong field correspondence, emit one mapping candidate.
3. Use relation='exact' only when the source field clearly matches the target semantics.
4. Use relation='close' when likely but not perfect. Use 'unknown' when uncertain.
5. Prefer fewer high-quality mappings over broad guessing.
6. Mention ambiguity in warnings and global_warnings.

Tradecraft:
- Use field names, sample values, primitive types, and semantic_hints together.
- If timestamps lack timezone, warn about it.
- For communications data, directionality matters: originating/sender/A-party is not the same as recipient/B-party.
- Do not map one source field to multiple target fields.
- Only use target fields listed under the chosen record_type.
- If prior examples conflict with current evidence, follow current evidence and warn.

Output schema:
{schema_json}

Source metadata:
{json.dumps(req.source_metadata, indent=2)}

Shape fingerprint:
{req.profile.shape_fingerprint}

Fields:
{json.dumps(fields, indent=2)}

Ontology summary:
{json.dumps(ontology_summary, indent=2)}

Prior approved examples:
{json.dumps(req.prior_examples[:8], indent=2)}
"""
        return [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]

    def _summarise_ontology(self, ontology: dict) -> dict:
        record_types = ontology.get("record_types", [])
        fields = ontology.get("fields", {})
        return {"record_types": record_types, "fields": fields}


def get_mapper() -> Mapper:
    return OllamaMapper()
