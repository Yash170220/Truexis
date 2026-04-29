"""API endpoint for W3C PROV-O provenance tracing."""
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import PlainTextResponse

from src.common.provenance import get_provenance_tracker
from src.common.schemas import (
    ErrorResponse,
    LineageStep,
    ProvenanceActivity,
    ProvenanceResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/provenance", tags=["provenance"])

ALLOWED_FORMATS = {"turtle", "json-ld", "xml", "n3", "nt"}
FORMAT_MEDIA_TYPES = {
    "turtle": "text/turtle",
    "json-ld": "application/ld+json",
    "xml": "application/rdf+xml",
    "n3": "text/n3",
    "nt": "application/n-triples",
}

ENTITY_LABELS = {
    "uploaded_file": "Uploaded File",
    "matched_indicator": "Matched Indicator",
    "normalized_dataset": "Normalized Data",
    "validation_results": "Validation Results",
}

ACTIVITY_EXPLANATIONS = {
    "file_ingestion": "File was uploaded and parsed",
    "header_matching": "File headers were matched to ESG indicators",
    "data_normalization": "Values were converted to standard units",
    "data_validation": "Data quality rules were checked",
}


@router.get(
    "/{entity_id}",
    response_model=ProvenanceResponse,
    responses={
        404: {"model": ErrorResponse, "description": "Entity not found"},
        400: {"model": ErrorResponse, "description": "Invalid format"},
    },
    summary="Trace provenance lineage",
    description=(
        "Returns the full derivation chain for an entity back to the "
        "original uploaded file. Use ?format=turtle to get raw W3C PROV graph."
    ),
)
async def trace_provenance(
    entity_id: str,
    format: Optional[str] = Query(
        None,
        description="RDF serialization format (turtle, json-ld, xml, n3, nt)",
    ),
):
    tracker = get_provenance_tracker()

    if not tracker.entity_exists(entity_id):
        raise HTTPException(
            status_code=404,
            detail=f"Entity '{entity_id}' not found in provenance graph",
        )

    if format is not None:
        fmt = format.lower()
        if fmt not in ALLOWED_FORMATS:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported format '{format}'. Use one of: {', '.join(sorted(ALLOWED_FORMATS))}",
            )
        rdf_data = tracker.export_provenance(entity_id, fmt=fmt)
        return PlainTextResponse(
            content=rdf_data,
            media_type=FORMAT_MEDIA_TYPES.get(fmt, "text/plain"),
        )

    entity_type = tracker.get_entity_type(entity_id) or ""
    lineage_raw = tracker.query_lineage(entity_id)

    chain = [
        LineageStep(
            step_number=idx + 1,
            entity_id=step["entity_id"],
            entity_type=step.get("entity_type", ""),
            entity_label=ENTITY_LABELS.get(step.get("entity_type", ""), step.get("entity_type", "")),
            activity=ProvenanceActivity(**step.get("activity", {})),
        )
        for idx, step in enumerate(lineage_raw)
    ]

    # Inject plain-English explanation for activity in each step
    for line_step in chain:
        line_step.activity.what_happened = ACTIVITY_EXPLANATIONS.get(
            line_step.activity.type,
            "This item was produced by a processing step",
        )

    label = ENTITY_LABELS.get(entity_type, entity_type)
    if chain:
        summary = (
            f"This {label or 'item'} was created through {len(chain)} step(s) "
            f"from earlier data."
        )
    else:
        summary = (
            f"This {label or 'item'} is a source item and has no earlier lineage."
        )

    return ProvenanceResponse(
        entity_id=entity_id,
        entity_type=entity_type,
        entity_label=label,
        simple_summary=summary,
        lineage_chain=chain,
        total_steps=len(chain),
    )
