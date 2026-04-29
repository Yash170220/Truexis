"""API endpoints for data normalization."""

from pathlib import Path
from typing import Dict
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.common.database import get_db
from src.common.schemas import NormalizationResponse, ErrorResponse
from src.normalization import (
    NormalizationService,
    UnitNormalizer,
    NormalizationError,
)

router = APIRouter(prefix="/api/v1/normalization", tags=["normalization"])


def get_normalization_service(db: Session = Depends(get_db)) -> NormalizationService:
    """Get normalization service instance."""
    conversion_factors_path = (
        Path(__file__).parent.parent.parent
        / "data"
        / "validation-rules"
        / "conversion_factors.json"
    )
    normalizer = UnitNormalizer(str(conversion_factors_path))
    return NormalizationService(normalizer, db)


@router.post(
    "/{upload_id}",
    responses={
        400: {"model": ErrorResponse, "description": "Normalization error"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
    summary="Trigger normalization",
    description="Run normalization for all matched indicators in an upload",
)
def process_normalization(
    upload_id: UUID,
    service: NormalizationService = Depends(get_normalization_service),
) -> Dict:
    try:
        summary = service.normalize_data(upload_id)
        return {
            "status": "completed",
            "processed_count": summary.successfully_normalized,
        }
    except NormalizationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Normalization failed: {str(e)}"
        )


@router.get(
    "/{upload_id}",
    response_model=NormalizationResponse,
    responses={
        404: {"model": ErrorResponse, "description": "Upload not found"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
    summary="Get normalization results",
    description="Consolidated view: summary, conversions, errors, and data sample",
)
def get_normalization_details(
    upload_id: UUID,
    limit: int = Query(100, ge=1, le=1000, description="Max records in data_sample"),
    offset: int = Query(0, ge=0, description="Offset for data_sample pagination"),
    service: NormalizationService = Depends(get_normalization_service),
) -> Dict:
    try:
        data = service.get_comprehensive_results(upload_id, limit=limit, offset=offset)
        if data is None:
            raise HTTPException(
                status_code=404, detail=f"Upload {upload_id} not found"
            )
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve normalization results: {str(e)}",
        )
