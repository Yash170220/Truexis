"""API endpoints for data validation"""
from typing import Optional
from uuid import UUID
from pathlib import Path

from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session

from src.validation.engine import ValidationEngine
from src.validation.service import ValidationService
from src.common.database import get_db
from src.common.schemas import (
    ValidationDetailResponse,
    ValidationReviewRequest,
    ErrorResponse,
)

router = APIRouter(prefix="/api/v1/validation", tags=["validation"])

RULES_PATH = (
    Path(__file__).parent.parent.parent
    / "data"
    / "validation-rules"
    / "validation_rules.json"
)
try:
    validation_engine = ValidationEngine(str(RULES_PATH))
except FileNotFoundError:
    validation_engine = None


def get_validation_service(db: Session = Depends(get_db)) -> ValidationService:
    if validation_engine is None:
        raise HTTPException(status_code=503, detail="Validation engine not initialized")
    return ValidationService(validation_engine, db)


@router.post(
    "/{upload_id}",
    responses={
        404: {"model": ErrorResponse, "description": "Upload not found"},
        422: {"model": ErrorResponse, "description": "Validation error"},
        500: {"model": ErrorResponse, "description": "Server error"},
        503: {"model": ErrorResponse, "description": "Engine not ready"},
    },
    summary="Run validation or save reviews",
    description=(
        "Dual-purpose endpoint. "
        "Empty body + industry param: runs validation. "
        "Body with reviews list: saves review decisions."
    ),
)
async def process_or_review(
    upload_id: UUID,
    body: Optional[ValidationReviewRequest] = None,
    industry: Optional[str] = Query(None, description="Industry for validation (e.g. cement_industry)"),
    service: ValidationService = Depends(get_validation_service),
):
    # Mode 2: body has reviews -> save them
    if body and body.reviews:
        saved = 0
        for item in body.reviews:
            try:
                service.mark_error_as_reviewed(
                    result_id=item.result_id,
                    reviewer="api",
                    notes=item.notes or "",
                )
                saved += 1
            except ValueError as e:
                raise HTTPException(status_code=404, detail=str(e))

        unreviewed = service.get_unreviewed_errors(upload_id)
        return {
            "status": "reviews_saved",
            "count": saved,
            "unreviewed_errors_remaining": len(unreviewed),
        }

    # Mode 1: no body -> trigger validation
    if not industry:
        raise HTTPException(
            status_code=422,
            detail="Query parameter 'industry' is required when running validation",
        )
    try:
        summary = service.validate_upload(upload_id, industry)
        return {
            "status": "completed",
            "errors_found": summary.records_with_errors,
            "warnings_found": summary.records_with_warnings,
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation error: {str(e)}")


@router.get(
    "/{upload_id}",
    response_model=ValidationDetailResponse,
    responses={
        404: {"model": ErrorResponse, "description": "Upload not found"},
        500: {"model": ErrorResponse, "description": "Server error"},
        503: {"model": ErrorResponse, "description": "Engine not ready"},
    },
    summary="Get validation results",
    description="Consolidated view: summary, error/warning breakdowns, all errors, all warnings",
)
async def get_validation_details(
    upload_id: UUID,
    service: ValidationService = Depends(get_validation_service),
):
    try:
        data = service.get_comprehensive_results(upload_id)
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
            detail=f"Failed to retrieve validation results: {str(e)}",
        )
