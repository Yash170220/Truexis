"""Matching API endpoints"""
import logging
from typing import Optional
from uuid import UUID
from functools import lru_cache

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.common.database import get_db
from src.common.models import Upload, MatchedIndicator
from src.common.schemas import MatchingReviewRequest, MatchingResponse, ErrorResponse
from src.common.config import settings
from src.matching.service import MatchingService
from src.matching.rule_matcher import RuleBasedMatcher
from src.matching.llm_matcher import LLMMatcher

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/matching", tags=["matching"])


@lru_cache(maxsize=1)
def get_rule_matcher() -> RuleBasedMatcher:
    """Get cached rule matcher instance"""
    return RuleBasedMatcher("data/validation-rules/synonym_dictionary.json")


@lru_cache(maxsize=1)
def get_llm_matcher() -> LLMMatcher:
    """Get cached LLM matcher instance"""
    rule_matcher = get_rule_matcher()
    standard_indicators = [
        data["canonical_name"]
        for data in rule_matcher.indicators.values()
    ]
    return LLMMatcher(standard_indicators)


def get_matching_service(db: Session = Depends(get_db)) -> MatchingService:
    """Dependency to create matching service with cached matchers"""
    rule_matcher = get_rule_matcher()
    llm_matcher = get_llm_matcher()
    return MatchingService(rule_matcher, llm_matcher, db, actor="api")


@router.post(
    "/{upload_id}",
    status_code=status.HTTP_200_OK,
    responses={
        404: {"model": ErrorResponse, "description": "Upload not found"},
        422: {"model": ErrorResponse, "description": "Validation error"},
        500: {"model": ErrorResponse, "description": "Processing error"}
    },
    summary="Process matching or save reviews",
    description=(
        "Dual-purpose endpoint. "
        "Empty body: triggers auto-matching for all headers. "
        "Body with reviews list: saves manual review decisions."
    )
)
async def process_or_review(
    upload_id: UUID,
    body: Optional[MatchingReviewRequest] = None,
    db: Session = Depends(get_db),
    service: MatchingService = Depends(get_matching_service)
):
    upload = db.query(Upload).filter(Upload.id == upload_id).first()
    if not upload:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Upload {upload_id} not found"
        )

    # Mode 2: body has reviews -> save them
    if body and body.reviews:
        saved = 0
        for item in body.reviews:
            match = db.query(MatchedIndicator).filter(
                MatchedIndicator.id == item.indicator_id
            ).first()
            if not match:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Indicator {item.indicator_id} not found"
                )
            try:
                service.approve_match(
                    indicator_id=item.indicator_id,
                    approved=item.approved,
                    corrected_match=item.corrected_match,
                    notes=item.notes
                )
                saved += 1
            except ValueError as e:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=str(e)
                )
        return {"status": "reviews_saved", "count": saved}

    # Mode 1: no body -> trigger auto-matching
    metadata = upload.file_metadata or {}
    headers = metadata.get("column_names", [])
    if not headers:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="No headers found in upload metadata"
        )

    try:
        results = service.match_headers(upload_id, headers)
        return {"status": "completed", "processed_count": len(results)}
    except Exception as e:
        logger.exception(f"Error processing matching: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@router.get(
    "/{upload_id}",
    response_model=MatchingResponse,
    responses={
        404: {"model": ErrorResponse, "description": "Upload not found"}
    },
    summary="Get matching results",
    description="Consolidated view: stats, all results, and review queue in one response"
)
async def get_matching_details(
    upload_id: UUID,
    db: Session = Depends(get_db),
    service: MatchingService = Depends(get_matching_service)
):
    upload = db.query(Upload).filter(Upload.id == upload_id).first()
    if not upload:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Upload {upload_id} not found"
        )

    data = service.get_comprehensive_results(upload_id)
    if data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No matching data for upload {upload_id}"
        )

    return data
