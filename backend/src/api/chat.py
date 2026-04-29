"""API endpoint for conversational RAG chat over uploaded ESG data."""
import logging
import uuid as _uuid
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.common.config import settings
from src.common.database import get_db
from src.common.models import NormalizedData, Upload
from src.common.schemas import (
    ChatRequest,
    ChatResponse,
    ChatSource,
    ErrorResponse,
)
from src.generation.chat_service import ChatService
from src.generation.vector_store import VectorStore

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/chat", tags=["chat"])

_vector_store: VectorStore | None = None
_chat_service: ChatService | None = None


def _get_vector_store() -> VectorStore:
    global _vector_store
    if _vector_store is None:
        _vector_store = VectorStore()
    return _vector_store


def _get_chat_service() -> ChatService:
    global _chat_service
    if _chat_service is None:
        _chat_service = ChatService(
            vector_store=_get_vector_store(),
            api_key=settings.claude.api_key,
            model=settings.claude.model,
            redis_url=settings.redis.url,
        )
    return _chat_service


def _ensure_data_in_qdrant(upload_id: UUID, db: Session, vs: VectorStore) -> int:
    """Load validated data into Qdrant if not already loaded."""
    rows = (
        db.query(NormalizedData)
        .filter(NormalizedData.upload_id == upload_id)
        .all()
    )
    if not rows:
        return 0

    records = []
    for r in rows:
        indicator_name = (
            r.indicator.matched_indicator if r.indicator else str(r.indicator_id)
        )
        facility = None
        period = None
        upload = r.upload
        if upload and upload.file_metadata:
            facility = upload.file_metadata.get("facility_name")
            period = upload.file_metadata.get("reporting_period")

        records.append({
            "data_id": str(r.id),
            "indicator": indicator_name,
            "value": r.normalized_value,
            "unit": r.normalized_unit,
            "period": period or "",
            "facility": facility or "",
        })

    return vs.add_validated_data(upload_id, records)


@router.post(
    "/{upload_id}",
    response_model=ChatResponse,
    responses={
        404: {"model": ErrorResponse, "description": "Upload not found"},
        409: {"model": ErrorResponse, "description": "No data to chat about"},
        500: {"model": ErrorResponse, "description": "Chat service error"},
    },
    summary="Chat with your ESG data",
    description=(
        "Ask questions about your uploaded ESG data in plain English. "
        "Answers are sourced ONLY from your data — never from general knowledge."
    ),
)
async def chat_with_data(
    upload_id: UUID,
    body: ChatRequest,
    db: Session = Depends(get_db),
):
    upload = db.query(Upload).filter(Upload.id == upload_id).first()
    if not upload:
        raise HTTPException(status_code=404, detail=f"Upload {upload_id} not found")

    has_data = (
        db.query(NormalizedData)
        .filter(NormalizedData.upload_id == upload_id)
        .first()
    )
    if not has_data:
        raise HTTPException(
            status_code=409,
            detail="No normalized data found. Run the pipeline (match → normalize) first.",
        )

    try:
        vs = _get_vector_store()
        chat_svc = _get_chat_service()
    except Exception as exc:
        logger.error(f"Failed to init chat services: {exc}")
        raise HTTPException(
            status_code=500,
            detail="Chat services unavailable (Qdrant or Groq not reachable)",
        )

    try:
        _ensure_data_in_qdrant(upload_id, db, vs)
    except Exception as exc:
        logger.error(f"Failed to load data into Qdrant: {exc}")
        raise HTTPException(status_code=500, detail=f"Vector store error: {exc}")

    session_id = body.session_id or str(_uuid.uuid4())

    try:
        result = chat_svc.chat(
            upload_id=upload_id,
            question=body.question,
            session_id=session_id,
        )
    except Exception as exc:
        logger.error(f"Chat failed: {exc}")
        raise HTTPException(status_code=500, detail=f"Chat error: {exc}")

    sources = [ChatSource(**s) for s in result.get("sources", [])]

    return ChatResponse(
        answer=result["answer"],
        sources=sources,
        confidence=result.get("confidence", 0.0),
        session_id=session_id,
    )


@router.delete(
    "/history/{session_id}",
    summary="Clear chat history",
    description="Delete conversation history for a session.",
)
async def clear_chat_history(session_id: str):
    try:
        chat_svc = _get_chat_service()
        chat_svc.clear_history(session_id)
    except Exception:
        pass
    return {"status": "cleared", "session_id": session_id}


@router.get(
    "/history/{session_id}",
    summary="Get chat history",
    description="Retrieve conversation history for a session.",
)
async def get_chat_history(session_id: str):
    try:
        chat_svc = _get_chat_service()
        history = chat_svc._get_history(session_id)
    except Exception:
        history = []
    return {"session_id": session_id, "history": history}
