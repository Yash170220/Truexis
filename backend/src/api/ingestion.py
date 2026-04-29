"""Ingestion API endpoints"""
import logging
from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from src.common.config import settings
from src.common.database import get_db
from src.common.models import Upload, UploadStatus
from src.common.schemas import UploadResponse, UploadDetailResponse, ErrorResponse
from src.ingestion.service import IngestionService
from src.ingestion.exceptions import ParseError, UnsupportedFileTypeError
from src.api.auth import get_current_user
from src.common.models import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ingest", tags=["ingestion"])


@router.get("/uploads")
async def list_uploads(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """List the current user's uploads — most recent first"""
    uploads = (
        db.query(Upload)
        .filter(Upload.user_id == current_user.id)
        .order_by(Upload.upload_time.desc())
        .limit(50)
        .all()
    )

    return {
        "uploads": [
            {
                "id": str(u.id),
                "filename": u.filename,
                "status": u.status.value if hasattr(u.status, 'value') else str(u.status),
                "upload_time": u.upload_time.isoformat() if u.upload_time else "",
                "facility_name": (u.file_metadata or {}).get("facility_name", ""),
                "industry": (u.file_metadata or {}).get("industry", ""),
            }
            for u in uploads
        ]
    }


@router.post(
    "/upload",
    response_model=UploadResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid file format"},
        413: {"model": ErrorResponse, "description": "File too large"},
        422: {"model": ErrorResponse, "description": "Validation error"},
        500: {"model": ErrorResponse, "description": "Server error"}
    },
    summary="Upload and ingest a file",
    description="Upload Excel or CSV file for ESG data ingestion"
)
async def upload_file(
    file: UploadFile = File(..., description="File to upload (.xlsx, .xls, .csv)"),
    facility_name: str = Form(..., description="Facility name"),
    reporting_period: str = Form(None, description="Reporting period (YYYY-MM)"),
    industry: str = Form(None, description="Industry sector"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Upload and ingest a file"""
    logger.info(f"Received upload request: {file.filename}")

    file_ext = Path(file.filename).suffix.lower()
    if file_ext not in settings.app.allowed_extensions:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid file type. Allowed: {', '.join(settings.app.allowed_extensions)}"
        )

    file.file.seek(0, 2)
    file_size = file.file.tell()
    file.file.seek(0)

    max_size = settings.app.max_file_size_mb * 1024 * 1024
    if file_size > max_size:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File too large. Maximum size: {settings.app.max_file_size_mb}MB"
        )

    try:
        service = IngestionService(db)

        # Use reporting_period if provided, otherwise default to current month
        period = reporting_period or "2024-01"

        result = service.ingest_file_from_upload(file, facility_name, period)

        # Attach owner and optional fields
        upload_record = db.query(Upload).filter(Upload.id == result.upload_id).first()
        if upload_record:
            upload_record.user_id = current_user.id
            if industry:
                upload_record.file_metadata = upload_record.file_metadata or {}
                upload_record.file_metadata["industry"] = industry
            db.commit()

        return UploadResponse(
            upload_id=result.upload_id,
            filename=result.filename,
            status="completed",
            detected_headers=result.headers,
            preview_data=result.preview
        )

    except UnsupportedFileTypeError as e:
        logger.error(f"Unsupported file type: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except ParseError as e:
        logger.error(f"Parse error: {e}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Failed to parse file: {str(e)}"
        )
    except Exception as e:
        logger.exception(f"Unexpected error during upload: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error during file processing"
        )


@router.get(
    "/{upload_id}",
    response_model=UploadDetailResponse,
    responses={
        404: {"model": ErrorResponse, "description": "Upload not found"}
    },
    summary="Get upload details",
    description="Get full upload info: status, metadata, headers, and first 10 rows preview"
)
async def get_upload_details(
    upload_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get consolidated upload details (status + preview)"""
    logger.info(f"Getting details for upload: {upload_id}")

    upload = db.query(Upload).filter(Upload.id == upload_id).first()
    if not upload:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Upload {upload_id} not found")
    if upload.user_id is not None and upload.user_id != current_user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

    service = IngestionService(db)
    details = service.get_upload_details(upload_id)

    if not details:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Upload {upload_id} not found"
        )

    return details


@router.delete(
    "/{upload_id}",
    status_code=status.HTTP_200_OK,
    responses={
        404: {"model": ErrorResponse, "description": "Upload not found"}
    },
    summary="Delete upload",
    description="Soft delete an upload (marks as deleted, keeps file for audit)"
)
async def delete_upload(
    upload_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Soft delete an upload"""
    logger.info(f"Deleting upload: {upload_id}")

    upload = db.query(Upload).filter(Upload.id == upload_id).first()

    if not upload:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Upload {upload_id} not found"
        )
    if upload.user_id is not None and upload.user_id != current_user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

    upload.status = UploadStatus.FAILED
    upload.file_metadata = upload.file_metadata or {}
    upload.file_metadata["deleted"] = True

    db.commit()

    logger.info(f"Successfully deleted upload: {upload_id}")

    return {
        "message": "Upload deleted successfully",
        "upload_id": str(upload_id),
        "note": "File kept for audit purposes"
    }