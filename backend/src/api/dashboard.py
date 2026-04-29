"""Dashboard API endpoint — returns all chart/summary data in one call."""
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.common.database import get_db
from src.generation.dashboard_service import DashboardService

router = APIRouter(prefix="/api/v1", tags=["dashboard"])


@router.get("/dashboard/{upload_id}")
def get_dashboard(upload_id: UUID, db: Session = Depends(get_db)):
    """Return consolidated dashboard data for frontend charts."""
    try:
        service = DashboardService(db)
        return service.build_dashboard(upload_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Dashboard error: {e}")