"""AI ESG Reporting System - Main Application"""
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.common.database import init_db
from src.api.auth import get_current_user, router as auth_router
from src.api.ingestion import router as ingestion_router
from src.api.matching import router as matching_router
from src.api.normalization import router as normalization_router
from src.api.validation import router as validation_router
from src.api.generation import router as generation_router
from src.api.provenance import router as provenance_router
from src.api.chat import router as chat_router
from src.api.dashboard import router as dashboard_router
from src.api.export import router as export_router
from src.generation.vector_store import VectorStore

app = FastAPI(
    title="AI ESG Reporting System",
    description="Automated ESG reporting with AI-powered data processing",
    version="0.1.0"
)


@app.on_event("startup")
def startup():
    init_db()
    try:
        VectorStore()
        print("Qdrant collections initialized successfully")
    except Exception as e:
        print(f"Failed to initialize Qdrant collections: {e}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pipeline & dashboard: JWT required (see /api/v1/auth/* for tokens)
_require_auth = [Depends(get_current_user)]

app.include_router(ingestion_router, dependencies=_require_auth)
app.include_router(matching_router, dependencies=_require_auth)
app.include_router(normalization_router, dependencies=_require_auth)
app.include_router(validation_router, dependencies=_require_auth)
app.include_router(generation_router, dependencies=_require_auth)
app.include_router(provenance_router, dependencies=_require_auth)
app.include_router(chat_router, dependencies=_require_auth)
app.include_router(dashboard_router, dependencies=_require_auth)
app.include_router(export_router, dependencies=_require_auth)
app.include_router(auth_router)


@app.get("/")
async def root():
    return {"message": "AI ESG Reporting System", "status": "running"}

@app.get("/health")
async def health():
    return {"status": "healthy"}
