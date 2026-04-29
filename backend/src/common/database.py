"""Database connection and session management"""
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from src.common.config import settings

# Create database engine
engine = create_engine(
    settings.database.url,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator[Session, None, None]:
    """
    Dependency function to get database session
    
    Yields:
        Database session
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    """Create all tables if they don't exist. Called on FastAPI startup."""
    import logging
    logger = logging.getLogger(__name__)
    try:
        # Import all models so SQLAlchemy registers them against Base
        from src.common.models import (  # noqa: F401
            Base, Upload, MatchedIndicator, NormalizedData,
            ValidationResult, AuditLog,
        )
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created / verified OK")
    except Exception as exc:
        logger.error(f"init_db failed: {exc}")
        raise
