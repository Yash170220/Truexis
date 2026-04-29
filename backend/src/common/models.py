"""SQLAlchemy database models"""
import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean, Column, DateTime, Enum, Float, ForeignKey,
    Index, JSON, String, Text
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class FileType(str, enum.Enum):
    XLSX = "xlsx"
    CSV = "csv"
    PDF = "pdf"


class UploadStatus(str, enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class MatchingMethod(str, enum.Enum):
    RULE = "rule"
    LLM = "llm"
    MANUAL = "manual"


class Severity(str, enum.Enum):
    ERROR = "error"
    WARNING = "warning"


class AuditAction(str, enum.Enum):
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
    REVIEWED = "reviewed"
    NORMALIZE = "normalize"


# ── NEW: User model ──────────────────────────────────────────
class User(Base):
    """User accounts"""
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False, unique=True)
    company = Column(String(255), nullable=True)
    industry = Column(String(100), nullable=True)
    hashed_password = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_users_email", "email"),
    )

    uploads = relationship("Upload", back_populates="user")

    def __repr__(self):
        return f"<User(id={self.id}, email={self.email}, company={self.company})>"
# ─────────────────────────────────────────────────────────────


class Upload(Base):
    """Tracks uploaded files"""
    __tablename__ = "uploads"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=True, index=True)
    filename = Column(String(255), nullable=False)
    file_type = Column(Enum(FileType), nullable=False)
    upload_time = Column(DateTime, nullable=False, default=datetime.utcnow)
    status = Column(Enum(UploadStatus), nullable=False, default=UploadStatus.PENDING)
    file_path = Column(String(512), nullable=False)
    file_metadata = Column(JSON, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="uploads")
    matched_indicators = relationship("MatchedIndicator", back_populates="upload", cascade="all, delete-orphan")
    normalized_data = relationship("NormalizedData", back_populates="upload", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_uploads_status", "status"),
        Index("ix_uploads_upload_time", "upload_time"),
        Index("ix_uploads_user_id", "user_id"),
    )

    def __repr__(self):
        return f"<Upload(id={self.id}, filename={self.filename}, status={self.status})>"


class MatchedIndicator(Base):
    """Entity matching results"""
    __tablename__ = "matched_indicators"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    upload_id = Column(UUID(as_uuid=True), ForeignKey("uploads.id", ondelete="CASCADE"), nullable=False)
    original_header = Column(String(255), nullable=False)
    matched_indicator = Column(String(255), nullable=False)
    confidence_score = Column(Float, nullable=False)
    matching_method = Column(Enum(MatchingMethod), nullable=False)
    reviewed = Column(Boolean, nullable=False, default=False)
    reviewer_notes = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    upload = relationship("Upload", back_populates="matched_indicators")
    normalized_data = relationship("NormalizedData", back_populates="indicator", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_matched_indicators_upload_id", "upload_id"),
        Index("ix_matched_indicators_confidence", "confidence_score"),
        Index("ix_matched_indicators_reviewed", "reviewed"),
    )

    def __repr__(self):
        return f"<MatchedIndicator(id={self.id}, original={self.original_header}, matched={self.matched_indicator})>"


class NormalizedData(Base):
    """Processed and normalized data"""
    __tablename__ = "normalized_data"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    upload_id = Column(UUID(as_uuid=True), ForeignKey("uploads.id", ondelete="CASCADE"), nullable=False)
    indicator_id = Column(UUID(as_uuid=True), ForeignKey("matched_indicators.id", ondelete="CASCADE"), nullable=False)
    original_value = Column(Float, nullable=False)
    original_unit = Column(String(50), nullable=False)
    normalized_value = Column(Float, nullable=False)
    normalized_unit = Column(String(50), nullable=False)
    conversion_factor = Column(Float, nullable=False)
    conversion_source = Column(String(255), nullable=False)
    facility = Column(String(255), nullable=True)
    period = Column(String(50), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    upload = relationship("Upload", back_populates="normalized_data")
    indicator = relationship("MatchedIndicator", back_populates="normalized_data")
    validation_results = relationship("ValidationResult", back_populates="data", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_normalized_data_upload_id", "upload_id"),
        Index("ix_normalized_data_indicator_id", "indicator_id"),
        Index("ix_normalized_data_facility", "facility"),
        Index("ix_normalized_data_period", "period"),
    )

    def __repr__(self):
        return f"<NormalizedData(id={self.id}, value={self.normalized_value} {self.normalized_unit})>"


class ValidationResult(Base):
    """Validation outcomes"""
    __tablename__ = "validation_results"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    data_id = Column(UUID(as_uuid=True), ForeignKey("normalized_data.id", ondelete="CASCADE"), nullable=False)
    rule_name = Column(String(255), nullable=False)
    is_valid = Column(Boolean, nullable=False)
    severity = Column(Enum(Severity), nullable=False)
    message = Column(Text, nullable=False)
    citation = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    data = relationship("NormalizedData", back_populates="validation_results")

    __table_args__ = (
        Index("ix_validation_results_data_id", "data_id"),
        Index("ix_validation_results_is_valid", "is_valid"),
        Index("ix_validation_results_severity", "severity"),
    )

    def __repr__(self):
        return f"<ValidationResult(id={self.id}, rule={self.rule_name}, valid={self.is_valid})>"


class GeneratedReport(Base):
    """Persisted AI-generated narrative report for an upload."""
    __tablename__ = "generated_reports"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    upload_id = Column(UUID(as_uuid=True), ForeignKey("uploads.id", ondelete="CASCADE"), nullable=False)
    framework = Column(String(50), nullable=False, default="BRSR")
    narratives = Column(JSON, nullable=False, default=list)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_generated_reports_upload_id", "upload_id"),
    )

    def __repr__(self):
        return f"<GeneratedReport(upload_id={self.upload_id}, framework={self.framework})>"


class AuditLog(Base):
    """Provenance tracking for all entities"""
    __tablename__ = "audit_log"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id = Column(UUID(as_uuid=True), nullable=False)
    entity_type = Column(String(50), nullable=False)
    action = Column(Enum(AuditAction), nullable=False)
    actor = Column(String(255), nullable=False)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    changes = Column(JSON, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_audit_log_entity_id", "entity_id"),
        Index("ix_audit_log_entity_type", "entity_type"),
        Index("ix_audit_log_timestamp", "timestamp"),
        Index("ix_audit_log_actor", "actor"),
    )

    def __repr__(self):
        return f"<AuditLog(id={self.id}, entity_type={self.entity_type}, action={self.action})>"