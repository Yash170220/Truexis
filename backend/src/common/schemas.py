"""Pydantic schemas for API requests and responses"""
from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


# Request Schemas

class FileUploadRequest(BaseModel):
    """File upload request schema"""
    facility_name: str = Field(..., min_length=1, max_length=255)
    reporting_period: str = Field(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$")

    @field_validator("reporting_period")
    @classmethod
    def validate_period(cls, v: str) -> str:
        """Validate reporting period format"""
        try:
            year, month = v.split("-")
            if not (1900 <= int(year) <= 2100):
                raise ValueError("Year must be between 1900 and 2100")
        except Exception:
            raise ValueError("Invalid reporting period format. Use YYYY-MM")
        return v


class MatchingReviewItem(BaseModel):
    """Single review item within a bulk review request"""
    indicator_id: UUID
    approved: bool
    corrected_match: Optional[str] = Field(None, max_length=255)
    notes: Optional[str] = Field(None, max_length=1000)

    @field_validator("corrected_match")
    @classmethod
    def validate_corrected_match(cls, v: Optional[str], info) -> Optional[str]:
        if not info.data.get("approved") and not v:
            raise ValueError("corrected_match required when approved=False")
        return v


class MatchingReviewRequest(BaseModel):
    """Bulk review request body for POST /{upload_id}"""
    reviews: List[MatchingReviewItem]


# Response Schemas

class UploadResponse(BaseModel):
    """Upload response schema"""
    upload_id: UUID
    filename: str
    status: str
    detected_headers: List[str]
    preview_data: Dict[str, List]

    model_config = {"from_attributes": True}


class MatchingResult(BaseModel):
    """Matching result schema"""
    indicator_id: UUID
    original_header: str
    matched_indicator: str
    confidence: float = Field(..., ge=0.0, le=1.0)
    requires_review: bool

    @field_validator("requires_review", mode="before")
    @classmethod
    def set_requires_review(cls, v, info) -> bool:
        """Auto-set requires_review based on confidence"""
        confidence = info.data.get("confidence", 0.0)
        return confidence < 0.85

    model_config = {"from_attributes": True}


class MatchingStatsSchema(BaseModel):
    """Matching statistics"""
    total_headers: int = 0
    auto_approved: int = 0
    needs_review: int = 0
    avg_confidence: float = 0.0


class MatchingResultItem(BaseModel):
    """Single matching result row"""
    indicator_id: UUID
    original_header: str
    matched_indicator: str
    confidence: float = Field(..., ge=0.0, le=1.0)
    requires_review: bool


class ReviewQueueItem(MatchingResultItem):
    """Review queue entry (extends result with reasoning)"""
    reasoning: Optional[str] = None


class MatchingResponse(BaseModel):
    """Consolidated GET response for matching"""
    upload_id: UUID
    status: str
    stats: MatchingStatsSchema
    results: List[MatchingResultItem] = Field(default_factory=list)
    review_queue: List[ReviewQueueItem] = Field(default_factory=list)


class ValidationError(BaseModel):
    """Validation error detail schema"""
    data_id: UUID
    rule_name: str
    severity: str
    message: str
    citation: Optional[str] = None

    model_config = {"from_attributes": True}


class ValidationResponse(BaseModel):
    """Validation response schema"""
    total_records: int = Field(..., ge=0)
    valid_count: int = Field(..., ge=0)
    error_count: int = Field(..., ge=0)
    warning_count: int = Field(..., ge=0)
    errors: List[ValidationError] = Field(default_factory=list)

    @field_validator("total_records")
    @classmethod
    def validate_totals(cls, v: int, info) -> int:
        """Ensure counts add up correctly"""
        valid = info.data.get("valid_count", 0)
        errors = info.data.get("error_count", 0)
        warnings = info.data.get("warning_count", 0)
        if valid + errors != v:
            raise ValueError("valid_count + error_count must equal total_records")
        return v


# Additional Common Schemas

class UploadMetadata(BaseModel):
    """Metadata about an uploaded file"""
    row_count: int = 0
    column_count: int = 0
    file_size_mb: Optional[float] = None
    facility_name: Optional[str] = None
    reporting_period: Optional[str] = None


class UploadDetailResponse(BaseModel):
    """Consolidated upload detail: status + preview in one response"""
    upload_id: UUID
    filename: str
    file_type: str
    status: str
    upload_time: datetime
    metadata: UploadMetadata
    headers: List[str] = Field(default_factory=list)
    preview: List[Dict] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)

    model_config = {"from_attributes": True}


class IndicatorListResponse(BaseModel):
    """List of matched indicators"""
    upload_id: UUID
    indicators: List[MatchingResult]
    total_count: int
    review_required_count: int


# --- Normalization schemas ---

class NormalizationSummarySchema(BaseModel):
    """Summary stats for normalization"""
    total_records: int = 0
    successfully_normalized: int = 0
    failed_normalization: int = 0
    normalization_rate: float = 0.0


class NormalizationConversion(BaseModel):
    """Single conversion type applied"""
    indicator: str
    from_unit: str
    to_unit: str
    conversion_factor: float
    conversion_source: str = "Unknown"
    record_count: int = 0


class NormalizationErrorItem(BaseModel):
    """Single normalization error"""
    indicator: str
    issue: str
    suggestion: str


class NormalizationDataSample(BaseModel):
    """Single row from normalized data"""
    data_id: UUID
    indicator: str
    original_value: float
    original_unit: str
    normalized_value: float
    normalized_unit: str


class NormalizationResponse(BaseModel):
    """Consolidated GET response for normalization"""
    upload_id: UUID
    status: str
    summary: NormalizationSummarySchema
    conversions: List[NormalizationConversion] = Field(default_factory=list)
    errors: List[NormalizationErrorItem] = Field(default_factory=list)
    data_sample: List[NormalizationDataSample] = Field(default_factory=list)


# --- Validation consolidated schemas ---

class ValidationSummarySchema(BaseModel):
    """Validation summary stats"""
    total_records: int = 0
    valid_records: int = 0
    records_with_errors: int = 0
    records_with_warnings: int = 0
    validation_pass_rate: float = 0.0
    unreviewed_errors: int = 0


class ValidationErrorItem(BaseModel):
    """Single validation error"""
    result_id: UUID
    indicator: str
    rule_name: str
    severity: str
    message: str
    actual_value: Optional[float] = None
    expected_range: Optional[List[float]] = None
    citation: Optional[str] = None
    suggested_fixes: List[str] = Field(default_factory=list)
    reviewed: bool = False
    reviewer_notes: Optional[str] = None


class ValidationWarningItem(BaseModel):
    """Single validation warning"""
    result_id: UUID
    rule_name: str
    severity: str
    message: str
    reviewed: bool = False


class ValidationDetailResponse(BaseModel):
    """Consolidated GET response for validation"""
    upload_id: UUID
    status: str
    industry: Optional[str] = None
    summary: ValidationSummarySchema
    error_breakdown: Dict[str, int] = Field(default_factory=dict)
    warning_breakdown: Dict[str, int] = Field(default_factory=dict)
    errors: List[ValidationErrorItem] = Field(default_factory=list)
    warnings: List[ValidationWarningItem] = Field(default_factory=list)


class ValidationReviewItem(BaseModel):
    """Single review action"""
    result_id: UUID
    reviewed: bool = True
    notes: Optional[str] = None


class ValidationReviewRequest(BaseModel):
    """Bulk review request for POST /{upload_id}"""
    reviews: List[ValidationReviewItem]


# --- Generation schemas ---

class GenerationRequest(BaseModel):
    """Request body for narrative generation"""
    sections: List[str] = Field(
        default=["management_approach", "methodology", "boundary"],
        description="Section types to generate",
    )
    indicators: Optional[List[str]] = Field(
        default=None,
        description="Indicator names to generate for (all if omitted)",
    )
    framework: str = Field(default="BRSR", description="Framework to use")
    include_recommendations: bool = Field(
        default=True,
        description="Include AI-powered improvement recommendations",
    )


class CitationDetail(BaseModel):
    """Individual citation verification"""
    reference: str = ""
    value: float = 0.0
    verified: bool = False


class NarrativeCitation(BaseModel):
    """Aggregate citation verification stats"""
    total_claims: int = 0
    verified_claims: int = 0
    verification_rate: float = 1.0


class NarrativeItem(BaseModel):
    """Single generated narrative"""
    indicator: str
    section: str
    content: str
    citations: List[CitationDetail] = Field(default_factory=list)
    verification_rate: float = 1.0
    word_count: int = 0


class RecommendationItem(BaseModel):
    """Single AI recommendation"""
    indicator: str = ""
    current_value: float = 0.0
    unit: str = ""
    industry_average: float = 0.0
    best_in_class: float = 0.0
    gap_percentage: float = 0.0
    status: str = ""
    priority: str = "low"
    suggestions: List[str] = Field(default_factory=list)


class GenerationSummary(BaseModel):
    """Summary stats for the generation batch"""
    total_narratives: int = 0
    total_citations: int = 0
    overall_verification_rate: float = 1.0
    high_priority_recommendations: int = 0


class GenerationResponse(BaseModel):
    """Response for POST /api/v1/generation/{upload_id}"""
    upload_id: UUID
    framework: str = "BRSR"
    narratives: List[NarrativeItem] = Field(default_factory=list)
    recommendations: Optional[List[RecommendationItem]] = None
    summary: GenerationSummary


# --- Provenance schemas ---

class ProvenanceActivity(BaseModel):
    """Activity that produced a lineage step"""
    type: str = Field(default="", description="Technical activity type")
    timestamp: str = Field(default="", description="When this activity happened (ISO format)")
    agent: str = Field(default="", description="System/user that performed this activity")
    what_happened: str = Field(
        default="",
        description="Plain-English explanation of this activity",
        examples=["Data was validated by the system"],
    )


class LineageStep(BaseModel):
    """Single step in the provenance chain"""
    step_number: int = Field(default=0, description="Step number in the lineage chain")
    entity_id: str = Field(description="Unique ID of the related data item")
    entity_type: str = Field(default="", description="Type of data item")
    entity_label: str = Field(
        default="",
        description="Human-readable name for this item type",
        examples=["Uploaded File", "Normalized Data", "Validation Results"],
    )
    activity: ProvenanceActivity = Field(default_factory=ProvenanceActivity)


class ProvenanceResponse(BaseModel):
    """Response for GET /api/v1/provenance/{entity_id}"""
    entity_id: str = Field(description="ID you asked to trace")
    entity_type: str = Field(default="", description="Type of the requested entity")
    entity_label: str = Field(
        default="",
        description="Human-readable label for the requested entity",
        examples=["Uploaded File", "Validation Results"],
    )
    simple_summary: str = Field(
        default="",
        description="Plain-English summary of the lineage result",
    )
    lineage_chain: List[LineageStep] = Field(default_factory=list)
    total_steps: int = 0


# --- Chat schemas ---

class ChatRequest(BaseModel):
    """Request body for chat endpoint"""
    question: str = Field(..., min_length=1, max_length=1000)
    session_id: Optional[str] = Field(
        None, description="Session ID for conversation history (auto-generated if empty)"
    )


class ChatMessage(BaseModel):
    """Single message in conversation history"""
    role: str = Field(description="'user' or 'assistant'")
    content: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ChatSource(BaseModel):
    """Single data source used in a chat answer"""
    indicator: str
    value: Optional[float] = None
    unit: str = ""
    period: Optional[str] = None
    facility: Optional[str] = None
    similarity: float = 0.0


class ChatResponse(BaseModel):
    """Response from the chat endpoint"""
    answer: str
    sources: List[ChatSource] = Field(default_factory=list)
    confidence: float = 0.0
    session_id: str = ""


class ErrorResponse(BaseModel):
    """Standard error response"""
    error: str
    detail: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
