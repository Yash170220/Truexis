"""Tests for Validation Service"""
import pytest
from uuid import uuid4
from pathlib import Path
from unittest.mock import Mock, MagicMock

from src.validation.engine import ValidationEngine, NormalizedRecord
from src.validation.service import ValidationService, ValidationSummary
from src.common.models import NormalizedData, ValidationResult as DBValidationResult, Severity


@pytest.fixture
def validation_engine():
    """Create validation engine with test rules"""
    rules_path = Path(__file__).parent.parent.parent / "data" / "validation-rules" / "validation_rules.json"
    return ValidationEngine(str(rules_path))


@pytest.fixture
def mock_db_session():
    """Create mock database session"""
    return Mock()


@pytest.fixture
def validation_service(validation_engine, mock_db_session):
    """Create validation service instance"""
    return ValidationService(validation_engine, mock_db_session)


@pytest.fixture
def sample_normalized_data():
    """Create sample normalized data records"""
    return [
        NormalizedData(
            id=uuid4(),
            upload_id=uuid4(),
            indicator_id=uuid4(),
            original_value=950.0,
            original_unit="kg CO₂/tonne",
            normalized_value=950.0,
            normalized_unit="kg CO₂/tonne",
            conversion_factor=1.0,
            conversion_source="no_conversion"
        ),
        NormalizedData(
            id=uuid4(),
            upload_id=uuid4(),
            indicator_id=uuid4(),
            original_value=1500.0,  # Invalid - too high
            original_unit="kg CO₂/tonne",
            normalized_value=1500.0,
            normalized_unit="kg CO₂/tonne",
            conversion_factor=1.0,
            conversion_source="no_conversion"
        )
    ]


class TestValidationService:
    """Test suite for ValidationService"""
    
    def test_service_initialization(self, validation_service):
        """Test service initializes correctly"""
        assert validation_service.engine is not None
        assert validation_service.db is not None
    
    def test_validate_indicator_batch(self, validation_service):
        """Test batch validation of records"""
        records = [
            NormalizedRecord(
                id=uuid4(),
                indicator="Scope 1 GHG Emissions per tonne clinker",
                value=950.0,
                unit="kg CO₂/tonne",
                original_value=950.0,
                original_unit="kg CO₂/tonne"
            ),
            NormalizedRecord(
                id=uuid4(),
                indicator="Scope 1 GHG Emissions per tonne clinker",
                value=1500.0,  # Invalid
                unit="kg CO₂/tonne",
                original_value=1500.0,
                original_unit="kg CO₂/tonne"
            )
        ]
        
        results = validation_service.validate_indicator_batch(records, "cement_industry")
        
        # Should have at least one validation failure
        assert len(results) > 0
        assert any(not r.is_valid for r in results)
    
    def test_generate_summary_all_valid(self, validation_service):
        """Test summary generation with all valid records"""
        normalized_records = [
            NormalizedData(
                id=uuid4(),
                upload_id=uuid4(),
                indicator_id=uuid4(),
                original_value=950.0,
                original_unit="kg CO₂/tonne",
                normalized_value=950.0,
                normalized_unit="kg CO₂/tonne",
                conversion_factor=1.0,
                conversion_source="no_conversion"
            )
        ]
        
        summary = validation_service._generate_summary(normalized_records, [])
        
        assert summary.total_records == 1
        assert summary.valid_records == 1
        assert summary.records_with_errors == 0
        assert summary.records_with_warnings == 0
        assert summary.validation_pass_rate == 100.0
    
    def test_generate_summary_with_errors(self, validation_service):
        """Test summary generation with validation errors"""
        from src.validation.engine import ValidationResult as EngineValidationResult
        
        record_id = uuid4()
        normalized_records = [
            NormalizedData(
                id=record_id,
                upload_id=uuid4(),
                indicator_id=uuid4(),
                original_value=1500.0,
                original_unit="kg CO₂/tonne",
                normalized_value=1500.0,
                normalized_unit="kg CO₂/tonne",
                conversion_factor=1.0,
                conversion_source="no_conversion"
            )
        ]
        
        validation_results = [
            EngineValidationResult(
                data_id=record_id,
                rule_name="cement_emission_range",
                is_valid=False,
                severity="error",
                message="Value outside range",
                citation="Test citation"
            )
        ]
        
        summary = validation_service._generate_summary(normalized_records, validation_results)
        
        assert summary.total_records == 1
        assert summary.records_with_errors == 1
        assert summary.validation_pass_rate == 0.0
        assert "cement_emission_range" in summary.error_breakdown
    
    def test_generate_recommendations_high_error_rate(self, validation_service):
        """Test recommendation generation for high error rates"""
        summary = ValidationSummary(
            total_records=100,
            valid_records=30,
            records_with_errors=70,
            records_with_warnings=0,
            validation_pass_rate=30.0,
            error_breakdown={"test_rule": 70}
        )
        
        recommendations = validation_service._generate_recommendations(summary, [], [])
        
        # Should recommend reviewing data collection
        assert any("Critical" in rec for rec in recommendations)
    
    def test_generate_recommendations_common_error(self, validation_service):
        """Test recommendation identifies most common error"""
        summary = ValidationSummary(
            total_records=100,
            valid_records=80,
            records_with_errors=20,
            records_with_warnings=0,
            validation_pass_rate=80.0,
            error_breakdown={
                "cement_emission_range": 15,
                "other_rule": 5
            }
        )
        
        recommendations = validation_service._generate_recommendations(summary, [], [])
        
        # Should identify cement_emission_range as most common
        assert any("cement_emission_range" in rec for rec in recommendations)
    
    def test_generate_recommendations_all_valid(self, validation_service):
        """Test recommendation for perfect validation"""
        summary = ValidationSummary(
            total_records=100,
            valid_records=100,
            records_with_errors=0,
            records_with_warnings=0,
            validation_pass_rate=100.0,
            error_breakdown={}
        )
        
        recommendations = validation_service._generate_recommendations(summary, [], [])
        
        # Should congratulate on success
        assert any("Excellent" in rec or "✅" in rec for rec in recommendations)
    
    def test_generate_recommendations_range_errors(self, validation_service):
        """Test recommendations for range validation errors"""
        summary = ValidationSummary(
            total_records=100,
            valid_records=80,
            records_with_errors=20,
            records_with_warnings=0,
            validation_pass_rate=80.0,
            error_breakdown={"cement_emission_range": 20}
        )
        
        errors = [
            {"rule_name": "cement_emission_range", "message": "Value outside range"}
        ] * 20
        
        recommendations = validation_service._generate_recommendations(summary, errors, [])
        
        # Should suggest checking unit conversions
        assert any("range" in rec.lower() or "unit" in rec.lower() for rec in recommendations)
    
    def test_generate_recommendations_outliers(self, validation_service):
        """Test recommendations for outlier detection"""
        summary = ValidationSummary(
            total_records=100,
            valid_records=95,
            records_with_errors=5,
            records_with_warnings=0,
            validation_pass_rate=95.0,
            error_breakdown={"detect_decimal_errors": 5}
        )
        
        errors = [
            {"rule_name": "detect_decimal_errors", "message": "Statistical outlier"}
        ] * 5
        
        recommendations = validation_service._generate_recommendations(summary, errors, [])
        
        # Should mention outliers
        assert any("outlier" in rec.lower() for rec in recommendations)
    
    def test_serialize_validation_result(self, validation_service):
        """Test serialization of database validation result"""
        db_result = DBValidationResult(
            id=uuid4(),
            data_id=uuid4(),
            rule_name="test_rule",
            is_valid=False,
            severity=Severity.ERROR,
            message="Test error message",
            citation="Test citation"
        )
        
        serialized = validation_service._serialize_validation_result(db_result)
        
        assert "id" in serialized
        assert "data_id" in serialized
        assert serialized["rule_name"] == "test_rule"
        assert serialized["is_valid"] is False
        assert serialized["severity"] == "error"


class TestValidationSummary:
    """Test ValidationSummary model"""
    
    def test_summary_creation(self):
        """Test creating validation summary"""
        summary = ValidationSummary(
            total_records=100,
            valid_records=80,
            records_with_errors=15,
            records_with_warnings=5,
            validation_pass_rate=85.0,
            error_breakdown={"rule1": 10, "rule2": 5},
            warning_breakdown={"rule3": 5}
        )
        
        assert summary.total_records == 100
        assert summary.valid_records == 80
        assert summary.validation_pass_rate == 85.0
        assert len(summary.error_breakdown) == 2
    
    def test_summary_serialization(self):
        """Test that summary can be serialized"""
        summary = ValidationSummary(
            total_records=100,
            valid_records=80,
            records_with_errors=15,
            records_with_warnings=5,
            validation_pass_rate=85.0
        )
        
        summary_dict = summary.model_dump()
        assert summary_dict["total_records"] == 100
        assert summary_dict["validation_pass_rate"] == 85.0


class TestValidationReport:
    """Test ValidationReport model"""
    
    def test_report_creation(self):
        """Test creating validation report"""
        from src.validation.service import ValidationReport
        
        summary = ValidationSummary(
            total_records=100,
            valid_records=80,
            records_with_errors=15,
            records_with_warnings=5,
            validation_pass_rate=85.0
        )
        
        report = ValidationReport(
            upload_id=uuid4(),
            summary=summary,
            errors=[],
            warnings=[],
            recommendations=["Fix the errors", "Review warnings"]
        )
        
        assert report.summary == summary
        assert len(report.recommendations) == 2
        assert report.generated_at is not None
    
    def test_report_with_data(self):
        """Test report with errors and warnings"""
        from src.validation.service import ValidationReport
        
        summary = ValidationSummary(
            total_records=100,
            valid_records=80,
            records_with_errors=15,
            records_with_warnings=5,
            validation_pass_rate=85.0
        )
        
        errors = [
            {
                "rule_name": "test_rule",
                "message": "Test error",
                "severity": "error"
            }
        ]
        
        warnings = [
            {
                "rule_name": "test_warning",
                "message": "Test warning",
                "severity": "warning"
            }
        ]
        
        report = ValidationReport(
            upload_id=uuid4(),
            summary=summary,
            errors=errors,
            warnings=warnings,
            recommendations=[]
        )
        
        assert len(report.errors) == 1
        assert len(report.warnings) == 1


class TestReviewFunctionality:
    """Test suite for validation review and bypass functionality"""
    
    def test_mark_error_as_reviewed(self, validation_service, mock_db_session):
        """Test marking error as reviewed"""
        from src.common.models import ValidationResult as DBValidationResult, Severity
        
        result_id = uuid4()
        
        # Mock validation result
        mock_result = DBValidationResult(
            id=result_id,
            data_id=uuid4(),
            rule_name="test_rule",
            is_valid=False,
            severity=Severity.ERROR,
            message="Test error",
            citation="Test",
            reviewed=False
        )
        
        # Mock query chain
        mock_query = Mock()
        mock_query.filter.return_value.first.return_value = mock_result
        mock_db_session.query.return_value = mock_query
        
        # Mark as reviewed
        validation_service.mark_error_as_reviewed(
            result_id,
            "test.reviewer@example.com",
            "False positive - special case"
        )
        
        # Verify it was marked as reviewed
        assert mock_result.reviewed is True
        assert mock_result.reviewer_notes == "False positive - special case"
    
    def test_suppress_warning(self, validation_service, mock_db_session):
        """Test suppressing warning"""
        from src.common.models import ValidationResult as DBValidationResult, Severity
        
        result_id = uuid4()
        
        # Mock validation result (warning)
        mock_result = DBValidationResult(
            id=result_id,
            data_id=uuid4(),
            rule_name="test_warning",
            is_valid=False,
            severity=Severity.WARNING,
            message="Test warning",
            citation="Test",
            reviewed=False
        )
        
        # Mock query chain
        mock_query = Mock()
        mock_query.filter.return_value.first.return_value = mock_result
        mock_db_session.query.return_value = mock_query
        
        # Suppress warning
        validation_service.suppress_warning(
            result_id,
            "Acceptable variance for this facility",
            "test.reviewer@example.com"
        )
        
        # Verify it was suppressed
        assert mock_result.reviewed is True
        assert "SUPPRESSED" in mock_result.reviewer_notes
    
    def test_suppress_error_fails(self, validation_service, mock_db_session):
        """Test that suppressing errors is not allowed"""
        from src.common.models import ValidationResult as DBValidationResult, Severity
        
        result_id = uuid4()
        
        # Mock validation result (error)
        mock_result = DBValidationResult(
            id=result_id,
            data_id=uuid4(),
            rule_name="test_error",
            is_valid=False,
            severity=Severity.ERROR,
            message="Test error",
            citation="Test",
            reviewed=False
        )
        
        # Mock query chain
        mock_query = Mock()
        mock_query.filter.return_value.first.return_value = mock_result
        mock_db_session.query.return_value = mock_query
        
        # Try to suppress error - should raise error
        with pytest.raises(ValueError, match="Cannot suppress errors"):
            validation_service.suppress_warning(result_id, "Test", "reviewer")
    
    def test_calculate_final_pass_rate(self, validation_service, mock_db_session):
        """Test final pass rate calculation"""
        upload_id = uuid4()
        
        # Mock 100 total records
        mock_db_session.query.return_value.filter.return_value.count.return_value = 100
        
        # Mock 10 unreviewed errors (90% pass rate)
        mock_unreviewed = [Mock(data_id=uuid4()) for _ in range(10)]
        mock_db_session.query.return_value.join.return_value.filter.return_value.all.return_value = mock_unreviewed
        
        final_rate = validation_service.calculate_final_pass_rate(upload_id)
        
        # Should be 90% (90 out of 100 pass)
        assert final_rate == 90.0
    
    def test_calculate_final_pass_rate_all_reviewed(self, validation_service, mock_db_session):
        """Test final pass rate when all errors reviewed"""
        upload_id = uuid4()
        
        # Mock 100 total records
        mock_db_session.query.return_value.filter.return_value.count.return_value = 100
        
        # Mock 0 unreviewed errors (100% pass rate)
        mock_db_session.query.return_value.join.return_value.filter.return_value.all.return_value = []
        
        final_rate = validation_service.calculate_final_pass_rate(upload_id)
        
        # Should be 100%
        assert final_rate == 100.0
    
    def test_get_review_summary(self, validation_service, mock_db_session):
        """Test review summary generation"""
        from src.common.models import ValidationResult as DBValidationResult, Severity
        
        upload_id = uuid4()
        
        # Mock validation results
        mock_results = [
            # 3 unreviewed errors
            Mock(severity=Severity.ERROR, is_valid=False, reviewed=False),
            Mock(severity=Severity.ERROR, is_valid=False, reviewed=False),
            Mock(severity=Severity.ERROR, is_valid=False, reviewed=False),
            # 2 reviewed errors
            Mock(severity=Severity.ERROR, is_valid=False, reviewed=True),
            Mock(severity=Severity.ERROR, is_valid=False, reviewed=True),
            # 5 warnings (2 suppressed)
            Mock(severity=Severity.WARNING, is_valid=False, reviewed=False),
            Mock(severity=Severity.WARNING, is_valid=False, reviewed=False),
            Mock(severity=Severity.WARNING, is_valid=False, reviewed=False),
            Mock(severity=Severity.WARNING, is_valid=False, reviewed=True),
            Mock(severity=Severity.WARNING, is_valid=False, reviewed=True),
        ]
        
        mock_db_session.query.return_value.join.return_value.filter.return_value.all.return_value = mock_results
        
        # Mock calculate_final_pass_rate
        validation_service.calculate_final_pass_rate = Mock(return_value=95.0)
        
        summary = validation_service.get_review_summary(upload_id)
        
        assert summary["total_errors"] == 5
        assert summary["reviewed_errors"] == 2
        assert summary["unreviewed_errors"] == 3
        assert summary["total_warnings"] == 5
        assert summary["suppressed_warnings"] == 2
        assert summary["active_warnings"] == 3
        assert summary["ready_for_export"] is False  # Has unreviewed errors
        assert summary["final_pass_rate"] == 95.0
    
    def test_ready_for_export(self, validation_service, mock_db_session):
        """Test ready for export status"""
        from src.common.models import ValidationResult as DBValidationResult, Severity
        
        upload_id = uuid4()
        
        # Mock all errors reviewed
        mock_results = [
            Mock(severity=Severity.ERROR, is_valid=False, reviewed=True),
            Mock(severity=Severity.ERROR, is_valid=False, reviewed=True),
        ]
        
        mock_db_session.query.return_value.join.return_value.filter.return_value.all.return_value = mock_results
        
        validation_service.calculate_final_pass_rate = Mock(return_value=100.0)
        
        summary = validation_service.get_review_summary(upload_id)
        
        assert summary["unreviewed_errors"] == 0
        assert summary["ready_for_export"] is True
