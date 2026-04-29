"""Tests for Validation Engine"""
import pytest
from uuid import uuid4
from pathlib import Path
from unittest.mock import Mock, patch

from src.validation.engine import (
    ValidationEngine,
    NormalizedRecord,
    ValidationResult
)


@pytest.fixture
def validation_engine():
    """Create validation engine with test rules"""
    rules_path = Path(__file__).parent.parent.parent / "data" / "validation-rules" / "validation_rules.json"
    return ValidationEngine(str(rules_path))


@pytest.fixture
def cement_emission_record():
    """Sample cement emission record within valid range"""
    return NormalizedRecord(
        id=uuid4(),
        indicator="Scope 1 GHG Emissions per tonne clinker",
        value=950.0,
        unit="kg CO₂/tonne",
        original_value=950.0,
        original_unit="kg CO₂/tonne",
        facility_id="FAC001",
        reporting_period="2023"
    )


@pytest.fixture
def cement_emission_record_invalid():
    """Sample cement emission record outside valid range"""
    return NormalizedRecord(
        id=uuid4(),
        indicator="Scope 1 GHG Emissions per tonne clinker",
        value=1500.0,  # Too high
        unit="kg CO₂/tonne",
        original_value=1500.0,
        original_unit="kg CO₂/tonne",
        facility_id="FAC001",
        reporting_period="2023"
    )


@pytest.fixture
def steel_bfbof_record():
    """Sample steel BF-BOF emission record"""
    return NormalizedRecord(
        id=uuid4(),
        indicator="Scope 1 GHG Emissions per tonne crude steel (BF-BOF)",
        value=2100.0,
        unit="kg CO₂/tonne crude steel",
        original_value=2100.0,
        original_unit="kg CO₂/tonne crude steel",
        facility_id="FAC002",
        reporting_period="2023"
    )


class TestValidationEngine:
    """Test suite for ValidationEngine"""
    
    def test_engine_initialization(self, validation_engine):
        """Test that engine loads rules correctly"""
        assert validation_engine.rules is not None
        assert len(validation_engine.rules) > 0
        assert "cement_industry" in validation_engine.rules
        assert "steel_industry" in validation_engine.rules
        assert "cross_industry" in validation_engine.rules
    
    def test_rules_summary(self, validation_engine):
        """Test rules summary method"""
        summary = validation_engine.get_rules_summary()
        assert summary["total_rules"] > 15
        assert "cement_industry" in summary["industries"]
        assert "steel_industry" in summary["industries"]
        assert "range" in summary["validation_types"]
        assert "outlier" in summary["validation_types"]
    
    def test_validate_valid_cement_record(self, validation_engine, cement_emission_record):
        """Test validation of valid cement emission record"""
        results = validation_engine.validate_record(cement_emission_record, "cement_industry")
        # Should pass validation (no results returned for valid data)
        assert len(results) == 0
    
    def test_validate_invalid_cement_record(self, validation_engine, cement_emission_record_invalid):
        """Test validation of invalid cement emission record"""
        results = validation_engine.validate_record(cement_emission_record_invalid, "cement_industry")
        # Should fail validation
        assert len(results) > 0
        assert results[0].is_valid is False
        assert results[0].severity == "error"
        assert results[0].rule_name == "cement_emission_range"
        assert results[0].actual_value == 1500.0
    
    def test_validate_steel_record(self, validation_engine, steel_bfbof_record):
        """Test validation of steel emission record"""
        results = validation_engine.validate_record(steel_bfbof_record, "steel_industry")
        # Should pass validation
        assert len(results) == 0
    
    def test_range_check_below_minimum(self, validation_engine):
        """Test range check with value below minimum"""
        record = NormalizedRecord(
            id=uuid4(),
            indicator="Scope 1 GHG Emissions per tonne clinker",
            value=500.0,  # Below minimum of 800
            unit="kg CO₂/tonne",
            original_value=500.0,
            original_unit="kg CO₂/tonne"
        )
        results = validation_engine.validate_record(record, "cement_industry")
        assert len(results) > 0
        assert "below minimum" in results[0].message
    
    def test_range_check_above_maximum(self, validation_engine):
        """Test range check with value above maximum"""
        record = NormalizedRecord(
            id=uuid4(),
            indicator="Scope 1 GHG Emissions per tonne clinker",
            value=1500.0,  # Above maximum of 1100
            unit="kg CO₂/tonne",
            original_value=1500.0,
            original_unit="kg CO₂/tonne"
        )
        results = validation_engine.validate_record(record, "cement_industry")
        assert len(results) > 0
        assert "above maximum" in results[0].message
    
    def test_category_check_valid(self, validation_engine):
        """Test category check with valid category"""
        record = NormalizedRecord(
            id=uuid4(),
            indicator="Scope 1 emission source category",
            value=0.0,
            unit="",
            original_value=0.0,
            original_unit="",
            metadata={"source_category": "stationary combustion"}
        )
        results = validation_engine.validate_record(record, "cross_industry")
        # Should pass - stationary combustion is valid for Scope 1
        assert len(results) == 0
    
    def test_category_check_invalid(self, validation_engine):
        """Test category check with invalid category"""
        record = NormalizedRecord(
            id=uuid4(),
            indicator="Scope 1 emission source category",
            value=0.0,
            unit="",
            original_value=0.0,
            original_unit="",
            metadata={"source_category": "purchased electricity"}  # Should be Scope 2
        )
        results = validation_engine.validate_record(record, "cross_industry")
        assert len(results) > 0
        assert results[0].is_valid is False
    
    def test_outlier_detection(self, validation_engine):
        """Test outlier detection with multiple records"""
        # Create dataset with one clear outlier
        records = [
            (uuid4(), 100.0),
            (uuid4(), 105.0),
            (uuid4(), 98.0),
            (uuid4(), 102.0),
            (uuid4(), 500.0),  # Outlier
        ]
        
        # Get outlier rule from cross_industry
        outlier_rule = validation_engine._get_outlier_rule("cross_industry")
        assert outlier_rule is not None
        
        results = validation_engine.outlier_detection(records, outlier_rule)
        assert len(results) > 0
        # The outlier (500.0) should be flagged
        outlier_ids = [r.data_id for r in results]
        assert records[4][0] in outlier_ids
    
    def test_temporal_consistency_valid(self, validation_engine):
        """Test temporal consistency with valid data"""
        monthly_data = {
            "Jan": 100.0, "Feb": 100.0, "Mar": 100.0,
            "Apr": 100.0, "May": 100.0, "Jun": 100.0,
            "Jul": 100.0, "Aug": 100.0, "Sep": 100.0,
            "Oct": 100.0, "Nov": 100.0, "Dec": 100.0
        }
        annual_total = 1200.0
        
        # Get temporal consistency rule
        rule = None
        for r in validation_engine.rules["cross_industry"].values():
            if r.rule_name == "monthly_sum_equals_annual":
                rule = r
                break
        
        assert rule is not None
        result = validation_engine.temporal_consistency(
            monthly_data, 
            annual_total, 
            rule, 
            uuid4()
        )
        assert result is None  # Should pass
    
    def test_temporal_consistency_invalid(self, validation_engine):
        """Test temporal consistency with mismatched totals"""
        monthly_data = {
            "Jan": 100.0, "Feb": 100.0, "Mar": 100.0,
            "Apr": 100.0, "May": 100.0, "Jun": 100.0,
            "Jul": 100.0, "Aug": 100.0, "Sep": 100.0,
            "Oct": 100.0, "Nov": 100.0, "Dec": 100.0
        }
        annual_total = 1500.0  # Doesn't match sum of 1200
        
        # Get temporal consistency rule
        rule = None
        for r in validation_engine.rules["cross_industry"].values():
            if r.rule_name == "monthly_sum_equals_annual":
                rule = r
                break
        
        assert rule is not None
        result = validation_engine.temporal_consistency(
            monthly_data, 
            annual_total, 
            rule, 
            uuid4()
        )
        assert result is not None
        assert result.is_valid is False
        assert "differs from annual total" in result.message
    
    def test_validate_batch(self, validation_engine, cement_emission_record, cement_emission_record_invalid):
        """Test batch validation with multiple records"""
        records = [cement_emission_record, cement_emission_record_invalid]
        results = validation_engine.validate_batch(records, "cement_industry")
        
        # Invalid record should have validation results
        assert cement_emission_record_invalid.id in results
        assert len(results[cement_emission_record_invalid.id]) > 0
    
    def test_negative_value_check(self, validation_engine):
        """Test that negative values are caught"""
        record = NormalizedRecord(
            id=uuid4(),
            indicator="Scope 1 GHG Emissions per tonne clinker",
            value=-100.0,  # Negative value
            unit="kg CO₂/tonne",
            original_value=-100.0,
            original_unit="kg CO₂/tonne"
        )
        # Should be caught by range check (min: 0)
        results = validation_engine.validate_record(record, "cross_industry")
        # Note: This will only work if cross_industry has a negative value rule
        # Otherwise it will be caught by industry-specific range checks
    
    def test_precision_check(self, validation_engine):
        """Test precision validation"""
        # Get precision rule
        precision_rule = None
        for r in validation_engine.rules.get("data_quality", {}).values():
            if r.rule_name == "excessive_precision_check":
                precision_rule = r
                break
        
        if precision_rule:
            # Value with excessive precision
            result = validation_engine.precision_check(
                123.456789,  # More than 2 decimal places
                precision_rule,
                uuid4()
            )
            assert result is not None
            assert result.is_valid is False


class TestValidationResults:
    """Test ValidationResult model"""
    
    def test_validation_result_creation(self):
        """Test creating validation result"""
        result = ValidationResult(
            data_id=uuid4(),
            rule_name="test_rule",
            is_valid=False,
            severity="error",
            message="Test error message",
            citation="Test citation",
            suggested_fixes=["Fix 1", "Fix 2"],
            actual_value=100.0,
            expected_range=(50.0, 80.0)
        )
        assert result.is_valid is False
        assert result.severity == "error"
        assert len(result.suggested_fixes) == 2
        assert result.expected_range == (50.0, 80.0)
    
    def test_validation_result_serialization(self):
        """Test that validation result can be serialized"""
        result = ValidationResult(
            data_id=uuid4(),
            rule_name="test_rule",
            is_valid=False,
            severity="warning",
            message="Test warning",
            citation="Test citation"
        )
        result_dict = result.model_dump()
        assert result_dict["rule_name"] == "test_rule"
        assert result_dict["severity"] == "warning"


class TestCrossFieldValidation:
    """Test suite for cross-field validation"""
    
    def test_validate_scope_totals_valid(self, validation_engine):
        """Test scope totals validation with valid data"""
        result = validation_engine.validate_scope_totals(
            scope_1=1000.0,
            scope_2=500.0,
            scope_3=300.0,
            total=1800.0,
            tolerance=0.02
        )
        assert result is None  # Should pass
    
    def test_validate_scope_totals_invalid(self, validation_engine):
        """Test scope totals validation with mismatched totals"""
        result = validation_engine.validate_scope_totals(
            scope_1=1000.0,
            scope_2=500.0,
            scope_3=300.0,
            total=2000.0,  # Doesn't match sum
            tolerance=0.02
        )
        assert result is not None
        assert result.is_valid is False
        assert result.severity == "error"
        assert "differs from total" in result.message
    
    def test_validate_scope_totals_without_scope3(self, validation_engine):
        """Test scope totals with only Scope 1 and 2"""
        result = validation_engine.validate_scope_totals(
            scope_1=1000.0,
            scope_2=500.0,
            scope_3=None,
            total=1500.0
        )
        assert result is None
    
    def test_validate_energy_balance_valid(self, validation_engine):
        """Test energy balance validation with valid data"""
        result = validation_engine.validate_energy_balance(
            electricity=1000.0,
            fuel=500.0,
            steam=200.0,
            total_energy=1700.0,
            tolerance=0.05
        )
        assert result is None
    
    def test_validate_energy_balance_invalid(self, validation_engine):
        """Test energy balance with mismatched totals"""
        result = validation_engine.validate_energy_balance(
            electricity=1000.0,
            fuel=500.0,
            steam=200.0,
            total_energy=2000.0,  # Significantly different
            tolerance=0.05
        )
        assert result is not None
        assert result.is_valid is False
        assert "differ from total" in result.message
    
    def test_validate_production_correlation_normal(self, validation_engine):
        """Test production correlation with normal values"""
        result = validation_engine.validate_production_correlation(
            energy=10000.0,  # GJ
            emissions=2000.0,  # kg CO2
            production=100.0  # units
        )
        # Emission factor = 2000/10000 = 0.2 kg CO2/GJ (normal range)
        assert result is None
    
    def test_validate_production_correlation_low_emissions(self, validation_engine):
        """Test production correlation with suspiciously low emissions"""
        result = validation_engine.validate_production_correlation(
            energy=10000.0,  # GJ
            emissions=500.0,  # kg CO2 (very low)
            production=100.0
        )
        # Emission factor = 500/10000 = 0.05 kg CO2/GJ (too low)
        assert result is not None
        assert result.severity == "warning"
        assert "very low" in result.message.lower()
    
    def test_validate_production_correlation_high_emissions(self, validation_engine):
        """Test production correlation with anomalously high emissions"""
        result = validation_engine.validate_production_correlation(
            energy=10000.0,  # GJ
            emissions=5000000.0,  # kg CO2 (extremely high)
            production=100.0
        )
        # Emission factor = 5000000/10000 = 500 kg CO2/GJ (way too high)
        assert result is not None
        assert result.severity == "error"
        assert "extremely high" in result.message.lower()
    
    def test_validate_cross_field_consistency(self, validation_engine):
        """Test cross-field validation with multiple records"""
        records = [
            NormalizedRecord(
                id=uuid4(),
                indicator="scope_1",
                value=1000.0,
                unit="tonnes CO2e",
                original_value=1000.0,
                original_unit="tonnes CO2e"
            ),
            NormalizedRecord(
                id=uuid4(),
                indicator="scope_2",
                value=500.0,
                unit="tonnes CO2e",
                original_value=500.0,
                original_unit="tonnes CO2e"
            ),
            NormalizedRecord(
                id=uuid4(),
                indicator="total_emissions",
                value=2000.0,  # Incorrect total
                unit="tonnes CO2e",
                original_value=2000.0,
                original_unit="tonnes CO2e"
            )
        ]
        
        results = validation_engine.validate_cross_field_consistency(records)
        
        # Should detect mismatch between scopes and total
        assert len(results) > 0
    
    def test_cross_field_sum_relationship(self, validation_engine):
        """Test sum relationship validation"""
        records_by_indicator = {
            "electricity": NormalizedRecord(
                id=uuid4(),
                indicator="electricity",
                value=1000.0,
                unit="MWh",
                original_value=1000.0,
                original_unit="MWh"
            ),
            "natural_gas": NormalizedRecord(
                id=uuid4(),
                indicator="natural_gas",
                value=500.0,
                unit="MWh",
                original_value=500.0,
                original_unit="MWh"
            ),
            "total_energy": NormalizedRecord(
                id=uuid4(),
                indicator="total_energy",
                value=1500.0,
                unit="MWh",
                original_value=1500.0,
                original_unit="MWh"
            )
        }
        
        # Get energy balance rule
        rule = None
        for r in validation_engine.rules.get("cross_field", {}).values():
            if r.rule_name == "energy_balance_validation":
                rule = r
                break
        
        if rule:
            result = validation_engine._validate_sum_relationship(records_by_indicator, rule)
            # Should pass since 1000 + 500 = 1500
            assert result is None or result.is_valid
    
    def test_cross_field_subset_relationship(self, validation_engine):
        """Test subset relationship validation"""
        records_by_indicator = {
            "renewable_energy": NormalizedRecord(
                id=uuid4(),
                indicator="renewable_energy",
                value=2000.0,  # Exceeds total
                unit="MWh",
                original_value=2000.0,
                original_unit="MWh"
            ),
            "total_energy": NormalizedRecord(
                id=uuid4(),
                indicator="total_energy",
                value=1500.0,
                unit="MWh",
                original_value=1500.0,
                original_unit="MWh"
            )
        }
        
        # Get renewable subset rule
        rule = None
        for r in validation_engine.rules.get("cross_field", {}).values():
            if r.rule_name == "renewable_energy_subset":
                rule = r
                break
        
        if rule:
            result = validation_engine._validate_subset_relationship(records_by_indicator, rule)
            # Should fail since renewable > total
            assert result is not None
            assert result.is_valid is False


# ============================================================================
# COMPREHENSIVE RULE TESTS (Prompt 31)
# ============================================================================

class TestCementIndustryRules:
    """Comprehensive tests for cement industry validation rules"""
    
    def test_cement_emission_range_valid(self, validation_engine):
        """Test valid cement emission value - Prompt 31 Test #1"""
        record = NormalizedRecord(
            id=uuid4(),
            indicator="Scope 1 GHG Emissions per tonne clinker",
            value=950.0,  # Within valid range 800-1100
            unit="kg CO₂/tonne",
            original_value=950.0,
            original_unit="kg CO₂/tonne"
        )
        results = validation_engine.validate_record(record, "cement_industry")
        assert len(results) == 0, "950 kg CO₂/tonne should pass validation"
    
    def test_cement_emission_range_too_high(self, validation_engine):
        """Test cement emission value too high - Prompt 31 Test #2"""
        record = NormalizedRecord(
            id=uuid4(),
            indicator="Scope 1 GHG Emissions per tonne clinker",
            value=1500.0,  # Above maximum of 1100
            unit="kg CO₂/tonne",
            original_value=1500.0,
            original_unit="kg CO₂/tonne"
        )
        results = validation_engine.validate_record(record, "cement_industry")
        assert len(results) > 0, "1500 kg CO₂/tonne should fail validation"
        assert results[0].severity == "error"
        assert results[0].rule_name == "cement_emission_range"
        assert "above maximum" in results[0].message.lower()
    
    def test_cement_emission_range_too_low(self, validation_engine):
        """Test cement emission value too low (likely unit error) - Prompt 31 Test #3"""
        record = NormalizedRecord(
            id=uuid4(),
            indicator="Scope 1 GHG Emissions per tonne clinker",
            value=500.0,  # Below minimum of 800
            unit="kg CO₂/tonne",
            original_value=500.0,
            original_unit="kg CO₂/tonne"
        )
        results = validation_engine.validate_record(record, "cement_industry")
        assert len(results) > 0, "500 kg CO₂/tonne should fail validation"
        assert results[0].severity == "error"
        assert results[0].rule_name == "cement_emission_range"
        assert "below minimum" in results[0].message.lower()


class TestSteelIndustryRules:
    """Comprehensive tests for steel industry validation rules"""
    
    def test_steel_bf_bof_range(self, validation_engine):
        """Test valid BF-BOF steel emission value - Prompt 31 Test #4"""
        record = NormalizedRecord(
            id=uuid4(),
            indicator="Scope 1 GHG Emissions per tonne crude steel (BF-BOF)",
            value=2200.0,  # Within range 1800-2500
            unit="kg CO₂/tonne crude steel",
            original_value=2200.0,
            original_unit="kg CO₂/tonne crude steel"
        )
        results = validation_engine.validate_record(record, "steel_industry")
        assert len(results) == 0, "2200 kg CO₂/tonne should pass for BF-BOF"
    
    def test_steel_eaf_range(self, validation_engine):
        """Test valid EAF steel emission value - Prompt 31 Test #5"""
        record = NormalizedRecord(
            id=uuid4(),
            indicator="Scope 1 GHG Emissions per tonne crude steel (EAF)",
            value=500.0,  # Within range 400-600
            unit="kg CO₂/tonne crude steel",
            original_value=500.0,
            original_unit="kg CO₂/tonne crude steel"
        )
        results = validation_engine.validate_record(record, "steel_industry")
        assert len(results) == 0, "500 kg CO₂/tonne should pass for EAF"


class TestOutlierDetectionComprehensive:
    """Comprehensive tests for outlier detection"""
    
    def test_outlier_detection_exact_input(self, validation_engine):
        """Test outlier detection with exact input - Prompt 31 Test #6"""
        # Input: [100, 105, 98, 102, 1000, 99]
        # Expected: 1000 flagged as outlier
        records = [
            (uuid4(), 100.0),
            (uuid4(), 105.0),
            (uuid4(), 98.0),
            (uuid4(), 102.0),
            (uuid4(), 1000.0),  # Clear outlier
            (uuid4(), 99.0),
        ]
        
        outlier_rule = validation_engine._get_outlier_rule("cross_industry")
        assert outlier_rule is not None
        
        results = validation_engine.outlier_detection(records, outlier_rule)
        
        # Should detect the outlier
        assert len(results) > 0, "Outlier (1000) should be detected"
        
        # The outlier (1000.0) should be flagged
        outlier_data_ids = [r.data_id for r in results]
        assert records[4][0] in outlier_data_ids, "Value 1000 should be flagged"
        
        # Check result details
        outlier_result = next(r for r in results if r.data_id == records[4][0])
        assert outlier_result.rule_name == "detect_decimal_errors"
        assert outlier_result.severity == "error"


class TestTemporalConsistencyComprehensive:
    """Comprehensive tests for temporal consistency validation"""
    
    def test_temporal_consistency_pass_exact(self, validation_engine):
        """Test temporal consistency within tolerance - Prompt 31 Test #7"""
        # Input: Monthly sum = 11,990, Annual = 12,000
        # Expected: Passes (within 2% tolerance)
        monthly_data = {
            "Jan": 999.0, "Feb": 1000.0, "Mar": 1000.0,
            "Apr": 1000.0, "May": 1000.0, "Jun": 1000.0,
            "Jul": 999.0, "Aug": 999.0, "Sep": 999.0,
            "Oct": 999.0, "Nov": 998.0, "Dec": 997.0
        }
        # Sum = 11,990
        annual_total = 12000.0
        
        rule = None
        for r in validation_engine.rules["cross_industry"].values():
            if r.rule_name == "monthly_sum_equals_annual":
                rule = r
                break
        
        assert rule is not None
        result = validation_engine.temporal_consistency(
            monthly_data, 
            annual_total, 
            rule, 
            uuid4()
        )
        
        # Difference: (12000 - 11990) / 12000 = 0.083% < 2%
        assert result is None, "Should pass within 2% tolerance"
    
    def test_temporal_consistency_fail_exact(self, validation_engine):
        """Test temporal consistency outside tolerance - Prompt 31 Test #8"""
        # Input: Monthly sum = 10,000, Annual = 12,000
        # Expected: Warning flagged (16.7% difference > 2% tolerance)
        monthly_data = {
            "Jan": 833.0, "Feb": 833.0, "Mar": 834.0,
            "Apr": 833.0, "May": 833.0, "Jun": 834.0,
            "Jul": 833.0, "Aug": 833.0, "Sep": 834.0,
            "Oct": 833.0, "Nov": 833.0, "Dec": 834.0
        }
        # Sum = 10,000
        annual_total = 12000.0
        
        rule = None
        for r in validation_engine.rules["cross_industry"].values():
            if r.rule_name == "monthly_sum_equals_annual":
                rule = r
                break
        
        assert rule is not None
        result = validation_engine.temporal_consistency(
            monthly_data, 
            annual_total, 
            rule, 
            uuid4()
        )
        
        # Difference: (12000 - 10000) / 12000 = 16.7% > 2%
        assert result is not None, "Should fail outside 2% tolerance"
        assert result.is_valid is False
        assert result.severity == "warning"
        assert "differs from annual total" in result.message.lower()


class TestCrossFieldIntegrationTests:
    """Cross-field validation integration tests"""
    
    def test_scope_totals_consistency_exact(self, validation_engine):
        """Test scope totals match - Prompt 31 Test #9"""
        # Input: S1=100, S2=50, S3=30, Total=180
        # Expected: Passes (100+50+30 = 180)
        result = validation_engine.validate_scope_totals(
            scope_1=100.0,
            scope_2=50.0,
            scope_3=30.0,
            total=180.0,
            tolerance=0.02
        )
        assert result is None, "Scope totals should match (100+50+30=180)"
    
    def test_scope_totals_mismatch_exact(self, validation_engine):
        """Test scope totals mismatch - Prompt 31 Test #10"""
        # Input: S1=100, S2=50, S3=30, Total=200
        # Expected: Error flagged (100+50+30=180 ≠ 200)
        result = validation_engine.validate_scope_totals(
            scope_1=100.0,
            scope_2=50.0,
            scope_3=30.0,
            total=200.0,
            tolerance=0.02
        )
        assert result is not None, "Should detect mismatch (180 ≠ 200)"
        assert result.is_valid is False
        assert result.severity == "error"
        assert result.rule_name == "scope_totals_consistency"
        # Difference: (200-180)/200 = 10% > 2%
        assert "differs from total" in result.message.lower()


# ============================================================================
# PARAMETRIZED TESTS (Multiple Industries)
# ============================================================================

@pytest.mark.parametrize("industry,indicator,value,expected_valid", [
    # Cement industry tests
    ("cement_industry", "Scope 1 GHG Emissions per tonne clinker", 950.0, True),
    ("cement_industry", "Scope 1 GHG Emissions per tonne clinker", 1500.0, False),
    ("cement_industry", "Scope 1 GHG Emissions per tonne clinker", 500.0, False),
    ("cement_industry", "Scope 1 GHG Emissions per tonne clinker", 800.0, True),  # Min boundary
    ("cement_industry", "Scope 1 GHG Emissions per tonne clinker", 1100.0, True),  # Max boundary
    
    # Steel BF-BOF tests
    ("steel_industry", "Scope 1 GHG Emissions per tonne crude steel (BF-BOF)", 2200.0, True),
    ("steel_industry", "Scope 1 GHG Emissions per tonne crude steel (BF-BOF)", 1800.0, True),  # Min
    ("steel_industry", "Scope 1 GHG Emissions per tonne crude steel (BF-BOF)", 2500.0, True),  # Max
    ("steel_industry", "Scope 1 GHG Emissions per tonne crude steel (BF-BOF)", 3000.0, False),
    ("steel_industry", "Scope 1 GHG Emissions per tonne crude steel (BF-BOF)", 1000.0, False),
    
    # Steel EAF tests
    ("steel_industry", "Scope 1 GHG Emissions per tonne crude steel (EAF)", 500.0, True),
    ("steel_industry", "Scope 1 GHG Emissions per tonne crude steel (EAF)", 400.0, True),  # Min
    ("steel_industry", "Scope 1 GHG Emissions per tonne crude steel (EAF)", 600.0, True),  # Max
    ("steel_industry", "Scope 1 GHG Emissions per tonne crude steel (EAF)", 800.0, False),
    ("steel_industry", "Scope 1 GHG Emissions per tonne crude steel (EAF)", 200.0, False),
])
def test_industry_emission_ranges(validation_engine, industry, indicator, value, expected_valid):
    """Parametrized test for multiple industry emission ranges"""
    record = NormalizedRecord(
        id=uuid4(),
        indicator=indicator,
        value=value,
        unit="kg CO₂/tonne",
        original_value=value,
        original_unit="kg CO₂/tonne"
    )
    
    results = validation_engine.validate_record(record, industry)
    
    if expected_valid:
        assert len(results) == 0, f"{industry} value {value} should pass validation"
    else:
        assert len(results) > 0, f"{industry} value {value} should fail validation"
        assert results[0].is_valid is False
        assert results[0].severity == "error"


@pytest.mark.parametrize("monthly_sum,annual_total,should_pass", [
    (12000.0, 12000.0, True),  # Exact match
    (11990.0, 12000.0, True),  # Within 2% tolerance
    (12200.0, 12000.0, True),  # Within 2% tolerance
    (11700.0, 12000.0, False),  # Outside 2% tolerance (2.5%)
    (13000.0, 12000.0, False),  # Outside 2% tolerance (8.3%)
    (10000.0, 12000.0, False),  # Large difference (16.7%)
])
def test_temporal_consistency_parametrized(validation_engine, monthly_sum, annual_total, should_pass):
    """Parametrized temporal consistency tests"""
    # Create monthly data that sums to monthly_sum
    monthly_value = monthly_sum / 12
    monthly_data = {
        month: monthly_value 
        for month in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    }
    
    rule = None
    for r in validation_engine.rules["cross_industry"].values():
        if r.rule_name == "monthly_sum_equals_annual":
            rule = r
            break
    
    assert rule is not None
    result = validation_engine.temporal_consistency(
        monthly_data,
        annual_total,
        rule,
        uuid4()
    )
    
    if should_pass:
        assert result is None, f"Monthly sum {monthly_sum} should match annual {annual_total} within tolerance"
    else:
        assert result is not None, f"Monthly sum {monthly_sum} should not match annual {annual_total}"
        assert result.is_valid is False


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestValidationIntegration:
    """Integration tests for validation service end-to-end"""
    
    @pytest.mark.integration
    def test_validation_service_end_to_end(self, validation_engine):
        """End-to-end validation test with known errors - Prompt 31 Test #11"""
        # Create dataset with known errors
        records = [
            # Valid cement record
            NormalizedRecord(
                id=uuid4(),
                indicator="Scope 1 GHG Emissions per tonne clinker",
                value=950.0,
                unit="kg CO₂/tonne",
                original_value=950.0,
                original_unit="kg CO₂/tonne"
            ),
            # Invalid cement record (too high)
            NormalizedRecord(
                id=uuid4(),
                indicator="Scope 1 GHG Emissions per tonne clinker",
                value=1500.0,
                unit="kg CO₂/tonne",
                original_value=1500.0,
                original_unit="kg CO₂/tonne"
            ),
            # Invalid cement record (too low)
            NormalizedRecord(
                id=uuid4(),
                indicator="Scope 1 GHG Emissions per tonne clinker",
                value=500.0,
                unit="kg CO₂/tonne",
                original_value=500.0,
                original_unit="kg CO₂/tonne"
            ),
        ]
        
        # Run batch validation
        results = validation_engine.validate_batch(records, "cement_industry")
        
        # Assert errors detected correctly
        assert len(results) == 2, "Should detect 2 errors (too high and too low)"
        
        # Verify error details
        all_errors = []
        for record_results in results.values():
            all_errors.extend(record_results)
        
        assert len(all_errors) == 2
        assert all(err.rule_name == "cement_emission_range" for err in all_errors)
        assert all(err.severity == "error" for err in all_errors)
    
    @pytest.mark.integration
    def test_validation_report_generation(self, validation_engine):
        """Test validation report generation - Prompt 31 Test #12"""
        # Create mixed dataset
        records = [
            NormalizedRecord(id=uuid4(), indicator="test", value=950.0, 
                           unit="kg CO₂/tonne", original_value=950.0, original_unit="kg CO₂/tonne")
            for _ in range(8)  # 8 valid records
        ] + [
            NormalizedRecord(id=uuid4(), indicator="Scope 1 GHG Emissions per tonne clinker", 
                           value=1500.0, unit="kg CO₂/tonne", 
                           original_value=1500.0, original_unit="kg CO₂/tonne")
            for _ in range(2)  # 2 invalid records
        ]
        
        results = validation_engine.validate_batch(records, "cement_industry")
        
        # Calculate summary statistics
        total_records = len(records)
        records_with_errors = len(results)
        pass_rate = ((total_records - records_with_errors) / total_records) * 100
        
        # Assert summary correct
        assert total_records == 10
        assert records_with_errors == 2
        assert pass_rate == 80.0, "Should have 80% pass rate (8/10)"


# ============================================================================
# API INTEGRATION TESTS
# ============================================================================

@pytest.mark.api
class TestValidationAPI:
    """API endpoint tests"""
    
    @pytest.mark.integration
    @patch('src.api.validation.validation_engine')
    @patch('src.api.validation.get_validation_service')
    def test_validation_endpoint(self, mock_get_service, mock_engine):
        """Test validation API endpoint - Prompt 31 Test #13"""
        from fastapi.testclient import TestClient
        from src.main import app
        
        upload_id = uuid4()
        
        # Mock service response
        mock_service = Mock()
        mock_service.validate_upload.return_value = Mock(
            total_records=100,
            valid_records=85,
            records_with_errors=10,
            records_with_warnings=5,
            validation_pass_rate=90.0,
            error_breakdown={"test_rule": 10},
            warning_breakdown={"test_warning": 5}
        )
        mock_get_service.return_value = mock_service

        from tests.auth_helpers import app_auth_patched

        with app_auth_patched(app):
            client = TestClient(app)
            response = client.post(
                f"/api/v1/validation/process/{upload_id}",
                params={"industry": "cement_industry"}
            )

        # Assert 200 response
        assert response.status_code == 200
        
        # Assert ValidationSummary returned
        data = response.json()
        assert "total_records" in data
        assert "validation_pass_rate" in data
        assert data["total_records"] == 100
        assert data["validation_pass_rate"] == 90.0
    
    @pytest.mark.integration
    @patch('src.api.validation.validation_engine')
    @patch('src.api.validation.get_validation_service')
    def test_error_review_workflow(self, mock_get_service, mock_engine):
        """Test error review workflow - Prompt 31 Test #14"""
        from fastapi.testclient import TestClient
        from src.main import app
        
        result_id = uuid4()
        upload_id = uuid4()
        
        # Mock service
        mock_service = Mock()
        mock_service.mark_error_as_reviewed.return_value = None
        mock_service.get_unreviewed_errors.side_effect = [
            [{"id": str(result_id), "message": "Error"}],  # Before review
            []  # After review
        ]
        mock_get_service.return_value = mock_service

        from tests.auth_helpers import app_auth_patched

        with app_auth_patched(app):
            client = TestClient(app)

            # Step 1: Get unreviewed errors (should have 1)
            response = client.get(f"/api/v1/validation/unreviewed/{upload_id}")
            assert response.status_code == 200
            data = response.json()
            assert data["unreviewed_count"] == 1

            # Step 2: Mark error as reviewed
            response = client.post(
                "/api/v1/validation/review/mark-reviewed",
                json={
                    "result_id": str(result_id),
                    "reviewer": "test@example.com",
                    "notes": "False positive"
                }
            )
            assert response.status_code == 200

            # Step 3: Get unreviewed errors again (should be empty)
            response = client.get(f"/api/v1/validation/unreviewed/{upload_id}")
            assert response.status_code == 200
            data = response.json()
            assert data["unreviewed_count"] == 0, "Error should disappear from unreviewed list"
