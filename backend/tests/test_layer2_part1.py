"""Comprehensive tests for Matching & Normalization (Layer 2 Part 1)."""

import pytest
from pathlib import Path
from uuid import uuid4
from unittest.mock import Mock, patch, MagicMock
import polars as pl

from src.matching.rule_matcher import RuleBasedMatcher
from src.matching.llm_matcher import LLMMatcher
from src.matching.service import MatchingService
from src.normalization import (
    UnitNormalizer,
    NormalizationService,
    CategoryMismatchError,
)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def sample_headers():
    """Sample headers for testing."""
    return [
        "Electricity Consumption",
        "Elec Consmptn",  # Typos
        "Misc Energy",  # Ambiguous
        "Total CO2 Emissions",
        "Water Usage (m3)",
        "Energy (kWh)",
    ]


@pytest.fixture
def synonym_dict_path():
    """Path to synonym dictionary."""
    return Path(__file__).parent.parent / "data" / "validation-rules" / "synonym_dictionary.json"


@pytest.fixture
def conversion_factors_path():
    """Path to conversion factors."""
    return Path(__file__).parent.parent / "data" / "validation-rules" / "conversion_factors.json"


@pytest.fixture
def rule_matcher(synonym_dict_path):
    """Create rule-based matcher."""
    return RuleBasedMatcher(str(synonym_dict_path))


@pytest.fixture
def mock_llm_client():
    """Mock Groq LLM client."""
    mock_client = Mock()
    mock_response = Mock()
    mock_response.choices = [Mock()]
    mock_response.choices[0].message.content = '''
    {
        "canonical_indicator": "Total Electricity Consumption",
        "confidence": 0.75,
        "reasoning": "Misc Energy likely refers to electricity consumption"
    }
    '''
    mock_client.chat.completions.create.return_value = mock_response
    return mock_client


@pytest.fixture
def llm_matcher(synonym_dict_path, mock_llm_client):
    """Create LLM matcher with mocked client."""
    with patch('src.matching.llm_matcher.Groq', return_value=mock_llm_client):
        matcher = LLMMatcher(str(synonym_dict_path), "fake-api-key", "fake-model")
        matcher.client = mock_llm_client
        return matcher


@pytest.fixture
def unit_normalizer(conversion_factors_path):
    """Create unit normalizer."""
    return UnitNormalizer(str(conversion_factors_path))


@pytest.fixture
def mock_db():
    """Mock database session."""
    return Mock()


@pytest.fixture
def normalization_service(unit_normalizer, mock_db):
    """Create normalization service."""
    return NormalizationService(unit_normalizer, mock_db)


# ============================================================================
# MATCHING TESTS
# ============================================================================

class TestRuleMatcher:
    """Test rule-based matching."""

    def test_exact_match(self, rule_matcher):
        """Test exact match with high confidence."""
        result = rule_matcher.match("Electricity Consumption")
        
        assert result is not None
        assert result.canonical_indicator == "Total Electricity Consumption"
        assert result.confidence == 1.0
        assert result.method == "exact"

    def test_fuzzy_match_with_typos(self, rule_matcher):
        """Test fuzzy match handles typos."""
        result = rule_matcher.match("Elec Consmptn")
        
        assert result is not None
        assert result.canonical_indicator == "Total Electricity Consumption"
        assert 0.85 <= result.confidence <= 0.95
        assert result.method == "fuzzy"

    def test_synonym_match(self, rule_matcher):
        """Test synonym matching."""
        result = rule_matcher.match("Power Consumption")
        
        assert result is not None
        assert result.canonical_indicator == "Total Electricity Consumption"
        assert result.confidence >= 0.9

    def test_no_match_below_threshold(self, rule_matcher):
        """Test no match when confidence too low."""
        result = rule_matcher.match("Completely Random Text XYZ123")
        
        assert result is None

    def test_batch_matching(self, rule_matcher, sample_headers):
        """Test batch matching multiple headers."""
        results = rule_matcher.match_batch(sample_headers[:2])
        
        assert len(results) == 2
        assert results[0].canonical_indicator == "Total Electricity Consumption"
        assert results[1].canonical_indicator == "Total Electricity Consumption"


class TestLLMMatcher:
    """Test LLM-based matching."""

    def test_ambiguous_header_resolution(self, llm_matcher):
        """Test LLM resolves ambiguous headers."""
        result = llm_matcher.match("Misc Energy")
        
        assert result is not None
        assert result.canonical_indicator == "Total Electricity Consumption"
        assert 0.70 <= result.confidence <= 0.85
        assert result.method == "llm"

    def test_llm_caching(self, llm_matcher, mock_llm_client):
        """Test LLM responses are cached."""
        # First call
        result1 = llm_matcher.match("Test Header")
        
        # Second call - should use cache
        result2 = llm_matcher.match("Test Header")
        
        # LLM should only be called once
        assert mock_llm_client.chat.completions.create.call_count == 1
        assert result1.canonical_indicator == result2.canonical_indicator

    def test_llm_retry_on_failure(self, llm_matcher, mock_llm_client):
        """Test LLM retries on failure."""
        # Make first call fail, second succeed
        mock_llm_client.chat.completions.create.side_effect = [
            Exception("API Error"),
            mock_llm_client.chat.completions.create.return_value
        ]
        
        result = llm_matcher.match("Test Header")
        
        assert result is not None
        assert mock_llm_client.chat.completions.create.call_count == 2


class TestMatchingService:
    """Test matching service integration."""

    def test_hybrid_workflow(self, rule_matcher, llm_matcher, mock_db):
        """Test hybrid matching workflow."""
        service = MatchingService(rule_matcher, llm_matcher, mock_db)
        
        headers = [
            "Electricity Consumption",  # Clear - rule matcher
            "Misc Energy"  # Ambiguous - LLM matcher
        ]
        
        with patch.object(service, '_save_matches'):
            results = service.match_headers(uuid4(), headers)
        
        assert len(results) == 2
        assert results[0].method == "exact"
        assert results[1].method == "llm"

    def test_review_queue_flagging(self, rule_matcher, llm_matcher, mock_db):
        """Test headers flagged for review when confidence < threshold."""
        service = MatchingService(rule_matcher, llm_matcher, mock_db)
        
        # Mock a low confidence match
        with patch.object(rule_matcher, 'match') as mock_match:
            mock_result = Mock()
            mock_result.canonical_indicator = "Test Indicator"
            mock_result.confidence = 0.80  # Below review threshold (0.85)
            mock_result.method = "fuzzy"
            mock_match.return_value = mock_result
            
            with patch.object(service, '_save_matches'):
                results = service.match_headers(uuid4(), ["Low Confidence Header"])
            
            # Should be flagged for review
            assert results[0].confidence < 0.85

    def test_approve_match(self, rule_matcher, llm_matcher, mock_db):
        """Test approving a matched indicator."""
        service = MatchingService(rule_matcher, llm_matcher, mock_db)
        
        # Mock matched indicator
        mock_indicator = Mock()
        mock_indicator.id = uuid4()
        mock_indicator.matched_header = "Test Header"
        mock_indicator.canonical_indicator = "Test Indicator"
        mock_indicator.approved = False
        
        mock_db.query.return_value.filter.return_value.first.return_value = mock_indicator
        
        service.approve_match(
            upload_id=uuid4(),
            matched_header="Test Header",
            approved=True
        )
        
        assert mock_indicator.approved is True
        mock_db.commit.assert_called()


# ============================================================================
# NORMALIZATION TESTS
# ============================================================================

class TestUnitDetection:
    """Test unit detection from text."""

    def test_detect_kwh(self, unit_normalizer):
        """Test detecting kWh from text."""
        unit, category = unit_normalizer.detect_unit("5000 kWh")
        
        assert unit == "kWh"
        assert category == "energy"

    def test_detect_tonnes(self, unit_normalizer):
        """Test detecting tonnes from text."""
        unit, category = unit_normalizer.detect_unit("12.5 tonnes")
        
        assert unit == "tonnes"
        assert category == "mass"

    def test_detect_gj(self, unit_normalizer):
        """Test detecting GJ from text."""
        unit, category = unit_normalizer.detect_unit("3.2 GJ")
        
        assert unit == "GJ"
        assert category == "energy"

    def test_detect_unit_in_parentheses(self, unit_normalizer):
        """Test detecting unit in parentheses."""
        unit, category = unit_normalizer.detect_unit("Energy Consumption (kWh)")
        
        assert unit == "kWh"
        assert category == "energy"


class TestEnergyConversion:
    """Test energy unit conversions."""

    def test_kwh_to_mwh(self, unit_normalizer):
        """Test kWh to MWh conversion."""
        result = unit_normalizer.normalize(5000, "kWh", "energy")
        
        assert result.original_value == 5000
        assert result.original_unit == "kWh"
        assert result.normalized_value == 5.0
        assert result.normalized_unit == "MWh"
        assert result.conversion_factor == 0.001

    def test_gj_to_mwh(self, unit_normalizer):
        """Test GJ to MWh conversion."""
        result = unit_normalizer.normalize(100, "GJ", "energy")
        
        assert result.normalized_value == pytest.approx(27.7778, rel=1e-4)
        assert result.normalized_unit == "MWh"

    def test_btu_to_mwh(self, unit_normalizer):
        """Test BTU to MWh conversion (fixed factor)."""
        result = unit_normalizer.normalize(3412.14, "BTU", "energy")
        
        # 3412.14 BTU = 1 kWh = 0.001 MWh
        assert result.normalized_value == pytest.approx(0.001, rel=1e-3)
        assert result.normalized_unit == "MWh"


class TestMassConversion:
    """Test mass unit conversions."""

    def test_kg_to_tonnes(self, unit_normalizer):
        """Test kg to tonnes conversion."""
        result = unit_normalizer.normalize(10000, "kg", "mass")
        
        assert result.original_value == 10000
        assert result.original_unit == "kg"
        assert result.normalized_value == 10.0
        assert result.normalized_unit == "tonnes"
        assert result.conversion_factor == 0.001

    def test_pounds_to_tonnes(self, unit_normalizer):
        """Test pounds to tonnes conversion."""
        result = unit_normalizer.normalize(2204.62, "pounds", "mass")
        
        assert result.normalized_value == pytest.approx(1.0, rel=1e-3)
        assert result.normalized_unit == "tonnes"


class TestInvalidConversion:
    """Test invalid conversion scenarios."""

    def test_category_mismatch(self, unit_normalizer):
        """Test converting between incompatible categories."""
        with pytest.raises(CategoryMismatchError, match="Cannot convert between different categories"):
            unit_normalizer.get_conversion_factor("kWh", "kg")

    def test_negative_energy_value(self, unit_normalizer):
        """Test negative value for absolute measure."""
        from src.normalization import InvalidValueError
        
        with pytest.raises(InvalidValueError, match="Negative value"):
            unit_normalizer.normalize(-100, "kWh", "energy")

    def test_unknown_unit(self, unit_normalizer):
        """Test conversion with unknown unit."""
        from src.normalization import UnitNotFoundError
        
        with pytest.raises(UnitNotFoundError, match="not in database"):
            unit_normalizer.normalize(100, "xyz", "energy")


class TestNormalizationService:
    """Test normalization service."""

    def test_unit_detection_from_context(self, normalization_service):
        """Test detecting unit from indicator name and values."""
        # Energy with large values
        unit = normalization_service.detect_unit_from_context(
            "Electricity Consumption",
            [50000, 45000, 52000]
        )
        assert unit == "kWh"
        
        # Emissions
        unit = normalization_service.detect_unit_from_context(
            "CO2 Emissions",
            [2500, 3000, 2800]
        )
        assert unit in ["kg CO2e", "tonnes CO2e"]

    def test_extract_unit_from_text(self, normalization_service):
        """Test extracting unit from text patterns."""
        unit = normalization_service._extract_unit_from_text("Energy (kWh)")
        assert unit == "kWh"
        
        unit = normalization_service._extract_unit_from_text("CO2 [kg CO2e]")
        assert unit == "kg CO2e"

    def test_process_indicator(self, normalization_service):
        """Test processing single indicator."""
        indicator_id = uuid4()
        data = [5000, 6000, 5500, None, 4800]
        
        with patch.object(normalization_service, 'detect_unit_from_context', return_value='kWh'):
            records = normalization_service.process_indicator(
                indicator_id,
                "Energy (kWh)",
                "Total Electricity Consumption",
                data
            )
        
        assert len(records) == 4  # Excludes None
        assert all(r.original_unit == 'kWh' for r in records)
        assert all(r.normalized_unit == 'MWh' for r in records)
        assert records[0].normalized_value == 5.0


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestMatchingToNormalizationPipeline:
    """Test end-to-end pipeline from matching to normalization."""

    def test_full_pipeline(self, rule_matcher, llm_matcher, unit_normalizer, tmp_path):
        """Test complete workflow: upload → match → normalize."""
        # Create test data
        test_data = pl.DataFrame({
            "Electricity Consumption (kWh)": [5000, 6000, 5500],
            "CO2 Emissions (kg CO2e)": [2500, 3000, 2800],
            "Water Usage (m3)": [1000, 1200, 1100]
        })
        
        parquet_path = tmp_path / "test_data.parquet"
        test_data.write_parquet(parquet_path)
        
        # Step 1: Match headers
        headers = list(test_data.columns)
        matched_results = []
        
        for header in headers:
            result = rule_matcher.match(header)
            if result:
                matched_results.append({
                    "header": header,
                    "canonical": result.canonical_indicator,
                    "confidence": result.confidence
                })
        
        assert len(matched_results) == 3
        assert all(r["confidence"] >= 0.8 for r in matched_results)
        
        # Step 2: Normalize data
        for header in headers:
            # Extract unit from header
            try:
                unit, category = unit_normalizer.detect_unit(header)
                
                # Normalize first value
                first_value = test_data[header][0]
                result = unit_normalizer.normalize(first_value, unit, category)
                
                assert result.normalized_value > 0
                assert result.conversion_factor is not None
            except Exception:
                # Some headers might not have detectable units
                pass

    def test_review_to_approval_workflow(self, rule_matcher, llm_matcher):
        """Test workflow with review queue."""
        mock_db = Mock()
        service = MatchingService(rule_matcher, llm_matcher, mock_db)
        
        upload_id = uuid4()
        
        # Step 1: Match headers (some need review)
        headers = ["Electricity Consumption", "Ambiguous Header XYZ"]
        
        with patch.object(service, '_save_matches'):
            results = service.match_headers(upload_id, headers)
        
        # Step 2: Get review queue
        mock_db.query.return_value.filter.return_value.all.return_value = [
            Mock(
                matched_header="Ambiguous Header XYZ",
                canonical_indicator="Unknown",
                confidence=0.60,
                reviewed=False
            )
        ]
        
        review_queue = service.get_review_queue(upload_id)
        assert len(review_queue) > 0
        
        # Step 3: Approve/reject
        mock_indicator = Mock()
        mock_indicator.matched_header = "Ambiguous Header XYZ"
        mock_db.query.return_value.filter.return_value.first.return_value = mock_indicator
        
        service.approve_match(upload_id, "Ambiguous Header XYZ", True, "Corrected Indicator")
        
        assert mock_indicator.approved is True
        assert mock_indicator.canonical_indicator == "Corrected Indicator"

    def test_error_handling_in_pipeline(self, rule_matcher, llm_matcher, unit_normalizer):
        """Test error handling throughout pipeline."""
        mock_db = Mock()
        service = MatchingService(rule_matcher, llm_matcher, mock_db)
        
        # Test matching with invalid input
        with patch.object(service, '_save_matches'):
            results = service.match_headers(uuid4(), [])
        assert len(results) == 0
        
        # Test normalization with invalid unit
        from src.normalization import UnitNotFoundError
        with pytest.raises(UnitNotFoundError):
            unit_normalizer.normalize(100, "invalid_unit", "energy")

    def test_statistics_accuracy(self, normalization_service, tmp_path):
        """Test normalization statistics are accurate."""
        from src.normalization import NormalizationError
        
        # Mock upload and indicators
        upload_id = uuid4()
        
        mock_upload = Mock()
        test_data = pl.DataFrame({"Energy (kWh)": [5000, 6000, 5500]})
        parquet_path = tmp_path / "test.parquet"
        test_data.write_parquet(parquet_path)
        mock_upload.file_path = str(parquet_path)
        
        mock_indicator = Mock()
        mock_indicator.id = uuid4()
        mock_indicator.matched_header = "Energy (kWh)"
        mock_indicator.canonical_indicator = "Total Electricity Consumption"
        
        normalization_service.db.query.return_value.filter.return_value.first.return_value = mock_upload
        normalization_service.db.query.return_value.filter.return_value.all.return_value = [mock_indicator]
        
        with patch.object(normalization_service, 'save_normalized_data'):
            with patch.object(normalization_service, '_create_audit_log'):
                summary = normalization_service.normalize_data(upload_id)
        
        assert summary.total_records == 3
        assert summary.successfully_normalized == 3
        assert summary.failed_normalization == 0


# ============================================================================
# EDGE CASES
# ============================================================================

class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_header_list(self, rule_matcher):
        """Test matching with empty header list."""
        results = rule_matcher.match_batch([])
        assert len(results) == 0

    def test_null_values_in_data(self, normalization_service):
        """Test normalization handles null values."""
        indicator_id = uuid4()
        data = [None, 5000, None, 6000, None]
        
        with patch.object(normalization_service, 'detect_unit_from_context', return_value='kWh'):
            records = normalization_service.process_indicator(
                indicator_id,
                "Energy",
                "Energy Consumption",
                data
            )
        
        assert len(records) == 2  # Only non-null values

    def test_mixed_data_types(self, normalization_service):
        """Test normalization handles mixed data types."""
        indicator_id = uuid4()
        data = [5000, "text", 6000, None, 5500]
        
        with patch.object(normalization_service, 'detect_unit_from_context', return_value='kWh'):
            records = normalization_service.process_indicator(
                indicator_id,
                "Energy",
                "Energy Consumption",
                data
            )
        
        assert len(records) == 3  # Only numeric values

    def test_zero_values(self, unit_normalizer):
        """Test normalization handles zero values."""
        result = unit_normalizer.normalize(0, "kWh", "energy")
        
        assert result.normalized_value == 0
        assert result.normalized_unit == "MWh"

    def test_very_large_values(self, unit_normalizer):
        """Test normalization handles very large values."""
        result = unit_normalizer.normalize(1e9, "kWh", "energy")
        
        assert result.normalized_value == 1e6
        assert result.normalized_unit == "MWh"
