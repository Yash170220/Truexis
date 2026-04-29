"""Unit tests for NormalizationService."""

import pytest
from pathlib import Path
from uuid import uuid4
from unittest.mock import Mock, MagicMock, patch
import polars as pl

from src.normalization import (
    NormalizationService,
    UnitNormalizer,
    NormalizationError,
)
from src.common.models import Upload, MatchedIndicator, NormalizedData


@pytest.fixture
def normalizer():
    """Create normalizer instance."""
    conversion_factors_path = Path(__file__).parent.parent.parent / "data" / "validation-rules" / "conversion_factors.json"
    return UnitNormalizer(str(conversion_factors_path))


@pytest.fixture
def mock_db():
    """Create mock database session."""
    return Mock()


@pytest.fixture
def service(normalizer, mock_db):
    """Create normalization service instance."""
    return NormalizationService(normalizer, mock_db)


class TestUnitDetection:
    """Test unit detection from context."""

    def test_detect_unit_from_name_with_parentheses(self, service):
        """Test detecting unit from name with parentheses."""
        unit = service._extract_unit_from_text("Energy Consumption (kWh)")
        assert unit == "kWh"

    def test_detect_unit_from_name_with_brackets(self, service):
        """Test detecting unit from name with brackets."""
        unit = service._extract_unit_from_text("CO2 Emissions [kg CO2e]")
        assert unit == "kg CO2e"

    def test_detect_energy_unit_from_magnitude(self, service):
        """Test detecting energy unit from value magnitude."""
        # Large values suggest kWh
        unit = service.detect_unit_from_context(
            "Electricity Consumption",
            [50000, 45000, 52000]
        )
        assert unit == "kWh"

    def test_detect_emissions_unit_from_keyword(self, service):
        """Test detecting emissions unit from keyword."""
        unit = service.detect_unit_from_context(
            "CO2 Emissions",
            [2500, 3000, 2800]
        )
        assert unit in ["kg CO2e", "tonnes CO2e"]

    def test_detect_water_unit_from_keyword(self, service):
        """Test detecting water unit from keyword."""
        unit = service.detect_unit_from_context(
            "Water Consumption",
            [15000, 18000, 16000]
        )
        assert unit == "liters"

    def test_detect_unit_returns_none_for_ambiguous(self, service):
        """Test that ambiguous cases return None."""
        unit = service.detect_unit_from_context(
            "Some Random Metric",
            [10, 20, 30]
        )
        assert unit is None


class TestProcessIndicator:
    """Test indicator processing."""

    def test_process_indicator_with_valid_data(self, service):
        """Test processing indicator with valid numeric data."""
        indicator_id = uuid4()
        data = [5000, 6000, 5500, None, 4800]
        
        with patch.object(service, 'detect_unit_from_context', return_value='kWh'):
            records = service.process_indicator(
                indicator_id,
                "Energy (kWh)",
                "Energy Consumption",
                data
            )
        
        assert len(records) == 4  # Excludes None
        assert all(r.original_unit == 'kWh' for r in records)
        assert all(r.normalized_unit == 'MWh' for r in records)
        assert records[0].normalized_value == 5.0

    def test_process_indicator_with_no_numeric_data(self, service):
        """Test processing indicator with no numeric data."""
        indicator_id = uuid4()
        data = [None, "text", None]
        
        records = service.process_indicator(
            indicator_id,
            "Some Column",
            "Some Indicator",
            data
        )
        
        assert len(records) == 0

    def test_process_indicator_without_detectable_unit(self, service):
        """Test processing indicator when unit cannot be detected."""
        indicator_id = uuid4()
        data = [100, 200, 300]
        
        with patch.object(service, 'detect_unit_from_context', return_value=None):
            with pytest.raises(NormalizationError, match="Could not detect unit"):
                service.process_indicator(
                    indicator_id,
                    "Unknown Metric",
                    "Unknown",
                    data
                )


class TestNormalizeData:
    """Test full data normalization."""

    def test_normalize_data_upload_not_found(self, service, mock_db):
        """Test error when upload not found."""
        upload_id = uuid4()
        mock_db.query.return_value.filter.return_value.first.return_value = None
        
        with pytest.raises(NormalizationError, match="not found"):
            service.normalize_data(upload_id)

    def test_normalize_data_no_approved_indicators(self, service, mock_db):
        """Test error when no approved indicators."""
        upload_id = uuid4()
        
        # Mock upload exists
        mock_upload = Mock(spec=Upload)
        mock_db.query.return_value.filter.return_value.first.return_value = mock_upload
        
        # Mock no approved indicators
        mock_db.query.return_value.filter.return_value.all.return_value = []
        
        with pytest.raises(NormalizationError, match="No approved indicators"):
            service.normalize_data(upload_id)

    def test_normalize_data_success(self, service, mock_db, tmp_path):
        """Test successful data normalization."""
        upload_id = uuid4()
        
        # Create test parquet file
        test_data = pl.DataFrame({
            "Energy (kWh)": [5000, 6000, 5500],
            "CO2 (kg)": [2500, 3000, 2800]
        })
        parquet_path = tmp_path / "test.parquet"
        test_data.write_parquet(parquet_path)
        
        # Mock upload
        mock_upload = Mock(spec=Upload)
        mock_upload.file_path = str(parquet_path)
        
        # Mock matched indicators
        indicator1 = Mock(spec=MatchedIndicator)
        indicator1.id = uuid4()
        indicator1.matched_header = "Energy (kWh)"
        indicator1.canonical_indicator = "Energy Consumption"
        
        # Setup query mocks
        query_mock = Mock()
        query_mock.filter.return_value.first.return_value = mock_upload
        query_mock.filter.return_value.all.return_value = [indicator1]
        mock_db.query.return_value = query_mock
        
        # Mock save and audit
        with patch.object(service, 'save_normalized_data'):
            with patch.object(service, '_create_audit_log'):
                summary = service.normalize_data(upload_id)
        
        assert summary.total_records == 3
        assert summary.successfully_normalized == 3
        assert summary.failed_normalization == 0


class TestSaveNormalizedData:
    """Test saving normalized data."""

    def test_save_normalized_data(self, service, mock_db):
        """Test bulk saving of normalized records."""
        from src.normalization.service import NormalizedRecord
        
        records = [
            NormalizedRecord(
                matched_indicator_id=uuid4(),
                original_value=5000,
                original_unit="kWh",
                normalized_value=5.0,
                normalized_unit="MWh",
                conversion_factor=0.001,
                row_index=0,
                metadata={"test": "data"}
            )
        ]
        
        service.save_normalized_data(records)
        
        mock_db.bulk_save_objects.assert_called_once()
        mock_db.commit.assert_called_once()


class TestGetNormalizedData:
    """Test retrieving normalized data."""

    def test_get_normalized_data_empty(self, service, mock_db):
        """Test getting normalized data when none exists."""
        upload_id = uuid4()
        
        mock_db.query.return_value.join.return_value.filter.return_value.all.return_value = []
        
        df = service.get_normalized_data(upload_id)
        
        assert df.is_empty()

    def test_get_normalized_data_with_filter(self, service, mock_db):
        """Test getting normalized data with indicator filter."""
        upload_id = uuid4()
        
        # Mock normalized data
        mock_record = Mock(spec=NormalizedData)
        mock_record.matched_indicator.canonical_indicator = "Energy Consumption"
        mock_record.original_value = 5000
        mock_record.original_unit = "kWh"
        mock_record.normalized_value = 5.0
        mock_record.normalized_unit = "MWh"
        mock_record.row_index = 0
        
        mock_db.query.return_value.join.return_value.filter.return_value.all.return_value = [mock_record]
        
        df = service.get_normalized_data(upload_id, "Energy Consumption")
        
        assert not df.is_empty()
        assert "indicator" in df.columns
        assert "normalized_value" in df.columns


class TestCheckUnitConflicts:
    """Test unit conflict detection."""

    def test_check_unit_conflicts_none(self, service, mock_db):
        """Test when no conflicts exist."""
        upload_id = uuid4()
        
        # Mock indicator with single unit
        mock_indicator = Mock(spec=MatchedIndicator)
        mock_indicator.id = uuid4()
        mock_indicator.canonical_indicator = "Energy Consumption"
        
        mock_db.query.return_value.filter.return_value.all.return_value = [mock_indicator]
        mock_db.query.return_value.filter.return_value.distinct.return_value.all.return_value = [("kWh",)]
        
        conflicts = service.check_unit_conflicts(upload_id)
        
        assert len(conflicts) == 0

    def test_check_unit_conflicts_detected(self, service, mock_db):
        """Test when conflicts are detected."""
        upload_id = uuid4()
        
        # Mock indicator with multiple units
        mock_indicator = Mock(spec=MatchedIndicator)
        mock_indicator.id = uuid4()
        mock_indicator.canonical_indicator = "Energy Consumption"
        
        # Setup query chain
        filter_mock = Mock()
        filter_mock.all.return_value = [mock_indicator]
        
        query_mock = Mock()
        query_mock.filter.return_value = filter_mock
        
        # For the units query
        units_query = Mock()
        units_query.filter.return_value.distinct.return_value.all.return_value = [("kWh",), ("MWh",)]
        
        mock_db.query.side_effect = [query_mock, units_query]
        
        conflicts = service.check_unit_conflicts(upload_id)
        
        assert "Energy Consumption" in conflicts
        assert len(conflicts["Energy Consumption"]) == 2


class TestEdgeCases:
    """Test edge cases."""

    def test_process_indicator_with_mixed_types(self, service):
        """Test processing data with mixed types."""
        indicator_id = uuid4()
        data = [5000, "text", 6000, None, 5500, 0]
        
        with patch.object(service, 'detect_unit_from_context', return_value='kWh'):
            records = service.process_indicator(
                indicator_id,
                "Energy (kWh)",
                "Energy Consumption",
                data
            )
        
        # Should process only numeric values
        assert len(records) == 4  # 5000, 6000, 5500, 0

    def test_detect_unit_with_empty_values(self, service):
        """Test unit detection with empty value list."""
        unit = service.detect_unit_from_context("Energy", [])
        assert unit is None

    def test_extract_unit_with_multiple_patterns(self, service):
        """Test extracting unit when multiple patterns exist."""
        unit = service._extract_unit_from_text("Energy (kWh) in MWh")
        # Should match first pattern
        assert unit == "kWh"
