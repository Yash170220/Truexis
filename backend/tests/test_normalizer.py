"""Unit tests for UnitNormalizer."""

import pytest
from pathlib import Path

from src.normalization import (
    UnitNormalizer,
    NormalizationResult,
    ConversionFactor,
    UnitNotFoundError,
    CategoryMismatchError,
    InvalidValueError,
    ConversionDataError,
)


@pytest.fixture
def normalizer():
    """Create normalizer instance with test conversion factors."""
    conversion_factors_path = Path(__file__).parent.parent.parent / "data" / "validation-rules" / "conversion_factors.json"
    return UnitNormalizer(str(conversion_factors_path))


class TestEnergyConversions:
    """Test energy unit conversions."""

    def test_kwh_to_mwh(self, normalizer):
        """Test kWh to MWh conversion."""
        result = normalizer.normalize(5000, "kWh", "energy")
        assert result.original_value == 5000
        assert result.original_unit == "kWh"
        assert result.normalized_value == 5.0
        assert result.normalized_unit == "MWh"
        assert result.conversion_factor == 0.001
        assert "SI standard" in result.conversion_source

    def test_gj_to_mwh(self, normalizer):
        """Test GJ to MWh conversion."""
        result = normalizer.normalize(100, "GJ", "energy")
        assert result.normalized_value == pytest.approx(27.7778, rel=1e-4)
        assert result.normalized_unit == "MWh"
        assert "NIST" in result.conversion_source

    def test_btu_to_mwh(self, normalizer):
        """Test BTU to MWh conversion."""
        result = normalizer.normalize(1000000, "BTU", "energy")
        assert result.normalized_value == pytest.approx(293.071, rel=1e-3)
        assert result.normalized_unit == "MWh"

    def test_mwh_identity(self, normalizer):
        """Test MWh to MWh (identity conversion)."""
        result = normalizer.normalize(10.5, "MWh", "energy")
        assert result.normalized_value == 10.5
        assert result.conversion_factor == 1.0

    def test_auto_detect_energy_category(self, normalizer):
        """Test automatic category detection for energy units."""
        result = normalizer.normalize(2000, "kWh")
        assert result.normalized_unit == "MWh"
        assert result.normalized_value == 2.0


class TestMassConversions:
    """Test mass unit conversions."""

    def test_kg_to_tonnes(self, normalizer):
        """Test kg to tonnes conversion."""
        result = normalizer.normalize(1500, "kg", "mass")
        assert result.normalized_value == 1.5
        assert result.normalized_unit == "tonnes"
        assert result.conversion_factor == 0.001

    def test_pounds_to_tonnes(self, normalizer):
        """Test pounds to tonnes conversion."""
        result = normalizer.normalize(2204.62, "pounds", "mass")
        assert result.normalized_value == pytest.approx(1.0, rel=1e-3)
        assert result.normalized_unit == "tonnes"

    def test_short_tons_to_tonnes(self, normalizer):
        """Test short tons to tonnes conversion."""
        result = normalizer.normalize(10, "short_tons", "mass")
        assert result.normalized_value == pytest.approx(9.07185, rel=1e-4)
        assert result.normalized_unit == "tonnes"

    def test_metric_tons_identity(self, normalizer):
        """Test metric tons to tonnes (identity)."""
        result = normalizer.normalize(5.5, "metric_tons", "mass")
        assert result.normalized_value == 5.5
        assert result.normalized_unit == "tonnes"


class TestVolumeConversions:
    """Test volume unit conversions."""

    def test_liters_to_cubic_meters(self, normalizer):
        """Test liters to m³ conversion."""
        result = normalizer.normalize(1000, "liters", "volume")
        assert result.normalized_value == 1.0
        assert result.normalized_unit == "m³"

    def test_gallons_to_cubic_meters(self, normalizer):
        """Test gallons to m³ conversion."""
        result = normalizer.normalize(264.172, "gallons", "volume")
        assert result.normalized_value == pytest.approx(1.0, rel=1e-2)
        assert result.normalized_unit == "m³"


class TestEmissionConversions:
    """Test emission unit conversions."""

    def test_kg_co2e_to_tonnes(self, normalizer):
        """Test kg CO₂e to tonnes CO₂e conversion."""
        result = normalizer.normalize(2500, "kg CO₂e", "emissions")
        assert result.normalized_value == 2.5
        assert result.normalized_unit == "tonnes CO₂e"

    def test_pounds_co2e_to_tonnes(self, normalizer):
        """Test pounds CO₂e to tonnes CO₂e conversion."""
        result = normalizer.normalize(1000, "pounds CO₂e", "emissions")
        assert result.normalized_value == pytest.approx(0.453592, rel=1e-4)
        assert result.normalized_unit == "tonnes CO₂e"


class TestCompoundUnitConversions:
    """Test compound unit conversions."""

    def test_kwh_per_kg_to_mwh_per_tonne(self, normalizer):
        """Test kWh/kg to MWh/tonne conversion."""
        result = normalizer.normalize(0.5, "kWh/kg", "compound_energy_intensity")
        assert result.normalized_value == 0.5
        assert result.normalized_unit == "MWh/tonne"

    def test_gj_per_tonne_to_mwh_per_tonne(self, normalizer):
        """Test GJ/tonne to MWh/tonne conversion."""
        result = normalizer.normalize(10, "GJ/tonne", "compound_energy_intensity")
        assert result.normalized_value == pytest.approx(2.77778, rel=1e-4)
        assert result.normalized_unit == "MWh/tonne"


class TestUnitDetection:
    """Test unit detection from text."""

    def test_detect_kwh(self, normalizer):
        """Test detecting kWh from text."""
        unit, category = normalizer.detect_unit("5000 kWh")
        assert unit == "kWh"
        assert category == "energy"

    def test_detect_tonnes_co2(self, normalizer):
        """Test detecting tonnes CO₂e from text."""
        unit, category = normalizer.detect_unit("12.5 tonnes CO₂e")
        assert unit == "tonnes CO₂e"
        assert category == "emissions"

    def test_detect_kg(self, normalizer):
        """Test detecting kg from text."""
        unit, category = normalizer.detect_unit("1500kg")
        assert unit == "kg"
        assert category == "mass"

    def test_detect_gj(self, normalizer):
        """Test detecting GJ from text."""
        unit, category = normalizer.detect_unit("100 GJ")
        assert unit == "GJ"
        assert category == "energy"

    def test_detect_unit_not_found(self, normalizer):
        """Test unit detection with unknown unit."""
        with pytest.raises(UnitNotFoundError, match="No recognizable unit found"):
            normalizer.detect_unit("5000 xyz")


class TestConversionFactors:
    """Test conversion factor retrieval."""

    def test_get_conversion_factor_same_category(self, normalizer):
        """Test getting conversion factor between units in same category."""
        factor = normalizer.get_conversion_factor("kWh", "MWh")
        assert factor.factor == 0.001
        assert "SI standard" in factor.source

    def test_get_conversion_factor_identity(self, normalizer):
        """Test getting conversion factor for same unit."""
        factor = normalizer.get_conversion_factor("MWh", "MWh")
        assert factor.factor == 1.0
        assert "Identity" in factor.source

    def test_get_conversion_factor_reverse(self, normalizer):
        """Test getting reverse conversion factor."""
        factor = normalizer.get_conversion_factor("MWh", "kWh")
        assert factor.factor == 1000.0

    def test_get_conversion_factor_different_categories(self, normalizer):
        """Test getting conversion factor between different categories."""
        with pytest.raises(CategoryMismatchError, match="Cannot convert between different categories"):
            normalizer.get_conversion_factor("kWh", "kg")

    def test_get_conversion_factor_unknown_unit(self, normalizer):
        """Test getting conversion factor with unknown unit."""
        with pytest.raises(UnitNotFoundError, match="not in database"):
            normalizer.get_conversion_factor("xyz", "MWh")


class TestConversionValidation:
    """Test conversion validation."""

    def test_validate_valid_conversion(self, normalizer):
        """Test validating a valid conversion."""
        assert normalizer.validate_conversion("kWh", "MWh") is True

    def test_validate_invalid_category_mismatch(self, normalizer):
        """Test validating conversion with category mismatch."""
        assert normalizer.validate_conversion("kWh", "kg") is False

    def test_validate_unknown_unit(self, normalizer):
        """Test validating conversion with unknown unit."""
        assert normalizer.validate_conversion("xyz", "MWh") is False


class TestInvalidConversions:
    """Test error handling for invalid conversions."""

    def test_negative_energy_value(self, normalizer):
        """Test negative value for energy (should fail)."""
        with pytest.raises(InvalidValueError, match="Negative value for absolute measure"):
            normalizer.normalize(-100, "kWh", "energy")

    def test_negative_mass_value(self, normalizer):
        """Test negative value for mass (should fail)."""
        with pytest.raises(InvalidValueError, match="Negative value for absolute measure"):
            normalizer.normalize(-50, "kg", "mass")

    def test_unknown_unit(self, normalizer):
        """Test conversion with unknown unit."""
        with pytest.raises(UnitNotFoundError, match="not in database"):
            normalizer.normalize(100, "xyz", "energy")

    def test_unknown_category(self, normalizer):
        """Test conversion with unknown category."""
        with pytest.raises(UnitNotFoundError, match="Category .* not in database"):
            normalizer.normalize(100, "kWh", "unknown_category")

    def test_unit_not_in_category(self, normalizer):
        """Test unit not found in specified category."""
        with pytest.raises(UnitNotFoundError, match="not found in category"):
            normalizer.normalize(100, "kg", "energy")

    def test_non_linear_conversion(self, normalizer):
        """Test non-linear conversion (temperature)."""
        with pytest.raises(InvalidValueError, match="Non-linear conversion"):
            normalizer.normalize(32, "°F", "temperature")


class TestUtilityMethods:
    """Test utility methods."""

    def test_get_base_unit(self, normalizer):
        """Test getting base unit for category."""
        assert normalizer.get_base_unit("energy") == "MWh"
        assert normalizer.get_base_unit("mass") == "tonnes"
        assert normalizer.get_base_unit("volume") == "m3"

    def test_get_base_unit_unknown_category(self, normalizer):
        """Test getting base unit for unknown category."""
        with pytest.raises(UnitNotFoundError, match="Category .* not in database"):
            normalizer.get_base_unit("unknown")

    def test_get_supported_units_all(self, normalizer):
        """Test getting all supported units."""
        units = normalizer.get_supported_units()
        assert "energy" in units
        assert "mass" in units
        assert "MWh" in units["energy"]
        assert "kWh" in units["energy"]
        assert "tonnes" in units["mass"]
        assert "kg" in units["mass"]

    def test_get_supported_units_by_category(self, normalizer):
        """Test getting supported units for specific category."""
        units = normalizer.get_supported_units("energy")
        assert "energy" in units
        assert "mass" not in units
        assert "MWh" in units["energy"]
        assert "kWh" in units["energy"]

    def test_get_supported_units_unknown_category(self, normalizer):
        """Test getting supported units for unknown category raises exception."""
        with pytest.raises(UnitNotFoundError, match="Category .* not in database"):
            normalizer.get_supported_units("unknown")


class TestEdgeCases:
    """Test edge cases."""

    def test_zero_value(self, normalizer):
        """Test conversion with zero value."""
        result = normalizer.normalize(0, "kWh", "energy")
        assert result.normalized_value == 0
        assert result.normalized_unit == "MWh"

    def test_very_large_value(self, normalizer):
        """Test conversion with very large value."""
        result = normalizer.normalize(1e9, "kWh", "energy")
        assert result.normalized_value == 1e6
        assert result.normalized_unit == "MWh"

    def test_very_small_value(self, normalizer):
        """Test conversion with very small value."""
        result = normalizer.normalize(0.001, "kWh", "energy")
        assert result.normalized_value == pytest.approx(0.000001, rel=1e-6)
        assert result.normalized_unit == "MWh"

    def test_decimal_precision(self, normalizer):
        """Test conversion maintains precision."""
        result = normalizer.normalize(1234.5678, "kWh", "energy")
        assert result.normalized_value == pytest.approx(1.2345678, rel=1e-6)


class TestFileErrors:
    """Test file loading error handling."""

    def test_missing_file(self):
        """Test error when conversion factors file is missing."""
        with pytest.raises(ConversionDataError, match="file not found"):
            UnitNormalizer("nonexistent.json")

    def test_invalid_json(self, tmp_path):
        """Test error when conversion factors file has invalid JSON."""
        invalid_file = tmp_path / "invalid.json"
        invalid_file.write_text("{invalid json}")
        with pytest.raises(ConversionDataError, match="Invalid JSON"):
            UnitNormalizer(str(invalid_file))
