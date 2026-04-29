"""Unit normalization service for ESG data."""

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple


class UnitNotFoundError(Exception):
    """Raised when a unit is not found in the conversion database."""
    pass


class CategoryMismatchError(Exception):
    """Raised when attempting to convert between incompatible unit categories."""
    pass


class InvalidValueError(Exception):
    """Raised when a value is invalid for the given unit type."""
    pass


class ConversionDataError(Exception):
    """Raised when conversion data file cannot be loaded or is invalid."""
    pass


@dataclass
class ConversionFactor:
    """Conversion factor with metadata."""
    factor: Optional[float]
    source: str
    formula: str


@dataclass
class NormalizationResult:
    """Result of unit normalization."""
    original_value: float
    original_unit: str
    normalized_value: float
    normalized_unit: str
    conversion_factor: Optional[float]
    conversion_source: str
    formula: str


class UnitNormalizer:
    """Normalizes units to standard base units using conversion factors."""

    def __init__(self, conversion_factors_path: str):
        """Initialize normalizer with conversion factors.
        
        Args:
            conversion_factors_path: Path to conversion_factors.json
        """
        self.conversion_factors_path = Path(conversion_factors_path)
        self._load_conversion_factors()
        self._build_unit_lookup()

    def _load_conversion_factors(self) -> None:
        """Load conversion factors from JSON file.
        
        Raises:
            ConversionDataError: If file cannot be loaded or parsed
        """
        try:
            with open(self.conversion_factors_path, 'r', encoding='utf-8') as f:
                self.conversion_data = json.load(f)
        except FileNotFoundError as e:
            raise ConversionDataError(
                f"Conversion factors file not found: {self.conversion_factors_path}"
            ) from e
        except json.JSONDecodeError as e:
            raise ConversionDataError(
                f"Invalid JSON in conversion factors file: {e}"
            ) from e
        except Exception as e:
            raise ConversionDataError(
                f"Error loading conversion factors: {e}"
            ) from e

    def _build_unit_lookup(self) -> None:
        """Build reverse lookup: unit -> (category, base_unit)."""
        self.unit_lookup: Dict[str, Tuple[str, str]] = {}
        
        for category, data in self.conversion_data.items():
            base_unit = data['base_unit']
            # Add base unit itself
            self.unit_lookup[base_unit] = (category, base_unit)
            # Add all convertible units
            for unit in data['conversions'].keys():
                self.unit_lookup[unit] = (category, base_unit)

    def normalize(
        self,
        value: float,
        from_unit: str,
        category: Optional[str] = None
    ) -> NormalizationResult:
        """Normalize a value to the base unit of its category.
        
        Args:
            value: The numeric value to normalize
            from_unit: The unit to convert from
            category: Optional category hint (auto-detected if not provided)
            
        Returns:
            NormalizationResult with normalized value and metadata
            
        Raises:
            UnitNotFoundError: If unit is not in database
            InvalidValueError: If value is invalid for the unit type
        """
        # Detect category if not provided
        if category is None:
            if from_unit not in self.unit_lookup:
                raise UnitNotFoundError(f"Unit '{from_unit}' not in database")
            category, base_unit = self.unit_lookup[from_unit]
        else:
            if category not in self.conversion_data:
                raise UnitNotFoundError(f"Category '{category}' not in database")
            base_unit = self.conversion_data[category]['base_unit']

        # Validate value (check if category requires non-negative values)
        # Dynamically determine from conversion data instead of hardcoded list
        non_negative_categories = {'energy', 'mass', 'volume', 'emissions', 'area', 'power', 'pressure'}
        if value < 0 and category in non_negative_categories:
            raise InvalidValueError(f"Negative value for absolute measure: {value} {from_unit}")

        # If already in base unit, return as-is
        if from_unit == base_unit:
            return NormalizationResult(
                original_value=value,
                original_unit=from_unit,
                normalized_value=value,
                normalized_unit=base_unit,
                conversion_factor=1.0,
                conversion_source="No conversion needed",
                formula=f"{from_unit} * 1.0 = {base_unit}"
            )

        # Get conversion factor
        conversions = self.conversion_data[category]['conversions']
        if from_unit not in conversions:
            raise UnitNotFoundError(f"Unit '{from_unit}' not found in category '{category}'")

        conversion_info = conversions[from_unit]
        factor = conversion_info['factor']
        
        # Handle non-linear conversions (e.g., temperature)
        if factor is None:
            raise InvalidValueError(
                f"Non-linear conversion for {from_unit} requires special handling. "
                f"Formula: {conversion_info['formula']}"
            )

        # Apply conversion
        normalized_value = value * factor

        return NormalizationResult(
            original_value=value,
            original_unit=from_unit,
            normalized_value=normalized_value,
            normalized_unit=base_unit,
            conversion_factor=factor,
            conversion_source=conversion_info['source'],
            formula=conversion_info['formula']
        )

    def detect_unit(self, text: str) -> Tuple[str, str]:
        """Detect unit and category from text.
        
        Args:
            text: Text containing a value and unit (e.g., "5000 kWh")
            
        Returns:
            Tuple of (unit, category)
            
        Raises:
            UnitNotFoundError: If no recognizable unit found
        """
        text = text.strip()
        
        # Try to match known units (longest first to avoid partial matches)
        sorted_units = sorted(self.unit_lookup.keys(), key=len, reverse=True)
        
        for unit in sorted_units:
            # Escape special regex characters in unit
            escaped_unit = re.escape(unit)
            # Match unit at word boundary or end of string
            pattern = rf'\b{escaped_unit}(?:\b|$)'
            if re.search(pattern, text, re.IGNORECASE):
                category, _ = self.unit_lookup[unit]
                return (unit, category)
        
        raise UnitNotFoundError(f"No recognizable unit found in text: '{text}'")

    def get_conversion_factor(
        self,
        from_unit: str,
        to_unit: str
    ) -> ConversionFactor:
        """Get conversion factor between two units.
        
        Args:
            from_unit: Source unit
            to_unit: Target unit
            
        Returns:
            ConversionFactor with factor, source, and formula
            
        Raises:
            UnitNotFoundError: If either unit is not in database
            CategoryMismatchError: If units are from different categories
        """
        # Validate both units exist
        if from_unit not in self.unit_lookup:
            raise UnitNotFoundError(f"Unit '{from_unit}' not in database")
        if to_unit not in self.unit_lookup:
            raise UnitNotFoundError(f"Unit '{to_unit}' not in database")

        # Validate same category
        from_category, from_base = self.unit_lookup[from_unit]
        to_category, to_base = self.unit_lookup[to_unit]
        
        if from_category != to_category:
            raise CategoryMismatchError(
                f"Cannot convert between different categories: "
                f"{from_unit} ({from_category}) to {to_unit} ({to_category})"
            )

        # If same unit, return identity
        if from_unit == to_unit:
            return ConversionFactor(
                factor=1.0,
                source="Identity conversion",
                formula=f"{from_unit} * 1.0 = {to_unit}"
            )

        # Get conversion to base unit
        conversions = self.conversion_data[from_category]['conversions']
        
        # Convert from_unit to base
        if from_unit == from_base:
            from_factor = 1.0
            from_source = "Base unit"
        else:
            from_info = conversions[from_unit]
            from_factor = from_info['factor']
            from_source = from_info['source']
            if from_factor is None:
                raise InvalidValueError(f"Non-linear conversion not supported: {from_unit}")

        # Convert base to to_unit
        if to_unit == to_base:
            to_factor = 1.0
            to_source = "Base unit"
        else:
            to_info = conversions[to_unit]
            to_factor = to_info['factor']
            to_source = to_info['source']
            if to_factor is None:
                raise InvalidValueError(f"Non-linear conversion not supported: {to_unit}")

        # Combined factor: from_unit -> base -> to_unit
        combined_factor = from_factor / to_factor
        
        return ConversionFactor(
            factor=combined_factor,
            source=f"{from_source}, {to_source}",
            formula=f"{from_unit} * {combined_factor} = {to_unit}"
        )

    def validate_conversion(self, from_unit: str, to_unit: str) -> bool:
        """Check if conversion between two units is valid.
        
        Args:
            from_unit: Source unit
            to_unit: Target unit
            
        Returns:
            True if conversion is valid, False otherwise
        """
        try:
            self.get_conversion_factor(from_unit, to_unit)
            return True
        except (UnitNotFoundError, CategoryMismatchError, InvalidValueError):
            return False

    def get_base_unit(self, category: str) -> str:
        """Get the base unit for a category.
        
        Args:
            category: Category name
            
        Returns:
            Base unit string
            
        Raises:
            UnitNotFoundError: If category not found
        """
        if category not in self.conversion_data:
            raise UnitNotFoundError(f"Category '{category}' not in database")
        return self.conversion_data[category]['base_unit']

    def get_supported_units(self, category: Optional[str] = None) -> Dict[str, list]:
        """Get all supported units, optionally filtered by category.
        
        Args:
            category: Optional category to filter by
            
        Returns:
            Dictionary mapping categories to lists of supported units
            
        Raises:
            UnitNotFoundError: If specified category not found
        """
        if category:
            if category not in self.conversion_data:
                raise UnitNotFoundError(f"Category '{category}' not in database")
            return {
                category: [
                    self.conversion_data[category]['base_unit']
                ] + list(self.conversion_data[category]['conversions'].keys())
            }
        
        result = {}
        for cat, data in self.conversion_data.items():
            result[cat] = [data['base_unit']] + list(data['conversions'].keys())
        return result
