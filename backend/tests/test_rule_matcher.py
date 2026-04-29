"""Unit tests for rule-based matcher"""
import pytest

from src.matching.rule_matcher import RuleBasedMatcher, MatchResult


@pytest.fixture
def matcher():
    """Create matcher instance with synonym dictionary"""
    return RuleBasedMatcher("data/validation-rules/synonym_dictionary.json")


class TestExactMatch:
    """Tests for exact matching"""
    
    def test_exact_match_canonical_name(self, matcher):
        """Test exact match with canonical name"""
        result = matcher.match("Total Electricity Consumption")
        
        assert result is not None
        assert result.canonical_name == "Total Electricity Consumption"
        assert result.confidence == 1.0
        assert result.method == "exact"
        assert result.unit == "MWh"
    
    def test_exact_match_synonym(self, matcher):
        """Test exact match with synonym"""
        result = matcher.match("electricity consumption")
        
        assert result is not None
        assert result.canonical_name == "Total Electricity Consumption"
        assert result.confidence == 1.0
        assert result.method == "exact"
    
    def test_exact_match_case_insensitive(self, matcher):
        """Test exact match is case insensitive"""
        result = matcher.match("ELECTRICITY CONSUMPTION")
        
        assert result is not None
        assert result.canonical_name == "Total Electricity Consumption"
        assert result.confidence == 1.0
    
    def test_exact_match_with_special_chars(self, matcher):
        """Test exact match with special characters"""
        result = matcher.match("Electricity Consumption (kWh)")
        
        assert result is not None
        assert result.canonical_name == "Total Electricity Consumption"


class TestFuzzyMatch:
    """Tests for fuzzy matching"""
    
    def test_fuzzy_match_high_confidence(self, matcher):
        """Test fuzzy match with high confidence"""
        result = matcher.match("elec consumtion")  # Typo
        
        assert result is not None
        assert result.canonical_name == "Total Electricity Consumption"
        assert result.confidence >= 0.85
        assert result.method == "fuzzy"
    
    def test_fuzzy_match_co2(self, matcher):
        """Test fuzzy match for CO2 emissions"""
        result = matcher.match("carbon dioxide")
        
        assert result is not None
        assert result.canonical_name == "Carbon Dioxide Emissions"
        assert result.confidence >= 0.85
    
    def test_fuzzy_match_water(self, matcher):
        """Test fuzzy match for water"""
        result = matcher.match("water used")
        
        assert result is not None
        assert result.canonical_name == "Total Water Consumption"
        assert result.confidence >= 0.85
    
    def test_fuzzy_match_production(self, matcher):
        """Test fuzzy match for production"""
        result = matcher.match("production tonnes")
        
        assert result is not None
        assert result.canonical_name == "Production Output"
        assert result.confidence >= 0.85


class TestFuzzyMatchLowConfidence:
    """Tests for fuzzy matches below threshold"""
    
    def test_fuzzy_match_low_confidence(self, matcher):
        """Test fuzzy match with low confidence returns None"""
        result = matcher.match("random text that doesnt match")
        
        assert result is None
    
    def test_fuzzy_match_partial_word(self, matcher):
        """Test fuzzy match with very different text"""
        result = matcher.match("xyz")
        
        assert result is None


class TestNoMatch:
    """Tests for no match scenarios"""
    
    def test_no_match_random_text(self, matcher):
        """Test no match for random text"""
        result = matcher.match("completely unrelated header")
        
        assert result is None
    
    def test_no_match_empty_string(self, matcher):
        """Test no match for empty string"""
        result = matcher.match("")
        
        assert result is None
    
    def test_no_match_numbers_only(self, matcher):
        """Test no match for numbers only"""
        result = matcher.match("12345")
        
        assert result is None


class TestBatchMatching:
    """Tests for batch matching"""
    
    def test_match_batch(self, matcher):
        """Test matching multiple headers at once"""
        headers = [
            "Electricity (kWh)",
            "CO2 Emissions (kg)",
            "Water (m³)",
            "Production (tonnes)"
        ]
        
        results = matcher.match_batch(headers)
        
        assert len(results) == 4
        assert results["Electricity (kWh)"] is not None
        assert results["CO2 Emissions (kg)"] is not None
        assert results["Water (m³)"] is not None
        assert results["Production (tonnes)"] is not None
    
    def test_match_batch_mixed(self, matcher):
        """Test batch matching with some matches and some non-matches"""
        headers = [
            "Electricity (kWh)",
            "Random Header",
            "CO2 Emissions"
        ]
        
        results = matcher.match_batch(headers)
        
        assert results["Electricity (kWh)"] is not None
        assert results["Random Header"] is None
        assert results["CO2 Emissions"] is not None


class TestIndicatorInfo:
    """Tests for indicator information retrieval"""
    
    def test_get_indicator_info(self, matcher):
        """Test getting indicator information"""
        info = matcher.get_indicator_info("total_electricity")
        
        assert info is not None
        assert info['canonical_name'] == "Total Electricity Consumption"
        assert info['unit'] == "MWh"
        assert info['category'] == "energy"
        assert len(info['synonyms']) > 0
    
    def test_list_indicators(self, matcher):
        """Test listing all indicators"""
        indicators = matcher.list_indicators()
        
        assert len(indicators) > 0
        assert all('canonical_name' in ind for ind in indicators)
    
    def test_list_indicators_by_category(self, matcher):
        """Test listing indicators by category"""
        energy_indicators = matcher.list_indicators(category="energy")
        
        assert len(energy_indicators) > 0
        assert all(ind['category'] == "energy" for ind in energy_indicators)


class TestRealWorldHeaders:
    """Tests with real-world header variations"""
    
    def test_cement_plant_headers(self, matcher):
        """Test headers from cement plant data"""
        headers = [
            "Electricity Consumption (kWh)",
            "Power Usage (MWh)",
            "Energy - Electrical (GJ)",
            "CO2 Emissions (tonnes)",
            "Carbon Dioxide (kg)",
            "GHG - Scope 1 (tCO2e)"
        ]
        
        results = matcher.match_batch(headers)
        
        # All should match
        assert all(result is not None for result in results.values())
        
        # First three should map to electricity
        assert results["Electricity Consumption (kWh)"].canonical_name == "Total Electricity Consumption"
        assert results["Power Usage (MWh)"].canonical_name == "Total Electricity Consumption"
        assert results["Energy - Electrical (GJ)"].canonical_name == "Total Electricity Consumption"
        
        # Last three should map to scope 1 emissions
        assert "Emissions" in results["CO2 Emissions (tonnes)"].canonical_name
    
    def test_steel_facility_headers(self, matcher):
        """Test headers from steel facility data"""
        headers = [
            "Production (tonnes)",
            "Fuel Consumption (GJ)",
            "Emissions (kg CO2)"
        ]
        
        results = matcher.match_batch(headers)
        
        assert results["Production (tonnes)"] is not None
        assert results["Fuel Consumption (GJ)"] is not None
        assert results["Emissions (kg CO2)"] is not None
