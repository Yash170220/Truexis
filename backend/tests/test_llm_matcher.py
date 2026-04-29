"""Unit tests for LLM matcher"""
import json
from unittest.mock import Mock, patch

import pytest

from src.matching.llm_matcher import LLMMatcher, MatchResult


@pytest.fixture
def standard_indicators():
    """Standard ESG indicators list"""
    return [
        "Total Electricity Consumption",
        "Scope 1 GHG Emissions",
        "Total Water Consumption",
        "Production Output",
        "Waste Recycled"
    ]


@pytest.fixture
def llm_matcher(standard_indicators):
    """Create LLM matcher instance without cache"""
    with patch('src.matching.llm_matcher.settings') as mock_settings:
        mock_settings.groq.api_key = "test-key"
        mock_settings.groq.model = "llama-3.3-70b-versatile"
        mock_settings.groq.temperature = 0.7
        mock_settings.groq.max_tokens = 2048
        mock_settings.redis.url = "redis://localhost:6379/0"
        
        matcher = LLMMatcher(standard_indicators, use_cache=False)
        return matcher


class TestLLMMatcherInit:
    """Tests for LLM matcher initialization"""
    
    def test_init_with_indicators(self, standard_indicators):
        """Test initialization with standard indicators"""
        with patch('src.matching.llm_matcher.settings') as mock_settings:
            mock_settings.groq.api_key = "test-key"
            mock_settings.groq.model = "llama-3.3-70b-versatile"
            mock_settings.groq.temperature = 0.7
            mock_settings.groq.max_tokens = 2048
            
            matcher = LLMMatcher(standard_indicators, use_cache=False)
            
            assert matcher.standard_indicators == standard_indicators
            assert matcher.model == "llama-3.3-70b-versatile"
            assert matcher.use_cache == False


class TestPromptBuilding:
    """Tests for prompt building"""
    
    def test_build_prompt_includes_header(self, llm_matcher):
        """Test that prompt includes the header"""
        header = "Electricity Usage"
        prompt = llm_matcher.build_prompt(header)
        
        assert header in prompt
        assert "STANDARD ESG INDICATORS" in prompt
        assert "FEW-SHOT EXAMPLES" in prompt
    
    def test_build_prompt_includes_examples(self, llm_matcher):
        """Test that prompt includes few-shot examples"""
        prompt = llm_matcher.build_prompt("Test Header")
        
        assert "Pwr Consumption" in prompt
        assert "CO₂ output" in prompt
        assert "Total Electricity Consumption" in prompt


class TestResponseParsing:
    """Tests for response parsing"""
    
    def test_parse_valid_response(self, llm_matcher):
        """Test parsing valid JSON response"""
        response = json.dumps({
            "canonical_name": "Total Electricity Consumption",
            "confidence": 0.92,
            "reasoning": "Clear match"
        })
        
        result = llm_matcher.parse_response(response)
        
        assert result is not None
        assert result.canonical_name == "Total Electricity Consumption"
        assert result.confidence == 0.92
        assert result.method == "llm"
        assert result.reasoning == "Clear match"
    
    def test_parse_low_confidence(self, llm_matcher):
        """Test parsing response with low confidence"""
        response = json.dumps({
            "canonical_name": "Some Indicator",
            "confidence": 0.5,
            "reasoning": "Weak match"
        })
        
        result = llm_matcher.parse_response(response)
        
        assert result is None  # Below 0.7 threshold
    
    def test_parse_invalid_json(self, llm_matcher):
        """Test parsing invalid JSON"""
        response = "This is not JSON"
        
        result = llm_matcher.parse_response(response)
        
        assert result is not None
        assert result.confidence == 0.0
        assert result.reasoning == "Invalid JSON response"
    
    def test_parse_invalid_confidence(self, llm_matcher):
        """Test parsing response with invalid confidence value"""
        response = json.dumps({
            "canonical_name": "Test Indicator",
            "confidence": 1.5,  # Invalid: > 1.0
            "reasoning": "Test"
        })
        
        result = llm_matcher.parse_response(response)
        
        assert result is not None
        assert result.confidence == 1.0  # Clamped to 1.0


class TestLLMMatching:
    """Tests for LLM matching with mocked API"""
    
    @patch('src.matching.llm_matcher.Groq')
    def test_match_success(self, mock_groq_class, llm_matcher):
        """Test successful match"""
        # Mock Groq API response
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = json.dumps({
            "canonical_name": "Total Electricity Consumption",
            "confidence": 0.92,
            "reasoning": "Clear match"
        })
        
        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        llm_matcher.client = mock_client
        
        result = llm_matcher.match("Electricity Usage")
        
        assert result is not None
        assert result.canonical_name == "Total Electricity Consumption"
        assert result.confidence == 0.92
        assert result.method == "llm"
    
    @patch('src.matching.llm_matcher.Groq')
    def test_match_with_retry(self, mock_groq_class, llm_matcher):
        """Test match with retry on failure"""
        # First call fails, second succeeds
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = json.dumps({
            "canonical_name": "Total Water Consumption",
            "confidence": 0.88,
            "reasoning": "Match found"
        })
        
        mock_client = Mock()
        mock_client.chat.completions.create.side_effect = [
            Exception("API Error"),
            mock_response
        ]
        llm_matcher.client = mock_client
        
        result = llm_matcher.match("Water Usage", max_retries=2)
        
        assert result is not None
        assert result.canonical_name == "Total Water Consumption"
    
    @patch('src.matching.llm_matcher.Groq')
    def test_match_all_retries_fail(self, mock_groq_class, llm_matcher):
        """Test match when all retries fail"""
        mock_client = Mock()
        mock_client.chat.completions.create.side_effect = Exception("API Error")
        llm_matcher.client = mock_client
        
        result = llm_matcher.match("Test Header", max_retries=2)
        
        assert result is not None
        assert result.confidence == 0.0
        assert "failed" in result.reasoning.lower()


class TestBatchMatching:
    """Tests for batch matching"""
    
    @patch('src.matching.llm_matcher.Groq')
    def test_match_batch(self, mock_groq_class, llm_matcher):
        """Test batch matching"""
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = json.dumps({
            "canonical_name": "Test Indicator",
            "confidence": 0.90,
            "reasoning": "Match"
        })
        
        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        llm_matcher.client = mock_client
        
        headers = ["Header 1", "Header 2"]
        results = llm_matcher.match_batch(headers)
        
        assert len(results) == 2
        assert "Header 1" in results
        assert "Header 2" in results
