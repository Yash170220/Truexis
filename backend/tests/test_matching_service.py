"""Unit tests for matching service"""
from unittest.mock import Mock, MagicMock
from uuid import uuid4

import pytest

from src.matching.service import MatchingService, MatchingResult


@pytest.fixture
def mock_rule_matcher():
    """Mock rule-based matcher"""
    matcher = Mock()
    return matcher


@pytest.fixture
def mock_llm_matcher():
    """Mock LLM matcher"""
    matcher = Mock()
    return matcher


@pytest.fixture
def mock_db():
    """Mock database session"""
    db = Mock()
    db.commit = Mock()
    db.add = Mock()
    db.query = Mock()
    return db


@pytest.fixture
def matching_service(mock_rule_matcher, mock_llm_matcher, mock_db):
    """Create matching service instance"""
    return MatchingService(mock_rule_matcher, mock_llm_matcher, mock_db)


class TestGetBestMatch:
    """Tests for get_best_match method"""
    
    def test_rule_match_high_confidence(self, matching_service, mock_rule_matcher):
        """Test rule match with high confidence is accepted"""
        # Mock rule matcher result
        rule_result = Mock()
        rule_result.canonical_name = "Total Electricity Consumption"
        rule_result.confidence = 0.95
        rule_result.method = "exact"
        rule_result.unit = "MWh"
        rule_result.category = "energy"
        
        mock_rule_matcher.match.return_value = rule_result
        
        result = matching_service.get_best_match("Electricity (kWh)")
        
        assert result is not None
        assert result.matched_indicator == "Total Electricity Consumption"
        assert result.confidence == 0.95
        assert result.method == "exact"
        assert result.requires_review == False  # Above 0.85 threshold
    
    def test_rule_match_low_confidence_fallback_llm(
        self, matching_service, mock_rule_matcher, mock_llm_matcher
    ):
        """Test fallback to LLM when rule confidence is low"""
        # Mock rule matcher with low confidence
        rule_result = Mock()
        rule_result.confidence = 0.70
        mock_rule_matcher.match.return_value = rule_result
        
        # Mock LLM matcher result
        llm_result = Mock()
        llm_result.canonical_name = "Total Water Consumption"
        llm_result.confidence = 0.88
        llm_result.method = "llm"
        llm_result.reasoning = "LLM match"
        
        mock_llm_matcher.match.return_value = llm_result
        
        result = matching_service.get_best_match("Water Usage")
        
        assert result is not None
        assert result.matched_indicator == "Total Water Consumption"
        assert result.confidence == 0.88
        assert result.method == "llm"
        assert mock_llm_matcher.match.called
    
    def test_no_match_found(self, matching_service, mock_rule_matcher, mock_llm_matcher):
        """Test when no match is found"""
        mock_rule_matcher.match.return_value = None
        mock_llm_matcher.match.return_value = None
        
        result = matching_service.get_best_match("Random Header")
        
        assert result is None


class TestMatchHeaders:
    """Tests for match_headers method"""
    
    def test_match_multiple_headers(
        self, matching_service, mock_rule_matcher, mock_db
    ):
        """Test matching multiple headers"""
        headers = ["Electricity", "Water", "CO2"]
        upload_id = uuid4()
        
        # Mock rule matcher to return results
        def mock_match(header):
            result = Mock()
            result.canonical_name = f"Matched {header}"
            result.confidence = 0.90
            result.method = "exact"
            result.unit = "unit"
            result.category = "category"
            return result
        
        mock_rule_matcher.match.side_effect = mock_match
        
        # Mock database
        mock_db.commit = Mock()
        mock_db.add = Mock()
        
        results = matching_service.match_headers(upload_id, headers)
        
        assert len(results) == 3
        assert all(r.confidence == 0.90 for r in results)


class TestSaveMatch:
    """Tests for save_match method"""
    
    def test_save_match_to_database(self, matching_service, mock_db):
        """Test saving match to database"""
        upload_id = uuid4()
        header = "Test Header"
        
        result = MatchingResult(
            original_header=header,
            matched_indicator="Test Indicator",
            confidence=0.90,
            method="exact",
            requires_review=False
        )
        
        # Mock database
        mock_indicator = Mock()
        mock_indicator.id = uuid4()
        mock_db.add = Mock()
        mock_db.commit = Mock()
        mock_db.refresh = Mock(side_effect=lambda x: setattr(x, 'id', mock_indicator.id))
        
        indicator_id = matching_service.save_match(upload_id, header, result)
        
        assert mock_db.add.called
        assert mock_db.commit.called


class TestReviewQueue:
    """Tests for review queue methods"""
    
    def test_get_review_queue(self, matching_service, mock_db):
        """Test getting review queue"""
        upload_id = uuid4()
        
        # Mock database query
        mock_match = Mock()
        mock_match.id = uuid4()
        mock_match.original_header = "Test"
        mock_match.matched_indicator = "Matched"
        mock_match.confidence_score = 0.75
        mock_match.matching_method.value = "llm"
        mock_match.reviewer_notes = None
        
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = [mock_match]
        
        mock_db.query.return_value = mock_query
        
        results = matching_service.get_review_queue(upload_id)
        
        assert len(results) == 1
        assert results[0].requires_review == True
    
    def test_approve_match(self, matching_service, mock_db):
        """Test approving a match"""
        indicator_id = uuid4()
        
        # Mock database query
        mock_match = Mock()
        mock_match.id = indicator_id
        mock_match.matched_indicator = "Original"
        mock_match.reviewed = False
        
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = mock_match
        
        mock_db.query.return_value = mock_query
        mock_db.commit = Mock()
        mock_db.add = Mock()
        
        matching_service.approve_match(indicator_id, True, notes="Looks good")
        
        assert mock_match.reviewed == True
        assert mock_db.commit.called


class TestMatchingStats:
    """Tests for matching statistics"""

    def test_get_matching_stats(self, matching_service, mock_db):
        """Test getting matching statistics via get_matching_stats"""
        upload_id = uuid4()

        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.scalar.side_effect = [5, 3, 0.88]
        mock_query.group_by.return_value = mock_query
        mock_query.all.return_value = []

        mock_db.query.return_value = mock_query

        stats = matching_service.get_matching_stats(upload_id)

        assert stats["total"] == 5
        assert stats["reviewed"] == 3
        assert stats["requires_review"] == 2


class TestComprehensiveResults:
    """Tests for get_comprehensive_results"""

    def test_comprehensive_results(self, matching_service, mock_db):
        """Test consolidated results include stats, results and review queue"""
        upload_id = uuid4()

        mock_high = Mock()
        mock_high.id = uuid4()
        mock_high.original_header = "Electricity"
        mock_high.matched_indicator = "Total Electricity"
        mock_high.confidence_score = 0.95
        mock_high.reviewed = True
        mock_high.reviewer_notes = None

        mock_low = Mock()
        mock_low.id = uuid4()
        mock_low.original_header = "Misc Energy"
        mock_low.matched_indicator = "Total Electricity"
        mock_low.confidence_score = 0.72
        mock_low.reviewed = False
        mock_low.reviewer_notes = "Ambiguous header"

        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = [mock_high, mock_low]
        mock_db.query.return_value = mock_query

        data = matching_service.get_comprehensive_results(upload_id)

        assert data is not None
        assert data["upload_id"] == upload_id
        assert data["stats"]["total_headers"] == 2
        assert data["stats"]["auto_approved"] == 1
        assert data["stats"]["needs_review"] == 1
        assert len(data["results"]) == 2
        assert len(data["review_queue"]) == 1
        assert data["review_queue"][0]["reasoning"] == "Ambiguous header"

    def test_comprehensive_results_empty(self, matching_service, mock_db):
        """Test returns empty stats when no matches exist"""
        upload_id = uuid4()

        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = []
        mock_db.query.return_value = mock_query

        data = matching_service.get_comprehensive_results(upload_id)

        assert data["stats"]["total_headers"] == 0
        assert data["results"] == []
        assert data["review_queue"] == []
