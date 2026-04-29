"""Rule-based entity matcher using synonym dictionary"""
import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from rapidfuzz import fuzz, process

logger = logging.getLogger(__name__)


class MatchResult:
    """Result of a matching operation"""
    def __init__(
        self,
        canonical_name: str,
        confidence: float,
        method: str,
        matched_synonym: str,
        indicator_key: str,
        unit: str,
        category: str
    ):
        self.canonical_name = canonical_name
        self.confidence = confidence
        self.method = method
        self.matched_synonym = matched_synonym
        self.indicator_key = indicator_key
        self.unit = unit
        self.category = category

    def __repr__(self):
        return f"<MatchResult({self.canonical_name}, {self.confidence:.2f}, {self.method})>"


class RuleBasedMatcher:
    """Rule-based matcher using synonym dictionary and fuzzy matching"""

    def __init__(self, synonym_dict_path: str):
        """Initialize matcher with synonym dictionary"""
        self.synonym_dict_path = synonym_dict_path
        self.indicators = {}
        self.reverse_lookup = {}
        self.all_synonyms = []
        self._load_dictionary()

    def _load_dictionary(self):
        """Load synonym dictionary from JSON"""
        logger.info(f"Loading synonym dictionary from {self.synonym_dict_path}")
        
        with open(self.synonym_dict_path, 'r') as f:
            data = json.load(f)
        
        self.indicators = data['standard_indicators']
        
        # Build reverse lookup: synonym -> indicator_key
        for indicator_key, indicator_data in self.indicators.items():
            canonical = indicator_data['canonical_name'].lower()
            
            # Add canonical name to reverse lookup
            self.reverse_lookup[canonical] = indicator_key
            self.all_synonyms.append(canonical)
            
            # Add all synonyms
            for synonym in indicator_data['synonyms']:
                synonym_clean = synonym.lower()
                self.reverse_lookup[synonym_clean] = indicator_key
                self.all_synonyms.append(synonym_clean)
        
        logger.info(f"Loaded {len(self.indicators)} indicators with {len(self.all_synonyms)} total synonyms")

    def _clean_header(self, header: str) -> str:
        """Clean header for matching"""
        # Convert to lowercase
        cleaned = header.lower()
        
        # Remove special characters but keep spaces and hyphens
        cleaned = re.sub(r'[^\w\s\-]', ' ', cleaned)
        
        # Remove extra whitespace
        cleaned = ' '.join(cleaned.split())
        
        # Strip
        cleaned = cleaned.strip()
        
        return cleaned

    def match(self, header: str) -> Optional[MatchResult]:
        """Match header to standard indicator"""
        logger.debug(f"Matching header: {header}")
        
        cleaned_header = self._clean_header(header)
        
        # Try exact match first
        exact_result = self.exact_match(cleaned_header)
        if exact_result:
            return exact_result
        
        # Try fuzzy match
        fuzzy_result = self.fuzzy_match(cleaned_header)
        if fuzzy_result:
            return fuzzy_result
        
        logger.debug(f"No match found for: {header}")
        return None

    def exact_match(self, header: str) -> Optional[MatchResult]:
        """Perform exact match lookup"""
        cleaned = self._clean_header(header)
        
        if cleaned in self.reverse_lookup:
            indicator_key = self.reverse_lookup[cleaned]
            indicator_data = self.indicators[indicator_key]
            
            logger.debug(f"Exact match: {header} -> {indicator_data['canonical_name']}")
            
            return MatchResult(
                canonical_name=indicator_data['canonical_name'],
                confidence=1.0,
                method="exact",
                matched_synonym=cleaned,
                indicator_key=indicator_key,
                unit=indicator_data['unit'],
                category=indicator_data['category']
            )
        
        return None

    def fuzzy_match(self, header: str) -> Optional[Tuple[MatchResult, float]]:
        """Perform fuzzy match using RapidFuzz"""
        cleaned = self._clean_header(header)
        
        # Use RapidFuzz to find best match
        result = process.extractOne(
            cleaned,
            self.all_synonyms,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=70  # Minimum score threshold
        )
        
        if not result:
            return None
        
        matched_synonym, score, _ = result
        confidence = score / 100.0  # Convert to 0-1 scale
        
        # Get indicator key from matched synonym
        indicator_key = self.reverse_lookup[matched_synonym]
        indicator_data = self.indicators[indicator_key]
        
        # Check against indicator's fuzzy threshold
        threshold = indicator_data.get('fuzzy_threshold', 0.85)
        
        if confidence < threshold:
            logger.debug(f"Fuzzy match below threshold: {header} -> {matched_synonym} ({confidence:.2f} < {threshold})")
            return None
        
        logger.debug(f"Fuzzy match: {header} -> {indicator_data['canonical_name']} ({confidence:.2f})")
        
        return MatchResult(
            canonical_name=indicator_data['canonical_name'],
            confidence=confidence,
            method="fuzzy",
            matched_synonym=matched_synonym,
            indicator_key=indicator_key,
            unit=indicator_data['unit'],
            category=indicator_data['category']
        )

    def match_batch(self, headers: List[str]) -> Dict[str, Optional[MatchResult]]:
        """Match multiple headers at once"""
        results = {}
        for header in headers:
            results[header] = self.match(header)
        return results

    def get_indicator_info(self, indicator_key: str) -> Optional[Dict]:
        """Get full information about an indicator"""
        return self.indicators.get(indicator_key)

    def list_indicators(self, category: Optional[str] = None) -> List[Dict]:
        """List all indicators, optionally filtered by category"""
        indicators = []
        for key, data in self.indicators.items():
            if category is None or data['category'] == category:
                indicators.append({
                    'key': key,
                    'canonical_name': data['canonical_name'],
                    'unit': data['unit'],
                    'category': data['category']
                })
        return indicators
