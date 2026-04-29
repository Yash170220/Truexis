"""LLM-based entity matcher using Anthropic Claude API"""
import json
import logging
import time
from typing import List, Optional

import anthropic
import redis

from src.common.config import settings

logger = logging.getLogger(__name__)


class MatchResult:
    """Result of a matching operation"""
    def __init__(
        self,
        canonical_name: str,
        confidence: float,
        method: str,
        reasoning: str = ""
    ):
        self.canonical_name = canonical_name
        self.confidence = confidence
        self.method = method
        self.reasoning = reasoning

    def __repr__(self):
        return f"<MatchResult({self.canonical_name}, {self.confidence:.2f}, {self.method})>"


class LLMMatcher:
    """LLM-based matcher using Anthropic Claude with caching"""

    def __init__(self, standard_indicators: List[str], use_cache: bool = True):
        """Initialize LLM matcher with Anthropic client"""
        self.client = anthropic.Anthropic(api_key=settings.claude.api_key)
        self.model = settings.claude.model
        self.temperature = settings.claude.temperature
        self.standard_indicators = standard_indicators
        self.use_cache = use_cache
        
        # Initialize Redis cache
        if self.use_cache:
            try:
                self.cache = redis.from_url(settings.redis.url, decode_responses=True)
                self.cache.ping()
                logger.info("Redis cache connected")
            except Exception as e:
                logger.warning(f"Redis cache unavailable: {e}. Proceeding without cache.")
                self.use_cache = False

    def match(self, header: str, max_retries: int = 3) -> Optional[MatchResult]:
        """Match header using LLM with retry logic"""
        logger.info(f"LLM matching header: {header}")
        
        # Check cache first
        if self.use_cache:
            cached_result = self._get_from_cache(header)
            if cached_result:
                logger.info(f"Cache hit for: {header}")
                return cached_result
        
        # Build prompt
        prompt = self.build_prompt(header)
        
        # Call LLM with retry logic
        for attempt in range(max_retries):
            try:
                response = self.client.messages.create(
                    model=self.model,
                    system=self._get_system_prompt(),
                    messages=[{"role": "user", "content": prompt}],
                    temperature=self.temperature,
                    max_tokens=settings.claude.max_tokens,
                )

                # Parse response
                result = self.parse_response(response.content[0].text)
                
                # Cache result
                if self.use_cache and result:
                    self._save_to_cache(header, result)
                
                return result
                
            except Exception as e:
                logger.warning(f"LLM API call failed (attempt {attempt + 1}/{max_retries}): {e}")
                
                if attempt < max_retries - 1:
                    # Exponential backoff
                    wait_time = 2 ** attempt
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"All retry attempts failed for: {header}")
                    return MatchResult(
                        canonical_name="",
                        confidence=0.0,
                        method="llm",
                        reasoning="API call failed after retries"
                    )
        
        return None

    def _get_system_prompt(self) -> str:
        """Get system prompt for LLM"""
        return """You are an ESG (Environmental, Social, and Governance) data expert specializing in standardizing data column headers from various industrial facilities.

Your task is to match user-provided column headers to standard ESG indicators. Consider:
- Common abbreviations (e.g., "Pwr" for "Power", "Elec" for "Electricity")
- Unit variations (kWh, MWh, GJ are all energy units)
- Industry terminology (e.g., "Scope 1" for direct emissions)
- Typos and formatting inconsistencies

Respond ONLY with valid JSON in this exact format:
{
  "canonical_name": "Standard Indicator Name",
  "confidence": 0.92,
  "reasoning": "Brief explanation of the match"
}

Confidence scale:
- 0.95-1.0: Exact or near-exact match
- 0.85-0.94: Strong match with minor variations
- 0.70-0.84: Probable match with some uncertainty
- Below 0.70: Weak match, likely incorrect"""

    def build_prompt(self, header: str) -> str:
        """Build few-shot prompt for LLM"""
        # Format standard indicators list
        indicators_list = "\n".join([f"- {ind}" for ind in self.standard_indicators[:20]])  # Limit to 20 for token efficiency
        
        prompt = f"""Match the following column header to a standard ESG indicator.

STANDARD ESG INDICATORS:
{indicators_list}

FEW-SHOT EXAMPLES:

Example 1:
Header: "Pwr Consumption"
Response: {{"canonical_name": "Total Electricity Consumption", "confidence": 0.92, "reasoning": "Pwr is common abbreviation for Power, and consumption indicates usage of electrical energy"}}

Example 2:
Header: "CO₂ output"
Response: {{"canonical_name": "Scope 1 GHG Emissions", "confidence": 0.88, "reasoning": "CO₂ output refers to carbon dioxide emissions, which are typically categorized as Scope 1 direct emissions"}}

Example 3:
Header: "H2O used"
Response: {{"canonical_name": "Total Water Consumption", "confidence": 0.90, "reasoning": "H2O is chemical formula for water, and 'used' indicates consumption"}}

Example 4:
Header: "Waste - Recycled (%)"
Response: {{"canonical_name": "Waste Diversion Rate", "confidence": 0.95, "reasoning": "Recycled waste percentage directly corresponds to waste diversion rate metric"}}

Example 5:
Header: "Random Column XYZ"
Response: {{"canonical_name": "", "confidence": 0.0, "reasoning": "No clear match to any standard ESG indicator"}}

NOW MATCH THIS HEADER:
Header: "{header}"

Respond with JSON only:"""
        
        return prompt

    def parse_response(self, llm_output: str) -> Optional[MatchResult]:
        """Parse LLM response and extract match result"""
        try:
            # Parse JSON
            data = json.loads(llm_output)
            
            # Extract fields
            canonical_name = data.get("canonical_name", "")
            confidence = float(data.get("confidence", 0.0))
            reasoning = data.get("reasoning", "")
            
            # Validate confidence
            if not (0.0 <= confidence <= 1.0):
                logger.warning(f"Invalid confidence value: {confidence}. Clamping to [0, 1]")
                confidence = max(0.0, min(1.0, confidence))
            
            # Return None if no match
            if not canonical_name or confidence < 0.7:
                logger.info(f"Low confidence match: {confidence}")
                return None
            
            return MatchResult(
                canonical_name=canonical_name,
                confidence=confidence,
                method="llm",
                reasoning=reasoning
            )
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            logger.debug(f"Raw response: {llm_output}")
            return MatchResult(
                canonical_name="",
                confidence=0.0,
                method="llm",
                reasoning="Invalid JSON response"
            )
        except Exception as e:
            logger.error(f"Error parsing LLM response: {e}")
            return None

    def _get_from_cache(self, header: str) -> Optional[MatchResult]:
        """Get cached result from Redis"""
        try:
            cache_key = f"llm_match:{header.lower()}"
            cached = self.cache.get(cache_key)
            
            if cached:
                data = json.loads(cached)
                return MatchResult(
                    canonical_name=data["canonical_name"],
                    confidence=data["confidence"],
                    method="llm",
                    reasoning=data.get("reasoning", "")
                )
        except Exception as e:
            logger.warning(f"Cache read error: {e}")
        
        return None

    def _save_to_cache(self, header: str, result: MatchResult):
        """Save result to Redis cache"""
        try:
            cache_key = f"llm_match:{header.lower()}"
            cache_data = {
                "canonical_name": result.canonical_name,
                "confidence": result.confidence,
                "reasoning": result.reasoning
            }
            
            # Cache for 7 days
            self.cache.setex(
                cache_key,
                7 * 24 * 60 * 60,
                json.dumps(cache_data)
            )
        except Exception as e:
            logger.warning(f"Cache write error: {e}")

    def match_batch(self, headers: List[str]) -> dict:
        """Match multiple headers"""
        results = {}
        for header in headers:
            results[header] = self.match(header)
        return results
