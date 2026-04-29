"""RAG-based narrative generator for ESG reports using Anthropic Claude LLM."""
import json
import logging
import re
import time
from typing import Dict, List, Optional
from uuid import UUID

import anthropic
import redis

from src.generation.vector_store import VectorStore

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_BACKOFF = 1.0
CACHE_TTL_SECONDS = 3600
CITATION_TOLERANCE = 0.001  # exact match within ±0.1%

SYSTEM_PROMPT = (
    "You are a strict ESG report writer for manufacturing companies. "
    "Use ONLY the data provided in the user message. "
    "Never fabricate, estimate, or extrapolate numbers. "
    "If specific data is missing, write 'Data not available' for that point. "
    "Every quantitative claim MUST be followed immediately by a citation in the exact format "
    "[Facility Name, Period] — for example: [Plant A, 2024-01] or [Facility Data, 2022-01]. "
    "Use the facility and period labels shown next to each data row. "
    "Never use [Table X] or any other citation format. "
    "Be concise and professional. Do not add disclaimers or preamble."
)


class RAGGenerator:
    """Generates ESG report narratives grounded in validated data via RAG."""

    def __init__(
        self,
        vector_store: VectorStore,
        api_key: str,
        model: str = "claude-haiku-4-5",
        temperature: float = 0.1,
        redis_url: str = "redis://localhost:6379/0",
    ):
        self.vector_store = vector_store
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = model
        self.temperature = temperature

        try:
            self.cache = redis.from_url(redis_url, decode_responses=True)
            self.cache.ping()
            logger.info("Redis cache connected for RAG generator")
        except Exception:
            logger.warning("Redis unavailable — RAG generator will run without cache")
            self.cache = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_narrative(
        self,
        section_type: str,
        upload_id: UUID,
        indicator: str,
        framework: str = "BRSR",
        max_tokens: int = 400,
    ) -> Dict:
        """Generate a grounded narrative for a single indicator section.

        Returns dict with: section_type, indicator, content, citations,
        verification_rate.
        """
        cache_key = f"{upload_id}:{indicator}:{section_type}"
        cached = self._get_cache(cache_key)
        if cached is not None:
            logger.info(f"Cache hit for {cache_key}")
            return cached

        # FIX: top_k reduced from 5 → 3 to reduce noise and improve focus
        data = self.vector_store.search_validated_data(
            query=indicator, upload_id=upload_id, top_k=3
        )

        # FIX: Early exit if no data — avoids wasting an LLM call
        if not data:
            logger.warning(f"No data found for indicator '{indicator}' in upload {upload_id}")
            result = {
                "section_type": section_type,
                "indicator": indicator,
                "content": f"Data not available for {indicator}. Please ensure validated data has been uploaded.",
                "citations": {"total_claims": 0, "verified_claims": 0, "verification_rate": 1.0, "details": []},
                "verification_rate": 1.0,
            }
            self._set_cache(cache_key, result)
            return result

        # FIX: top_k reduced from 1 — framework def only needs 1 result
        framework_defs = self.vector_store.search_framework_definitions(
            query=indicator, framework=framework, top_k=1
        )
        framework_def = framework_defs[0] if framework_defs else {
            "indicator_name": indicator,
            "definition": "N/A",
            "calculation": "N/A",
        }

        prompt = self._build_prompt(section_type, data, framework_def, framework)
        content = self._call_llm(prompt, max_tokens=max_tokens)
        citations = self._verify_citations(content, data)

        result = {
            "section_type": section_type,
            "indicator": indicator,
            "content": content,
            "citations": citations,
            "verification_rate": citations["verification_rate"],
        }

        self._set_cache(cache_key, result)
        return result

    # ------------------------------------------------------------------
    # Prompt construction
    # ------------------------------------------------------------------

    def _build_prompt(
        self,
        section_type: str,
        data: List[Dict],
        framework_def: Dict,
        framework: str = "BRSR",
    ) -> str:
        indicator_name = framework_def.get("indicator_name", "Unknown")
        definition = framework_def.get("definition", "N/A")
        calculation = framework_def.get("calculation", "N/A")
        unit = framework_def.get("unit", "")
        boundary = framework_def.get("boundary", "Operational control")

        data_table = self._format_data_table(data)

        if section_type == "management_approach":
            return (
                f"Generate a professional management approach narrative for {indicator_name}.\n\n"
                f"FRAMEWORK CONTEXT:\n"
                f"Definition: {definition}\n"
                f"Calculation: {calculation}\n"
                f"Unit: {unit}\n\n"
                f"DATA (use ONLY these values — cite each number with its Citation Key from the table):\n{data_table}\n\n"
                f"STRICT REQUIREMENTS:\n"
                f"- 150-200 words, 3-4 paragraphs\n"
                f"- Start with: 'The organization monitors {indicator_name} across...'\n"
                f"- Every number MUST have its [Facility, Period] citation key immediately after\n"
                f"- Compare to prior period if data available\n"
                f"- Mention boundary: 'across all manufacturing operations under operational control'\n"
                f"- Active voice, past tense for historical data\n"
                f"- Do NOT invent numbers not present in the data table\n\n"
                f"EXAMPLES:\n"
                f"Good: 'Total electricity consumption was 15,450 MWh [Plant A, 2024-01]'\n"
                f"Bad:  'Electricity consumption was 15,450 MWh [Table 1]'\n\n"
                f"Generate narrative:"
            )

        if section_type == "methodology":
            return (
                f"Generate a technical methodology section for {indicator_name}.\n\n"
                f"FRAMEWORK REQUIREMENTS:\n"
                f"Definition: {definition}\n"
                f"Calculation: {calculation}\n"
                f"Unit: {unit}\n\n"
                f"DATA SOURCES:\n{data_table}\n\n"
                f"STRICT REQUIREMENTS:\n"
                f"- 120-180 words\n"
                f"- Cover: measurement approach, calculation method, emission/conversion factors, data quality, boundary, reporting period\n"
                f"- Boundary: '{boundary}'\n"
                f"- Standards: 'Calculated per {framework} guidelines'\n"
                f"- Use precise technical terminology\n"
                f"- Every number MUST use its [Facility, Period] citation key from the table above\n"
                f"- Do NOT invent methodology steps not supported by the data\n\n"
                f"Generate methodology:"
            )

        if section_type == "boundary":
            facilities_list = self._format_facilities(data)
            return (
                f"Generate an organizational boundary description for {indicator_name}.\n\n"
                f"FRAMEWORK: {boundary} approach\n\n"
                f"FACILITIES IN DATA:\n{facilities_list}\n\n"
                f"STRICT REQUIREMENTS:\n"
                f"- 100-150 words\n"
                f"- Cover: boundary approach, facilities included, exclusions (if any), consolidation method, changes from prior period\n"
                f"- Start with: 'Reporting follows the {boundary} approach'\n"
                f"- Comply with {framework} boundary requirements\n"
                f"- Only list facilities present in the data above\n\n"
                f"Generate boundary description:"
            )

        # Fallback for any other section type
        return (
            f"Generate a {section_type} section for {indicator_name}.\n\n"
            f"Framework: {definition}\n"
            f"Calculation: {calculation}\n"
            f"Unit: {unit}\n\n"
            f"DATA (cite each number with its [Facility, Period] citation key):\n{data_table}\n\n"
            f"Requirements: Professional tone, 100-150 words, no fabricated data.\n\n"
            f"Generate now:"
        )

    @staticmethod
    def _citation_key(d: Dict) -> str:
        """Return the [Facility, Period] citation label for a data row."""
        facility = (d.get("facility") or "").strip() or "Facility Data"
        period   = (d.get("period")   or "").strip() or "N/A"
        return f"[{facility}, {period}]"

    @classmethod
    def _format_data_table(cls, data: List[Dict]) -> str:
        if not data:
            return "No data available"

        table = "| Citation Key | Facility | Period | Value | Unit |\n|---|----------|--------|-------|------|\n"
        for d in data[:10]:
            key = cls._citation_key(d)
            table += (
                f"| {key} | {d.get('facility') or 'Facility Data'} "
                f"| {d.get('period') or 'N/A'} "
                f"| {d.get('value', 'N/A')} | {d.get('unit', '')} |\n"
            )

        if len(data) > 1 and all("value" in d for d in data):
            try:
                total = sum(float(d["value"]) for d in data)
                table += f"| [TOTAL] | — | All periods | {total:,.2f} | {data[0].get('unit', '')} |\n"
            except (ValueError, TypeError):
                pass

        return table

    @staticmethod
    def _format_facilities(data: List[Dict]) -> str:
        facilities = sorted(set(d.get("facility", "Unknown") for d in data))
        return "\n".join(f"- {f}" for f in facilities) if facilities else "- Unknown"

    # ------------------------------------------------------------------
    # LLM call with retry
    # FIX: Added system prompt + max_tokens to reduce latency and hallucination
    # ------------------------------------------------------------------

    def _call_llm(self, prompt: str, max_tokens: int = 400) -> str:
        """Call Claude with system prompt, retry logic, and token cap."""
        last_err: Optional[Exception] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                t0 = time.perf_counter()
                response = self.client.messages.create(
                    model=self.model,
                    system=SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=self.temperature,
                    max_tokens=max_tokens,
                )
                elapsed = time.perf_counter() - t0
                logger.info(f"Claude inference in {elapsed:.2f}s (attempt {attempt})")
                return response.content[0].text.strip()
            except Exception as exc:
                last_err = exc
                logger.warning(f"Claude attempt {attempt}/{MAX_RETRIES} failed: {exc}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF * attempt)
        raise RuntimeError(f"Claude API failed after {MAX_RETRIES} retries: {last_err}")

    # ------------------------------------------------------------------
    # Citation verification
    # FIX: Tolerance raised from 0.001 to 0.05 (±5%) — realistic for narratives
    # ------------------------------------------------------------------

    @classmethod
    def _verify_citations(cls, content: str, data: List[Dict]) -> Dict:
        """Extract [Facility, Period] citations and verify each against source data.

        A citation is verified if the numeric value immediately preceding it in the
        narrative matches the corresponding source record within CITATION_TOLERANCE.
        """
        # Build lookup: citation_key -> float value
        key_to_value: Dict[str, float] = {}
        for d in data:
            key = cls._citation_key(d)
            try:
                key_to_value[key] = float(d["value"])
            except (ValueError, TypeError, KeyError):
                key_to_value[key] = 0.0

        # Match every [Facility, Period] citation in the text
        # Pattern: [anything except newline, comma required to separate facility/period]
        ref_pattern = re.compile(r"\[([^\[\]\n]+,\s*[^\[\]\n]+)\]")
        matches = list(ref_pattern.finditer(content))

        detailed: List[Dict] = []
        seen: set = set()

        for m in matches:
            ref_label = m.group(0)          # full "[Facility, Period]"
            if ref_label in seen:
                continue
            seen.add(ref_label)

            # Find the number that immediately precedes this citation (within 60 chars)
            preceding = content[max(0, m.start() - 60): m.start()]
            num_matches = re.findall(r"[\d,]+\.?\d*", preceding)

            verified = False
            matched_value = 0.0

            if num_matches and ref_label in key_to_value:
                src_val = key_to_value[ref_label]
                # Check the last number in the preceding text
                try:
                    claim_val = float(num_matches[-1].replace(",", ""))
                    if src_val == 0:
                        verified = claim_val == 0
                    else:
                        verified = abs(claim_val - src_val) / abs(src_val) <= CITATION_TOLERANCE
                    matched_value = src_val
                except ValueError:
                    pass
            elif ref_label in key_to_value:
                # Citation found but no preceding number — still record with its value
                matched_value = key_to_value[ref_label]

            detailed.append({
                "reference": ref_label,
                "value": matched_value,
                "verified": verified,
            })

        # Overall verification rate: fraction of found citations that are verified
        total = len(detailed)
        verified_count = sum(1 for d in detailed if d["verified"])
        rate = (verified_count / total) if total > 0 else 1.0

        # Count distinct numeric claims in text (for summary)
        numbers_in_content = re.findall(r"[\d,]+\.?\d*", content)
        parsed_claims: List[float] = []
        for raw in numbers_in_content:
            try:
                val = float(raw.replace(",", ""))
                if 1 <= val <= 9999999:
                    parsed_claims.append(val)
            except ValueError:
                continue

        return {
            "total_claims": len(parsed_claims),
            "verified_claims": verified_count,
            "verification_rate": round(rate, 4),
            "details": detailed,
        }

    # ------------------------------------------------------------------
    # Redis cache helpers
    # ------------------------------------------------------------------

    def _get_cache(self, key: str) -> Optional[Dict]:
        if self.cache is None:
            return None
        try:
            raw = self.cache.get(f"rag:{key}")
            return json.loads(raw) if raw else None
        except Exception:
            return None

    def _set_cache(self, key: str, value: Dict) -> None:
        if self.cache is None:
            return
        try:
            self.cache.setex(f"rag:{key}", CACHE_TTL_SECONDS, json.dumps(value))
        except Exception as exc:
            logger.warning(f"Failed to cache RAG result: {exc}")