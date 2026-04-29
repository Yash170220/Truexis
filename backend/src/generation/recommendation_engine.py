"""AI-powered recommendation engine comparing facility data against industry benchmarks."""
import logging
from typing import Dict, List, Optional

import anthropic

logger = logging.getLogger(__name__)

BENCHMARKS: Dict[str, Dict[str, Dict]] = {
    "cement": {
        "Scope 1 Emissions Intensity": {
            "avg": 950, "best": 800, "unit": "kg CO2/tonne clinker",
        },
        "Energy Intensity": {
            "avg": 3.5, "best": 2.9, "unit": "GJ/tonne clinker",
        },
        "Total Electricity Consumption": {
            "avg": 90, "best": 75, "unit": "kWh/tonne cement",
        },
        "Water Consumption": {
            "avg": 0.6, "best": 0.4, "unit": "m3/tonne cement",
        },
    },
    "steel": {
        "Scope 1 Emissions Intensity (BF-BOF)": {
            "avg": 2100, "best": 1800, "unit": "kg CO2/tonne steel",
        },
        "Scope 1 Emissions Intensity (EAF)": {
            "avg": 500, "best": 400, "unit": "kg CO2/tonne steel",
        },
        "Energy Intensity": {
            "avg": 20, "best": 16, "unit": "GJ/tonne steel",
        },
        "Water Consumption": {
            "avg": 4.0, "best": 2.5, "unit": "m3/tonne steel",
        },
    },
    # FIX: Added automotive industry — missing from original despite being listed
    #      in project summary as a target sector
    "automotive": {
        "Scope 1 Emissions Intensity": {
            "avg": 0.6, "best": 0.35, "unit": "tCO2/vehicle",
        },
        "Energy Intensity": {
            "avg": 2.8, "best": 1.8, "unit": "MWh/vehicle",
        },
        "Water Consumption": {
            "avg": 3.5, "best": 2.0, "unit": "m3/vehicle",
        },
        "Waste Generation": {
            "avg": 180, "best": 100, "unit": "kg/vehicle",
        },
    },
}

PRIORITY_ORDER = {"high": 0, "medium": 1, "low": 2}

# FIX: Added system prompt — was missing, causing generic non-industry-specific suggestions
RECOMMENDATION_SYSTEM_PROMPT = (
    "You are a senior ESG improvement consultant specializing in heavy manufacturing. "
    "Provide ONLY specific, proven, commercially-available recommendations. "
    "Never suggest research-stage technologies. "
    "Always include realistic cost ranges and payback periods. "
    "Format each recommendation as a numbered list item."
)


class RecommendationEngine:
    """Generates actionable recommendations by comparing performance to benchmarks."""

    def __init__(self, api_key: str, model: str = "claude-haiku-4-5"):
        self.claude = anthropic.Anthropic(api_key=api_key)
        self.model = model
        self.benchmarks = BENCHMARKS

    def generate_recommendations(
        self,
        upload_id: str,
        validated_data: List[Dict],
        industry: str,
    ) -> List[Dict]:
        # FIX: Normalize industry string to lowercase for safe lookup
        industry = industry.lower().strip()

        by_indicator: Dict[str, List[Dict]] = {}
        for record in validated_data:
            by_indicator.setdefault(record["indicator"], []).append(record)

        recommendations: List[Dict] = []

        for indicator, records in by_indicator.items():
            if not records:
                continue

            # FIX: Robust value extraction — was crashing silently on non-numeric values
            values = []
            for r in records:
                try:
                    val = float(r["value"]) if r.get("value") is not None else None
                    if val is not None:
                        values.append(val)
                except (ValueError, TypeError):
                    continue

            if not values:
                continue

            avg_value = sum(values) / len(values)
            benchmark = self._get_benchmark(indicator, industry)
            if not benchmark:
                continue

            gap_pct = ((avg_value - benchmark["avg"]) / benchmark["avg"]) * 100

            if abs(gap_pct) < 5:
                priority = "low"
                status = "On par with industry average"
            elif gap_pct > 5:
                priority = "high" if gap_pct > 20 else "medium"
                status = f"{abs(gap_pct):.0f}% above industry average"
            else:
                priority = "low"
                status = f"{abs(gap_pct):.0f}% below industry average (good performance)"

            suggestions: List[str] = []
            if gap_pct > 5:
                suggestions = self._generate_ai_suggestions(
                    indicator=indicator,
                    current=avg_value,
                    benchmark=benchmark,
                    industry=industry,
                    gap_pct=gap_pct,
                )

            recommendations.append({
                "indicator": indicator,
                "current_value": round(avg_value, 2),
                "unit": records[0].get("unit", benchmark["unit"]),  # FIX: fallback to benchmark unit
                "industry_average": benchmark["avg"],
                "best_in_class": benchmark["best"],
                "gap_percentage": round(gap_pct, 1),
                "status": status,
                "priority": priority,
                "suggestions": suggestions,
            })

        recommendations.sort(key=lambda x: PRIORITY_ORDER.get(x["priority"], 2))
        return recommendations

    # ------------------------------------------------------------------

    def _get_benchmark(self, indicator: str, industry: str) -> Optional[Dict]:
        industry_benchmarks = self.benchmarks.get(industry)
        if not industry_benchmarks:
            logger.warning(f"No benchmarks found for industry: '{industry}'")
            return None
        ind_lower = indicator.lower()
        for bench_name, bench_data in industry_benchmarks.items():
            if bench_name.lower() in ind_lower or ind_lower in bench_name.lower():
                return bench_data
        return None

    def _generate_ai_suggestions(
        self,
        indicator: str,
        current: float,
        benchmark: Dict,
        industry: str,
        gap_pct: float,
    ) -> List[str]:
        prompt = (
            f"Industry: {industry} manufacturing\n"
            f"Metric: {indicator}\n"
            f"Current: {current:.2f} {benchmark['unit']}\n"
            f"Industry average: {benchmark['avg']} {benchmark['unit']}\n"
            f"Best-in-class: {benchmark['best']} {benchmark['unit']}\n"
            f"Gap: {gap_pct:.0f}% above average\n\n"
            f"Generate 3 specific recommendations to close this gap.\n"
            f"Each must include: technology name, % reduction potential, "
            f"investment range (INR Lakhs), payback period, complexity (Low/Medium/High).\n"
            f"Order by best ROI first. 2 lines max per recommendation.\n\n"
            f"Recommendations:"
        )

        try:
            response = self.claude.messages.create(
                model=self.model,
                system=RECOMMENDATION_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=400,
            )
            text = response.content[0].text

            # FIX: More robust parsing — handles "1." "1)" "1 -" formats
            suggestions = []
            for line in text.split("\n"):
                line = line.strip()
                if not line:
                    continue
                # Match lines starting with a number followed by . ) or -
                if line and line[0].isdigit():
                    # Strip the leading "1. " or "1) " prefix
                    cleaned = line.lstrip("0123456789").lstrip(". ):-").strip()
                    if cleaned:
                        suggestions.append(cleaned)

            return suggestions[:3]  # FIX: was [:4] — capped at 3 to match prompt

        except Exception as exc:
            logger.error(f"AI suggestion generation failed: {exc}")
            return [f"Unable to generate suggestions: {exc}"]