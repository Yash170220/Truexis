"""Dashboard service — aggregates data from existing DB tables for frontend charts."""
import logging
from collections import defaultdict
from typing import Dict, List
from uuid import UUID

from sqlalchemy.orm import Session

from src.common.models import (
    Upload,
    MatchedIndicator,
    NormalizedData,
    ValidationResult,
)

logger = logging.getLogger(__name__)


class DashboardService:
    """Builds chart-ready dashboard payload from existing DB data.
    No new processing — purely reads and aggregates what's already stored.
    """

    def __init__(self, db: Session):
        self.db = db

    def build_dashboard(self, upload_id: UUID) -> Dict:
        upload = self.db.query(Upload).filter(Upload.id == upload_id).first()
        if not upload:
            raise ValueError(f"Upload {upload_id} not found")

        normalized = self._get_normalized(upload_id)
        validation = self._get_validation(upload_id)    # FIX: joins through NormalizedData
        matched    = self._get_matched(upload_id)

        return {
            "upload_id":     str(upload_id),
            "upload_status": upload.status,
            "summary_cards": self._build_summary_cards(normalized, validation, matched),
            "charts": {
                "emissions_by_indicator": self._emissions_by_indicator(normalized),
                "energy_by_indicator":    self._energy_by_indicator(normalized),
                "water_by_indicator":     self._water_by_indicator(normalized),
                "indicator_trend":        self._indicator_trend(normalized),
                "benchmark_comparison":   self._benchmark_comparison(normalized),
                "validation_summary":     self._validation_summary(validation),
                "scope_breakdown":        self._scope_breakdown(normalized),
            },
            "top_issues": self._top_validation_issues(validation, normalized),
        }

    # ------------------------------------------------------------------
    # Summary cards
    # ------------------------------------------------------------------

    def _build_summary_cards(
        self,
        normalized: List[NormalizedData],
        validation: List[ValidationResult],
        matched: List[MatchedIndicator],
    ) -> Dict:
        total    = len(validation)
        errors   = sum(1 for v in validation if v.severity.value == "error")
        warnings = sum(1 for v in validation if v.severity.value == "warning")
        passed   = total - errors - warnings

        # FIX: NormalizedData has no .indicator string column.
        #      Name lives on the relationship: normalized.indicator.matched_indicator
        indicator_names = list({
            n.indicator.matched_indicator
            for n in normalized
            if n.indicator
        })

        return {
            "total_indicators":     len(indicator_names),
            "total_data_points":    len(normalized),
            "validation_pass_rate": round(passed / total * 100, 1) if total else 100.0,
            "error_count":          errors,
            "warning_count":        warnings,
            "frameworks_covered":   ["BRSR"],
            "matched_count":        len(matched),
            "auto_approved":        sum(1 for m in matched if m.confidence_score >= 0.85),
            "needs_review":         sum(1 for m in matched if not m.reviewed and m.confidence_score < 0.85),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_indicator_name(self, record: NormalizedData) -> str:
        """Safely get indicator name via relationship."""
        try:
            return record.indicator.matched_indicator or ""
        except AttributeError:
            return ""

    # ------------------------------------------------------------------
    # Chart builders
    # FIX: NormalizedData has no .indicator string or .facility column.
    #      Must traverse relationship: record.indicator.matched_indicator
    #      No period column either — use created_at for trend chart.
    # ------------------------------------------------------------------

    def _emissions_by_indicator(self, normalized: List[NormalizedData]) -> List[Dict]:
        """Bar chart: total emissions value per indicator."""
        result: Dict[str, float] = defaultdict(float)
        unit = ""
        for r in normalized:
            name = self._get_indicator_name(r)
            if any(kw in name.lower() for kw in ["emission", "scope 1", "scope 2", "ghg", "co2"]):
                try:
                    result[name] += float(r.normalized_value or 0)
                    unit = r.normalized_unit or unit
                except (TypeError, ValueError):
                    continue
        return [{"indicator": k, "value": round(v, 2), "unit": unit}
                for k, v in sorted(result.items())]

    def _energy_by_indicator(self, normalized: List[NormalizedData]) -> List[Dict]:
        """Bar chart: total energy value per indicator."""
        result: Dict[str, float] = defaultdict(float)
        unit = ""
        for r in normalized:
            name = self._get_indicator_name(r)
            if any(kw in name.lower() for kw in ["energy", "electricity", "fuel", "mwh", "gj"]):
                try:
                    result[name] += float(r.normalized_value or 0)
                    unit = r.normalized_unit or unit
                except (TypeError, ValueError):
                    continue
        return [{"indicator": k, "value": round(v, 2), "unit": unit}
                for k, v in sorted(result.items())]

    def _water_by_indicator(self, normalized: List[NormalizedData]) -> List[Dict]:
        """Bar chart: total water value per indicator."""
        result: Dict[str, float] = defaultdict(float)
        unit = ""
        for r in normalized:
            name = self._get_indicator_name(r)
            if "water" in name.lower():
                try:
                    result[name] += float(r.normalized_value or 0)
                    unit = r.normalized_unit or unit
                except (TypeError, ValueError):
                    continue
        return [{"indicator": k, "value": round(v, 2), "unit": unit}
                for k, v in sorted(result.items())]

    def _indicator_trend(self, normalized: List[NormalizedData]) -> List[Dict]:
        """Line chart: value per indicator per month (uses created_at — no period column)."""
        result = []
        for r in normalized:
            try:
                result.append({
                    "indicator": self._get_indicator_name(r),
                    "period":    r.created_at.strftime("%Y-%m") if r.created_at else "Unknown",
                    "value":     round(float(r.normalized_value or 0), 2),
                    "unit":      r.normalized_unit or "",
                })
            except (TypeError, ValueError):
                continue
        result.sort(key=lambda x: (x["indicator"], x["period"]))
        return result

    def _benchmark_comparison(self, normalized: List[NormalizedData]) -> List[Dict]:
        """Grouped bar: your value vs industry avg vs best-in-class."""
        try:
            from src.generation.recommendation_engine import BENCHMARKS
        except ImportError:
            return []

        totals: Dict[str, List[float]] = defaultdict(list)
        units:  Dict[str, str] = {}
        for r in normalized:
            name = self._get_indicator_name(r)
            if not name:
                continue
            try:
                totals[name].append(float(r.normalized_value or 0))
                units[name] = r.normalized_unit or ""
            except (TypeError, ValueError):
                continue

        result = []
        for industry_benchmarks in BENCHMARKS.values():
            for bench_name, bench_data in industry_benchmarks.items():
                for indicator, values in totals.items():
                    if (bench_name.lower() in indicator.lower()
                            or indicator.lower() in bench_name.lower()):
                        avg_val = sum(values) / len(values)
                        result.append({
                            "indicator":     indicator,
                            "your_value":    round(avg_val, 2),
                            "industry_avg":  bench_data["avg"],
                            "best_in_class": bench_data["best"],
                            "unit":          bench_data["unit"],
                            "gap_pct":       round(
                                (avg_val - bench_data["avg"]) / bench_data["avg"] * 100, 1
                            ),
                        })
        return result

    def _validation_summary(self, validation: List[ValidationResult]) -> Dict:
        """Donut chart: passed / warnings / errors."""
        errors   = sum(1 for v in validation if v.severity.value == "error")
        warnings = sum(1 for v in validation if v.severity.value == "warning")
        passed   = len(validation) - errors - warnings
        return {"passed": passed, "warnings": warnings,
                "errors": errors, "total": len(validation)}

    def _scope_breakdown(self, normalized: List[NormalizedData]) -> List[Dict]:
        """Pie chart: Scope 1 / 2 / 3."""
        scopes = {"Scope 1": 0.0, "Scope 2": 0.0, "Scope 3": 0.0}
        unit = ""
        for r in normalized:
            name = self._get_indicator_name(r).lower()
            try:
                val = float(r.normalized_value or 0)
            except (TypeError, ValueError):
                continue
            if "scope 1" in name or "scope1" in name:
                scopes["Scope 1"] += val
                unit = r.normalized_unit or unit
            elif "scope 2" in name or "scope2" in name:
                scopes["Scope 2"] += val
            elif "scope 3" in name or "scope3" in name:
                scopes["Scope 3"] += val
        return [{"scope": k, "value": round(v, 2), "unit": unit}
                for k, v in scopes.items() if v > 0]

    # ------------------------------------------------------------------
    # Top issues
    # FIX: ValidationResult has no .upload_id or .indicator/.facility columns.
    #      Must resolve indicator name via NormalizedData lookup dict.
    # ------------------------------------------------------------------

    def _top_validation_issues(
        self,
        validation: List[ValidationResult],
        normalized: List[NormalizedData],
    ) -> List[Dict]:
        # Build id → indicator name lookup
        nd_lookup: Dict = {n.id: self._get_indicator_name(n) for n in normalized}

        issues = []
        for v in validation:
            if v.severity.value not in ("error", "warning"):
                continue
            issues.append({
                "indicator": nd_lookup.get(v.data_id, "Unknown"),
                "severity":  v.severity.value,
                "rule":      v.rule_name,
                "message":   v.message,
                "value":     v.data.normalized_value if v.data else None,
                "unit":      v.data.normalized_unit if v.data else "",
                "citation":  v.citation,
            })

        issues.sort(key=lambda x: 0 if x["severity"] == "error" else 1)
        return issues[:10]

    # ------------------------------------------------------------------
    # DB queries
    # FIX: ValidationResult has no upload_id column — must JOIN NormalizedData
    # ------------------------------------------------------------------

    def _get_normalized(self, upload_id: UUID) -> List[NormalizedData]:
        return (
            self.db.query(NormalizedData)
            .filter(NormalizedData.upload_id == upload_id)
            .all()
        )

    def _get_validation(self, upload_id: UUID) -> List[ValidationResult]:
        # FIX: ValidationResult → NormalizedData join required (no direct upload_id)
        return (
            self.db.query(ValidationResult)
            .join(NormalizedData, ValidationResult.data_id == NormalizedData.id)
            .filter(NormalizedData.upload_id == upload_id)
            .all()
        )

    def _get_matched(self, upload_id: UUID) -> List[MatchedIndicator]:
        return (
            self.db.query(MatchedIndicator)
            .filter(MatchedIndicator.upload_id == upload_id)
            .all()
        )