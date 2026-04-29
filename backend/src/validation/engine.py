"""Validation Engine for ESG Data Quality Control"""
import json
import statistics
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from uuid import UUID, uuid4
from datetime import datetime

from pydantic import BaseModel, Field


class ValidationRule(BaseModel):
    """Schema for validation rule"""
    rule_name: str
    description: str
    indicator: str
    validation_type: str
    parameters: Dict[str, Any]
    severity: str
    citation: str
    error_message: str
    suggested_fixes: List[str] = Field(default_factory=list)


class NormalizedRecord(BaseModel):
    """Schema for normalized data record"""
    id: UUID
    indicator: str
    value: float
    unit: str
    original_value: float
    original_unit: str
    facility_id: Optional[str] = None
    reporting_period: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ValidationResult(BaseModel):
    """Schema for validation result"""
    data_id: UUID
    rule_name: str
    is_valid: bool
    severity: str  # "error" | "warning"
    message: str
    citation: str
    suggested_fixes: List[str] = Field(default_factory=list)
    actual_value: Optional[float] = None
    expected_range: Optional[Tuple[float, float]] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ValidationEngine:
    """Engine for validating ESG data against industry-specific rules"""

    def __init__(self, rules_path: str):
        self.rules_path = Path(rules_path)
        self.rules: Dict[str, Dict[str, ValidationRule]] = {}
        self.rules_index: Dict[str, List[ValidationRule]] = {}
        self._load_rules()
        self._index_rules()

    def _load_rules(self) -> None:
        if not self.rules_path.exists():
            raise FileNotFoundError(f"Validation rules file not found: {self.rules_path}")
        with open(self.rules_path, 'r', encoding='utf-8') as f:
            raw_rules = json.load(f)
        for industry, industry_rules in raw_rules.items():
            self.rules[industry] = {}
            for rule_key, rule_data in industry_rules.items():
                self.rules[industry][rule_key] = ValidationRule(**rule_data)

    def _index_rules(self) -> None:
        for industry, industry_rules in self.rules.items():
            for rule_key, rule in industry_rules.items():
                index_key = f"{industry}"
                if index_key not in self.rules_index:
                    self.rules_index[index_key] = []
                self.rules_index[index_key].append(rule)

                if rule.indicator:
                    indicator_key = f"{industry}:{rule.indicator}"
                    if indicator_key not in self.rules_index:
                        self.rules_index[indicator_key] = []
                    self.rules_index[indicator_key].append(rule)

    # FIX: Normalize industry name — API passes "cement" but JSON has "cement_industry"
    @staticmethod
    def _normalize_industry(industry: str) -> str:
        """Ensure industry name ends with '_industry' suffix."""
        industry = industry.lower().strip()
        if not industry.endswith("_industry"):
            industry = f"{industry}_industry"
        return industry

    def validate_record(self, record: NormalizedRecord, industry: str) -> List[ValidationResult]:
        results: List[ValidationResult] = []

        # FIX: Normalize industry before lookup
        industry = self._normalize_industry(industry)

        applicable_rules = self._get_applicable_rules(industry, record.indicator)
        if not applicable_rules:
            applicable_rules = self._get_applicable_rules("cross_industry", record.indicator)

        for rule in applicable_rules:
            result = self._execute_validation(record, rule)
            if result:
                results.append(result)

        return results

    def _get_applicable_rules(self, industry: str, indicator: str) -> List[ValidationRule]:
        """Get rules applicable to given industry and indicator."""
        # FIX: Normalize industry here too for safety
        industry = self._normalize_industry(industry)

        rules = []

        indicator_key = f"{industry}:{indicator}"
        if indicator_key in self.rules_index:
            rules.extend(self.rules_index[indicator_key])

        industry_key = industry
        if industry_key in self.rules_index:
            for rule in self.rules_index[industry_key]:
                if rule.indicator == indicator or not rule.indicator:
                    if rule not in rules:
                        rules.append(rule)

        return rules

    def _execute_validation(self, record: NormalizedRecord, rule: ValidationRule) -> Optional[ValidationResult]:
        validation_type = rule.validation_type

        if validation_type == "range":
            return self.range_check(record.value, rule, record.id)
        elif validation_type == "category_check":
            return self.category_check(record.metadata.get("source_category", ""), rule, record.id)
        elif validation_type == "outlier":
            return None
        elif validation_type == "pattern_match":
            return self.pattern_match(record.unit, rule, record.id)
        elif validation_type == "null_check":
            return self.null_check(record, rule)
        elif validation_type == "precision_check":
            return self.precision_check(record.value, rule, record.id)
        elif validation_type == "cross_field":
            return None
        else:
            return None

    def range_check(self, value: float, rule: ValidationRule, data_id: UUID) -> Optional[ValidationResult]:
        min_val = rule.parameters.get("min")
        max_val = rule.parameters.get("max")

        if min_val is not None and value < min_val:
            return ValidationResult(
                data_id=data_id, rule_name=rule.rule_name, is_valid=False,
                severity=rule.severity,
                message=f"{rule.error_message} Value {value:.4f} is below minimum {min_val}.",
                citation=rule.citation, suggested_fixes=rule.suggested_fixes,
                actual_value=value, expected_range=(min_val, max_val) if max_val else None
            )

        if max_val is not None and value > max_val:
            return ValidationResult(
                data_id=data_id, rule_name=rule.rule_name, is_valid=False,
                severity=rule.severity,
                message=f"{rule.error_message} Value {value:.4f} is above maximum {max_val}.",
                citation=rule.citation, suggested_fixes=rule.suggested_fixes,
                actual_value=value, expected_range=(min_val, max_val) if min_val else None
            )

        return None

    def category_check(self, value: str, rule: ValidationRule, data_id: UUID) -> Optional[ValidationResult]:
        allowed_categories = (
            rule.parameters.get("allowed_sources", []) or
            rule.parameters.get("allowed_categories", [])
        )
        if value.lower() not in [cat.lower() for cat in allowed_categories]:
            return ValidationResult(
                data_id=data_id, rule_name=rule.rule_name, is_valid=False,
                severity=rule.severity,
                message=f"{rule.error_message} Found '{value}', expected one of: {', '.join(allowed_categories)}",
                citation=rule.citation, suggested_fixes=rule.suggested_fixes
            )
        return None

    def outlier_detection(self, values: List[Tuple[UUID, float]], rule: ValidationRule) -> List[ValidationResult]:
        if len(values) < 3:
            return []
        results: List[ValidationResult] = []
        threshold = rule.parameters.get("z_score_threshold", 3.0)
        numeric_values = [v[1] for v in values]
        mean = statistics.mean(numeric_values)
        stdev = statistics.stdev(numeric_values) if len(numeric_values) > 1 else 0
        if stdev == 0:
            return results
        for data_id, value in values:
            z_score = abs((value - mean) / stdev)
            if z_score > threshold:
                results.append(ValidationResult(
                    data_id=data_id, rule_name=rule.rule_name, is_valid=False,
                    severity=rule.severity,
                    message=f"{rule.error_message} Z-score: {z_score:.2f} (threshold: {threshold})",
                    citation=rule.citation, suggested_fixes=rule.suggested_fixes,
                    actual_value=value,
                    expected_range=(mean - threshold * stdev, mean + threshold * stdev)
                ))
        return results

    def temporal_consistency(self, monthly_data: Dict[str, float], annual_total: float,
                              rule: ValidationRule, data_id: UUID) -> Optional[ValidationResult]:
        if not monthly_data:
            return None
        monthly_sum = sum(monthly_data.values())
        tolerance = rule.parameters.get("tolerance", 0.02)
        diff_pct = abs(monthly_sum - annual_total) / annual_total if annual_total != 0 else abs(monthly_sum)
        if diff_pct > tolerance:
            return ValidationResult(
                data_id=data_id, rule_name=rule.rule_name, is_valid=False,
                severity=rule.severity,
                message=f"Monthly sum ({monthly_sum:.2f}) differs from annual ({annual_total:.2f}) by {diff_pct*100:.1f}%",
                citation=rule.citation, suggested_fixes=rule.suggested_fixes,
                actual_value=monthly_sum,
                expected_range=(annual_total * (1 - tolerance), annual_total * (1 + tolerance))
            )
        return None

    def pattern_match(self, value: str, rule: ValidationRule, data_id: UUID) -> Optional[ValidationResult]:
        allowed_patterns = rule.parameters.get("allowed_patterns", [])
        if not any(pattern in value for pattern in allowed_patterns):
            return ValidationResult(
                data_id=data_id, rule_name=rule.rule_name, is_valid=False,
                severity=rule.severity,
                message=f"{rule.error_message} Found '{value}', expected: {', '.join(allowed_patterns)}",
                citation=rule.citation, suggested_fixes=rule.suggested_fixes
            )
        return None

    def null_check(self, record: NormalizedRecord, rule: ValidationRule) -> Optional[ValidationResult]:
        required_fields = rule.parameters.get("required_fields", [])
        missing_fields = []
        record_dict = record.model_dump()
        record_dict.update(record.metadata)
        for field in required_fields:
            value = record_dict.get(field)
            if value is None or value == "" or value == []:
                missing_fields.append(field)
        if missing_fields:
            return ValidationResult(
                data_id=record.id, rule_name=rule.rule_name, is_valid=False,
                severity=rule.severity,
                message=f"{rule.error_message} Missing fields: {', '.join(missing_fields)}",
                citation=rule.citation, suggested_fixes=rule.suggested_fixes
            )
        return None

    def precision_check(self, value: float, rule: ValidationRule, data_id: UUID) -> Optional[ValidationResult]:
        max_decimals = rule.parameters.get("max_decimal_places", 2)
        value_str = f"{value:.10f}".rstrip('0')
        decimal_places = len(value_str.split('.')[1]) if '.' in value_str else 0
        if decimal_places > max_decimals:
            return ValidationResult(
                data_id=data_id, rule_name=rule.rule_name, is_valid=False,
                severity=rule.severity,
                message=f"{rule.error_message} Value has {decimal_places} decimal places, max {max_decimals}.",
                citation=rule.citation, suggested_fixes=rule.suggested_fixes, actual_value=value
            )
        return None

    def validate_cross_field_consistency(self, records: List[NormalizedRecord]) -> List[ValidationResult]:
        results: List[ValidationResult] = []
        records_by_indicator: Dict[str, NormalizedRecord] = {}
        for record in records:
            records_by_indicator[record.indicator.lower().replace(" ", "_")] = record

        cross_field_rules = self.rules.get("cross_field", {})
        for rule in cross_field_rules.values():
            relationship = rule.parameters.get("relationship")
            if relationship == "sum":
                result = self._validate_sum_relationship(records_by_indicator, rule)
                if result:
                    results.append(result)
            elif relationship == "subset":
                result = self._validate_subset_relationship(records_by_indicator, rule)
                if result:
                    results.append(result)
            elif relationship == "correlation":
                result = self._validate_correlation_relationship(records_by_indicator, rule)
                if result:
                    results.append(result)
        return results

    def _validate_sum_relationship(self, records_by_indicator, rule) -> Optional[ValidationResult]:
        fields = rule.parameters.get("fields", [])
        tolerance = rule.parameters.get("tolerance", 0.02)
        if len(fields) < 2:
            return None
        component_fields = [f.lower().replace(" ", "_") for f in fields[:-1]]
        total_field = fields[-1].lower().replace(" ", "_")
        component_sum = 0.0
        data_id = None
        for field in component_fields:
            if field in records_by_indicator:
                record = records_by_indicator[field]
                component_sum += record.value
                if data_id is None:
                    data_id = record.id
        if total_field not in records_by_indicator:
            return None
        total_record = records_by_indicator[total_field]
        total_value = total_record.value
        diff_pct = abs(component_sum - total_value) / total_value if total_value != 0 else abs(component_sum)
        if diff_pct > tolerance:
            return ValidationResult(
                data_id=data_id or total_record.id, rule_name=rule.rule_name, is_valid=False,
                severity=rule.severity,
                message=f"{rule.error_message} Sum ({component_sum:.2f}) differs from total ({total_value:.2f}) by {diff_pct*100:.1f}%",
                citation=rule.citation, suggested_fixes=rule.suggested_fixes,
                actual_value=component_sum,
                expected_range=(total_value * (1 - tolerance), total_value * (1 + tolerance))
            )
        return None

    def _validate_subset_relationship(self, records_by_indicator, rule) -> Optional[ValidationResult]:
        fields = rule.parameters.get("fields", [])
        tolerance = rule.parameters.get("tolerance", 0.0)
        if len(fields) < 2:
            return None
        subset_fields = [f.lower().replace(" ", "_") for f in fields[:-1]]
        superset_field = fields[-1].lower().replace(" ", "_")
        if superset_field not in records_by_indicator:
            return None
        superset_record = records_by_indicator[superset_field]
        superset_value = superset_record.value
        for field in subset_fields:
            if field in records_by_indicator:
                subset_record = records_by_indicator[field]
                subset_value = subset_record.value
                if subset_value > superset_value * (1 + tolerance):
                    return ValidationResult(
                        data_id=subset_record.id, rule_name=rule.rule_name, is_valid=False,
                        severity=rule.severity,
                        message=f"{rule.error_message} {field} ({subset_value:.2f}) exceeds {superset_field} ({superset_value:.2f})",
                        citation=rule.citation, suggested_fixes=rule.suggested_fixes,
                        actual_value=subset_value, expected_range=(0, superset_value)
                    )
        return None

    def _validate_correlation_relationship(self, records_by_indicator, rule) -> Optional[ValidationResult]:
        fields = rule.parameters.get("fields", [])
        if len(fields) < 2:
            return None
        field1 = fields[0].lower().replace(" ", "_")
        field2 = fields[1].lower().replace(" ", "_")
        if field1 not in records_by_indicator or field2 not in records_by_indicator:
            return None
        record1 = records_by_indicator[field1]
        record2 = records_by_indicator[field2]
        if record2.value == 0:
            return None
        intensity = record1.value / record2.value
        intensity_range = rule.parameters.get("intensity_range")
        if intensity_range:
            min_intensity = intensity_range.get("min", 0)
            max_intensity = intensity_range.get("max", float('inf'))
            if intensity < min_intensity or intensity > max_intensity:
                return ValidationResult(
                    data_id=record1.id, rule_name=rule.rule_name, is_valid=False,
                    severity=rule.severity,
                    message=f"{rule.error_message} Intensity {field1}/{field2} = {intensity:.2f} outside ({min_intensity}-{max_intensity})",
                    citation=rule.citation, suggested_fixes=rule.suggested_fixes,
                    actual_value=intensity, expected_range=(min_intensity, max_intensity)
                )
        return None

    def validate_batch(self, records: List[NormalizedRecord], industry: str) -> Dict[UUID, List[ValidationResult]]:
        results: Dict[UUID, List[ValidationResult]] = {}

        # FIX: Normalize industry once for the whole batch
        industry = self._normalize_industry(industry)

        for record in records:
            record_results = self.validate_record(record, industry)
            if record_results:
                results[record.id] = record_results

        outlier_rule = self._get_outlier_rule(industry)
        if outlier_rule and len(records) >= 3:
            values = [(r.id, r.value) for r in records]
            outlier_results = self.outlier_detection(values, outlier_rule)
            for outlier_result in outlier_results:
                if outlier_result.data_id not in results:
                    results[outlier_result.data_id] = []
                results[outlier_result.data_id].append(outlier_result)

        cross_field_results = self.validate_cross_field_consistency(records)
        for cross_field_result in cross_field_results:
            if cross_field_result.data_id not in results:
                results[cross_field_result.data_id] = []
            results[cross_field_result.data_id].append(cross_field_result)

        return results

    def _get_outlier_rule(self, industry: str) -> Optional[ValidationRule]:
        cross_industry_rules = self.rules.get("cross_industry", {})
        for rule in cross_industry_rules.values():
            if rule.validation_type == "outlier":
                return rule
        return None

    def get_rules_summary(self) -> Dict[str, Any]:
        summary = {
            "total_rules": sum(len(rules) for rules in self.rules.values()),
            "industries": list(self.rules.keys()),
            "rules_by_industry": {industry: len(rules) for industry, rules in self.rules.items()},
            "validation_types": set()
        }
        for industry_rules in self.rules.values():
            for rule in industry_rules.values():
                summary["validation_types"].add(rule.validation_type)
        summary["validation_types"] = list(summary["validation_types"])
        return summary

    def validate_scope_totals(self, scope_1, scope_2, scope_3, total, tolerance=0.02):
        scope_sum = scope_1 + scope_2 + (scope_3 or 0)
        diff_pct = abs(scope_sum - total) / total if total != 0 else abs(scope_sum)
        if diff_pct > tolerance:
            return ValidationResult(
                data_id=uuid4(), rule_name="scope_totals_consistency", is_valid=False,
                severity="error",
                message=f"Sum of scopes ({scope_sum:.2f}) differs from total ({total:.2f}) by {diff_pct*100:.1f}%",
                citation="GHG Protocol - Corporate Accounting and Reporting Standard",
                suggested_fixes=["Verify all scope emissions included", "Check for double-counting"],
                actual_value=scope_sum,
                expected_range=(total * (1 - tolerance), total * (1 + tolerance))
            )
        return None

    def validate_energy_balance(self, electricity, fuel, steam, total_energy, tolerance=0.05):
        energy_sum = electricity + fuel + steam
        diff_pct = abs(energy_sum - total_energy) / total_energy if total_energy != 0 else abs(energy_sum)
        if diff_pct > tolerance:
            return ValidationResult(
                data_id=uuid4(), rule_name="energy_balance_validation", is_valid=False,
                severity="warning",
                message=f"Energy sources ({energy_sum:.2f}) differ from total ({total_energy:.2f}) by {diff_pct*100:.1f}%",
                citation="ISO 50001 - Energy Management Systems",
                suggested_fixes=["Verify all energy sources included", "Check unit conversions"],
                actual_value=energy_sum,
                expected_range=(total_energy * (1 - tolerance), total_energy * (1 + tolerance))
            )
        return None

    def validate_production_correlation(self, energy, emissions, production):
        if production == 0 or energy == 0:
            return None
        emission_factor = emissions / energy
        if emission_factor < 0.1 and emission_factor > 0:
            return ValidationResult(
                data_id=uuid4(), rule_name="production_emission_correlation", is_valid=False,
                severity="warning",
                message=f"Anomalous emission factor: {emission_factor:.2f} kg CO₂/GJ is very low.",
                citation="GHG Protocol - Intensity Metrics",
                suggested_fixes=["Verify renewable energy usage", "Check emission factors"],
                actual_value=emission_factor
            )
        if emission_factor > 300:
            return ValidationResult(
                data_id=uuid4(), rule_name="production_emission_correlation", is_valid=False,
                severity="error",
                message=f"Anomalous emission factor: {emission_factor:.2f} kg CO₂/GJ is extremely high.",
                citation="GHG Protocol - Intensity Metrics",
                suggested_fixes=["Check unit conversion errors", "Verify emission factors not double-counted"],
                actual_value=emission_factor
            )
        return None