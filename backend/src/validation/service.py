"""Validation Service for ESG Data Quality Management"""
import uuid
from typing import List, Dict, Optional, Any
from uuid import UUID
from datetime import datetime
from collections import defaultdict

from sqlalchemy.orm import Session
from sqlalchemy import and_

from src.validation.engine import (
    ValidationEngine,
    NormalizedRecord,
    ValidationResult as EngineValidationResult
)
from src.common.models import (
    NormalizedData,
    ValidationResult as DBValidationResult,
    Upload,
    AuditLog,
    AuditAction,
    Severity
)
from pydantic import BaseModel, Field
from src.common.provenance import get_provenance_tracker


class ValidationSummary(BaseModel):
    total_records: int
    valid_records: int
    records_with_errors: int
    records_with_warnings: int
    validation_pass_rate: float
    error_breakdown: Dict[str, int] = Field(default_factory=dict)
    warning_breakdown: Dict[str, int] = Field(default_factory=dict)


class ValidationReport(BaseModel):
    upload_id: UUID
    summary: ValidationSummary
    errors: List[Dict[str, Any]] = Field(default_factory=list)
    warnings: List[Dict[str, Any]] = Field(default_factory=list)
    recommendations: List[str] = Field(default_factory=list)
    generated_at: datetime = Field(default_factory=datetime.utcnow)


class ValidationService:

    def __init__(self, validation_engine: ValidationEngine, db_session: Session):
        self.engine = validation_engine
        self.db = db_session

    def validate_upload(self, upload_id: UUID, industry: str) -> ValidationSummary:
        from src.normalization.service import NormalizationService
        from src.normalization.normalizer import UnitNormalizer

        normalized_records = self.db.query(NormalizedData).filter(
            NormalizedData.upload_id == upload_id
        ).all()

        if not normalized_records:
            raise ValueError(f"No normalized data found for upload {upload_id}")

        normalizer = UnitNormalizer("data/validation-rules/conversion_factors.json")
        norm_service = NormalizationService(normalizer, self.db)
        intensity_map = norm_service.calculate_intensity_for_validation(upload_id)

        # Build absolute value validation records
        validation_records = []
        for record in normalized_records:
            indicator_name = (
                record.indicator.matched_indicator if record.indicator
                else str(record.indicator_id)
            )
            validation_records.append((record.id, NormalizedRecord(
                id=record.id,
                indicator=indicator_name,
                value=record.normalized_value,
                unit=record.normalized_unit,
                original_value=record.original_value,
                original_unit=record.original_unit,
                metadata={"facility": record.facility, "period": record.period}
            )))

        # FIX: Build synthetic intensity records AND track synthetic_id → real_data_id mapping
        synthetic_ids: set = set()
        synthetic_to_real: Dict[UUID, UUID] = {}  # synthetic_id → real NormalizedData.id
        intensity_validation_records = []

        for intensity_indicator, rows in intensity_map.items():
            for row in rows:
                synthetic_id = uuid.uuid4()
                synthetic_ids.add(synthetic_id)

                # Map synthetic → real data_id so we can save results against real records
                real_data_id = row.get("data_id")
                if real_data_id:
                    synthetic_to_real[synthetic_id] = UUID(real_data_id)

                synthetic_record = NormalizedRecord(
                    id=synthetic_id,
                    indicator=intensity_indicator,
                    value=row["value"],
                    unit=row["unit"],
                    original_value=row["absolute_value"],
                    original_unit="",
                    metadata={
                        "facility": row["facility"],
                        "period": row["period"],
                        "is_intensity": True,
                    }
                )
                intensity_validation_records.append((synthetic_id, synthetic_record))

        all_input_records = validation_records + intensity_validation_records

        # Group by indicator for batch validation
        records_by_indicator: Dict[str, List] = defaultdict(list)
        for record_id, record in all_input_records:
            records_by_indicator[record.indicator].append((record_id, record))

        all_validation_results = []
        for indicator, records in records_by_indicator.items():
            records_only = [r[1] for r in records]
            batch_results = self.engine.validate_batch(records_only, industry)
            for data_id, results in batch_results.items():
                for result in results:
                    if result.data_id in synthetic_ids:
                        # FIX: Remap synthetic ID → real data_id before saving
                        real_id = synthetic_to_real.get(result.data_id)
                        if real_id:
                            result.data_id = real_id
                            all_validation_results.append(result)
                        # If no real_id mapping, discard (shouldn't happen)
                    else:
                        # Normal absolute-value result — save as-is
                        all_validation_results.append(result)

        if all_validation_results:
            self.save_validation_results(all_validation_results, upload_id)

        summary = self._generate_summary(normalized_records, all_validation_results)
        self._log_validation_audit(upload_id, summary)

        prov = get_provenance_tracker()
        activity_id = f"validation_{upload_id}"
        prov.record_activity(activity_id, "data_validation",
            datetime.utcnow(), datetime.utcnow(), "validation_service")
        prov.record_entity(f"validated_{upload_id}", "validation_results", {
            "total_records": summary.total_records,
            "errors": summary.records_with_errors,
            "warnings": summary.records_with_warnings,
            "pass_rate": summary.validation_pass_rate,
        })
        prov.record_derivation(f"normalized_{upload_id}", f"validated_{upload_id}", activity_id)
        prov.flush()

        return summary

    def validate_indicator_batch(self, records: List[NormalizedRecord], industry: str) -> List[EngineValidationResult]:
        batch_results = self.engine.validate_batch(records, industry)
        all_results = []
        for data_id, results in batch_results.items():
            all_results.extend(results)
        return all_results

    def get_validation_errors(self, upload_id: UUID) -> List[Dict[str, Any]]:
        errors = self.db.query(DBValidationResult).join(
            NormalizedData, DBValidationResult.data_id == NormalizedData.id
        ).filter(and_(
            NormalizedData.upload_id == upload_id,
            DBValidationResult.severity == Severity.ERROR,
            DBValidationResult.is_valid == False
        )).order_by(DBValidationResult.rule_name).all()
        return [self._serialize_validation_result(e) for e in errors]

    def get_validation_warnings(self, upload_id: UUID) -> List[Dict[str, Any]]:
        warnings = self.db.query(DBValidationResult).join(
            NormalizedData, DBValidationResult.data_id == NormalizedData.id
        ).filter(and_(
            NormalizedData.upload_id == upload_id,
            DBValidationResult.severity == Severity.WARNING,
            DBValidationResult.is_valid == False
        )).order_by(DBValidationResult.rule_name).all()
        return [self._serialize_validation_result(w) for w in warnings]

    def save_validation_results(self, results: List[EngineValidationResult], upload_id: Optional[UUID] = None) -> None:
        if not results:
            return
        db_results = []
        for result in results:
            citation = result.citation[:500] if result.citation else ""
            db_results.append(DBValidationResult(
                data_id=result.data_id,
                rule_name=result.rule_name,
                is_valid=result.is_valid,
                severity=Severity.ERROR if result.severity == "error" else Severity.WARNING,
                message=result.message,
                citation=citation
            ))
        self.db.bulk_save_objects(db_results)
        self.db.commit()
        if upload_id:
            self.db.add(AuditLog(
                entity_id=upload_id, entity_type="upload", action=AuditAction.REVIEWED,
                actor="validation_service",
                changes={"validation_results_count": len(results),
                         "errors": sum(1 for r in results if r.severity == "error"),
                         "warnings": sum(1 for r in results if r.severity == "warning")}
            ))
            self.db.commit()

    def generate_validation_report(self, upload_id: UUID) -> ValidationReport:
        normalized_records = self.db.query(NormalizedData).filter(NormalizedData.upload_id == upload_id).all()
        all_results = self.db.query(DBValidationResult).join(
            NormalizedData, DBValidationResult.data_id == NormalizedData.id
        ).filter(NormalizedData.upload_id == upload_id).all()
        engine_results = []
        for db_result in all_results:
            if not db_result.is_valid:
                engine_results.append(EngineValidationResult(
                    data_id=db_result.data_id, rule_name=db_result.rule_name,
                    is_valid=db_result.is_valid,
                    severity="error" if db_result.severity == Severity.ERROR else "warning",
                    message=db_result.message, citation=db_result.citation, suggested_fixes=[]
                ))
        summary = self._generate_summary(normalized_records, engine_results)
        errors = self.get_validation_errors(upload_id)
        warnings = self.get_validation_warnings(upload_id)
        return ValidationReport(
            upload_id=upload_id, summary=summary, errors=errors, warnings=warnings,
            recommendations=self._generate_recommendations(summary, errors, warnings)
        )

    def _generate_summary(self, normalized_records, validation_results) -> ValidationSummary:
        total_records = len(normalized_records)
        records_with_errors = set()
        records_with_warnings = set()
        error_breakdown = defaultdict(int)
        warning_breakdown = defaultdict(int)
        for result in validation_results:
            if result.severity == "error":
                records_with_errors.add(result.data_id)
                error_breakdown[result.rule_name] += 1
            else:
                records_with_warnings.add(result.data_id)
                warning_breakdown[result.rule_name] += 1
        valid_records = total_records - len(records_with_errors.union(records_with_warnings))
        pass_rate = ((total_records - len(records_with_errors)) / total_records * 100) if total_records > 0 else 100.0
        return ValidationSummary(
            total_records=total_records, valid_records=valid_records,
            records_with_errors=len(records_with_errors),
            records_with_warnings=len(records_with_warnings),
            validation_pass_rate=round(pass_rate, 2),
            error_breakdown=dict(error_breakdown), warning_breakdown=dict(warning_breakdown)
        )

    def _serialize_validation_result(self, db_result: DBValidationResult) -> Dict[str, Any]:
        return {
            "id": str(db_result.id), "data_id": str(db_result.data_id),
            "rule_name": db_result.rule_name, "is_valid": db_result.is_valid,
            "severity": db_result.severity.value, "message": db_result.message,
            "citation": db_result.citation,
            "created_at": db_result.created_at.isoformat() if db_result.created_at else None
        }

    def _generate_recommendations(self, summary, errors, warnings) -> List[str]:
        recommendations = []
        if summary.validation_pass_rate < 50:
            recommendations.append("⚠️ Critical: Over 50% of records have validation errors.")
        if summary.error_breakdown:
            most_common = max(summary.error_breakdown.items(), key=lambda x: x[1])
            recommendations.append(f"🔍 Most common error: '{most_common[0]}' ({most_common[1]} occurrences).")
        if any("range" in e.get("rule_name", "").lower() for e in errors):
            recommendations.append("📏 Values outside expected ranges. Check unit conversion errors.")
        if any("outlier" in e.get("rule_name", "").lower() for e in errors):
            recommendations.append("📊 Statistical outliers detected. Review for data entry errors.")
        if any("temporal" in w.get("rule_name", "").lower() for w in warnings):
            recommendations.append("📅 Temporal consistency issues. Verify monthly vs annual totals.")
        if summary.records_with_errors == 0 and summary.records_with_warnings == 0:
            recommendations.append("✅ All records passed. Data ready for report generation.")
        elif summary.records_with_errors == 0:
            recommendations.append("✅ No errors, only warnings. Review warnings for improvements.")
        return recommendations

    def _log_validation_audit(self, upload_id: UUID, summary: ValidationSummary) -> None:
        self.db.add(AuditLog(
            entity_id=upload_id, entity_type="upload", action=AuditAction.REVIEWED,
            actor="validation_service",
            changes={"total_records": summary.total_records, "valid_records": summary.valid_records,
                     "records_with_errors": summary.records_with_errors,
                     "validation_pass_rate": summary.validation_pass_rate}
        ))
        self.db.commit()

    def get_validation_statistics(self, upload_id: UUID) -> Dict[str, Any]:
        all_results = self.db.query(DBValidationResult).join(
            NormalizedData, DBValidationResult.data_id == NormalizedData.id
        ).filter(NormalizedData.upload_id == upload_id).all()
        total = len(all_results)
        passed = sum(1 for r in all_results if r.is_valid)
        errors = sum(1 for r in all_results if r.severity == Severity.ERROR and not r.is_valid)
        warnings = sum(1 for r in all_results if r.severity == Severity.WARNING and not r.is_valid)
        rules_applied = set(r.rule_name for r in all_results)
        return {
            "total_validations": total, "passed": passed, "failed": total - passed,
            "errors": errors, "warnings": warnings,
            "pass_rate": round((passed / total * 100) if total > 0 else 0, 2),
            "rules_applied": list(rules_applied), "rules_count": len(rules_applied)
        }

    def revalidate_record(self, data_id: UUID, industry: str) -> List[EngineValidationResult]:
        record = self.db.query(NormalizedData).filter(NormalizedData.id == data_id).first()
        if not record:
            raise ValueError(f"Record {data_id} not found")
        indicator_name = record.indicator.matched_indicator if record.indicator else str(record.indicator_id)
        validation_record = NormalizedRecord(
            id=record.id, indicator=indicator_name, value=record.normalized_value,
            unit=record.normalized_unit, original_value=record.original_value,
            original_unit=record.original_unit, metadata={}
        )
        results = self.engine.validate_record(validation_record, industry)
        self.db.query(DBValidationResult).filter(DBValidationResult.data_id == data_id).delete()
        if results:
            self.save_validation_results(results, record.upload_id)
        return results

    def mark_error_as_reviewed(self, result_id: UUID, reviewer: str, notes: str) -> None:
        vr = self.db.query(DBValidationResult).filter(DBValidationResult.id == result_id).first()
        if not vr:
            raise ValueError(f"Validation result {result_id} not found")
        vr.reviewed = True
        vr.reviewer_notes = notes
        vr.updated_at = datetime.utcnow()
        self.db.commit()
        self.db.add(AuditLog(entity_id=result_id, entity_type="validation_result",
            action=AuditAction.REVIEWED, actor=reviewer,
            changes={"reviewed": True, "reviewer": reviewer, "notes": notes}))
        self.db.commit()

    def suppress_warning(self, result_id: UUID, reason: str, reviewer: str = "system") -> None:
        vr = self.db.query(DBValidationResult).filter(DBValidationResult.id == result_id).first()
        if not vr:
            raise ValueError(f"Validation result {result_id} not found")
        if vr.severity == Severity.ERROR:
            raise ValueError("Cannot suppress errors. Use mark_error_as_reviewed instead.")
        vr.reviewed = True
        vr.reviewer_notes = f"SUPPRESSED: {reason}"
        vr.updated_at = datetime.utcnow()
        self.db.commit()
        self.db.add(AuditLog(entity_id=result_id, entity_type="validation_result",
            action=AuditAction.REVIEWED, actor=reviewer,
            changes={"action": "suppressed", "reason": reason}))
        self.db.commit()

    def get_unreviewed_errors(self, upload_id: UUID) -> List[Dict[str, Any]]:
        errors = self.db.query(DBValidationResult).join(
            NormalizedData, DBValidationResult.data_id == NormalizedData.id
        ).filter(and_(
            NormalizedData.upload_id == upload_id,
            DBValidationResult.severity == Severity.ERROR,
            DBValidationResult.is_valid == False,
            DBValidationResult.reviewed == False
        )).order_by(DBValidationResult.rule_name).all()
        return [self._serialize_validation_result(e) for e in errors]

    def calculate_final_pass_rate(self, upload_id: UUID) -> float:
        total = self.db.query(NormalizedData).filter(NormalizedData.upload_id == upload_id).count()
        if total == 0:
            return 100.0
        unreviewed = self.db.query(DBValidationResult).join(
            NormalizedData, DBValidationResult.data_id == NormalizedData.id
        ).filter(and_(
            NormalizedData.upload_id == upload_id,
            DBValidationResult.severity == Severity.ERROR,
            DBValidationResult.is_valid == False,
            DBValidationResult.reviewed == False
        )).all()
        return round((total - len(set(e.data_id for e in unreviewed))) / total * 100, 2)

    def get_review_summary(self, upload_id: UUID) -> Dict[str, Any]:
        all_results = self.db.query(DBValidationResult).join(
            NormalizedData, DBValidationResult.data_id == NormalizedData.id
        ).filter(NormalizedData.upload_id == upload_id).all()
        total_errors = sum(1 for r in all_results if r.severity == Severity.ERROR and not r.is_valid)
        reviewed_errors = sum(1 for r in all_results if r.severity == Severity.ERROR and not r.is_valid and r.reviewed)
        total_warnings = sum(1 for r in all_results if r.severity == Severity.WARNING and not r.is_valid)
        suppressed = sum(1 for r in all_results if r.severity == Severity.WARNING and not r.is_valid and r.reviewed)
        return {
            "total_errors": total_errors, "reviewed_errors": reviewed_errors,
            "unreviewed_errors": total_errors - reviewed_errors,
            "total_warnings": total_warnings, "suppressed_warnings": suppressed,
            "active_warnings": total_warnings - suppressed,
            "ready_for_export": (total_errors - reviewed_errors) == 0,
            "final_pass_rate": self.calculate_final_pass_rate(upload_id)
        }

    def bulk_review_errors(self, result_ids: List[UUID], reviewer: str, notes: str) -> int:
        count = 0
        for result_id in result_ids:
            try:
                self.mark_error_as_reviewed(result_id, reviewer, notes)
                count += 1
            except ValueError:
                continue
        return count

    def get_comprehensive_results(self, upload_id: UUID) -> Optional[Dict[str, Any]]:
        upload = self.db.query(Upload).filter(Upload.id == upload_id).first()
        if not upload:
            return None
        normalized_count = self.db.query(NormalizedData).filter(NormalizedData.upload_id == upload_id).count()
        all_results = self.db.query(DBValidationResult).join(
            NormalizedData, DBValidationResult.data_id == NormalizedData.id
        ).filter(NormalizedData.upload_id == upload_id).all()

        records_with_errors: set = set()
        records_with_warnings: set = set()
        error_breakdown: Dict[str, int] = defaultdict(int)
        warning_breakdown: Dict[str, int] = defaultdict(int)
        errors_list, warnings_list = [], []
        unreviewed_errors = 0

        for r in all_results:
            if r.is_valid:
                continue
            reviewed = getattr(r, "reviewed", False) or False
            notes = getattr(r, "reviewer_notes", None)
            rule = self._lookup_rule(r.rule_name)
            citation = r.citation or (rule.citation if rule else "")
            if r.severity == Severity.ERROR:
                records_with_errors.add(r.data_id)
                error_breakdown[r.rule_name] += 1
                if not reviewed:
                    unreviewed_errors += 1
                errors_list.append({
                    "result_id": r.id, "indicator": r.rule_name, "rule_name": r.rule_name,
                    "severity": "error", "message": r.message, "citation": citation,
                    "suggested_fixes": rule.suggested_fixes if rule else [],
                    "reviewed": reviewed, "reviewer_notes": notes,
                })
            else:
                records_with_warnings.add(r.data_id)
                warning_breakdown[r.rule_name] += 1
                warnings_list.append({
                    "result_id": r.id, "rule_name": r.rule_name,
                    "severity": "warning", "message": r.message, "reviewed": reviewed,
                })

        valid_records = normalized_count - len(records_with_errors.union(records_with_warnings))
        pass_rate = ((normalized_count - len(records_with_errors)) / normalized_count * 100) if normalized_count > 0 else 100.0
        meta = upload.file_metadata or {}

        return {
            "upload_id": upload_id,
            "status": "completed" if len(all_results) > 0 or normalized_count > 0 else "pending",
            "industry": meta.get("industry"),
            "summary": {
                "total_records": normalized_count, "valid_records": max(valid_records, 0),
                "records_with_errors": len(records_with_errors),
                "records_with_warnings": len(records_with_warnings),
                "validation_pass_rate": round(pass_rate, 4),
                "unreviewed_errors": unreviewed_errors,
            },
            "error_breakdown": dict(error_breakdown),
            "warning_breakdown": dict(warning_breakdown),
            "errors": errors_list, "warnings": warnings_list,
        }

    def _lookup_rule(self, rule_name: str):
        for industry_rules in self.engine.rules.values():
            for rule in industry_rules.values():
                if rule.rule_name == rule_name:
                    return rule
        return None

    def get_reviewed_items(self, upload_id: UUID) -> Dict[str, List[Dict[str, Any]]]:
        reviewed_items = self.db.query(DBValidationResult).join(
            NormalizedData, DBValidationResult.data_id == NormalizedData.id
        ).filter(and_(
            NormalizedData.upload_id == upload_id,
            DBValidationResult.reviewed == True
        )).all()
        reviewed_errors, suppressed_warnings = [], []
        for item in reviewed_items:
            s = self._serialize_validation_result(item)
            s["reviewer_notes"] = item.reviewer_notes
            (reviewed_errors if item.severity == Severity.ERROR else suppressed_warnings).append(s)
        return {"reviewed_errors": reviewed_errors, "suppressed_warnings": suppressed_warnings}