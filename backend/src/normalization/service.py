"""Normalization service for ESG data."""

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional
from uuid import UUID

import polars as pl
from sqlalchemy.orm import Session

from src.common.models import (
    MatchedIndicator,
    NormalizedData,
    Upload,
    AuditLog,
    AuditAction,
)
from src.common.provenance import get_provenance_tracker
from src.normalization.normalizer import UnitNormalizer, UnitNotFoundError

logger = logging.getLogger(__name__)


class NormalizationError(Exception):
    pass


@dataclass
class NormalizedRecord:
    matched_indicator_id: UUID
    original_value: float
    original_unit: str
    normalized_value: float
    normalized_unit: str
    conversion_factor: Optional[float]
    row_index: int
    metadata: Dict
    facility: Optional[str] = None
    period: Optional[str] = None


@dataclass
class NormalizationSummary:
    total_records: int
    successfully_normalized: int
    failed_normalization: int
    unique_units_detected: List[str]
    conversions_applied: Dict[str, int]
    errors: List[str]


class NormalizationService:

    def __init__(self, normalizer: UnitNormalizer, db_session: Session):
        self.normalizer = normalizer
        self.db = db_session

    def normalize_data(self, upload_id: UUID) -> NormalizationSummary:
        upload = self.db.query(Upload).filter(Upload.id == upload_id).first()
        if not upload:
            raise NormalizationError(f"Upload {upload_id} not found")

        meta = upload.file_metadata or {}
        facility = meta.get("facility_name")
        reporting_period = meta.get("reporting_period")

        matched_indicators = (
            self.db.query(MatchedIndicator)
            .filter(MatchedIndicator.upload_id == upload_id)
            .all()
        )

        # Deduplicate by original_header
        seen_headers = set()
        unique_indicators = []
        for mi in matched_indicators:
            if mi.original_header not in seen_headers:
                seen_headers.add(mi.original_header)
                unique_indicators.append(mi)
        matched_indicators = unique_indicators

        if not matched_indicators:
            raise NormalizationError(f"No indicators found for upload {upload_id}")

        file_path = upload.file_path
        if file_path.endswith(".csv"):
            data_df = pl.read_csv(file_path)
        elif file_path.endswith((".xlsx", ".xls")):
            data_df = pl.read_excel(file_path)
        elif file_path.endswith(".parquet"):
            data_df = pl.read_parquet(file_path)
        else:
            raise NormalizationError(f"Unsupported file format: {file_path}")

        date_column = self._detect_date_column(data_df)

        total_records = 0
        successfully_normalized = 0
        failed_normalization = 0
        unique_units: set = set()
        conversions_applied: Dict[str, int] = {}
        errors = []

        try:
            for indicator in matched_indicators:
                indicator_total = 0
                indicator_success = 0
                column_data = []

                try:
                    if indicator.original_header not in data_df.columns:
                        errors.append(f"Column '{indicator.original_header}' not found")
                        continue

                    column_data = data_df[indicator.original_header].to_list()
                    date_data = data_df[date_column].to_list() if date_column else None

                    indicator_total = len([
                        v for v in column_data
                        if isinstance(v, (int, float)) and v is not None
                    ])

                    records = self.process_indicator(
                        indicator.id,
                        indicator.original_header,
                        indicator.matched_indicator,
                        column_data,
                        facility=facility,
                        reporting_period=reporting_period,
                        date_data=date_data,
                    )

                    for rec in records:
                        rec.metadata["upload_id"] = upload_id

                    if records:
                        self.save_normalized_data(records)
                        indicator_success = len(records)
                        for record in records:
                            unique_units.add(record.original_unit)
                            key = f"{record.original_unit}->{record.normalized_unit}"
                            conversions_applied[key] = conversions_applied.get(key, 0) + 1

                    successfully_normalized += indicator_success
                    total_records += indicator_total

                except Exception as e:
                    error_msg = f"Error processing {indicator.original_header}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg, exc_info=True)
                    self.db.rollback()
                    failed_normalization += indicator_total or len(column_data)
                    total_records += indicator_total or len(column_data)

            prov = get_provenance_tracker()
            activity_id = f"normalization_{upload_id}"
            prov.record_activity(activity_id, "data_normalization",
                datetime.now(timezone.utc), datetime.now(timezone.utc), "system")
            prov.record_entity(f"normalized_{upload_id}", "normalized_dataset", {
                "total": total_records, "success": successfully_normalized,
                "failed": failed_normalization,
            })
            prov.record_derivation(str(upload_id), f"normalized_{upload_id}", activity_id)
            prov.flush()
            self._create_audit_log(upload_id, f"Normalized {successfully_normalized}/{total_records} records")

        except Exception as e:
            self.db.rollback()
            logger.error(f"Critical normalization error: {str(e)}", exc_info=True)
            raise NormalizationError(f"Normalization failed: {str(e)}") from e

        return NormalizationSummary(
            total_records=total_records,
            successfully_normalized=successfully_normalized,
            failed_normalization=failed_normalization,
            unique_units_detected=list(unique_units),
            conversions_applied=conversions_applied,
            errors=errors,
        )

    def process_indicator(
        self,
        indicator_id: UUID,
        header_name: str,
        canonical_indicator: str,
        data: List,
        facility: Optional[str] = None,
        reporting_period: Optional[str] = None,
        date_data: Optional[List] = None,
    ) -> List[NormalizedRecord]:
        numeric_values = [v for v in data if isinstance(v, (int, float)) and v is not None]
        if not numeric_values:
            return []

        detected_unit = self.detect_unit_from_context(header_name, numeric_values[:100])
        if not detected_unit:
            raise NormalizationError(f"Could not detect unit for '{header_name}'.")

        records = []
        for idx, value in enumerate(data):
            if not isinstance(value, (int, float)) or value is None:
                continue
            try:
                result = self.normalizer.normalize(float(value), detected_unit)
                row_period = reporting_period
                if not row_period and date_data and idx < len(date_data):
                    row_period = self._parse_period_from_date(date_data[idx])
                record = NormalizedRecord(
                    matched_indicator_id=indicator_id,
                    original_value=value,
                    original_unit=detected_unit,
                    normalized_value=result.normalized_value,
                    normalized_unit=result.normalized_unit,
                    conversion_factor=result.conversion_factor,
                    row_index=idx,
                    facility=facility,
                    period=row_period,
                    metadata={
                        "header_name": header_name,
                        "canonical_indicator": canonical_indicator,
                        "conversion_source": result.conversion_source,
                        "formula": result.formula,
                    },
                )
                records.append(record)
            except Exception as e:
                logger.debug(f"Failed to normalize value {value} at index {idx}: {e}")
                continue

        return records

    def calculate_intensity_for_validation(self, upload_id: UUID) -> Dict[str, List[Dict]]:
        """Calculate intensity metrics by dividing absolute values by production.
        Returns {indicator_name: [{value, unit, period, facility, data_id, ...}]}
        """
        all_records = (
            self.db.query(NormalizedData)
            .filter(NormalizedData.upload_id == upload_id)
            .all()
        )

        if not all_records:
            return {}

        # Build production lookup: key = "period|facility" → value
        production_map: Dict[str, float] = {}
        for r in all_records:
            name = r.indicator.matched_indicator if r.indicator else ""
            if "production" in name.lower():
                period = r.period or "unknown"
                facility = r.facility or "unknown"
                key = f"{period}|{facility}"
                production_map[key] = r.normalized_value

        indicator_map: Dict[str, List] = {}

        for r in all_records:
            name = r.indicator.matched_indicator if r.indicator else ""
            if not name or "production" in name.lower():
                continue

            period = r.period or "unknown"
            facility = r.facility or "unknown"
            key = f"{period}|{facility}"
            production = production_map.get(key)

            if not production or production <= 0:
                continue

            intensity = r.normalized_value / production

            # Convert units to match validation rule ranges
            intensity_value = intensity
            intensity_unit = f"{r.normalized_unit}/tonne"

            if r.normalized_unit in ("tonnes", "tonnes CO2e", "tCO2e", "t", "tonne"):
                # tonnes → kg to match rules expecting "800-1100 kg CO2/tonne"
                intensity_value = intensity * 1000
                intensity_unit = "kg/tonne"
            elif r.normalized_unit == "kWh":
                # kWh → GJ to match rules expecting "2.9-4.5 GJ/tonne"
                intensity_value = intensity * 3.6 / 1000
                intensity_unit = "GJ/tonne"
            elif r.normalized_unit == "MWh":
                # MWh → GJ to match rules expecting "2.9-4.5 GJ/tonne"
                intensity_value = intensity * 3.6
                intensity_unit = "GJ/tonne"

            intensity_name = f"{name} Intensity"
            if intensity_name not in indicator_map:
                indicator_map[intensity_name] = []
            indicator_map[intensity_name].append({
                "value": intensity_value,
                "unit": intensity_unit,
                "period": period,
                "facility": facility,
                "absolute_value": r.normalized_value,
                "production": production,
                "data_id": str(r.id),
            })

        return indicator_map

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _detect_date_column(self, df: pl.DataFrame) -> Optional[str]:
        for col in df.columns:
            if any(kw in col.lower() for kw in ["date", "month", "period", "year", "time"]):
                return col
        return None

    def _parse_period_from_date(self, date_value) -> Optional[str]:
        if date_value is None:
            return None
        try:
            if isinstance(date_value, str):
                for fmt in ["%d/%m/%y", "%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y"]:
                    try:
                        dt = datetime.strptime(date_value, fmt)
                        return dt.strftime("%Y-%m")
                    except ValueError:
                        continue
            elif hasattr(date_value, "strftime"):
                return date_value.strftime("%Y-%m")
        except Exception:
            pass
        return str(date_value)[:7] if date_value else None

    def detect_unit_from_context(self, indicator_name: str, sample_values: List[float]) -> Optional[str]:
        unit_from_name = self._extract_unit_from_text(indicator_name)
        if unit_from_name:
            return unit_from_name
        if not sample_values:
            return None
        max_value = max(sample_values)
        if any(kw in indicator_name.lower() for kw in ["energy", "electricity", "power"]):
            return "kWh" if max_value > 100000 else ("MWh" if max_value > 100 else "GJ")
        if any(kw in indicator_name.lower() for kw in ["emission", "co2", "ghg", "carbon"]):
            if "kg" in indicator_name.lower():
                return "kg CO2e"
            return "tonnes CO2e" if max_value <= 1000 else "kg CO2e"
        if any(kw in indicator_name.lower() for kw in ["water", "consumption"]):
            return "liters" if max_value > 10000 else "m3"
        if any(kw in indicator_name.lower() for kw in ["waste", "material", "mass", "weight"]):
            return "kg" if max_value > 10000 else "tonnes"
        if any(kw in indicator_name.lower() for kw in ["gas", "fuel", "diesel", "coal"]):
            if "m³" in indicator_name or "m3" in indicator_name.lower():
                return "m3"
            return "liters" if max_value > 10000 else "tonnes"
        if any(kw in indicator_name.lower() for kw in ["incident", "employee", "worker", "count"]):
            return "count"
        return None

    def _extract_unit_from_text(self, text: str) -> Optional[str]:
        patterns = [r'\(([^)]+)\)', r'\[([^\]]+)\]', r'\s+in\s+(\w+)']
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                potential_unit = match.group(1).strip()
                try:
                    unit, _ = self.normalizer.detect_unit(potential_unit)
                    return unit
                except UnitNotFoundError:
                    continue
        return None

    def save_normalized_data(self, records: List[NormalizedRecord]) -> None:
        db_records = []
        for record in records:
            db_records.append(NormalizedData(
                upload_id=record.metadata.get("upload_id") if record.metadata else None,
                indicator_id=record.matched_indicator_id,
                original_value=record.original_value,
                original_unit=record.original_unit,
                normalized_value=record.normalized_value,
                normalized_unit=record.normalized_unit,
                conversion_factor=record.conversion_factor or 1.0,
                conversion_source=record.metadata.get("conversion_source", "detected") if record.metadata else "detected",
                facility=record.facility,
                period=record.period,
            ))
        try:
            self.db.bulk_save_objects(db_records)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to save normalized data: {e}", exc_info=True)
            raise

    def get_normalized_data(self, upload_id: UUID, indicator_name: Optional[str] = None) -> pl.DataFrame:
        from sqlalchemy.orm import joinedload
        query = (
            self.db.query(NormalizedData)
            .join(MatchedIndicator, NormalizedData.indicator_id == MatchedIndicator.id)
            .options(joinedload(NormalizedData.indicator))
            .filter(MatchedIndicator.upload_id == upload_id)
        )
        if indicator_name:
            query = query.filter(MatchedIndicator.matched_indicator == indicator_name)
        records = query.all()
        if not records:
            return pl.DataFrame()
        return pl.DataFrame({
            "indicator": [r.indicator.matched_indicator for r in records],
            "original_value": [r.original_value for r in records],
            "original_unit": [r.original_unit for r in records],
            "normalized_value": [r.normalized_value for r in records],
            "normalized_unit": [r.normalized_unit for r in records],
            "facility": [r.facility for r in records],
            "period": [r.period for r in records],
            "row_index": [r.row_index for r in records],
        })

    def check_unit_conflicts(self, upload_id: UUID) -> Dict[str, List[str]]:
        conflicts = {}
        matched_indicators = (
            self.db.query(MatchedIndicator)
            .filter(MatchedIndicator.upload_id == upload_id)
            .all()
        )
        for indicator in matched_indicators:
            units = (
                self.db.query(NormalizedData.original_unit)
                .filter(NormalizedData.indicator_id == indicator.id)
                .distinct()
                .all()
            )
            unit_list = [u[0] for u in units]
            if len(unit_list) > 1:
                conflicts[indicator.matched_indicator] = unit_list
        return conflicts

    def get_comprehensive_results(self, upload_id: UUID, limit: int = 100, offset: int = 0) -> Optional[Dict]:
        upload = self.db.query(Upload).filter(Upload.id == upload_id).first()
        if not upload:
            return None
        all_records = self.db.query(NormalizedData).filter(NormalizedData.upload_id == upload_id).all()
        total = len(all_records)
        status = "completed" if total > 0 else "pending"

        conversions_map: Dict[str, Dict] = {}
        for r in all_records:
            indicator_name = r.indicator.matched_indicator if r.indicator else "Unknown"
            key = f"{indicator_name}|{r.original_unit}|{r.normalized_unit}"
            if key not in conversions_map:
                conversions_map[key] = {
                    "indicator": indicator_name, "from_unit": r.original_unit,
                    "to_unit": r.normalized_unit, "conversion_factor": r.conversion_factor,
                    "conversion_source": r.conversion_source or "Unknown", "record_count": 0,
                }
            conversions_map[key]["record_count"] += 1

        matched = self.db.query(MatchedIndicator).filter(MatchedIndicator.upload_id == upload_id).all()
        normalised_ids = {r.indicator_id for r in all_records}
        errors = []
        failed = 0
        for mi in matched:
            if mi.id not in normalised_ids:
                failed += 1
                errors.append({"indicator": mi.matched_indicator, "issue": "Unit not detected",
                                "suggestion": "Add unit to header or review manually"})

        rate = total / (total + failed) if (total + failed) > 0 else 0.0
        sample_records = (
            self.db.query(NormalizedData).filter(NormalizedData.upload_id == upload_id)
            .order_by(NormalizedData.id).offset(offset).limit(limit).all()
        )
        data_sample = []
        for r in sample_records:
            indicator_name = r.indicator.matched_indicator if r.indicator else "Unknown"
            data_sample.append({
                "data_id": r.id, "indicator": indicator_name,
                "original_value": r.original_value, "original_unit": r.original_unit,
                "normalized_value": r.normalized_value, "normalized_unit": r.normalized_unit,
                "facility": r.facility, "period": r.period,
            })

        return {
            "upload_id": upload_id, "status": status,
            "summary": {"total_records": total, "successfully_normalized": total,
                        "failed_normalization": failed, "normalization_rate": round(rate, 4)},
            "conversions": list(conversions_map.values()),
            "errors": errors, "data_sample": data_sample,
        }

    def _create_audit_log(self, upload_id: UUID, message: str) -> None:
        try:
            self.db.add(AuditLog(
                entity_id=upload_id, entity_type="upload", action=AuditAction.NORMALIZE,
                actor="system", changes={"message": message},
                timestamp=datetime.now(timezone.utc),
            ))
            self.db.commit()
        except Exception as e:
            logger.error(f"Failed to create audit log: {e}")
            self.db.rollback()