"""Matching service orchestrating rule-based and LLM matching"""
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Dict
from uuid import UUID

from sqlalchemy.orm import Session
from sqlalchemy import func

from src.common.models import MatchedIndicator, MatchingMethod, AuditLog, AuditAction
from src.common.config import settings
from src.common.provenance import get_provenance_tracker
from src.matching.rule_matcher import RuleBasedMatcher
from src.matching.llm_matcher import LLMMatcher

logger = logging.getLogger(__name__)

# Method mapping for consistency
METHOD_MAP = {
    "exact": MatchingMethod.RULE,
    "fuzzy": MatchingMethod.RULE,
    "llm": MatchingMethod.LLM,
    "manual": MatchingMethod.MANUAL
}


@dataclass
class MatchingResult:
    """Result of matching operation"""
    original_header: str
    matched_indicator: str
    confidence: float
    method: str
    requires_review: bool
    indicator_id: Optional[UUID] = None
    unit: Optional[str] = None
    category: Optional[str] = None
    reasoning: Optional[str] = None


class MatchingService:
    """Service for matching headers to standard indicators"""

    def __init__(
        self,
        rule_matcher: RuleBasedMatcher,
        llm_matcher: LLMMatcher,
        db: Session,
        actor: str = "system"
    ):
        """Initialize matching service"""
        self.rule_matcher = rule_matcher
        self.llm_matcher = llm_matcher
        self.db = db
        self.actor = actor
        
        # Build indicator lookup cache for efficiency
        self._indicator_lookup = {
            data['canonical_name']: {
                'unit': data.get('unit'),
                'category': data.get('category')
            }
            for data in rule_matcher.indicators.values()
        }

    def match_headers(
        self,
        upload_id: UUID,
        headers: List[str]
    ) -> List[MatchingResult]:
        """Match all headers for an upload"""
        logger.info(f"Matching {len(headers)} headers for upload {upload_id}")
        
        results = []
        matched_indicators = []
        
        for header in headers:
            try:
                # Get best match
                result = self.get_best_match(header)
                
                if result:
                    # Create database object (don't commit yet)
                    matched_indicator = self._create_match_record(upload_id, header, result)
                    matched_indicators.append(matched_indicator)
                    results.append(result)
                    
                    logger.info(
                        f"Matched '{header}' → '{result.matched_indicator}' "
                        f"({result.confidence:.2f}, {result.method})"
                    )
                else:
                    logger.warning(f"No match found for header: {header}")
                    
            except Exception as e:
                logger.error(f"Error matching header '{header}': {e}", exc_info=True)
                continue
        
        # Bulk commit all matches
        if matched_indicators:
            self.db.add_all(matched_indicators)
            self.db.flush()  # Get IDs without committing
            
            # Update result objects with IDs
            for result, indicator in zip(results, matched_indicators):
                result.indicator_id = indicator.id
        
        # Log audit trail
        audit = AuditLog(
            entity_id=upload_id,
            entity_type="uploads",
            action=AuditAction.UPDATED,
            actor=self.actor,
            timestamp=datetime.now(timezone.utc),
            changes={
                "matched_headers": len(results),
                "requires_review": sum(1 for r in results if r.requires_review)
            }
        )
        self.db.add(audit)
        self.db.commit()

        # Record provenance
        prov = get_provenance_tracker()
        start = datetime.now(timezone.utc)
        activity_id = f"matching_{upload_id}"
        prov.record_activity(
            activity_id, "header_matching", start, datetime.now(timezone.utc), self.actor,
        )
        for result in results:
            if result.indicator_id:
                entity_id = str(result.indicator_id)
                prov.record_entity(entity_id, "matched_indicator", {
                    "original_header": result.original_header,
                    "matched_indicator": result.matched_indicator,
                    "confidence": result.confidence,
                })
                prov.record_derivation(str(upload_id), entity_id, activity_id)
        prov.flush()

        logger.info(
            f"Matching complete: {len(results)}/{len(headers)} matched, "
            f"{sum(1 for r in results if r.requires_review)} require review"
        )
        
        return results

    def get_best_match(self, header: str) -> Optional[MatchingResult]:
        """Get best match using rule-based then LLM fallback"""
        logger.debug(f"Finding best match for: {header}")
        
        # Try rule-based matching first
        rule_result = self.rule_matcher.match(header)
        
        if rule_result and rule_result.confidence >= settings.matching.confidence_threshold:
            logger.debug(f"Rule-based match accepted: {rule_result.confidence:.2f}")
            return MatchingResult(
                original_header=header,
                matched_indicator=rule_result.canonical_name,
                confidence=rule_result.confidence,
                method=rule_result.method,
                requires_review=rule_result.confidence < settings.matching.review_threshold,
                unit=rule_result.unit,
                category=rule_result.category
            )
        
        # Fallback to LLM matching
        logger.debug("Rule-based match insufficient, trying LLM...")
        llm_result = self.llm_matcher.match(header)
        
        if llm_result and llm_result.confidence >= settings.matching.llm_threshold:
            logger.debug(f"LLM match accepted: {llm_result.confidence:.2f}")
            
            # Efficient lookup for unit/category
            metadata = self._indicator_lookup.get(llm_result.canonical_name, {})
            
            return MatchingResult(
                original_header=header,
                matched_indicator=llm_result.canonical_name,
                confidence=llm_result.confidence,
                method=llm_result.method,
                requires_review=llm_result.confidence < settings.matching.review_threshold,
                unit=metadata.get('unit'),
                category=metadata.get('category'),
                reasoning=llm_result.reasoning
            )
        
        # No good match found
        logger.debug(f"No sufficient match found for: {header}")
        return None

    def _create_match_record(
        self,
        upload_id: UUID,
        header: str,
        result: MatchingResult
    ) -> MatchedIndicator:
        """Create match record (without committing)"""
        # Use centralized method mapping
        method = METHOD_MAP.get(result.method)
        
        if method is None:
            logger.warning(f"Unknown matching method '{result.method}', using MANUAL")
            method = MatchingMethod.MANUAL
        
        # Create matched indicator record
        return MatchedIndicator(
            upload_id=upload_id,
            original_header=header,
            matched_indicator=result.matched_indicator,
            confidence_score=result.confidence,
            matching_method=method,
            reviewed=not result.requires_review,
            reviewer_notes=result.reasoning if result.reasoning else None
        )
    
    def save_match(
        self,
        upload_id: UUID,
        header: str,
        result: MatchingResult
    ) -> UUID:
        """Save match result to database (legacy method for compatibility)"""
        logger.debug(f"Saving match: {header} → {result.matched_indicator}")
        
        matched_indicator = self._create_match_record(upload_id, header, result)
        
        self.db.add(matched_indicator)
        self.db.commit()
        self.db.refresh(matched_indicator)
        
        logger.debug(f"Saved match with ID: {matched_indicator.id}")
        
        return matched_indicator.id

    def get_review_queue(self, upload_id: UUID) -> List[MatchingResult]:
        """Get headers requiring manual review"""
        logger.info(f"Fetching review queue for upload {upload_id}")
        
        # Query unreviewed matches using SQLAlchemy boolean comparison
        matches = (
            self.db.query(MatchedIndicator)
            .filter(
                MatchedIndicator.upload_id == upload_id,
                MatchedIndicator.reviewed.is_(False)
            )
            .order_by(MatchedIndicator.confidence_score.asc())
            .all()
        )
        
        results = []
        for match in matches:
            result = MatchingResult(
                original_header=match.original_header,
                matched_indicator=match.matched_indicator,
                confidence=match.confidence_score,
                method=match.matching_method.value,
                requires_review=True,
                indicator_id=match.id,
                reasoning=match.reviewer_notes
            )
            results.append(result)
        
        logger.info(f"Found {len(results)} matches requiring review")
        
        return results

    def approve_match(
        self,
        indicator_id: UUID,
        approved: bool,
        corrected_match: Optional[str] = None,
        notes: Optional[str] = None
    ) -> None:
        """Approve or correct a match"""
        logger.info(f"Reviewing match {indicator_id}: approved={approved}")
        
        # Get match record
        match = self.db.query(MatchedIndicator).filter(
            MatchedIndicator.id == indicator_id
        ).first()
        
        if not match:
            raise ValueError(f"Match {indicator_id} not found")
        
        # Validate: if not approved, must provide correction
        if not approved and not corrected_match:
            raise ValueError("Must provide corrected_match when rejecting a match")
        
        # Store original for audit
        original_match = match.matched_indicator
        
        # Update match
        match.reviewed = True
        match.reviewer_notes = notes
        
        if not approved and corrected_match:
            match.matched_indicator = corrected_match
            match.matching_method = MatchingMethod.MANUAL
            match.confidence_score = 1.0  # Manual corrections are 100% confident
            logger.info(f"Corrected match: {original_match} → {corrected_match}")
        
        # Log audit trail
        audit = AuditLog(
            entity_id=indicator_id,
            entity_type="matched_indicators",
            action=AuditAction.REVIEWED,
            actor=self.actor,
            timestamp=datetime.now(timezone.utc),
            changes={
                "approved": approved,
                "original_match": original_match,
                "corrected_match": corrected_match if corrected_match else original_match,
                "notes": notes
            }
        )
        self.db.add(audit)
        
        self.db.commit()
        
        logger.info(f"Match {indicator_id} reviewed successfully")

    def get_matching_stats(self, upload_id: UUID) -> dict:
        """Get matching statistics for an upload using database aggregation"""
        # Use database aggregation for efficiency with proper boolean comparison
        total = self.db.query(func.count(MatchedIndicator.id)).filter(
            MatchedIndicator.upload_id == upload_id
        ).scalar()
        
        if not total:
            return {
                "total": 0,
                "reviewed": 0,
                "requires_review": 0,
                "avg_confidence": 0.0,
                "by_method": {}
            }
        
        reviewed = self.db.query(func.count(MatchedIndicator.id)).filter(
            MatchedIndicator.upload_id == upload_id,
            MatchedIndicator.reviewed.is_(True)
        ).scalar()
        
        avg_confidence = self.db.query(func.avg(MatchedIndicator.confidence_score)).filter(
            MatchedIndicator.upload_id == upload_id
        ).scalar()
        
        # Get method counts
        method_counts = self.db.query(
            MatchedIndicator.matching_method,
            func.count(MatchedIndicator.id)
        ).filter(
            MatchedIndicator.upload_id == upload_id
        ).group_by(MatchedIndicator.matching_method).all()
        
        by_method = {method.value: count for method, count in method_counts}
        
        return {
            "total": total,
            "reviewed": reviewed,
            "requires_review": total - reviewed,
            "avg_confidence": float(avg_confidence) if avg_confidence else 0.0,
            "by_method": by_method
        }

    def get_comprehensive_results(self, upload_id: UUID) -> Optional[Dict]:
        """Get all matching data for an upload in one call: stats + results + review queue"""
        matches = (
            self.db.query(MatchedIndicator)
            .filter(MatchedIndicator.upload_id == upload_id)
            .order_by(MatchedIndicator.confidence_score.desc())
            .all()
        )

        if matches is None:
            return None

        total = len(matches)
        needs_review = sum(
            1 for m in matches
            if m.confidence_score < settings.matching.review_threshold
        )
        auto_approved = total - needs_review
        avg_confidence = (
            sum(m.confidence_score for m in matches) / total if total else 0.0
        )

        has_unreviewed = any(not m.reviewed for m in matches)
        status = "completed" if not has_unreviewed else "needs_review"

        results = []
        review_queue = []
        for m in matches:
            item = {
                "indicator_id": m.id,
                "original_header": m.original_header,
                "matched_indicator": m.matched_indicator,
                "confidence": round(m.confidence_score, 3),
                "requires_review": m.confidence_score < settings.matching.review_threshold,
            }
            results.append(item)

            if m.confidence_score < settings.matching.review_threshold:
                review_item = {**item, "reasoning": m.reviewer_notes}
                review_queue.append(review_item)

        return {
            "upload_id": upload_id,
            "status": status,
            "stats": {
                "total_headers": total,
                "auto_approved": auto_approved,
                "needs_review": needs_review,
                "avg_confidence": round(avg_confidence, 3),
            },
            "results": results,
            "review_queue": review_queue,
        }

    def rematch_header(self, indicator_id: UUID) -> Optional[MatchingResult]:
        """Rematch a single header (useful after corrections)"""
        match = self.db.query(MatchedIndicator).filter(
            MatchedIndicator.id == indicator_id
        ).first()
        
        if not match:
            raise ValueError(f"Match {indicator_id} not found")
        
        # Get new match
        result = self.get_best_match(match.original_header)
        
        if result:
            # Use centralized method mapping
            method = METHOD_MAP.get(result.method, MatchingMethod.MANUAL)
            
            # Update existing record
            match.matched_indicator = result.matched_indicator
            match.confidence_score = result.confidence
            match.matching_method = method
            match.reviewed = not result.requires_review
            
            # Log audit trail
            audit = AuditLog(
                entity_id=indicator_id,
                entity_type="matched_indicators",
                action=AuditAction.UPDATED,
                actor=self.actor,
                timestamp=datetime.now(timezone.utc),
                changes={
                    "operation": "rematch",
                    "new_match": result.matched_indicator,
                    "confidence": result.confidence,
                    "method": result.method
                }
            )
            self.db.add(audit)
            
            self.db.commit()
            
            result.indicator_id = indicator_id
        
        return result
