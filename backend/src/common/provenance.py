"""W3C PROV-O provenance tracker using RDF triples.

Keeps overhead <10ms per operation by buffering triples in-memory
and flushing to disk every FLUSH_INTERVAL records.
"""
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import UUID

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, XSD

logger = logging.getLogger(__name__)

PROV = Namespace("http://www.w3.org/ns/prov#")
EX = Namespace("http://example.org/esg/")
FLUSH_INTERVAL = 100


class ProvenanceTracker:
    """Thread-safe W3C PROV-O provenance tracker backed by an RDF graph."""

    def __init__(self, storage_path: str = "data/provenance.ttl"):
        self.storage_path = Path(storage_path)
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)

        self._lock = threading.Lock()
        self.graph = Graph()
        self.graph.bind("prov", PROV)
        self.graph.bind("ex", EX)

        if self.storage_path.exists():
            try:
                self.graph.parse(str(self.storage_path), format="turtle")
                logger.info(
                    f"Loaded {len(self.graph)} existing provenance triples"
                )
            except Exception as exc:
                logger.warning(f"Could not load provenance file, starting fresh: {exc}")

        self._ops_since_flush = 0

    # ------------------------------------------------------------------
    # Core recording methods
    # ------------------------------------------------------------------

    def record_activity(
        self,
        activity_id: str,
        activity_type: str,
        start_time: datetime,
        end_time: datetime,
        agent: str,
    ) -> None:
        activity = EX[activity_id]
        agent_uri = EX[f"agent/{agent}"]

        with self._lock:
            self.graph.add((activity, RDF.type, PROV.Activity))
            self.graph.add((activity, EX.activityType, Literal(activity_type)))
            self.graph.add((
                activity, PROV.startedAtTime,
                Literal(start_time.isoformat(), datatype=XSD.dateTime),
            ))
            self.graph.add((
                activity, PROV.endedAtTime,
                Literal(end_time.isoformat(), datatype=XSD.dateTime),
            ))
            self.graph.add((activity, PROV.wasAssociatedWith, agent_uri))
            self.graph.add((agent_uri, RDF.type, PROV.Agent))
            self._maybe_flush()

    def record_entity(
        self,
        entity_id: str,
        entity_type: str,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> None:
        entity = EX[str(entity_id)]

        with self._lock:
            self.graph.add((entity, RDF.type, PROV.Entity))
            self.graph.add((entity, EX.entityType, Literal(entity_type)))
            for key, value in (attributes or {}).items():
                self.graph.add((entity, EX[key], Literal(str(value))))
            self._maybe_flush()

    def record_derivation(
        self,
        source_id: str,
        derived_id: str,
        activity_id: str,
    ) -> None:
        source = EX[str(source_id)]
        derived = EX[str(derived_id)]
        activity = EX[activity_id]

        with self._lock:
            self.graph.add((derived, PROV.wasDerivedFrom, source))
            self.graph.add((derived, PROV.wasGeneratedBy, activity))
            self._maybe_flush()

    # ------------------------------------------------------------------
    # Querying
    # ------------------------------------------------------------------

    def query_lineage(self, entity_id: str) -> List[Dict[str, Any]]:
        """Return the full derivation chain for an entity via SPARQL.

        Each item contains entity_id, entity_type, and activity details.
        """
        entity_uri = str(EX[str(entity_id)])

        sparql = f"""
        PREFIX prov: <{PROV}>
        PREFIX ex:   <{EX}>
        SELECT ?source ?sourceType ?activity ?activityType ?agent ?startTime
        WHERE {{
            <{entity_uri}> prov:wasDerivedFrom+ ?source .
            OPTIONAL {{ ?source ex:entityType ?sourceType . }}
            OPTIONAL {{
                ?source prov:wasGeneratedBy ?activity .
                OPTIONAL {{ ?activity ex:activityType ?activityType . }}
                OPTIONAL {{ ?activity prov:wasAssociatedWith ?agent . }}
                OPTIONAL {{ ?activity prov:startedAtTime ?startTime . }}
            }}
        }}
        ORDER BY ?startTime
        """

        results = []
        with self._lock:
            for row in self.graph.query(sparql):
                source_str = str(row.source) if row.source else ""
                source_short = source_str.replace(str(EX), "") if source_str else ""
                agent_str = str(row.agent).replace(str(EX), "") if row.agent else ""

                results.append({
                    "entity_id": source_short,
                    "entity_type": str(row.sourceType) if row.sourceType else "",
                    "activity": {
                        "type": str(row.activityType) if row.activityType else "",
                        "timestamp": str(row.startTime) if row.startTime else "",
                        "agent": agent_str,
                    },
                })
        return results

    def get_entity_type(self, entity_id: str) -> Optional[str]:
        """Look up the ex:entityType for an entity, or None."""
        entity_uri = EX[str(entity_id)]
        with self._lock:
            for _, _, obj in self.graph.triples((entity_uri, EX.entityType, None)):
                return str(obj)
        return None

    def entity_exists(self, entity_id: str) -> bool:
        """Check whether any triples reference this entity."""
        entity_uri = EX[str(entity_id)]
        with self._lock:
            for _ in self.graph.triples((entity_uri, None, None)):
                return True
            for _ in self.graph.triples((None, None, entity_uri)):
                return True
        return False

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def export_provenance(
        self,
        entity_id: Optional[str] = None,
        fmt: str = "turtle",
    ) -> str:
        """Serialize the full graph (or a subgraph for one entity)."""
        with self._lock:
            if entity_id is None:
                return self.graph.serialize(format=fmt)

            entity_uri = EX[str(entity_id)]
            sub = Graph()
            sub.bind("prov", PROV)
            sub.bind("ex", EX)

            for s, p, o in self.graph.triples((entity_uri, None, None)):
                sub.add((s, p, o))
            for s, p, o in self.graph.triples((None, None, entity_uri)):
                sub.add((s, p, o))

            return sub.serialize(format=fmt)

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def flush(self) -> None:
        """Persist the graph to disk immediately."""
        with self._lock:
            self._flush_unlocked()

    def _maybe_flush(self) -> None:
        """Flush every FLUSH_INTERVAL operations (caller must hold lock)."""
        self._ops_since_flush += 1
        if self._ops_since_flush >= FLUSH_INTERVAL:
            self._flush_unlocked()

    def _flush_unlocked(self) -> None:
        try:
            self.graph.serialize(str(self.storage_path), format="turtle")
            self._ops_since_flush = 0
        except Exception as exc:
            logger.error(f"Failed to flush provenance graph: {exc}")


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
_tracker: Optional[ProvenanceTracker] = None


def get_provenance_tracker() -> ProvenanceTracker:
    """Return (and lazily create) the global ProvenanceTracker."""
    global _tracker
    if _tracker is None:
        _tracker = ProvenanceTracker()
    return _tracker
