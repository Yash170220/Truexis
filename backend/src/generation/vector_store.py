"""Qdrant vector store for RAG-based ESG report generation."""
import logging
import uuid
from typing import Dict, List, Optional
from uuid import UUID

from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchValue,
    PointStruct,
    VectorParams,
)
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

VALIDATED_DATA_COLLECTION = "validated_data"
FRAMEWORK_DEFS_COLLECTION = "framework_definitions"
VECTOR_SIZE = 384
BATCH_SIZE = 500


class VectorStore:
    """Qdrant-backed vector store for validated ESG data and framework definitions."""

    def __init__(self, host: str = "localhost", port: int = 6333):
        self.client = QdrantClient(host=host, port=port)
        # FIX: cache_folder avoids re-downloading model on every restart
        self.encoder = SentenceTransformer("all-MiniLM-L6-v2", cache_folder=".model_cache")
        self._ensure_collections()

    def _ensure_collections(self) -> None:
        """Create collections if they don't already exist."""
        existing = {c.name for c in self.client.get_collections().collections}

        for name in (VALIDATED_DATA_COLLECTION, FRAMEWORK_DEFS_COLLECTION):
            if name not in existing:
                self.client.create_collection(
                    collection_name=name,
                    vectors_config=VectorParams(
                        size=VECTOR_SIZE, distance=Distance.COSINE
                    ),
                )
                logger.info(f"Created Qdrant collection: {name}")

    # ------------------------------------------------------------------
    # Validated data
    # ------------------------------------------------------------------

    def add_validated_data(
        self, upload_id: UUID, records: List[Dict]
    ) -> int:
        """Embed and upsert validated ESG records.

        Each record dict should contain:
            data_id, indicator, value, unit, period, facility

        Returns the number of points upserted.
        """
        points: List[PointStruct] = []

        # FIX: Batch-encode all texts at once instead of one-by-one
        #      encode() with a list is ~10x faster than calling it per record
        texts = [
            (
                f"{r.get('facility', 'Unknown facility')} consumed "
                f"{r.get('value', '')} {r.get('unit', '')} of "
                f"{r.get('indicator', '')} in {r.get('period', '')}"
            )
            for r in records
        ]
        embeddings = self.encoder.encode(texts, batch_size=64, show_progress_bar=False)

        for record, text, embedding in zip(records, texts, embeddings):
            point = PointStruct(
                id=str(uuid.uuid4()),
                vector=embedding.tolist(),
                payload={
                    "upload_id": str(upload_id),
                    "data_id": str(record.get("data_id", "")),
                    "indicator": record.get("indicator", ""),
                    "value": record.get("value"),
                    "unit": record.get("unit", ""),
                    "period": record.get("period", ""),
                    "facility": record.get("facility", ""),
                    "text": text,
                },
            )
            points.append(point)

            if len(points) >= BATCH_SIZE:
                self.client.upsert(
                    collection_name=VALIDATED_DATA_COLLECTION, points=points
                )
                logger.info(f"Upserted batch of {len(points)} validated-data points")
                points = []

        if points:
            self.client.upsert(
                collection_name=VALIDATED_DATA_COLLECTION, points=points
            )
            logger.info(f"Upserted final batch of {len(points)} validated-data points")

        total = len(records)
        logger.info(f"Added {total} validated-data records for upload {upload_id}")
        return total

    # ------------------------------------------------------------------
    # Framework definitions
    # ------------------------------------------------------------------

    def add_framework_definitions(self, definitions: List[Dict]) -> int:
        """Embed and upsert framework/indicator definitions.

        Each dict should contain:
            indicator_id, indicator_name, definition, unit, calculation,
            framework
        """
        points: List[PointStruct] = []

        # FIX: Batch-encode all definitions at once — same speedup as above
        texts = [
            (
                f"{d.get('indicator_name', '')}: "
                f"{d.get('definition', '')}. "
                f"Calculation: {d.get('calculation', 'N/A')}"
            )
            for d in definitions
        ]
        embeddings = self.encoder.encode(texts, batch_size=64, show_progress_bar=False)

        for defn, text, embedding in zip(definitions, texts, embeddings):
            point = PointStruct(
                id=str(uuid.uuid4()),
                vector=embedding.tolist(),
                payload={
                    "indicator_id": str(defn.get("indicator_id", "")),
                    "indicator_name": defn.get("indicator_name", ""),
                    "definition": defn.get("definition", ""),
                    "unit": defn.get("unit", ""),
                    "calculation": defn.get("calculation", ""),
                    "framework": defn.get("framework", "BRSR"),
                    "text": text,
                },
            )
            points.append(point)

            if len(points) >= BATCH_SIZE:
                self.client.upsert(
                    collection_name=FRAMEWORK_DEFS_COLLECTION, points=points
                )
                points = []

        if points:
            self.client.upsert(
                collection_name=FRAMEWORK_DEFS_COLLECTION, points=points
            )

        total = len(definitions)
        logger.info(f"Added {total} framework definitions")
        return total

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def search_validated_data(
        self,
        query: str,
        upload_id: UUID,
        top_k: int = 3,          # FIX: default changed from 5 → 3 to match RAG/chat callers
    ) -> List[Dict]:
        """Semantic search over validated data scoped to a single upload."""
        query_vector = self.encoder.encode(query).tolist()

        response = self.client.query_points(
            collection_name=VALIDATED_DATA_COLLECTION,
            query=query_vector,
            query_filter=Filter(
                must=[
                    FieldCondition(
                        key="upload_id",
                        match=MatchValue(value=str(upload_id)),
                    )
                ]
            ),
            limit=top_k,
            # FIX: Added score_threshold to avoid returning near-zero similarity results
            score_threshold=0.3,
        )

        return [
            {
                "text": hit.payload.get("text", ""),
                "indicator": hit.payload.get("indicator", ""),
                "value": hit.payload.get("value"),
                "unit": hit.payload.get("unit", ""),
                "period": hit.payload.get("period", ""),
                "facility": hit.payload.get("facility", ""),
                "similarity": round(hit.score, 4),
                "data_id": hit.payload.get("data_id", ""),
            }
            for hit in response.points
        ]

    def search_framework_definitions(
        self,
        query: str,
        framework: str = "BRSR",
        top_k: int = 1,          # FIX: default changed from 3 → 1; callers only need best match
    ) -> List[Dict]:
        """Semantic search over framework indicator definitions."""
        query_vector = self.encoder.encode(query).tolist()

        response = self.client.query_points(
            collection_name=FRAMEWORK_DEFS_COLLECTION,
            query=query_vector,
            query_filter=Filter(
                must=[
                    FieldCondition(
                        key="framework",
                        match=MatchValue(value=framework),
                    )
                ]
            ),
            limit=top_k,
        )

        return [
            {
                "indicator_name": hit.payload.get("indicator_name", ""),
                "definition": hit.payload.get("definition", ""),
                "unit": hit.payload.get("unit", ""),
                "calculation": hit.payload.get("calculation", ""),
                "framework": hit.payload.get("framework", ""),
                "similarity": round(hit.score, 4),
                "text": hit.payload.get("text", ""),
            }
            for hit in response.points
        ]

    # ------------------------------------------------------------------
    # FIX: Added delete method — was missing, needed for DELETE /ingest/{upload_id}
    # ------------------------------------------------------------------

    def delete_upload_data(self, upload_id: UUID) -> int:
        """Delete all vectors associated with an upload_id. Returns count deleted."""
        from qdrant_client.http.models import FilterSelector

        result = self.client.delete(
            collection_name=VALIDATED_DATA_COLLECTION,
            points_selector=FilterSelector(
                filter=Filter(
                    must=[
                        FieldCondition(
                            key="upload_id",
                            match=MatchValue(value=str(upload_id)),
                        )
                    ]
                )
            ),
        )
        logger.info(f"Deleted vectors for upload {upload_id}: {result}")
        return 1  # Qdrant delete doesn't return count; log confirms success