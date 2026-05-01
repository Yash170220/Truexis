"""Qdrant vector store for RAG-based ESG report generation."""
import logging
import os
import uuid
from typing import Dict, List, Optional
from uuid import UUID

import voyageai
from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchValue,
    PointStruct,
    VectorParams,
)

logger = logging.getLogger(__name__)

VALIDATED_DATA_COLLECTION = "validated_data"
FRAMEWORK_DEFS_COLLECTION = "framework_definitions"
VECTOR_SIZE = 1024
VOYAGE_MODEL = "voyage-3"
BATCH_SIZE = 128


def _embed_texts(texts: List[str]) -> List[List[float]]:
    """Embed a list of texts using Voyage AI voyage-3 model."""
    api_key = os.getenv("VOYAGE_API_KEY") or os.getenv("ANTHROPIC_API_KEY")
    vo = voyageai.Client(api_key=api_key)
    all_embeddings: List[List[float]] = []
    for i in range(0, len(texts), BATCH_SIZE):
        chunk = texts[i : i + BATCH_SIZE]
        result = vo.embed(chunk, model=VOYAGE_MODEL)
        all_embeddings.extend(result.embeddings)
    return all_embeddings


class VectorStore:
    """Qdrant-backed vector store for validated ESG data and framework definitions."""

    def __init__(self, host: str = "localhost", port: int = 6333) -> None:
        qdrant_url = os.getenv("QDRANT_URL")
        qdrant_api_key = os.getenv("QDRANT_API_KEY")

        if qdrant_url:
            self.client = QdrantClient(url=qdrant_url, api_key=qdrant_api_key)
            logger.info("Connected to Qdrant Cloud: %s", qdrant_url)
        else:
            self.client = QdrantClient(host=host, port=port)
            logger.info("Connected to local Qdrant: %s:%d", host, port)

        self._ensure_collections()

    def _ensure_collections(self) -> None:
        """Create collections if they do not already exist.
        If a collection exists with the wrong vector size it is recreated."""
        existing = {c.name: c for c in self.client.get_collections().collections}

        for name in (VALIDATED_DATA_COLLECTION, FRAMEWORK_DEFS_COLLECTION):
            if name in existing:
                info = self.client.get_collection(name)
                stored_size = info.config.params.vectors.size
                if stored_size != VECTOR_SIZE:
                    logger.warning(
                        "Collection '%s' has vector size %d; expected %d. Recreating.",
                        name, stored_size, VECTOR_SIZE,
                    )
                    self.client.delete_collection(name)
                else:
                    continue

            self.client.create_collection(
                collection_name=name,
                vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
            )
            logger.info("Created Qdrant collection: %s (size=%d)", name, VECTOR_SIZE)

    def add_validated_data(self, upload_id: UUID, records: List[Dict]) -> int:
        """Embed and upsert validated ESG records."""
        if not records:
            return 0

        texts = [
            (
                f"{r.get('facility', 'Unknown facility')} consumed "
                f"{r.get('value', '')} {r.get('unit', '')} of "
                f"{r.get('indicator', '')} in {r.get('period', '')}"
            )
            for r in records
        ]

        try:
            embeddings = _embed_texts(texts)
        except Exception as e:
            logger.error("Failed to embed validated data: %s", e)
            raise

        points: List[PointStruct] = []
        for record, text, embedding in zip(records, texts, embeddings):
            points.append(
                PointStruct(
                    id=str(uuid.uuid4()),
                    vector=embedding,
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
            )

            if len(points) >= 500:
                self.client.upsert(collection_name=VALIDATED_DATA_COLLECTION, points=points)
                logger.info("Upserted batch of %d validated-data points", len(points))
                points = []

        if points:
            self.client.upsert(collection_name=VALIDATED_DATA_COLLECTION, points=points)
            logger.info("Upserted final batch of %d validated-data points", len(points))

        total = len(records)
        logger.info("Added %d validated-data records for upload %s", total, upload_id)
        return total

    def add_framework_definitions(self, definitions: List[Dict]) -> int:
        """Embed and upsert framework indicator definitions."""
        if not definitions:
            return 0

        texts = [
            (
                f"{d.get('indicator_name', '')}: "
                f"{d.get('definition', '')}. "
                f"Calculation: {d.get('calculation', 'N/A')}"
            )
            for d in definitions
        ]

        try:
            embeddings = _embed_texts(texts)
        except Exception as e:
            logger.error("Failed to embed framework definitions: %s", e)
            raise

        points: List[PointStruct] = []
        for defn, text, embedding in zip(definitions, texts, embeddings):
            points.append(
                PointStruct(
                    id=str(uuid.uuid4()),
                    vector=embedding,
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
            )

            if len(points) >= 500:
                self.client.upsert(collection_name=FRAMEWORK_DEFS_COLLECTION, points=points)
                points = []

        if points:
            self.client.upsert(collection_name=FRAMEWORK_DEFS_COLLECTION, points=points)

        total = len(definitions)
        logger.info("Added %d framework definitions", total)
        return total

    def search_validated_data(
        self,
        query: str,
        upload_id: UUID,
        top_k: int = 3,
    ) -> List[Dict]:
        """Semantic search over validated data scoped to a single upload."""
        try:
            query_vector = _embed_texts([query])[0]
        except Exception as e:
            logger.error("Failed to embed search query: %s", e)
            return []

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
        top_k: int = 1,
    ) -> List[Dict]:
        """Semantic search over framework indicator definitions."""
        try:
            query_vector = _embed_texts([query])[0]
        except Exception as e:
            logger.error("Failed to embed framework search query: %s", e)
            return []

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

    def delete_upload_data(self, upload_id: UUID) -> int:
        """Delete all vectors associated with an upload_id."""
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
        logger.info("Deleted vectors for upload %s: %s", upload_id, result)
        return 1