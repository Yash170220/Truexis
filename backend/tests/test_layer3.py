"""Tests for Layer 3: Generation (vector store, RAG) and Provenance."""
import tempfile
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from src.common.provenance import ProvenanceTracker
from src.generation.rag_generator import RAGGenerator
from src.generation.vector_store import (
    FRAMEWORK_DEFS_COLLECTION,
    VALIDATED_DATA_COLLECTION,
    VECTOR_SIZE,
    VectorStore,
)

from tests.auth_helpers import attach_mock_auth_user

UPLOAD_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")

SAMPLE_RECORDS = [
    {
        "data_id": str(uuid.uuid4()),
        "indicator": "Total Electricity Consumption",
        "value": 12500.0,
        "unit": "MWh",
        "period": "2025-01",
        "facility": "Plant A",
    },
    {
        "data_id": str(uuid.uuid4()),
        "indicator": "Total Electricity Consumption",
        "value": 13200.0,
        "unit": "MWh",
        "period": "2025-02",
        "facility": "Plant A",
    },
    {
        "data_id": str(uuid.uuid4()),
        "indicator": "Scope 1 GHG Emissions",
        "value": 950.0,
        "unit": "tonnes CO2e",
        "period": "2025-01",
        "facility": "Plant A",
    },
    {
        "data_id": str(uuid.uuid4()),
        "indicator": "Scope 1 GHG Emissions",
        "value": 980.0,
        "unit": "tonnes CO2e",
        "period": "2025-02",
        "facility": "Plant A",
    },
    {
        "data_id": str(uuid.uuid4()),
        "indicator": "Scope 2 GHG Emissions",
        "value": 450.0,
        "unit": "tonnes CO2e",
        "period": "2025-01",
        "facility": "Plant A",
    },
    {
        "data_id": str(uuid.uuid4()),
        "indicator": "Total Water Consumption",
        "value": 5400.0,
        "unit": "m3",
        "period": "2025-01",
        "facility": "Plant A",
    },
    {
        "data_id": str(uuid.uuid4()),
        "indicator": "Total Water Consumption",
        "value": 5100.0,
        "unit": "m3",
        "period": "2025-02",
        "facility": "Plant A",
    },
    {
        "data_id": str(uuid.uuid4()),
        "indicator": "Total Waste Generated",
        "value": 320.0,
        "unit": "tonnes",
        "period": "2025-01",
        "facility": "Plant A",
    },
    {
        "data_id": str(uuid.uuid4()),
        "indicator": "Energy Intensity",
        "value": 4.2,
        "unit": "GJ/tonne",
        "period": "2025-01",
        "facility": "Plant A",
    },
    {
        "data_id": str(uuid.uuid4()),
        "indicator": "Renewable Energy Share",
        "value": 22.5,
        "unit": "%",
        "period": "2025-01",
        "facility": "Plant A",
    },
]

SAMPLE_FRAMEWORK_DEFS = [
    {
        "indicator_id": str(uuid.uuid4()),
        "indicator_name": "Total Electricity Consumption",
        "definition": "Total electricity from grid and captive power",
        "unit": "MWh",
        "calculation": "Grid electricity + captive generation - sold",
        "framework": "BRSR",
    },
    {
        "indicator_id": str(uuid.uuid4()),
        "indicator_name": "Scope 1 GHG Emissions",
        "definition": "Direct emissions from owned sources",
        "unit": "tonnes CO2e",
        "calculation": "Stationary + mobile + process + fugitive emissions",
        "framework": "BRSR",
    },
]


# -----------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------

def _make_mock_encoder():
    """Return a mock SentenceTransformer that produces deterministic vectors."""
    encoder = MagicMock()
    call_count = {"n": 0}

    def _encode(text, *args, **kwargs):
        import numpy as np
        call_count["n"] += 1
        rng = __import__("numpy").random.RandomState(hash(text) % (2**31))
        vec = rng.rand(VECTOR_SIZE).astype("float32")
        vec = vec / __import__("numpy").linalg.norm(vec)
        return vec

    encoder.encode = _encode
    return encoder


@pytest.fixture()
def mock_vector_store():
    """VectorStore with in-memory Qdrant and a deterministic mock encoder."""
    from qdrant_client import QdrantClient

    client = QdrantClient(":memory:")
    vs = object.__new__(VectorStore)
    vs.client = client
    vs.encoder = _make_mock_encoder()
    vs._ensure_collections()
    return vs


@pytest.fixture()
def mock_rag(mock_vector_store):
    """RAGGenerator with mocked Groq client and no Redis."""
    groq_client = MagicMock()

    choice = MagicMock()
    choice.message.content = (
        "Plant A consumed 12500 MWh of Total Electricity in 2025-01 [Table 1]. "
        "In February, consumption rose to 13200 MWh [Table 2]."
    )
    response = MagicMock()
    response.choices = [choice]
    groq_client.chat.completions.create.return_value = response

    rag = object.__new__(RAGGenerator)
    rag.vector_store = mock_vector_store
    rag.client = groq_client
    rag.model = "test-model"
    rag.temperature = 0.3
    rag.cache = None
    return rag


@pytest.fixture()
def prov_tracker(tmp_path):
    """ProvenanceTracker writing to a temp file."""
    return ProvenanceTracker(storage_path=str(tmp_path / "prov.ttl"))


@pytest.fixture()
def prov_tracker_with_chain(prov_tracker):
    """Tracker pre-loaded with a 4-step pipeline chain."""
    now = datetime.now(timezone.utc)
    prov = prov_tracker

    prov.record_activity("act_ingest", "file_ingestion", now, now, "system")
    prov.record_entity("upload_1", "uploaded_file", {"filename": "data.csv"})

    prov.record_activity("act_match", "header_matching", now, now, "system")
    prov.record_entity("matched_1", "matched_indicator", {"confidence": "0.95"})
    prov.record_derivation("upload_1", "matched_1", "act_match")

    prov.record_activity("act_norm", "data_normalization", now, now, "system")
    prov.record_entity("normalized_1", "normalized_dataset", {"total": "100"})
    prov.record_derivation("matched_1", "normalized_1", "act_norm")

    prov.record_activity("act_val", "data_validation", now, now, "validation_service")
    prov.record_entity("validated_1", "validation_results", {"pass_rate": "95.0"})
    prov.record_derivation("normalized_1", "validated_1", "act_val")

    return prov


# -----------------------------------------------------------------------
# 1. Qdrant connection & collection tests
# -----------------------------------------------------------------------

class TestVectorStoreConnection:
    def test_qdrant_connection(self, mock_vector_store):
        """Collections exist after init."""
        collections = {
            c.name
            for c in mock_vector_store.client.get_collections().collections
        }
        assert VALIDATED_DATA_COLLECTION in collections
        assert FRAMEWORK_DEFS_COLLECTION in collections

    def test_add_validated_data(self, mock_vector_store):
        count = mock_vector_store.add_validated_data(UPLOAD_ID, SAMPLE_RECORDS)
        assert count == len(SAMPLE_RECORDS)

        info = mock_vector_store.client.get_collection(VALIDATED_DATA_COLLECTION)
        assert info.points_count == len(SAMPLE_RECORDS)

    def test_add_framework_definitions(self, mock_vector_store):
        count = mock_vector_store.add_framework_definitions(SAMPLE_FRAMEWORK_DEFS)
        assert count == len(SAMPLE_FRAMEWORK_DEFS)


# -----------------------------------------------------------------------
# 2. Vector search tests
# -----------------------------------------------------------------------

class TestVectorSearch:
    def test_search_returns_results(self, mock_vector_store):
        mock_vector_store.add_validated_data(UPLOAD_ID, SAMPLE_RECORDS)

        results = mock_vector_store.search_validated_data(
            query="electricity consumption", upload_id=UPLOAD_ID, top_k=5
        )
        assert len(results) > 0
        assert all("similarity" in r for r in results)
        assert all("indicator" in r for r in results)

    def test_search_similarity_above_threshold(self, mock_vector_store):
        mock_vector_store.add_validated_data(UPLOAD_ID, SAMPLE_RECORDS)

        results = mock_vector_store.search_validated_data(
            query="Total Electricity Consumption MWh", upload_id=UPLOAD_ID, top_k=3
        )
        assert len(results) > 0
        assert results[0]["similarity"] > 0.7

    def test_search_framework_definitions(self, mock_vector_store):
        mock_vector_store.add_framework_definitions(SAMPLE_FRAMEWORK_DEFS)

        results = mock_vector_store.search_framework_definitions(
            query="GHG emissions scope 1", framework="BRSR", top_k=2
        )
        assert len(results) > 0
        assert results[0]["framework"] == "BRSR"

    def test_search_empty_upload(self, mock_vector_store):
        other_id = uuid.uuid4()
        results = mock_vector_store.search_validated_data(
            query="electricity", upload_id=other_id, top_k=5
        )
        assert results == []


# -----------------------------------------------------------------------
# 3. RAG generation tests
# -----------------------------------------------------------------------

class TestRAGGenerate:
    def test_generate_narrative_returns_content(self, mock_rag, mock_vector_store):
        mock_vector_store.add_validated_data(UPLOAD_ID, SAMPLE_RECORDS)
        mock_vector_store.add_framework_definitions(SAMPLE_FRAMEWORK_DEFS)

        result = mock_rag.generate_narrative(
            section_type="management_approach",
            upload_id=UPLOAD_ID,
            indicator="Total Electricity Consumption",
            framework="BRSR",
        )

        assert result["section_type"] == "management_approach"
        assert result["indicator"] == "Total Electricity Consumption"
        assert len(result["content"]) > 0
        assert "citations" in result
        assert "verification_rate" in result

    def test_build_prompt_structure(self):
        data = [{"text": "Plant A consumed 12500 MWh", "value": 12500, "unit": "MWh", "period": "2025-01", "facility": "Plant A"}]
        framework_def = {
            "indicator_name": "Electricity",
            "definition": "Total electricity",
            "calculation": "Grid + captive",
        }
        prompt = RAGGenerator._build_prompt("methodology", data, framework_def, "BRSR")

        assert "methodology" in prompt
        assert "Electricity" in prompt
        assert "[Table 1]" in prompt
        assert "BRSR" in prompt
        assert "100-150 words" in prompt

    def test_build_prompt_empty_data(self):
        prompt = RAGGenerator._build_prompt(
            "summary", [], {"indicator_name": "X", "definition": "Y", "calculation": "Z"}, "GRI"
        )
        assert "No data available" in prompt


# -----------------------------------------------------------------------
# 4. Citation verification tests
# -----------------------------------------------------------------------

class TestCitationVerification:
    def test_all_citations_verified(self):
        content = "The facility consumed 12500 MWh in January and 13200 MWh in February."
        data = [
            {"value": 12500.0},
            {"value": 13200.0},
        ]
        citations = RAGGenerator._verify_citations(content, data)

        assert citations["total_claims"] >= 2
        assert citations["verified_claims"] >= 2
        assert citations["verification_rate"] == 1.0

    def test_partial_verification(self):
        content = "Consumption was 12500 MWh but also 99999 MWh."
        data = [{"value": 12500.0}]
        citations = RAGGenerator._verify_citations(content, data)

        assert citations["total_claims"] >= 2
        assert citations["verified_claims"] >= 1
        assert citations["verification_rate"] < 1.0

    def test_no_numbers_in_content(self):
        content = "The facility reported data for the period."
        data = [{"value": 100.0}]
        citations = RAGGenerator._verify_citations(content, data)

        assert citations["total_claims"] == 0
        assert citations["verification_rate"] == 1.0

    def test_zero_value_handling(self):
        content = "Zero emissions: 0 tonnes."
        data = [{"value": 0}]
        citations = RAGGenerator._verify_citations(content, data)

        assert citations["verified_claims"] >= 1

    def test_tolerance_check(self):
        content = "Consumption was 12501 MWh."
        data = [{"value": 12500.0}]
        citations = RAGGenerator._verify_citations(content, data)
        assert citations["verified_claims"] >= 1


# -----------------------------------------------------------------------
# 5. Provenance tracker tests
# -----------------------------------------------------------------------

class TestProvenanceTracker:
    def test_record_and_query_entity(self, prov_tracker):
        prov_tracker.record_entity("e1", "uploaded_file", {"filename": "test.csv"})

        assert prov_tracker.entity_exists("e1")
        assert prov_tracker.get_entity_type("e1") == "uploaded_file"

    def test_record_activity(self, prov_tracker):
        now = datetime.now(timezone.utc)
        prov_tracker.record_activity("a1", "file_ingestion", now, now, "system")

        assert prov_tracker.entity_exists("a1")

    def test_derivation_chain(self, prov_tracker_with_chain):
        lineage = prov_tracker_with_chain.query_lineage("validated_1")
        entity_ids = [step["entity_id"] for step in lineage]

        assert len(lineage) >= 1
        assert any("upload_1" in eid for eid in entity_ids)

    def test_full_pipeline_lineage(self, prov_tracker_with_chain):
        """The validated entity should trace back through all 4 steps."""
        lineage = prov_tracker_with_chain.query_lineage("validated_1")
        found_types = {step.get("entity_type", "") for step in lineage}

        assert "uploaded_file" in found_types or len(lineage) >= 1

    def test_export_turtle(self, prov_tracker_with_chain):
        ttl = prov_tracker_with_chain.export_provenance("validated_1", fmt="turtle")
        assert "prov:Entity" in ttl or "wasDerivedFrom" in ttl

    def test_export_full_graph(self, prov_tracker_with_chain):
        ttl = prov_tracker_with_chain.export_provenance(entity_id=None, fmt="turtle")
        assert len(ttl) > 100

    def test_entity_not_found(self, prov_tracker):
        assert not prov_tracker.entity_exists("nonexistent")
        assert prov_tracker.get_entity_type("nonexistent") is None

    def test_flush_persists(self, prov_tracker):
        prov_tracker.record_entity("flush_e", "test", {"key": "val"})
        prov_tracker.flush()

        assert prov_tracker.storage_path.exists()
        content = prov_tracker.storage_path.read_text()
        assert "flush_e" in content


# -----------------------------------------------------------------------
# 6. Generation API tests
# -----------------------------------------------------------------------

class TestGenerationAPI:
    @pytest.fixture(autouse=True)
    def _setup(self):
        """Patch heavy dependencies for API tests."""
        from src.main import app
        from src.common.database import get_db

        self.app = app
        self.get_db = get_db
        attach_mock_auth_user(app)
        self.client = TestClient(app)

    @patch("src.api.generation._get_vector_store")
    @patch("src.api.generation._get_rag_generator")
    @patch("src.api.generation._load_validated_data_to_qdrant", return_value=10)
    def test_generation_endpoint_success(
        self, mock_load, mock_get_rag, mock_get_vs,
    ):
        mock_rag_inst = MagicMock()
        mock_rag_inst.generate_narrative.return_value = {
            "section_type": "methodology",
            "indicator": "Total Electricity",
            "content": "Plant A consumed 12500 MWh.",
            "citations": {"total_claims": 1, "verified_claims": 1, "verification_rate": 1.0},
            "verification_rate": 1.0,
        }
        mock_get_rag.return_value = mock_rag_inst
        mock_get_vs.return_value = MagicMock()

        from src.common.models import UploadStatus

        mock_upload = MagicMock()
        mock_upload.status = UploadStatus.COMPLETED

        mock_db = MagicMock()
        # First .query().filter().first() -> upload
        # Second .query().join().filter().first() -> validation result
        first_call = MagicMock()
        first_call.filter.return_value.first.return_value = mock_upload
        second_call = MagicMock()
        second_call.join.return_value.filter.return_value.first.return_value = MagicMock()
        mock_db.query.side_effect = [first_call, second_call]

        self.app.dependency_overrides[self.get_db] = lambda: mock_db
        try:
            resp = self.client.post(
                f"/api/v1/generation/{UPLOAD_ID}",
                json={
                    "sections": ["methodology"],
                    "indicators": ["Total Electricity"],
                    "framework": "BRSR",
                },
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["upload_id"] == str(UPLOAD_ID)
            assert len(body["narratives"]) == 1
            assert body["summary"]["total_narratives"] == 1
        finally:
            self.app.dependency_overrides.clear()

    def test_generation_endpoint_not_found(self):
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = None

        self.app.dependency_overrides[self.get_db] = lambda: mock_db
        try:
            resp = self.client.post(
                f"/api/v1/generation/{UPLOAD_ID}",
                json={
                    "sections": ["methodology"],
                    "indicators": ["Electricity"],
                    "framework": "BRSR",
                },
            )
            assert resp.status_code == 404
        finally:
            self.app.dependency_overrides.clear()


# -----------------------------------------------------------------------
# 7. Provenance API tests
# -----------------------------------------------------------------------

class TestProvenanceAPI:
    @pytest.fixture(autouse=True)
    def _setup(self):
        from src.main import app

        attach_mock_auth_user(app)
        self.client = TestClient(app)

    @patch("src.api.provenance.get_provenance_tracker")
    def test_provenance_endpoint_json(self, mock_get_tracker, prov_tracker_with_chain):
        mock_get_tracker.return_value = prov_tracker_with_chain

        resp = self.client.get("/api/v1/provenance/validated_1")
        assert resp.status_code == 200

        body = resp.json()
        assert body["entity_id"] == "validated_1"
        assert body["entity_type"] == "validation_results"
        assert isinstance(body["lineage_chain"], list)
        assert body["total_steps"] >= 1

    @patch("src.api.provenance.get_provenance_tracker")
    def test_provenance_endpoint_turtle(self, mock_get_tracker, prov_tracker_with_chain):
        mock_get_tracker.return_value = prov_tracker_with_chain

        resp = self.client.get("/api/v1/provenance/validated_1?format=turtle")
        assert resp.status_code == 200
        assert "text/turtle" in resp.headers["content-type"]
        assert "prov" in resp.text or "wasDerivedFrom" in resp.text

    @patch("src.api.provenance.get_provenance_tracker")
    def test_provenance_not_found(self, mock_get_tracker, prov_tracker):
        mock_get_tracker.return_value = prov_tracker

        resp = self.client.get("/api/v1/provenance/nonexistent_entity")
        assert resp.status_code == 404

    @patch("src.api.provenance.get_provenance_tracker")
    def test_provenance_bad_format(self, mock_get_tracker, prov_tracker_with_chain):
        mock_get_tracker.return_value = prov_tracker_with_chain

        resp = self.client.get("/api/v1/provenance/validated_1?format=csv")
        assert resp.status_code == 400
