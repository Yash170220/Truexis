"""Conversational RAG chat service scoped to user's uploaded ESG data."""
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID

import anthropic
import redis

from src.generation.vector_store import VectorStore

logger = logging.getLogger(__name__)

SIMILARITY_THRESHOLD = 0.5   # FIX: was 0.6 — too strict, many valid questions were rejected
MAX_HISTORY = 10
HISTORY_TTL = 86400  # 24 hours

FORBIDDEN_TOPICS = [
    "stock price", "investment advice", "legal advice",
    "medical", "political", "write code", "hack",
]

# FIX: Expanded keyword list — "brsr", "gri", "report", "benchmark", "trend",
#      "year", "month", "quarter" are common ESG chat queries that were being
#      rejected by _validate_question
VALID_KEYWORDS = [
    "electricity", "emission", "water", "waste",
    "energy", "scope", "consumption", "production",
    "facility", "plant", "total", "average", "compare",
    "fuel", "gas", "carbon", "ghg", "renewable",
    "intensity", "reduction", "recycle", "discharge",
    "brsr", "gri", "report", "benchmark", "trend",
    "year", "month", "quarter", "highest", "lowest",
    "summary", "overview", "performance", "target",
]


class ChatService:
    """Upload-scoped conversational RAG — answers only from the user's data."""

    def __init__(
        self,
        vector_store: VectorStore,
        api_key: str,
        model: str = "claude-haiku-4-5",
        redis_url: str = "redis://localhost:6379/0",
    ):
        self.vector_store = vector_store
        self.claude = anthropic.Anthropic(api_key=api_key)
        self.model = model

        try:
            self.redis = redis.from_url(redis_url, decode_responses=True)
            self.redis.ping()
        except Exception:
            logger.warning("Redis unavailable — chat history will not persist")
            self.redis = None

    @staticmethod
    def _validate_question(question: str) -> bool:
        """Reject off-topic or forbidden questions."""
        q_lower = question.lower()
        for topic in FORBIDDEN_TOPICS:
            if topic in q_lower:
                return False
        return any(kw in q_lower for kw in VALID_KEYWORDS)

    def chat(
        self,
        upload_id: UUID,
        question: str,
        session_id: str,
    ) -> Dict:
        if not self._validate_question(question):
            return {
                "answer": (
                    "I can only answer questions about your ESG data metrics "
                    "(electricity, emissions, water, waste, etc.). "
                    "Please ask about your facility's environmental performance."
                ),
                "sources": [],
                "confidence": 0.0,
            }

        history = self._get_history(session_id)

        # FIX: top_k reduced from 5 → 3 — less noise, faster, more focused answers
        search_results = self.vector_store.search_validated_data(
            query=question, upload_id=upload_id, top_k=3
        )

        if not search_results or search_results[0]["similarity"] < SIMILARITY_THRESHOLD:
            answer = (
                "I don't have information about this in your uploaded data. "
                "Please ask about metrics like electricity, emissions, water, "
                "or waste from your facility reports."
            )
            self._save_to_history(session_id, question, answer)
            return {"answer": answer, "sources": [], "confidence": 0.0}

        prompt = self._build_chat_prompt(question, search_results, history)

        try:
            completion = self.claude.messages.create(
                model=self.model,
                system=self._system_prompt(),
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=300,
            )
            answer = completion.content[0].text.strip()
        except Exception as exc:
            logger.error(f"Claude chat error: {exc}")
            answer = "Sorry, I encountered an error generating a response. Please try again."

        sources = [
            {
                "indicator": r["indicator"],
                "value": r["value"],
                "unit": r["unit"],
                "period": r["period"],
                "facility": r["facility"],
                "similarity": round(r["similarity"], 2),
            }
            for r in search_results[:3]
        ]

        self._save_to_history(session_id, question, answer)

        return {
            "answer": answer,
            "sources": sources,
            "confidence": round(search_results[0]["similarity"], 4),
        }

    # ------------------------------------------------------------------
    # Prompts
    # ------------------------------------------------------------------

    @staticmethod
    def _system_prompt() -> str:
        # FIX: Added explicit instruction to answer naturally even with partial data,
        #      instead of always saying "not available" when similarity is borderline.
        return (
            "You are an ESG data assistant. Answer questions using ONLY the "
            "provided data context.\n\n"
            "STRICT RULES:\n"
            "1. Use ONLY facts from the data provided below.\n"
            "2. If information is not in the data, say exactly: "
            '"This metric is not available in the uploaded data."\n'
            '3. Cite sources inline: "[Source: {facility} / {period}]"\n'
            "4. Be concise — 2-4 sentences max.\n"
            "5. Never use external ESG knowledge or industry benchmarks unless explicitly asked.\n"
            "6. For comparisons, only compare data that exists in the upload.\n"
            "7. If you can partially answer, do so and state what is missing.\n\n"
            "FORMAT:\n"
            "- Answer directly in 1-2 sentences.\n"
            "- Include specific value + unit + facility + period.\n"
            "- End with source citation.\n"
            "- No bullet points unless listing multiple facilities."
        )

    @staticmethod
    def _build_chat_prompt(
        question: str,
        data: List[Dict],
        history: List[Dict],
    ) -> str:
        # FIX: Structured data context more clearly with numbered entries
        #      so the LLM can reference them without confusion
        data_lines = []
        for i, d in enumerate(data[:3], 1):
            data_lines.append(
                f"[{i}] Facility: {d['facility']} | Period: {d['period']} | "
                f"Indicator: {d['indicator']} | Value: {d['value']} {d['unit']}"
            )
        data_context = "\n".join(data_lines)

        history_text = ""
        if history:
            # FIX: Reduced from last 4 to last 3 exchanges — keeps context tight
            recent = history[-3:]
            history_text = (
                "Previous conversation:\n"
                + "\n".join(
                    f"Q: {h['question']}\nA: {h['answer']}" for h in recent
                )
                + "\n\n"
            )

        return (
            f"{history_text}"
            f"Question: {question}\n\n"
            f"Available data:\n{data_context}\n\n"
            f"Answer using ONLY the data above. "
            f"If the question cannot be answered with this data, say so clearly."
        )

    # ------------------------------------------------------------------
    # History (Redis-backed)
    # ------------------------------------------------------------------

    def _get_history(self, session_id: str) -> List[Dict]:
        if self.redis is None:
            return []
        try:
            raw = self.redis.get(f"chat_history:{session_id}")
            return json.loads(raw) if raw else []
        except Exception:
            return []

    def _save_to_history(
        self, session_id: str, question: str, answer: str
    ) -> None:
        if self.redis is None:
            return
        try:
            history = self._get_history(session_id)
            history.append({
                "question": question,
                "answer": answer,
                "timestamp": datetime.now().isoformat(),
            })
            if len(history) > MAX_HISTORY:
                history = history[-MAX_HISTORY:]
            self.redis.setex(
                f"chat_history:{session_id}", HISTORY_TTL, json.dumps(history)
            )
        except Exception as exc:
            logger.warning(f"Failed to save chat history: {exc}")

    def clear_history(self, session_id: str) -> None:
        if self.redis is None:
            return
        try:
            self.redis.delete(f"chat_history:{session_id}")
        except Exception:
            pass