"""Vector store helpers for schema retrieval."""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any, Dict, List

from langchain_core.documents import Document
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_postgres import PGVector

from app.utils.logger import setup_logging

logger = setup_logging(__name__)

DEFAULT_EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL_NAME", "jinaai/jina-embeddings-v3")


def default_collection_name(db_flag: str) -> str:
	"""Resolve the PGVector collection name for a database flag."""

	normalized_flag = (db_flag or "").strip()
	if not normalized_flag:
		return os.getenv("PGVECTOR_COLLECTION_NAME", "default_docs")

	env_key = f"PGVECTOR_COLLECTION_NAME_{normalized_flag.upper()}"
	per_db = os.getenv(env_key)
	if per_db:
		return per_db

	global_default = os.getenv("PGVECTOR_COLLECTION_NAME")
	if global_default:
		return global_default

	return f"{normalized_flag}_docs"


@lru_cache(maxsize=1)
def get_embeddings() -> HuggingFaceEmbeddings:
	"""Return a cached HuggingFace embedding client."""

	model_name = DEFAULT_EMBEDDING_MODEL
	logger.debug("Initializing HuggingFace embeddings model=%s", model_name)
	return HuggingFaceEmbeddings(
		model_name=model_name,
		model_kwargs={"trust_remote_code": True},
	)


@lru_cache(maxsize=None)
def get_vector_store(collection_name: str) -> PGVector:
	"""Return a cached PGVector instance for the collection."""

	connection_string = os.getenv("POSTGRES_CONNECTION_STRING")
	if not connection_string:
		raise RuntimeError("POSTGRES_CONNECTION_STRING environment variable is required")

	logger.debug("Creating PGVector client for collection=%s", collection_name)
	return PGVector(
		embeddings=get_embeddings(),
		collection_name=collection_name,
		connection=connection_string,
		use_jsonb=True,
	)


def vector_search(
	query: str,
	collection_name: str,
	filters: Dict[str, Any] | None = None,
	k: int = 3,
) -> List[Document]:
	"""Run a similarity search and return top matching documents."""

	store = get_vector_store(collection_name)
	results = store.similarity_search(query, k=k, filter=filters or None)
	logger.debug(
		"vector_search collection=%s filters=%s hits=%d",
		collection_name,
		filters,
		len(results),
	)
	return results