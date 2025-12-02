"""Vector store helpers for schema retrieval."""

from __future__ import annotations

import os
from threading import Lock
from typing import Any, Dict, List

from langchain_core.documents import Document
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_postgres import PGVector

from app.utils.logger import setup_logging

logger = setup_logging(__name__, level="DEBUG")

DEFAULT_EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL_NAME", "jinaai/jina-embeddings-v3")

_embeddings_lock = Lock()
_embeddings_instance: HuggingFaceEmbeddings | None = None
_vector_store_lock = Lock()
_vector_store_cache: Dict[str, PGVector] = {}


def default_collection_name(db_flag: str) -> str:
	"""Resolve the PGVector collection name for a database flag."""
	normalized_flag = (db_flag or "").strip()
	return f"{normalized_flag}_docs"


def get_embeddings() -> HuggingFaceEmbeddings:
	"""Return a cached HuggingFace embedding client."""

	global _embeddings_instance
	if _embeddings_instance is not None:
		return _embeddings_instance
	with _embeddings_lock:
		if _embeddings_instance is not None:
			return _embeddings_instance
		model_name = DEFAULT_EMBEDDING_MODEL
		logger.debug("Initializing HuggingFace embeddings model=%s", model_name)
		_embeddings_instance = HuggingFaceEmbeddings(
			model_name=model_name,
			model_kwargs={"trust_remote_code": True},
		)
		return _embeddings_instance


def get_vector_store(collection_name: str) -> PGVector:
	"""Return a cached PGVector instance for the collection."""

	connection_string = os.getenv("POSTGRES_CONNECTION_STRING")
	if not connection_string:
		raise RuntimeError("POSTGRES_CONNECTION_STRING environment variable is required")
	if collection_name in _vector_store_cache:
		return _vector_store_cache[collection_name]
	with _vector_store_lock:
		if collection_name in _vector_store_cache:
			return _vector_store_cache[collection_name]
		logger.debug("Creating PGVector client for collection=%s", collection_name)
		store = PGVector(
			embeddings=get_embeddings(),
			collection_name=collection_name,
			connection=connection_string,
			use_jsonb=True,
		)
		_vector_store_cache[collection_name] = store
		return store


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