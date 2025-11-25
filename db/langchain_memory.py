"""Shared LangGraph/Store resources for conversation memory."""

from __future__ import annotations

import atexit
import os

from dotenv import load_dotenv
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.store.postgres import PostgresStore

from app.utils.logger import setup_logging

load_dotenv()

logger = setup_logging(__name__)

raw_uri = os.getenv("POSTGRES_CONNECTION_STRING")
if raw_uri and "+psycopg" in raw_uri:
    normalized_uri = raw_uri.replace("+psycopg", "", 1)
    logger.debug("Normalized Postgres URI for LangGraph (removed +psycopg): %s", normalized_uri[:15] + "****")
else:
    normalized_uri = raw_uri
POSTGRES_URI = normalized_uri
if not POSTGRES_URI:
    raise RuntimeError("POSTGRES_CONNECTION_STRING must be configured to use LangGraph memory")

_store_ctx = PostgresStore.from_conn_string(POSTGRES_URI)
_checkpointer_ctx = PostgresSaver.from_conn_string(POSTGRES_URI)

_store = _store_ctx.__enter__()
_checkpointer = _checkpointer_ctx.__enter__()

try:
    _store.setup()
    logger.info("PostgresStore initialized for conversation memory")
except Exception as exc:
    logger.error("Failed to create LangGraph store tables: %s", exc)

try:
    _checkpointer.setup()
    logger.info("Postgres checkpointer initialized")
except Exception as exc:
    logger.error("Failed to create LangGraph checkpointer tables: %s", exc)


def _cleanup() -> None:
    try:
        _checkpointer_ctx.__exit__(None, None, None)
    except Exception:  # pragma: no cover - best-effort cleanup
        logger.debug("Error closing checkpointer context")
    try:
        _store_ctx.__exit__(None, None, None)
    except Exception:  # pragma: no cover - best-effort cleanup
        logger.debug("Error closing store context")

atexit.register(_cleanup)


def get_store() -> PostgresStore:
    return _store


def get_checkpointer() -> PostgresSaver:
    return _checkpointer


__all__ = ["get_store", "get_checkpointer"]
