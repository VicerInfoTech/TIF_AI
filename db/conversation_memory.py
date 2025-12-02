"""Conversation memory helpers backed by LangGraph Postgres store."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

from langgraph.store.base import SearchItem

from app.utils.logger import setup_logging

from db.langchain_memory import get_store

logger = setup_logging(__name__, level="INFO")


def _get_store():
    """Return the store instance lazily; avoids initializing Postgres at import time."""
    return get_store()

QUERY_NAMESPACE = "queries"
SUMMARY_NAMESPACE = "conversation_summary"
SUMMARY_KEY = "meta"


def _query_namespace(user_id: str, session_id: str, db_flag: str) -> tuple[str, ...]:
    return (QUERY_NAMESPACE, user_id, session_id, db_flag)


def _summary_namespace(user_id: str, session_id: str, db_flag: str) -> tuple[str, ...]:
    return (SUMMARY_NAMESPACE, user_id, session_id, db_flag)


def _iterate_namespace(namespace: tuple[str, ...], limit: int = 100) -> Iterable[SearchItem]:
    offset = 0
    while True:
        page = _get_store().search(namespace, limit=limit, offset=offset, query="")
        if not page:
            break
        for item in page:
            yield item
        if len(page) < limit:
            break
        offset += len(page)


def store_query_context(
    user_id: str,
    session_id: str,
    db_flag: str,
    query_text: str,
    sql_generated: str,
    tables_used: Optional[List[str]] = None,
    follow_up_questions: Optional[List[str]] = None,
    contextual_insights: Optional[str] = None,
    execution_time: Optional[float] = None,
) -> str:
    """Persist a query turn in conversation memory."""
    namespace = _query_namespace(user_id, session_id, db_flag)
    timestamp = datetime.now(timezone.utc).isoformat()
    key = f"{timestamp}-{uuid4().hex}"
    entry: Dict[str, Any] = {
        "query_text": query_text,
        "sql_generated": sql_generated,
        "tables_used": tables_used or [],
        "follow_up_questions": follow_up_questions or [],
        "contextual_insights": contextual_insights,
        "execution_time": execution_time,
        "timestamp": timestamp,
        "db_flag": db_flag,
    }
    logger.debug(
        "Storing query context namespace=%s key=%s tables=%s followups=%s",
        "/".join(namespace),
        key,
        entry["tables_used"],
        entry["follow_up_questions"],
    )
    _get_store().put(namespace, key, entry)
    logger.info("Stored query context for %s/%s (key=%s)", user_id, session_id, key)
    return key


def get_query_history(
    user_id: str,
    session_id: str,
    db_flag: str,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """Retrieve the most recent query turns for a session."""
    namespace = _query_namespace(user_id, session_id, db_flag)
    logger.debug(
        "Fetching query history namespace=%s limit=%s",
        "/".join(namespace),
        limit,
    )
    try:
        items = _get_store().search(namespace, limit=limit, query="")
    except Exception as exc:
        logger.error("Failed to retrieve query history: %s", exc)
        return []
    result: List[Dict[str, Any]] = []
    for item in items:
        value = item.value
        result.append(
            {
                "key": item.key,
                "query_text": value.get("query_text"),
                "sql_generated": value.get("sql_generated"),
                "tables_used": value.get("tables_used") or [],
                "follow_up_questions": value.get("follow_up_questions") or [],
                "contextual_insights": value.get("contextual_insights"),
                "execution_time": value.get("execution_time"),
                "timestamp": value.get("timestamp"),
            }
        )
    logger.debug(
        "Retrieved %d history entries for %s/%s", len(result), user_id, session_id
    )
    return result


def format_conversation_summary(query_history: List[Dict[str, Any]]) -> str:
    """Create a readable summary of the latest history."""
    if not query_history:
        return "No conversation history yet."
    lines: List[str] = []
    for idx, record in enumerate(reversed(query_history), 1):
        line = f"{idx}. Query: {record['query_text']} | SQL: {record['sql_generated']}"
        insights = record.get("contextual_insights")
        if insights:
            line += f" | Facts: {insights}"
        follow_ups = record.get("follow_up_questions") or []
        if follow_ups:
            line += f" | Follow-ups: {', '.join(follow_ups)}"
        lines.append(line)
    return "\n".join(lines)


def get_session_accessed_tables(
    user_id: str,
    session_id: str,
    db_flag: str,
    limit: int = 5,
) -> set[str]:
    """Return the tables referenced in the most recent turns."""
    history = get_query_history(user_id, session_id, db_flag, limit=limit)
    tables: set[str] = set()
    for record in history:
        for table in record.get("tables_used", []):
            tables.add(table)
    return tables


def update_or_create_session_summary(
    user_id: str,
    session_id: str,
    db_flag: str,
) -> None:
    """Store summary metadata in the LangGraph store."""
    history = get_query_history(user_id, session_id, db_flag, limit=10)
    summary_text = format_conversation_summary(history)
    entry: Dict[str, Any] = {
        "summary": summary_text,
        "accessed_tables": list(get_session_accessed_tables(user_id, session_id, db_flag, limit=10)),
        "total_queries": len(history),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    namespace = _summary_namespace(user_id, session_id, db_flag)
    _get_store().put(namespace, SUMMARY_KEY, entry)
    logger.debug("Updated conversation summary for %s/%s", user_id, session_id)


def get_session_summary(
    user_id: str,
    session_id: str,
    db_flag: str,
) -> Optional[Dict[str, Any]]:
    """Retrieve the persisted summary metadata."""
    namespace = _summary_namespace(user_id, session_id, db_flag)
    try:
        item = _get_store().get(namespace, SUMMARY_KEY)
    except Exception as exc:
        logger.error("Failed to read session summary: %s", exc)
        item = None
    if item:
        return item.value
    history = get_query_history(user_id, session_id, db_flag, limit=5)
    if not history:
        return None
    return {
        "summary": format_conversation_summary(history),
        "accessed_tables": list(get_session_accessed_tables(user_id, session_id, db_flag, limit=5)),
        "total_queries": len(history),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def clear_conversation_history(user_id: str, session_id: str, db_flag: str) -> None:
    """Remove all conversation and summary records for a session."""
    query_namespace = _query_namespace(user_id, session_id, db_flag)
    summary_namespace = _summary_namespace(user_id, session_id, db_flag)
    for item in _iterate_namespace(query_namespace):
        _get_store().put(query_namespace, item.key, None)
    _get_store().put(summary_namespace, SUMMARY_KEY, None)
    logger.info("Cleared conversation history for %s/%s", user_id, session_id)


__all__ = [
    "store_query_context",
    "get_query_history",
    "get_session_accessed_tables",
    "update_or_create_session_summary",
    "get_session_summary",
    "format_conversation_summary",
    "clear_conversation_history",
]
