"""LangChain tools for schema retrieval and SQL validation."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Dict, Iterator, List

from langchain.tools import tool

from app.core import sql_validator
from app.core.retriever import default_collection_name, vector_search
from app.utils.logger import setup_logging

logger = setup_logging(__name__, level="DEBUG")

_current_db_flag: ContextVar[str | None] = ContextVar("agent_db_flag", default=None)
_current_collection: ContextVar[str | None] = ContextVar("agent_collection", default=None)
_current_user_id: ContextVar[str | None] = ContextVar("agent_user_id", default=None)
_current_session_id: ContextVar[str | None] = ContextVar("agent_session", default=None)
_accessed_tables: ContextVar[set[str] | None] = ContextVar("agent_tables", default=None)
_tool_cache: ContextVar[dict | None] = ContextVar("agent_tool_cache", default=None)
_tool_call_counts: ContextVar[dict | None] = ContextVar("agent_tool_call_counts", default=None)

VALID_SECTIONS = {"summary", "header", "columns", "relationships", "stats"}
_MAX_TOOL_CALLS_PER_TOOL = 8


def _record_table(table_name: str | None) -> None:
    if not table_name:
        return
    table = table_name.strip()
    if not table:
        return
    visited = _accessed_tables.get(None)
    if visited is None:
        visited = set()
        _accessed_tables.set(visited)
    visited.add(table)
    logger.debug("Recorded accessed table: %s", table)


def _filters_with_context(base: Dict[str, object] | None = None) -> Dict[str, object]:
    filters: Dict[str, object] = dict(base or {})
    db_flag = _current_db_flag.get(None)
    if db_flag:
        filters.setdefault("db_flag", db_flag)
    return filters


def _require_collection() -> str:
    collection = _current_collection.get(None)
    if not collection:
        raise RuntimeError("Vector collection context is not configured for the agent run")
    return collection


def _log_tool(name: str, params: Dict[str, object], result: str | None, extra: Dict[str, object] | None = None) -> None:
    log_data = {
        "params": params,
        "result_length": len(result) if result else 0,
    }
    if extra:
        log_data.update(extra)
    logger.debug("Tool call: %s %s", name, log_data)


@contextmanager
def agent_context(
    db_flag: str,
    collection_name: str | None = None,
    user_id: str | None = None,
    session_id: str | None = None,
) -> Iterator[None]:
    """Context manager that binds db_flag and collection for tool invocations."""

    token_db = _current_db_flag.set(db_flag)
    collection = collection_name or default_collection_name(db_flag)
    token_collection = _current_collection.set(collection)
    token_tables = _accessed_tables.set(set())
    token_cache = _tool_cache.set({})
    token_counts = _tool_call_counts.set({})
    token_user = _current_user_id.set(user_id)
    token_session = _current_session_id.set(session_id)
    logger.debug("Agent context set db_flag=%s collection=%s", db_flag, collection)
    try:
        yield
    finally:
        logger.debug("Clearing agent context db_flag=%s collection=%s", db_flag, collection)
        _accessed_tables.reset(token_tables)
        _tool_cache.reset(token_cache)
        _tool_call_counts.reset(token_counts)
        _current_collection.reset(token_collection)
        _current_db_flag.reset(token_db)
        _current_user_id.reset(token_user)
        _current_session_id.reset(token_session)


def get_collected_tables() -> List[str]:
    """Return the sorted list of table names accessed during the agent run."""

    visited = _accessed_tables.get(None)
    if not visited:
        return []
    return sorted(visited)


def get_context_user_id() -> str | None:
    return _current_user_id.get(None)


def get_context_session_id() -> str | None:
    return _current_session_id.get(None)


def get_context_db_flag() -> str | None:
    return _current_db_flag.get(None)


def _tool_cache_key(name: str, *args, **kwargs) -> str:
    return f"{name}:{args}:{sorted(kwargs.items())}"


def _tool_maybe_cache_or_count(name: str, key: str, value: str) -> str:
    cache = _tool_cache.get(None)
    counts = _tool_call_counts.get(None)
    if cache is None or counts is None:
        return value
    if key in cache:
        logger.debug("Cache hit for %s key=%s", name, key)
        return cache[key]
    counts[name] = counts.get(name, 0) + 1
    count = counts[name]
    cache[key] = value
    if count > _MAX_TOOL_CALLS_PER_TOOL:
        msg = (
            f"Tool '{name}' called {count} times in this agent run. "
            "Please avoid repeated identical calls and proceed to final output."
        )
        cache[key] = msg
        logger.warning("Tool '%s' hit limit: %d calls — returning abort hint", name, count)
        return msg
    return value


def get_tool_call_counts() -> Dict[str, int]:
    """Return a shallow copy of current per-run tool call counts for diagnostics."""
    counts = _tool_call_counts.get(None)
    return dict(counts or {})


def get_tool_cache() -> Dict[str, str]:
    """Return a shallow copy of the current per-run tool cache for diagnostics."""
    cache = _tool_cache.get(None)
    return dict(cache or {})


def _sanitize_text(value: str | None, limit: int = 2000) -> str:
    if not value:
        return ""
    cleaned = value.strip()
    return cleaned[:limit]


def _assemble_query(section: str, table_name: str | None, intent: str | None) -> str:
    if table_name:
        return f"{section} for {table_name}"
    if intent:
        return intent
    return section


def _format_doc(doc: Any, section: str) -> str:
    table_name = doc.metadata.get("table_name") or doc.metadata.get("table")
    schema = doc.metadata.get("schema", "dbo")
    source = doc.metadata.get("source", "vector")
    content = doc.page_content.strip()
    return (
        f"Section: {section} | Table: {table_name} | schema={schema} | source={source}\n{content}"
    )


def _vector_search_and_log(
    name: str,
    query_text: str,
    db_schema: str | None,
    section: str,
    table_name: str | None,
    k: int,
) -> str:
    logger.debug(
        "Vector search request: name=%s section=%s table=%s db_schema=%s k=%s",
        name,
        section,
        table_name,
        db_schema,
        k,
    )
    collection = _require_collection()
    filters = _filters_with_context({"section": section})
    if table_name:
        filters["table_name"] = table_name
    if db_schema:
        filters.setdefault("schema", db_schema)
    cache_key = _tool_cache_key(name, query_text, section, table_name, db_schema, frozenset(filters.items()))
    try:
        docs = vector_search(query_text, collection, filters=filters, k=k)
    except Exception as exc:
        logger.exception("Vector search failed for %s: %s", query_text, exc)
        return f"Vector search error: {exc}"
    if not docs:
        logger.debug("No vector docs found: name=%s section=%s table=%s", name, section, table_name)
        return f"No {section} results found for the requested context."
    formatted = [_format_doc(doc, section) for doc in docs]
    for doc in docs:
        recorded = doc.metadata.get("table_name") or doc.metadata.get("table")
        _record_table(recorded)
        logger.debug(
            "Document hit: table=%s schema=%s source=%s",
            recorded,
            doc.metadata.get("schema", "dbo"),
            doc.metadata.get("source", "vector"),
        )
    out = "\n\n".join(formatted)
    metadata = {
        "hits": len(docs),
        "tables": [doc.metadata.get("table_name") or doc.metadata.get("table") for doc in docs],
    }
    params = {"query": query_text, "section": section, "table_name": table_name, "filters": filters}
    _log_tool(name, params, out, metadata)
    logger.debug("Vector search results: name=%s hits=%s tables=%s", name, metadata["hits"], metadata["tables"]) 
    return _tool_maybe_cache_or_count(name, cache_key, out)


@tool("get_database_schema", return_direct=False)
def get_database_schema(
    intent: str | None = None,
    table_name: str | None = None,
    section: str = "summary",
    db_schema: str | None = None,
    k: int = 4,
) -> str:
    """Return vector-backed schema fragments depending on the requested section."""

    section = section.lower().strip()
    if section not in VALID_SECTIONS:
        return f"Unsupported section '{section}'. Valid options: {', '.join(sorted(VALID_SECTIONS))}."

    sanitized_table = _sanitize_text(table_name)
    sanitized_intent = _sanitize_text(intent)
    logger.debug("get_database_schema - sanitized inputs: table=%s intent=%s section=%s db_schema=%s", sanitized_table, sanitized_intent, section, db_schema)
    if not sanitized_table and not sanitized_intent:
        return "Provide either a table name or a descriptive intent to retrieve schema context."

    query_text = _assemble_query(section, sanitized_table or None, sanitized_intent or None)
    logger.debug("get_database_schema - assembled query_text=%s", query_text)
    return _vector_search_and_log("get_database_schema", query_text, db_schema, section, sanitized_table or None, k)


@tool("validate_sql", return_direct=False)
def validate_sql_tool(sql: str) -> str:
    """Validate that SQL is read-only and safe."""

    result = sql_validator.validate_sql(sql or "")
    logger.debug("validate_sql called len=%d", len(sql or ""))
    _log_tool(
        "validate_sql",
        {"sql": sql},
        "OK" if result.get("valid") else f"Invalid: {result.get('reason')}",
        {"valid": result.get("valid"), "reason": result.get("reason")},
    )
    if not result.get("valid"):
        logger.warning("validate_sql: invalid SQL provided: %s reason=%s", sql, result.get("reason"))
    return "OK" if result.get("valid") else f"Invalid: {result.get('reason')}"


__all__ = [
    "agent_context",
    "default_collection_name",
    "get_collected_tables",
    "get_database_schema",
    "validate_sql_tool",
    "get_context_user_id",
    "get_context_session_id",
    "get_context_db_flag",
    "get_tool_call_counts",
    "get_tool_cache",
]
