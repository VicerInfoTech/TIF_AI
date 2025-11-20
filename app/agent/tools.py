"""LangChain tools for schema retrieval and SQL validation."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Dict, Iterator, List

from langchain.tools import tool

from app.core import sql_validator
from app.core.retriever import default_collection_name, vector_search
from app.utils.logger import setup_logging

logger = setup_logging(__name__)

_current_db_flag: ContextVar[str | None] = ContextVar("agent_db_flag", default=None)
_current_collection: ContextVar[str | None] = ContextVar("agent_collection", default=None)
_accessed_tables: ContextVar[set[str] | None] = ContextVar("agent_tables", default=None)

VALID_SECTIONS = {"summary", "header", "columns", "relationships", "stats"}


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


@contextmanager
def agent_context(db_flag: str, collection_name: str | None = None) -> Iterator[None]:
	"""Context manager that binds db_flag and collection for tool invocations."""

	token_db = _current_db_flag.set(db_flag)
	collection = collection_name or default_collection_name(db_flag)
	token_collection = _current_collection.set(collection)
	token_tables = _accessed_tables.set(set())
	logger.debug("Agent context set db_flag=%s collection=%s", db_flag, collection)
	try:
		yield
	finally:
		logger.debug("Clearing agent context db_flag=%s collection=%s", db_flag, collection)
		_accessed_tables.reset(token_tables)
		_current_collection.reset(token_collection)
		_current_db_flag.reset(token_db)


def get_collected_tables() -> List[str]:
	"""Return the sorted list of table names accessed during the agent run."""

	visited = _accessed_tables.get(None)
	if not visited:
		return []
	return sorted(visited)


@tool("search_tables", return_direct=False)
def search_tables_tool(query: str, k: int = 4) -> str:
	"""Identify candidate tables for the user query via summary embeddings."""

	if not query.strip():
		return "No query provided."
	collection = _require_collection()
	filters = _filters_with_context({"section": "summary"})
	docs = vector_search(query, collection, filters=filters, k=k)
	if not docs:
		return "No matching tables found in summaries."
	formatted = []
	for doc in docs:
		table_name = doc.metadata.get("table_name") or doc.metadata.get("table")
		db_schema = doc.metadata.get("schema", "dbo")
		_record_table(table_name)
		formatted.append(
			f"Table: {table_name} (schema={db_schema})\nSummary: {doc.page_content.strip()}"
		)
	return "\n\n".join(formatted)


@tool("fetch_table_summary", return_direct=False)
def fetch_table_summary_tool(table_name: str, db_schema: str | None = None) -> str:
	"""Retrieve the minimal summary chunk for a given table."""

	if not table_name.strip():
		return "Table name is required."
	collection = _require_collection()
	filters = _filters_with_context({"section": "summary", "table_name": table_name})
	if db_schema:
		filters.setdefault("schema", db_schema)
	docs = vector_search(f"summary for {table_name}", collection, filters=filters, k=1)
	if not docs:
		return f"No summary found for table {table_name}."
	_record_table(table_name)
	return docs[0].page_content.strip()


@tool("fetch_table_section", return_direct=False)
def fetch_table_section_tool(table_name: str, section: str, db_schema: str | None = None) -> str:
	"""Retrieve a structured section (columns, relationships, stats) for a table."""

	if section not in VALID_SECTIONS:
		return f"Unsupported section '{section}'. Use one of: {', '.join(sorted(VALID_SECTIONS))}."
	if not table_name.strip():
		return "Table name is required."
	collection = _require_collection()
	filters = _filters_with_context({"section": section, "table_name": table_name})
	if db_schema:
		filters.setdefault("schema", db_schema)
	docs = vector_search(f"{section} for {table_name}", collection, filters=filters, k=1)
	if not docs:
		return f"No {section} section found for table {table_name}."
	_record_table(table_name)
	return docs[0].page_content.strip()


@tool("validate_sql", return_direct=False)
def validate_sql_tool(sql: str) -> str:
	"""Validate that SQL is read-only and safe."""

	result = sql_validator.validate_sql(sql or "")
	return "OK" if result.get("valid") else f"Invalid: {result.get('reason')}"


__all__ = [
	"agent_context",
	"default_collection_name",
	"get_collected_tables",
	"search_tables_tool",
	"fetch_table_summary_tool",
	"fetch_table_section_tool",
	"validate_sql_tool",
]