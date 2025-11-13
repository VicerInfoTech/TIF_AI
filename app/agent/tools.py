"""LangChain tool wrappers for the SQL agent."""

from __future__ import annotations

from typing import Dict, Optional

from langchain.tools import tool

from app.core import query_executor, sql_generator, sql_validator


@tool
def generate_sql_tool(query: str, ddl: str, plan: Optional[str] = None, feedback: Optional[str] = None) -> str:
	"""Generate SQL using the Groq LLM (placeholder prompt)."""

	return sql_generator.generate_sql(query=query, ddl_schema=ddl, plan=plan, feedback=feedback)


@tool
def validate_sql_tool(sql: str) -> Dict[str, object]:
	"""Validate SQL is read-only and safe for execution."""

	return sql_validator.validate_sql(sql)


@tool
def execute_sql_tool(sql: str, db_config: Dict[str, object]) -> Dict[str, object]:
	"""Execute SQL against the configured database."""

	return query_executor.execute_query(sql, db_config)

