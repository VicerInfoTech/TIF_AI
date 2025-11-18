"""Shared LangGraph state definition."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict

import pandas as pd

from app.models import BusinessQuerySpec


class AgentState(TypedDict, total=False):
	"""State that flows through the LangGraph workflow."""

	# Input
	query: str
	db_flag: str
	output_format: str

	# Configuration
	db_config: Dict[str, Any]
	schema_metadata: Dict[str, Any]
	schema_toolkit: Any
	ddl_schema: str
	selected_tables: Optional[List[str]]
	keyword_matches: Optional[List[str]]
	schema_context: Optional[str]
	join_summary: Optional[str]

	# Planning and generation
	planning_notes: Optional[str]
	business_spec: Optional[BusinessQuerySpec]
	regeneration_context: Optional[str]
	schema_summaries: Optional[List[str]]
	generated_sql: Optional[str]

	# Validation & execution
	validation_result: Optional[Dict[str, Any]]
	execution_result: Optional[pd.DataFrame]

	# Error handling & retries
	error_message: Optional[str]
	retry_count: int

	# Output metadata
	final_response: Optional[Dict[str, Any]]
	execution_time_ms: Optional[float]
	total_rows: Optional[int]
	token_usage: Optional[Dict[str, Any]]


def initialise_state(query: str, db_flag: str, output_format: str = "json") -> AgentState:
	"""Create the initial state payload for the workflow."""

	return AgentState(
		query=query,
		db_flag=db_flag,
		output_format=output_format,
		retry_count=0,
	)


