"""LangGraph node implementations."""

from __future__ import annotations

import json
from pathlib import Path
from time import perf_counter
from typing import Literal

from langchain_groq import ChatGroq

from app.agent.state import AgentState
from app.config import get_database_settings
from app.core.ddl_loader import load_schema
from app.core import query_executor, result_formatter, sql_generator, sql_validator
from app.core.schema_selector import SchemaSelector, format_tables_for_llm
from app.prompts.error_handler import build_error_feedback
from app.prompts.sql_planner import build_planning_prompt
from app.utils.token_tracker import get_token_tracker
from app.utils.logger import setup_logging

logger = setup_logging(__name__)

MAX_RETRIES = 2
_planner_llm = ChatGroq(model="meta-llama/llama-4-scout-17b-16e-instruct", temperature=0.1)


def load_config_node(state: AgentState) -> AgentState:
	"""Load database configuration and schema metadata."""

	logger.info("Loading configuration for db_flag=%s", state.get("db_flag"))
	updated: AgentState = {**state}

	settings = get_database_settings(state["db_flag"])
	db_config = settings.model_dump()
	schema_metadata = {}
	ddl_text = ""

	schema_json_path = Path(settings.ddl_file).with_suffix(".json")
	if schema_json_path.exists():
		with schema_json_path.open("r", encoding="utf-8") as handle:
			schema_metadata = json.load(handle)
	else:
		logger.warning("Preprocessed schema not found for %s; using raw DDL", state["db_flag"])
		ddl_text = load_schema(settings.ddl_file)
		schema_metadata = {
			"metadata": {"database_name": settings.connection_string},
			"tables": {},
			"relationships": {},
			"keyword_map": {},
		}

	updated["db_config"] = db_config
	updated["schema_metadata"] = schema_metadata
	updated["ddl_schema"] = ddl_text
	updated.setdefault("retry_count", 0)

	return updated


def select_schema_node(state: AgentState) -> AgentState:
	"""Select relevant tables based on query keywords."""

	updated: AgentState = {**state}
	schema_metadata = state.get("schema_metadata") or {}
	tables = schema_metadata.get("tables", {})

	if not tables:
		logger.info("Skipping schema selection; using fallback DDL for db_flag=%s", state.get("db_flag"))
		if not updated.get("ddl_schema"):
			db_config = updated.get("db_config", {})
			ddl_path = db_config.get("ddl_file")
			if ddl_path:
				updated["ddl_schema"] = load_schema(Path(ddl_path))
		return updated

	selector = SchemaSelector(schema_metadata)
	selected_tables = selector.select_relevant_tables(state.get("query", ""))
	ddl_text = format_tables_for_llm(schema_metadata, selected_tables)
 
	logger.debug("Selected tables: %s , ddl excerpt length: %d", selected_tables, len(ddl_text))
	updated["ddl_schema"] = ddl_text
	updated["selected_tables"] = selected_tables
	updated["keyword_matches"] = selector.last_tokens

	logger.info(
		"Schema selection reduced context to %d table(s) for db_flag=%s",
		len(selected_tables),
		state.get("db_flag"),
	)

	return updated


def plan_sql_node(state: AgentState) -> AgentState:
	"""Use LLM to outline a SQL strategy (placeholder prompt)."""

	updated: AgentState = {**state}
	prompt = build_planning_prompt(state["query"], state.get("ddl_schema", ""))
	logger.debug("Planning prompt prepared")

	try:
		response = _planner_llm.invoke(prompt)
		plan_text = getattr(response, "content", None) or str(response)
	except Exception as exc:  # noqa: BLE001
		logger.exception("Planner LLM failed: %s", exc)
		plan_text = "Planning failed – proceeding with direct generation."

	updated["planning_notes"] = plan_text.strip()
	return updated


def generate_sql_node(state: AgentState) -> AgentState:
	"""Generate SQL via Groq LLM."""

	updated: AgentState = {**state}
	feedback = state.get("regeneration_context")
	sql_text = sql_generator.generate_sql(
		query=state["query"],
		ddl_schema=state.get("ddl_schema", ""),
		plan=state.get("planning_notes"),
		feedback=feedback,
	)
	updated["generated_sql"] = sql_text
	logger.info("Generated SQL:\n%s", sql_text)

	tracker = get_token_tracker()
	usage = tracker.track_request(
		query=state["query"],
		schema_text=state.get("ddl_schema", ""),
		generated_sql=sql_text,
		response_text=sql_text,
		db_flag=state.get("db_flag", "unknown"),
	)
	updated["token_usage"] = {
		"query_tokens": usage.query_tokens,
		"schema_tokens": usage.schema_tokens,
		"generated_tokens": usage.generated_tokens,
		"response_tokens": usage.response_tokens,
		"total_input": usage.total_input_tokens,
		"total_output": usage.total_output_tokens,
		"cost_usd": usage.cost_usd,
	}
	return updated


def validate_sql_node(state: AgentState) -> AgentState:
	"""Validate generated SQL for read-only safety."""

	updated: AgentState = {**state}
	validation = sql_validator.validate_sql(state.get("generated_sql", ""))
	updated["validation_result"] = validation
	updated["error_message"] = None if validation.get("valid") else validation.get("reason")
	logger.info("Validation result: %s", validation)
	return updated


def execute_query_node(state: AgentState) -> AgentState:
	"""Execute SQL and capture DataFrame results."""

	updated: AgentState = {**state}
	sql_text = state.get("generated_sql", "")
	db_config = state.get("db_config", {})

	logger.info("Executing SQL for db_flag=%s", state.get("db_flag"))
	start_time = perf_counter()
	logger.debug("Query execution result: %s", sql_text)
	result = query_executor.execute_query(sql_text, db_config)
	elapsed_ms = (perf_counter() - start_time) * 1000

	if result["success"]:
		dataframe = result["dataframe"]
		updated["execution_result"] = dataframe
		updated["execution_time_ms"] = elapsed_ms
		updated["total_rows"] = len(dataframe.index)
		updated["error_message"] = None
	else:
		updated["execution_result"] = None
		updated["execution_time_ms"] = None
		updated["total_rows"] = None
		updated["error_message"] = result["error"]

	logger.info("Execution success=%s", result["success"])
	return updated


def format_results_node(state: AgentState) -> AgentState:
	"""Format results for API response."""

	updated: AgentState = {**state}
	response = result_formatter.format_results(
		dataframe=state.get("execution_result"),
		sql=state.get("generated_sql", ""),
		output_format=state.get("output_format", "json"),
		execution_time_ms=state.get("execution_time_ms"),
	)
	updated["final_response"] = response
	return updated


def error_handler_node(state: AgentState) -> AgentState:
	"""Handle validation/execution errors and prepare retry context."""

	updated: AgentState = {**state}
	current_retry = state.get("retry_count", 0) + 1
	updated["retry_count"] = current_retry

	error_details = state.get("error_message") or "Unknown error"
	last_sql = state.get("generated_sql", "")
	updated["regeneration_context"] = build_error_feedback(error_details, last_sql)
	updated["validation_result"] = None
	updated["execution_result"] = None

	if current_retry > MAX_RETRIES:
		updated["final_response"] = {
			"status": "error",
			"data": {},
			"message": error_details,
			"attempts": current_retry,
		}

	logger.warning("Retry %s due to error: %s", current_retry, error_details)
	return updated


def decide_validation_path(state: AgentState) -> Literal["valid", "invalid"]:
	"""Determine whether to proceed to execution or error handling."""

	validation = state.get("validation_result") or {}
	return "valid" if validation.get("valid") else "invalid"


def decide_retry_or_end(state: AgentState) -> Literal["retry", "end"]:
	"""Decide whether another generation attempt is allowed."""

	retries = state.get("retry_count", 0)
	if retries <= MAX_RETRIES:
		return "retry"
	return "end"

