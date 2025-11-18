"""LangGraph node implementations."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from time import perf_counter
from typing import Literal

from langchain_groq import ChatGroq

from app.agent.business_agent import BusinessIntentAgent
from app.agent.state import AgentState
from app.config import get_database_settings
from app.core import query_executor, result_formatter, sql_generator, sql_validator
from app.core.schema_tools import (
	format_table_for_prompt,
	get_schema_toolkit,
	summarize_join_paths,
)
from app.models import BusinessQuerySpec
from app.prompts.error_handler import build_error_feedback
from app.prompts.sql_planner import build_planning_prompt
from app.utils.token_tracker import get_token_tracker
from app.utils.logger import setup_logging

logger = setup_logging(__name__)

MAX_RETRIES = 2
_planner_llm = ChatGroq(model="meta-llama/llama-4-scout-17b-16e-instruct", temperature=0.1)
_intent_agent = BusinessIntentAgent()

def load_config_node(state: AgentState) -> AgentState:
	"""Load database configuration and enforce YAML schema artifacts."""

	logger.info("Loading configuration for db_flag=%s", state.get("db_flag"))
	updated: AgentState = {**state}
	db_config = state.get("db_config", {})

	settings = get_database_settings(state["db_flag"])
	db_config = settings.model_dump()
	schema_toolkit = None

	try:
		schema_toolkit = get_schema_toolkit(state["db_flag"])
	except FileNotFoundError as exc:
		logger.critical("Schema artifacts missing for %s: %s", state.get("db_flag"), exc)
		raise RuntimeError(
			"The required YAML schema artifacts are missing."
			" Please generate them via the schema extractor before running the agent."
		) from exc

	logger.debug("schema_toolkit=%s", schema_toolkit)	
 
	updated["db_config"] = db_config
	updated["schema_metadata"] = {}
	updated["schema_toolkit"] = schema_toolkit
	updated["ddl_schema"] = ""
	updated.setdefault("retry_count", 0)

	intro_path = Path(db_config.get("intro_template")) if db_config.get("intro_template") else None
	intro = ""
	if intro_path and intro_path.exists():
		try:
			intro = intro_path.read_text(encoding="utf-8").strip()
		except Exception as exc:
			logger.warning("Failed to read intro template at %s: %s", intro_path, exc)
	else:
		intro = db_config.get("description", "" ) or ""
	updated["business_intro"] = intro
 
	logger.debug("Business intro length: %d", len(intro))
	logger.debug(
		"SchemaToolkit ready for %s (%d tables) from %s",
		state.get("db_flag"),
		len(schema_toolkit.table_details),
		schema_toolkit.schema_root,
	)
	logger.info("Configuration loaded for db_flag=%s", state.get("db_flag"))
	return updated


def business_intent_node(state: AgentState) -> AgentState:
	"""Run the business intent agent to obtain a structured query spec."""

	updated: AgentState = {**state}
	toolkit = state.get("schema_toolkit")
	if toolkit is None:
		logger.critical("SchemaToolkit missing for db_flag=%s", state.get("db_flag"))
		raise RuntimeError("SchemaToolkit must be available to select schema context.")
	schema_summaries = []

	if toolkit:
		include_column_matches = not state.get("db_config", {}).get("exclude_column_matches", False)
		matches = toolkit.search_tables(
			state.get("query", ""),
			top_k=5,
			include_column_matches=include_column_matches,
		)
		for match in matches:
			detail = toolkit.describe_table(match.table_name)
			if detail:
				schema_summaries.append(format_table_for_prompt(detail))

	query = state.get("query", "")
	current_date = date.today().isoformat()
	try:
		logger.debug("Invoking business intent agent with current_date=%s and %d schema summaries", current_date, len(schema_summaries))
		business_intro = state.get("business_intro") or state.get("db_config", {}).get("description", "")
		spec: BusinessQuerySpec = _intent_agent.analyze(query, schema_summaries, current_date, business_intro=business_intro)
		logger.debug("LLM response (business_intent_node): %s", spec)
	except Exception as exc:  # noqa: BLE001
		logger.error("Business intent agent failed: %s", exc, exc_info=True)
		spec = BusinessQuerySpec(
			intent=state.get("query", ""),
			entities=[],
			metrics=[],
			dimensions=[],
			filters=[],
		)

	updated["business_spec"] = spec
 	
	logger.debug("Business spec: %s", spec)
	
	updated["schema_summaries"] = schema_summaries or state.get("schema_summaries")
	logger.info(
		"Business intent result '%s' (%d entities, %d metrics, %d dims, %d filters)",
		spec.intent,
		len(spec.entities),
		len(spec.metrics),
		len(spec.dimensions),
		len(spec.filters),
	)
	if schema_summaries:
		logger.debug("Schema summaries provided: %s", [s.splitlines()[0] for s in schema_summaries])
	return updated


def select_schema_node(state: AgentState) -> AgentState:
	"""Select relevant tables and render schema context for downstream nodes."""

	updated: AgentState = {**state}
	toolkit = state.get("schema_toolkit")
	query = state.get("query", "")
	spec = state.get("business_spec")

	if toolkit:
		search_terms = [query]
		if isinstance(spec, BusinessQuerySpec):
			search_terms.append(spec.intent)
			search_terms.extend(spec.entities or [])
			search_terms.extend(dim.name for dim in spec.dimensions or [])
			search_terms.extend(metric.name for metric in spec.metrics or [])
			search_terms.extend(flt.field for flt in spec.filters or [])
		elif isinstance(spec, dict):
			search_terms.extend(spec.get("entities", []))
			search_terms.extend(item.get("name", "") for item in spec.get("metrics", []))
			search_terms.extend(item.get("name", "") for item in spec.get("dimensions", []))
			search_terms.extend(item.get("field", "") for item in spec.get("filters", []))

		search_text = " ".join(term for term in search_terms if term)
		include_column_matches = not state.get("db_config", {}).get("exclude_column_matches", False)
		matches = toolkit.search_tables(
			search_text,
			top_k=6,
			include_column_matches=include_column_matches,
		)
		selected_tables = [match.table_name for match in matches] or toolkit.list_tables()[:5]
		schema_snippets = []
		for table_name in selected_tables:
			detail = toolkit.describe_table(table_name)
			if detail:
				schema_snippets.append(format_table_for_prompt(detail))
		schema_context = "\n\n".join(schema_snippets)
		join_summary = summarize_join_paths(toolkit, selected_tables)

		updated["ddl_schema"] = schema_context or state.get("ddl_schema", "")
		updated["schema_context"] = schema_context
		updated["join_summary"] = join_summary
		updated["selected_tables"] = selected_tables
		updated["keyword_matches"] = [match.reason for match in matches if match and match.reason]
		updated["schema_summaries"] = schema_snippets or state.get("schema_summaries")

		logger.info(
			"Schema toolkit selected %d table(s) for db_flag=%s",
			len(selected_tables),
			state.get("db_flag"),
		)
		table_files = [
			str(toolkit.table_paths.get(name.lower()).name)
			for name in selected_tables
			if toolkit.table_paths.get(name.lower())
		][:5]
		logger.debug("Selected table files: %s", table_files)
		logger.debug("Join summary length: %d", len(join_summary))
		return updated

	return updated


def plan_sql_node(state: AgentState) -> AgentState:
	"""Use LLM to outline a SQL strategy (placeholder prompt)."""

	updated: AgentState = {**state}
	spec = state.get("business_spec")
	if isinstance(spec, BusinessQuerySpec):
		spec_payload = spec.model_dump()
	else:
		spec_payload = spec
	schema_context = state.get("schema_context") or state.get("ddl_schema", "")
	prompt = build_planning_prompt(
		state["query"],
		schema_context,
		business_spec=spec_payload,
		join_summary=state.get("join_summary"),
	)
	logger.debug("Planning prompt for query='%s' with schema_context len=%d", state["query"], len(schema_context))
	logger.debug("Planning prompt prepared with schema context length %d", len(schema_context))

	try:
		response = _planner_llm.invoke(prompt)
		logger.debug("LLM response (plan_sql_node): %s", getattr(response, "content", None) or str(response))
		plan_text = getattr(response, "content", None) or str(response)
	except Exception as exc:  # noqa: BLE001
		logger.exception("Planner LLM failed: %s", exc)
		plan_text = "Planning failed – proceeding with direct generation."

	updated["planning_notes"] = plan_text.strip()
	logger.info("Planning completed for db_flag=%s", state.get("db_flag"))
	logger.debug("Planning notes length=%d", len(updated["planning_notes"].splitlines()))
	return updated


def generate_sql_node(state: AgentState) -> AgentState:
	"""Generate SQL via Groq LLM."""

	updated: AgentState = {**state}
	feedback = state.get("regeneration_context")
	schema_context = state.get("schema_context") or state.get("ddl_schema", "")
	spec = state.get("business_spec")
	logger.debug(
		"Generating SQL for query='%s' (plan len=%d, schema context len=%d)",
		state.get("query"),
		len(state.get("planning_notes", "")),
		len(schema_context),
	)
	if isinstance(spec, BusinessQuerySpec):
		spec_payload = spec.model_dump()
	else:
		spec_payload = spec
	sql_text = sql_generator.generate_sql(
		query=state["query"],
		schema_context=schema_context,
		plan=state.get("planning_notes"),
		feedback=feedback,
		business_spec=spec_payload,
		join_summary=state.get("join_summary"),
	)
	logger.debug("LLM response (generate_sql_node): %s", sql_text)
	updated["generated_sql"] = sql_text
	logger.info("Generated SQL:\n%s", sql_text)

	tracker = get_token_tracker()
	usage = tracker.track_request(
		query=state["query"],
		schema_text=schema_context,
		generated_sql=sql_text,
		response_text=sql_text,
		db_flag=state.get("db_flag", "unknown"),
	)
	logger.info(
		"SQL generation tokens (input=%d output=%d cost=$%.4f)",
		usage.query_tokens + usage.schema_tokens,
		usage.generated_tokens,
		usage.cost_usd,
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
		logger.info(
			"Execution success=True rows=%d elapsed_ms=%.1f",
			updated["total_rows"],
			updated["execution_time_ms"],
		)
	else:
		updated["execution_result"] = None
		updated["execution_time_ms"] = None
		updated["total_rows"] = None
		updated["error_message"] = result["error"]
		logger.warning(
			"Execution failed: %s",
			result.get("error"),
		)
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
	logger.info(
		"Formatted response status=%s, rows=%s",
		response.get("status"),
		response.get("data", {}).get("row_count"),
	)
	return updated


def error_handler_node(state: AgentState) -> AgentState:
	"""Handle validation/execution errors and prepare retry context."""

	updated: AgentState = {**state}
	current_retry = state.get("retry_count", 0) + 1
	updated["retry_count"] = current_retry

	error_details = state.get("error_message") or "Unknown error"
	last_sql = state.get("generated_sql", "")
	updated["regeneration_context"] = build_error_feedback(error_details, last_sql)
	updated["validation_result"] = {"valid": False, "reason": error_details}
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

