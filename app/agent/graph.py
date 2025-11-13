"""LangGraph workflow definition for the SQL agent."""

from __future__ import annotations

from langgraph.graph import END, StateGraph

from app.agent.state import AgentState
from app.agent import nodes


def create_sql_agent_graph() -> StateGraph:
	"""Construct and compile the LangGraph workflow."""

	workflow = StateGraph(AgentState)

	workflow.add_node("load_config", nodes.load_config_node)
	workflow.add_node("select_schema", nodes.select_schema_node)
	workflow.add_node("plan_sql", nodes.plan_sql_node)
	workflow.add_node("generate_sql", nodes.generate_sql_node)
	workflow.add_node("validate_sql", nodes.validate_sql_node)
	workflow.add_node("execute_query", nodes.execute_query_node)
	workflow.add_node("format_results", nodes.format_results_node)
	workflow.add_node("handle_error", nodes.error_handler_node)

	workflow.set_entry_point("load_config")

	workflow.add_edge("load_config", "select_schema")
	workflow.add_edge("select_schema", "plan_sql")
	workflow.add_edge("plan_sql", "generate_sql")
	workflow.add_edge("generate_sql", "validate_sql")

	workflow.add_conditional_edges(
		"validate_sql",
		nodes.decide_validation_path,
		{
			"valid": "execute_query",
			"invalid": "handle_error",
		},
	)

	workflow.add_conditional_edges(
		"handle_error",
		nodes.decide_retry_or_end,
		{
			"retry": "generate_sql",
			"end": END,
		},
	)

	workflow.add_edge("execute_query", "format_results")
	workflow.add_edge("format_results", END)

	return workflow.compile()

