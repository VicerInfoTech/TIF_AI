"""Prompt templates for SQL planning."""

from __future__ import annotations

import json
from typing import Any, Mapping


def _format_spec(spec: Mapping[str, Any] | None) -> str:
	if not spec:
		return "(No structured business spec provided.)"
	return json.dumps(spec, indent=2)


def build_planning_prompt(
	query: str,
	schema_context: str,
	*,
	business_spec: Mapping[str, Any] | None = None,
	join_summary: str | None = None,
) -> str:
	"""Prompt for LLM to plan SQL query steps using business intent + schema context."""
	from datetime import date
	today = date.today().isoformat()
	schema_excerpt = (schema_context or "No schema context provided.")[:3500]
	spec_text = _format_spec(business_spec)
	prompt = [
		f"You are an expert analytics planner tasked with outlining a SQL strategy. Today's date is {today}.",
		"Use the user's request, business intent, and schema context to derive step-by-step instructions.",
		"Focus on identifying key tables, joins, filters, metrics, and grouping logic.",
		"Do not output SQL – only produce a numbered analytical plan.",
		"",
		"User Query:",
		query,
		"",
		"Business Intent (JSON):",
		spec_text,
		"",
		"Schema Context:",
		schema_excerpt,
		f"Today's date: {today}",
	]
	if join_summary:
		prompt.extend(["", "Join Hints:", join_summary])
	prompt.extend(
		[
			"",
			"Example format:",
			"1. Identify base table(s)",
			"2. List columns needed", 
			"3. Describe filters/date constraints",
			"4. Outline joins and relationships",
			"5. Mention aggregation/grouping logic",
			"Return only the plan instructions.",
		]
	)
	return "\n".join(prompt)

