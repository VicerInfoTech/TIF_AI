"""Prompt template for SQL generation (for Microsoft SQL Server)."""

from __future__ import annotations

import json
from typing import Mapping, Optional


def _format_spec(spec: Mapping[str, object] | None) -> str:
	if not spec:
		return "(business spec unavailable)"
	return json.dumps(spec, indent=2)


def build_generation_prompt(
	*,
	query: str,
	schema_context: str,
	plan: Optional[str] = None,
	feedback: Optional[str] = None,
	business_spec: Mapping[str, object] | None = None,
	join_summary: str | None = None,
) -> str:
	"""Prompt for LLM to generate a safe, read-only SQL Server query."""
	from datetime import date
	today = date.today().isoformat()
	prompt = [
		f"You are a senior SQL Server (T-SQL) developer. Today's date is {today}.",
		"Generate one safe SELECT statement that answers the user's request using only the provided schema context.",
		"Rules:",
		"- Only SELECT statements (no INSERT/UPDATE/DELETE/DDL).",
		"- No semicolons or multi-statements.",
		"- Use table aliases and fully qualify columns where helpful.",
		"- Include JOINs, filters, GROUP BY, and ORDER BY only when justified by the plan or business intent.",
		"- If limiting rows, use TOP or OFFSET/FETCH.",
		"- Ensure the query is idempotent and read-only.",
		"",
		"User Query:",
		query,
		"",
		"Business Intent (JSON):",
		_format_spec(business_spec),
		"",
		"Schema Context:",
		schema_context,
		f"Today's date: {today}",
	]
	if join_summary:
		prompt.extend(["", "Join Hints:", join_summary])
	if plan:
		prompt.extend(["", "Execution Plan Notes:", plan])
	if feedback:
		prompt.extend(["", "Previous Attempt Feedback (fix these issues):", feedback])
	prompt.append("\nReturn only the SQL statement with no commentary or markdown fences.")
	return "\n".join(prompt)
