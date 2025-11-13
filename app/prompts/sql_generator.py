"""Prompt template for SQL generation (for Microsoft SQL Server)."""

from typing import Optional


def build_generation_prompt(query: str, ddl_schema: str, plan: Optional[str] = None, feedback: Optional[str] = None) -> str:
	"""Prompt for LLM to generate a safe, read-only SQL Server (T-SQL) query."""

	# ddl_excerpt = ddl_schema[:3000]
	ddl_excerpt = ddl_schema
	prompt = [
		"You are a senior SQL Server (T-SQL) developer."
		"\nGenerate a single, safe, read-only SQL statement that answers the user's question."
		"\nRules:"
		"\n- Only SELECT statements."
		"\n- No data modification (no INSERT, UPDATE, DELETE, DROP, ALTER, etc)."
		"\n- No semicolons, multi-statements, or DDL."
		"\n- Use only tables and columns present in the schema excerpt."
		"\n- If aggregation or joins are needed, include them."
		"\n- For limiting rows, use 'TOP 1000' or 'OFFSET ... FETCH' instead of LIMIT."
		"\n- Use aliases when appropriate for readability."
		"\n- Ensure compatibility with Microsoft SQL Server syntax."
		"\n\nUser Query:\n"
		f"{query}\n"
		"\nDatabase DDL (excerpt):\n"
		f"{ddl_excerpt}\n"
	]

	if plan:
		prompt.append(f"\nProposed Plan:\n{plan}")

	if feedback:
		prompt.append(f"\nPrevious Attempt Feedback:\n{feedback}\nPlease correct any issues above.")

	prompt.append("\nReturn only the SQL statement, no explanation or Markdown.")
	return "\n".join(prompt)
