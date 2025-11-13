"""Prompt templates for SQL planning (placeholder)."""


def build_planning_prompt(query: str, ddl_schema: str) -> str:
	"""Prompt for LLM to plan SQL query steps from user request and DDL."""

	ddl_excerpt = ddl_schema[:1500]
	return (
		"You are an expert SQL analyst."
		"\nGiven the user's request and the database schema excerpt below, outline the logical steps to answer the query."
		"\nFocus on identifying relevant tables, columns, filters, joins, and aggregation logic."
		"\nDo not write SQL yet—just describe the plan in clear, numbered steps."
		"\n\nUser Query:\n"
		f"{query}\n"
		"\nDatabase DDL (excerpt):\n"
		f"{ddl_excerpt}\n"
		"\nExample plan format:\n"
		"1. Identify main table(s)\n2. List required columns\n3. Specify filters/conditions\n4. Note any joins\n5. Describe aggregation/grouping if needed\n"
		"\nReturn only the plan."
	)

