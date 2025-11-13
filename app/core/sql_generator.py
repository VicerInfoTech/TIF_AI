"""Groq-powered SQL generation helpers."""

from __future__ import annotations

from langchain_groq import ChatGroq

from app.prompts.sql_generator import build_generation_prompt

# Using llama-3.3-70b-versatile for SQL generation
_llm = ChatGroq(model="meta-llama/llama-guard-4-12b", temperature=0.1)


def generate_sql(query: str, ddl_schema: str, plan: str | None = None, feedback: str | None = None) -> str:
	"""Generate SQL using the Groq LLM."""

	prompt = build_generation_prompt(query=query, ddl_schema=ddl_schema, plan=plan, feedback=feedback)
	response = _llm.invoke(prompt)
	content = getattr(response, "content", None) or str(response)
	cleaned = content.strip()
	if cleaned.startswith("```") and cleaned.endswith("```"):
		cleaned = cleaned.strip("`")
		cleaned = cleaned.lstrip("sql").lstrip()
	return cleaned.strip()

