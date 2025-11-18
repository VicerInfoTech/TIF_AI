"""Groq-powered SQL generation helpers."""

from __future__ import annotations

from typing import Mapping, Optional

from langchain_groq import ChatGroq

from app.prompts.sql_generator import build_generation_prompt

# Using llama-3.3-70b-versatile for SQL generation
_llm = ChatGroq(model="openai/gpt-oss-120b", temperature=0.2)


def generate_sql(
	*,
	query: str,
	schema_context: str,
	plan: Optional[str] = None,
	feedback: Optional[str] = None,
	business_spec: Mapping[str, object] | None = None,
	join_summary: str | None = None,
) -> str:
	"""Generate SQL using the Groq LLM."""

	prompt = build_generation_prompt(
		query=query,
		schema_context=schema_context,
		plan=plan,
		feedback=feedback,
		business_spec=business_spec,
		join_summary=join_summary,
	)
	response = _llm.invoke(prompt)
	content = getattr(response, "content", None) or str(response)
	cleaned = content.strip()
	if cleaned.startswith("```") and cleaned.endswith("```"):
		cleaned = cleaned.strip("`")
		cleaned = cleaned.lstrip("sql").lstrip()
	return cleaned.strip()

