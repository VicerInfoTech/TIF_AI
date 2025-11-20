"""LangChain-based SQL agent construction utilities."""

from __future__ import annotations

from datetime import datetime
from functools import lru_cache
from typing import Any

from langchain.agents import create_agent
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_groq import ChatGroq
from app.agent.prompt import RESULT_SUMMARY_PROMPT, SYSTEM_PROMPT_TEMPLATE

from app.agent.tools import (
	agent_context,
	default_collection_name,
	fetch_table_section_tool,
	fetch_table_summary_tool,
	get_collected_tables,
	search_tables_tool,
	validate_sql_tool,
)
from app.utils.logger import setup_logging

logger = setup_logging(__name__)

def _build_system_prompt(db_flag: str ) -> str:
	return SYSTEM_PROMPT_TEMPLATE.format(
		db_flag=db_flag,
		current_time=datetime.utcnow().isoformat(),
	)

def create_sql_agent(llm: BaseChatModel, system_prompt: str) -> Any:
	"""Instantiate the LangChain agent runnable for SQL generation."""

	tools = [
		search_tables_tool,
		fetch_table_summary_tool,
		fetch_table_section_tool,
		validate_sql_tool,
	]
	agent = create_agent(
		model=llm,
		tools=tools,
		system_prompt=system_prompt,
	)
	logger.info("Created LangChain SQL agent using model %s", getattr(llm, "model_name", repr(llm)))
	return agent


@lru_cache(maxsize=None)
def get_llm(provider: str) -> BaseChatModel:
	"""Return the preferred LLM client with provider fallback."""

	provider_normalized = (provider or "").lower()
	if provider_normalized in {"groq", "llama", "llama4"}:
		logger.debug("Initializing ChatGroq model for provider=%s", provider_normalized)
		return ChatGroq(model="meta-llama/llama-4-scout-17b-16e-instruct", temperature=0.1)
	if provider_normalized in {"gemini", "google"}:
		logger.debug("Initializing ChatGoogleGenerativeAI model for provider=%s", provider_normalized)
		return ChatGoogleGenerativeAI(model="gemini-2.5-pro", temperature=0.1)
	raise ValueError(f"Unsupported provider '{provider}'")


SUMMARY_JSON_LIMIT = 4000


def _truncate_json(raw_json: str, limit: int = SUMMARY_JSON_LIMIT) -> str:
	return raw_json[:limit] if raw_json and len(raw_json) > limit else (raw_json or "[]")


def summarize_query_results(provider: str, describe_text: str, raw_json: str) -> str | None:
	"""Ask the LLM to generate a natural-language summary for the returned dataset."""
	if not describe_text and not raw_json:
		return None
	llm = get_llm(provider)
	prompt = RESULT_SUMMARY_PROMPT.format(
		describe_text=describe_text or "No describe output available",
		raw_json=_truncate_json(raw_json),
	)
	try:
		response = llm.invoke(
			{"messages": [{"role": "user", "content": prompt}]}
		)
		content = getattr(response, "content", None)
		if isinstance(content, str):
			return content.strip()
		if isinstance(response, str):
			return response.strip()
		return str(content).strip() if content is not None else None
	except Exception as exc:  # pragma: no cover - best-effort summary
		logger.warning("Result summary generation failed: %s", exc)
		return None


@lru_cache(maxsize=None)
def get_cached_agent(provider: str, db_flag: str) -> Any:
	"""Return a cached agent runnable for the provider and database context."""

	llm = get_llm(provider)
	system_prompt = _build_system_prompt(db_flag)
	return create_sql_agent(llm, system_prompt)


__all__ = [
	"agent_context",
	"create_sql_agent",
	"default_collection_name",
	"get_cached_agent",
	"get_collected_tables",
	"get_llm",
	"summarize_query_results",
]