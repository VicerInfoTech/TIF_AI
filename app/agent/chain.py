"""LangChain-based SQL agent construction utilities."""

from __future__ import annotations

from datetime import datetime
import os
from functools import lru_cache
from typing import Any, List, Callable
from pydantic import BaseModel, Field, ValidationError
from langchain.agents import create_agent
from langchain.agents.middleware import ModelRequest, ModelResponse, wrap_model_call
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_groq import ChatGroq
from langchain_openai import ChatOpenAI
from langchain_deepseek import ChatDeepSeek
from langchain_anthropic import ChatAnthropic
from langchain.agents.structured_output import ToolStrategy
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

class LLMResponse(BaseModel):
	sql_query : str = Field(description="The generated SQL query string")
	follow_up_questions : List[str] = Field(
		default_factory=list,
		description="List of follow-up questions related to the SQL query"
	)

@wrap_model_call
def debug_model_call(
	request: ModelRequest,
	handler: Callable[[ModelRequest], ModelResponse],
) -> ModelResponse:
	"""Log inputs and outputs around every model invocation."""
	logger.debug("Agent middleware - before model call input=%s", getattr(request, "input", None))
	response = handler(request)
	logger.debug("Agent middleware - after model call output=%s", getattr(response, "output", response))
	return response



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
		response_format=ToolStrategy(LLMResponse),
		middleware=[debug_model_call],
	)
	logger.info("Created LangChain SQL agent using model %s", getattr(llm, "model_name", repr(llm)))
	return agent


@lru_cache(maxsize=None)
def get_llm(provider: str = None) -> BaseChatModel:
	"""Return the preferred LLM client with provider fallback. If provider is None, auto-select by API key presence."""
	try:
		# Provider order: OpenAI -> OpenRouter -> DeepSeek -> Groq -> Anthropic
		provider_keys = [
			("openai", "OPENAI_API_KEY"),
			("openrouter", "OPENROUTER_API_KEY"),
			("deepseek", "DEEPSEEK_API_KEY"),
			("groq", "GROQ_API_KEY"),
			("anthropic", "ANTHROPIC_API_KEY"),
		]
		provider_map = {
			"openai": lambda: ChatOpenAI(
				model="gpt-4o",  # or your preferred OpenAI model
				api_key=os.environ.get("OPENAI_API_KEY"),
				temperature=0.1,
			),
			"openrouter": lambda: ChatOpenAI(
				model="kwaipilot/kat-coder-pro:free",  # or your preferred OpenRouter model
				api_key=os.environ.get("OPENROUTER_API_KEY"),
				base_url="https://openrouter.ai/api/v1",
				temperature=0.1,
			),
			"deepseek": lambda: ChatDeepSeek(
				model="deepseek-chat",
				api_key=os.environ.get("DEEPSEEK_API_KEY"),
				api_base="https://api.deepseek.com/v1",
				temperature=0.1,
			),
			"groq": lambda: ChatGroq(
				model="meta-llama/llama-4-scout-17b-16e-instruct",
				temperature=0.1,
			),
			"anthropic": lambda: ChatAnthropic(
				model="claude-3-opus-20240229",  # or your preferred Anthropic model
				api_key=os.environ.get("ANTHROPIC_API_KEY"),
				temperature=0.1,
			),
			"gemini": lambda: ChatGoogleGenerativeAI(
				model="gemini-2.5-pro",
				temperature=0.1,
			),
		}

		if provider:
			provider_normalized = provider.lower()
			if provider_normalized in provider_map:
				# Only use if key is present or not required
				key_env = dict(provider_keys).get(provider_normalized)
				if not key_env or os.environ.get(key_env):
					logger.debug(f"Initializing {provider_normalized} model (explicit)")
					return provider_map[provider_normalized]()
				else:
					raise RuntimeError(f"API key for provider '{provider}' not found in environment.")
			elif provider_normalized in {"gemini", "google"}:
				logger.debug("Initializing ChatGoogleGenerativeAI model for provider=%s", provider_normalized)
				return provider_map["gemini"]()
			elif provider_normalized in {"groq", "llama", "llama4"}:
				logger.debug("Initializing ChatGroq model for provider=%s", provider_normalized)
				return provider_map["groq"]()
			else:
				raise ValueError(f"Unsupported provider '{provider}'")
		# Auto-select: pick the first provider with a key
		for prov, key in provider_keys:
			if key is None or os.environ.get(key):
				logger.debug(f"Auto-selecting {prov} model (API key found)")
				return provider_map[prov]()
		# Fallback to Gemini if no key-based provider found
		logger.debug("Falling back to Gemini (no API key provider found)")
		return provider_map["gemini"]()
	except Exception as exc:
		import traceback
		logger.error("LLM initialization failed for provider=%s: %s", provider, traceback.format_exc())
		raise RuntimeError(
			f"Failed to initialize LLM for provider '{provider}'"
		) from exc


SUMMARY_JSON_LIMIT = 4000


def _truncate_json(raw_json: str, limit: int = SUMMARY_JSON_LIMIT) -> str:
	return raw_json[:limit] if raw_json and len(raw_json) > limit else (raw_json or "[]")


def _resolve_structured_payload(agent_result: Any) -> Any | None:
	if isinstance(agent_result, dict):
		for key in ("structured_response", "structuredResponse"):
			if key in agent_result:
				return agent_result[key]
	for attr in ("structured_response", "structuredResponse"):
		payload = getattr(agent_result, attr, None)
		if payload is not None:
			return payload
	return None


def parse_structured_response(agent_result: Any) -> LLMResponse | None:
	"""Extract and validate the structured LLM response if available."""
	payload = _resolve_structured_payload(agent_result)
	if payload is None:
		return None
	if isinstance(payload, LLMResponse):
		return payload
	try:
		return LLMResponse.model_validate(payload)
	except ValidationError as exc:
		logger.warning("Failed to parse structured LLM response: %s", exc)
		return None


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
	"parse_structured_response",
]