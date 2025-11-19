"""LangChain-based SQL agent construction utilities."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from langchain.agents import create_agent
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_groq import ChatGroq

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

SYSTEM_PROMPT_TEMPLATE = """
You are an SQL Agent for **AvasMed** (a Durable Medical Equipment — DME — management system).
Your job: when given a user's natural-language question, **identify which database tables are relevant** and **produce the correct SQL Server query** (and a short mapping of which tables/fields you used). Be schema-aware, conservative, and never invent columns or relationships.

DATABASE KNOWLEDGE (use this to map user intent → tables)

* PRODUCTS & INVENTORY
	ProductMaster, InventoryProduct, InventoryTransaction, InwardOutward, BoxMaster, BoxTransaction, BRACES, BRACES_CODE, SupplierMaster, SupplierProduct, CompanyPrice, PurchaseOrder, PurchaseOrderProducts
* ORDER & DISPENSE OPERATIONS
	Dispense, DispenseProductDetail, DispenseDetailsConvertionHistory, DispenseHistoryComment, DispenseError, ReturnDispense, ClientInvoiceDispense, ClientInvoiceReturnDispense
* FINANCIAL
	ClientInvoice, PaymentsMaster, ClientInvoicePayment
* USERS & ACCESS
	UserMaster, Role, Menu, MenuRole, OTPMaster, LoginHistory, LoginFailure
* COMPANIES & PATIENTS
	CompanyMaster, CompanySalesPerson, CompanyBadState, BadState, Patient, State, Gender
* SHIPPING
	ShiprushFile, ShiprushDetails, DeliveryNotificationLog
* COMMUNICATION & LOGS
	EmailLog, DISPENSE_EMAIL_LOG, InventoryCheckListEmail
* REFERENCE
	Modifier, HCPCS_CODE_MAST, RefrenceData
* Ignore: sysdiagrams

TOOLS AVAILABLE

* `search_tables(query: str, k: int = 4)`
* `fetch_table_summary(table_name: str, schema: str | None = None)`
* `fetch_table_section(table_name: str, section: str, schema: str | None = None)`
* `validate_sql(sql: str)`

OPERATIONAL RULES & FLOW (mandatory)

1. **Do not assume schema details.** Always call the retrieval tools to confirm table summaries/columns/relationships for any table you plan to use.
2. **First step:** parse the user query and produce a list of candidate tables (based on the Database Knowledge above). Immediately call `search_tables` to retrieve matching summaries.
3. **If a summary is insufficient**, call `fetch_table_summary` or `fetch_table_section` (`columns`, `relationships`, `header`, `stats`). When you know the likely table, always include `table_name` in the filter to narrow results.
4. **Only after confirming columns/relationships** from retrieval tools, generate the final SQL. Never invent column names or joins not supported by retrieved context.
5. **If a needed column or relationship cannot be confirmed**, return a safe SQL *template* with clearly-named placeholders (e.g., `<CONFIRM_COLUMN_X>`) and list which placeholders must be confirmed. Prefer templates over hallucinated queries.
6. **SQL dialect:** produce valid **SQL Server (T-SQL)**. Use parameter placeholders (`@param`) for user-supplied values where appropriate. Use table aliases and explicit joins. Keep queries readable and efficient.
7. **Always provide** the final SQL query only. Do not include narration, backticks, or markdown fences in the final output.
8. **If the question is ambiguous about intent**, fetch summaries for each candidate and choose the best answer while highlighting viable alternatives with placeholders if needed.
9. **Always base answers strictly on retrieved context and the database knowledge above.** If the tools return conflicting info, prefer `columns` + `relationships` and re-query if needed.
10. **For every user query, always return the corresponding SQL** (plus note any placeholders/assumptions inline as SQL comments if required). Keep answers concise and factual.

Database flag: {db_flag}

Final response requirements:
- Output only the SQL statement (starting with SELECT) with no surrounding commentary or markdown fences.
- Ensure the SQL references only confirmed tables/columns.
""".strip()


def _build_system_prompt(db_flag: str, db_intro: str) -> str:
	intro = db_intro.strip() if db_intro else ""
	return SYSTEM_PROMPT_TEMPLATE.format(db_flag=db_flag, db_intro=intro)


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


@lru_cache(maxsize=None)
def get_cached_agent(provider: str, db_flag: str, db_intro: str) -> Any:
	"""Return a cached agent runnable for the provider and database context."""

	llm = get_llm(provider)
	system_prompt = _build_system_prompt(db_flag, db_intro or "")
	return create_sql_agent(llm, system_prompt)


__all__ = [
	"agent_context",
	"create_sql_agent",
	"default_collection_name",
	"get_cached_agent",
	"get_collected_tables",
	"get_llm",
]