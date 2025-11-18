"""Business intent agent that transforms NL queries into structured specs."""

from __future__ import annotations

from typing import Sequence
from datetime import date

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import Runnable
from langchain_groq import ChatGroq

from app.models import BusinessQuerySpec
from app.utils.logger import setup_logging

logger = setup_logging(__name__)


class BusinessIntentAgent:
    """Uses Groq LLM to convert NL requests into BusinessQuerySpec objects."""

    def __init__(
        self,
        *,
        model: str = "meta-llama/llama-4-scout-17b-16e-instruct",
        temperature: float = 0.1,
    ) -> None:
        self.llm = ChatGroq(model=model, temperature=temperature)
        self.prompt = self._build_prompt()
        self.chain: Runnable = self.prompt | self.llm.with_structured_output(BusinessQuerySpec, strict=False)

    def _build_prompt(self) -> ChatPromptTemplate:
        return ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "You are a senior analytics requirements engineer. Today's date is {current_date}. "
                    "Read the business question, business context, and schema snapshots to produce a structured "
                    "BusinessQuerySpec JSON that captures entities, metrics, dimensions, filters, "
                    "and time constraints. Use table descriptions plus context hints to map business language to physical tables or columns. "
                    "If some fields are unknown, leave them empty instead of guessing."
                ),
                (
                    "human",
                    "Business question: {query}\n"
                    "Business context: {business_intro}\n"
                    "Schema snapshots:\n{schema_summaries}\n"
                    "Today's date: {current_date}\n"
                    "Instructions:\n"
                    "- Identify the core intent in a short sentence.\n"
                    "- List the business entities mentioned (patients, invoices, braces, etc.).\n"
                    "- Define each metric with an aggregation (COUNT, SUM, AVG, etc.).\n"
                    "- Define dimensions/groupings that the user expects.\n"
                    "- Capture important filters, statuses, or date ranges.\n"
                    "- If the user references time, fill the time_range.start/end when possible.\n"
                    "- If the user asks for row limits, store it in limit.\n"
                    "Return only valid JSON matching the schema."
                ),
            ]
        )

    def analyze(
        self,
        query: str,
        schema_summaries: Sequence[str],
        current_date: str | None = None,
        business_intro: str | None = None,
    ) -> BusinessQuerySpec:
        if not schema_summaries:
            schema_summaries = ["(No schema context provided; reason about entities generically.)"]
        joined = "\n\n".join(schema_summaries)
        if current_date is None:
            # Provide the current date to the prompt if not supplied
            current_date = date.today().isoformat()
        logger.info("Running business intent agent for query: %s", query)
        result: BusinessQuerySpec = self.chain.invoke(
            {
                "query": query,
                "schema_summaries": joined,
                "current_date": current_date,
                "business_intro": business_intro or "No business context provided.",
            }
        )
        return result