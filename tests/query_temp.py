"""Manual driver for the LangChain SQL agent."""

from __future__ import annotations

from pathlib import Path
from typing import List

from dotenv import load_dotenv

from app.agent.chain import (
    agent_context,
    default_collection_name,
    get_cached_agent,
    get_collected_tables,
)
from app.user_db_config_loader import get_database_settings
from app.core import sql_validator
from app.main import _extract_agent_output, _sanitize_sql

load_dotenv()


def _load_intro(path: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def run_demo_query(user_query: str, db_flag: str = "avamed_db") -> None:
    """Run the SQL agent locally and print the generated SQL."""

    settings = get_database_settings(db_flag)
    intro = _load_intro(settings.intro_template) or (settings.description or "")
    collection_name = default_collection_name(db_flag)
    providers = ("groq", "gemini")

    response = None
    selected_tables: List[str] = []

    for provider in providers:
        agent = get_cached_agent(provider, db_flag, intro)
        try:
            with agent_context(db_flag, collection_name):
                response = agent.invoke(
                    {
                        "messages": [
                            {
                                "role": "user",
                                "content": user_query,
                            }
                        ]
                    }
                )
                selected_tables = get_collected_tables()
            print(f"Provider used: {provider}")
            break
        except Exception as exc:  # noqa: BLE001
            print(f"Provider {provider} failed: {exc}")
            continue

    if response is None:
        print("All providers failed to generate SQL.")
        return

    output = _extract_agent_output(response)
    sql_text = _sanitize_sql(output)
    print("\nTables used:", selected_tables)
    print("\nGenerated SQL:\n", sql_text or "<empty>")
    print("Validation:", sql_validator.validate_sql(sql_text or ""))


if __name__ == "__main__":
    QUERY = "I want to know how many active company I have?"
    run_demo_query(QUERY)
