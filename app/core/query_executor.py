"""SQL execution utilities."""

from __future__ import annotations

import asyncio
from typing import Dict, List
import re

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from db import database_manager


def execute_query(sql: str, db_config: Dict[str, object]) -> Dict[str, object]:
    """Execute SQL synchronously; used as fallback for non-Postgres drivers."""

    connection_string = str(db_config["connection_string"])
    query_timeout = int(db_config.get("query_timeout", 30))
    max_rows = int(db_config.get("max_rows", 1000))

    try:
        conn = database_manager.get_connection(connection_string)
        result = conn.execution_options(timeout=query_timeout).execute(text(sql))
        columns: List[str] = result.keys()
        rows = result.fetchmany(max_rows)
        conn.close()
    except SQLAlchemyError as exc:
        # Sanitize the error message to avoid leaking raw SQL to logs or API
        return {
            "success": False,
            "error": _short_error_message(exc),
            "dataframe": None,
        }

    df = pd.DataFrame(rows, columns=columns)
    return {
        "success": True,
        "error": None,
        "dataframe": df,
    }


async def execute_query_async(sql: str, db_config: Dict[str, object]) -> Dict[str, object]:
    """Execute SQL asynchronously using asyncpg for Postgres, or sync fallback for other drivers."""

    connection_string = str(db_config["connection_string"])
    query_timeout = int(db_config.get("query_timeout", 30))
    max_rows = int(db_config.get("max_rows", 1000))

    try:
        engine = database_manager.get_async_engine(connection_string)
        async with engine.connect() as conn:
            result = await conn.execution_options(timeout=query_timeout).execute(text(sql))
            columns: List[str] = result.keys()
            rows = await result.fetchmany(max_rows)
        df = pd.DataFrame(rows, columns=columns)
        return {
            "success": True,
            "error": None,
            "dataframe": df,
        }
    except (SQLAlchemyError, RuntimeError) as exc:
        # RuntimeError catches "asyncio extension requires an async driver" for non-Postgres DBs
        # Fall back to sync execution in threadpool
        if "async driver" in str(exc).lower() or "asyncpg" in str(exc).lower():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, execute_query, sql, db_config)
        # Re-raise other errors
        return {
            "success": False,
            "error": _short_error_message(exc),
            "dataframe": None,
        }


def _short_error_message(exc: Exception) -> str:
    """Return a concise, single-line error message with SQL removed.

    SQLAlchemy and driver exceptions often append the full SQL query in the
    exception string (e.g., "[SQL: SELECT ...]"). This would leak the SQL
    into logs and responses. Clean those patterns and return a short text.
    """
    if exc is None:
        return "Unknown SQL error"
    try:
        exc_text = str(exc)
    except Exception:
        return exc.__class__.__name__

    # Remove bracketed SQL blocks: [SQL: SELECT ...]
    exc_text = re.sub(r"\[SQL:.*?\]", "", exc_text, flags=re.DOTALL | re.IGNORECASE)
    # Remove SQLAlchemy/driver background references
    exc_text = re.sub(r"\(Background on this error.*", "", exc_text, flags=re.DOTALL | re.IGNORECASE)
    # Take the first meaningful line
    line = next((ln.strip() for ln in exc_text.splitlines() if ln.strip()), None)
    if not line:
        return exc.__class__.__name__
    # Limit length to 300 chars for logs/response
    if len(line) > 300:
        line = line[:300] + "..."
    return line