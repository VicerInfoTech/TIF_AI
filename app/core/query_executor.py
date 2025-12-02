"""SQL execution utilities."""

from __future__ import annotations

import asyncio
from typing import Dict, List

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
        return {
            "success": False,
            "error": str(exc),
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
            "error": str(exc),
            "dataframe": None,
        }