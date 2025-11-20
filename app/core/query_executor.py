"""SQL execution utilities."""

from __future__ import annotations

from typing import Dict, List

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from db import database_manager


def execute_query(sql: str, db_config: Dict[str, object]) -> Dict[str, object]:
    """Execute SQL and return tabular results."""

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