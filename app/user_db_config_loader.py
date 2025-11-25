"""User database configuration loading utilities."""

from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy.orm import Session

from app.models import DatabaseSettings
from app.utils.logger import setup_logging
from db.database_manager import get_project_db_connection_string, get_session
from db.model import DatabaseConfig

logger = setup_logging(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _resolve_path(path: str) -> str:
    if not path:
        return ""
    candidate = Path(path)
    resolved = candidate if candidate.is_absolute() else (PROJECT_ROOT / path).resolve()
    return str(resolved)


def _project_session() -> Session:
    project_connection = get_project_db_connection_string()
    return get_session(project_connection)


def get_user_database_settings(db_flag: str) -> DatabaseSettings:
    """Fetch a DatabaseSettings instance from the DatabaseConfig table for a user database."""
    session = _project_session()
    try:
        db_row = session.query(DatabaseConfig).filter_by(db_flag=db_flag).first()
        if not db_row:
            available = [row.db_flag for row in session.query(DatabaseConfig.db_flag).all()]
            raise KeyError(f"Unknown database flag '{db_flag}'. Available: {available}")
        logger.info("Fetched user database settings for db_flag=%s from DatabaseConfig", db_flag)
        intro_template = ""
        if db_row.intro_template:
            resolved = _resolve_path(db_row.intro_template)
            if Path(resolved).exists():
                intro_template = resolved
            else:
                fallback = PROJECT_ROOT / "database_schemas" / db_flag / "db_intro" / Path(db_row.intro_template).name
                if fallback.exists():
                    intro_template = str(fallback)
        else:
            default_path = PROJECT_ROOT / "database_schemas" / db_flag / "db_intro" / f"{db_flag}_intro.txt"
            if default_path.exists():
                intro_template = str(default_path)

        return DatabaseSettings(
            connection_string=os.path.expandvars(db_row.connection_string),
            intro_template=intro_template,
            description=db_row.description,
            max_rows=db_row.max_rows,
            query_timeout=db_row.query_timeout,
            exclude_column_matches=db_row.exclude_column_matches,
        )
    finally:
        session.close()



