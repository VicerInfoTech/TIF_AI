"""User database configuration loading utilities."""

from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from asyncpg.exceptions import InvalidPasswordError

from app.models import DatabaseSettings
from app.utils.logger import setup_logging
from db.database_manager import get_project_db_connection_string, get_project_db_session, get_session
from db.model import DatabaseConfig

logger = setup_logging(__name__, level="DEBUG")

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _resolve_path(path: str) -> str:
    if not path:
        return ""
    candidate = Path(path)
    resolved = candidate if candidate.is_absolute() else (PROJECT_ROOT / path).resolve()
    return str(resolved)


async def get_user_database_settings(db_flag: str) -> DatabaseSettings:
    """Fetch a DatabaseSettings instance from the DatabaseConfig table for a user database."""
    project_connection = get_project_db_connection_string()
    logger.debug("Fetching user database settings for db_flag=%s using project connection %s", db_flag, project_connection[:15] + "****")
    try:
        return await _get_user_database_settings_async(project_connection, db_flag)
    except (InvalidPasswordError, SQLAlchemyError) as exc:
        logger.warning(
            "Async project DB lookup failed (%s). Falling back to sync driver.",
            type(exc).__name__,
        )
        return _get_user_database_settings_sync(project_connection, db_flag)


async def _get_user_database_settings_async(project_connection: str, db_flag: str) -> DatabaseSettings:
    async with get_project_db_session(project_connection) as session:
        result = await session.execute(select(DatabaseConfig).filter_by(db_flag=db_flag))
        db_row = result.scalar_one_or_none()
        if not db_row:
            available_result = await session.execute(select(DatabaseConfig.db_flag))
            available = available_result.scalars().all()
            raise KeyError(f"Unknown database flag '{db_flag}'. Available: {available}")
        logger.info("Fetched user database settings for db_flag=%s from DatabaseConfig", db_flag)
        return _build_database_settings(db_row, db_flag)


def _get_user_database_settings_sync(project_connection: str, db_flag: str) -> DatabaseSettings:
    session = get_session(project_connection)
    try:
        db_row = session.query(DatabaseConfig).filter_by(db_flag=db_flag).first()
        if not db_row:
            available = [row.db_flag for row in session.query(DatabaseConfig.db_flag).all()]
            raise KeyError(f"Unknown database flag '{db_flag}'. Available: {available}")
        logger.info("Fetched user database settings for db_flag=%s from DatabaseConfig", db_flag)
        return _build_database_settings(db_row, db_flag)
    finally:
        session.close()


def _build_database_settings(db_row: DatabaseConfig, db_flag: str) -> DatabaseSettings:
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



