"""Centralized database engine and session helpers."""

from __future__ import annotations

from contextlib import asynccontextmanager
from functools import lru_cache
from urllib.parse import quote_plus
import asyncio
import logging
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, Session
from db.model import Base
from dotenv import load_dotenv
load_dotenv()
logger = logging.getLogger(__name__)

PROJECT_DB_CONNECTION_STRING = (
    os.getenv("PROJECT_DB_CONNECTION_STRING") or os.getenv("POSTGRES_CONNECTION_STRING")
)


def get_project_db_connection_string() -> str:
    if not PROJECT_DB_CONNECTION_STRING:
        raise RuntimeError("PROJECT_DB_CONNECTION_STRING environment variable is required.")
    return PROJECT_DB_CONNECTION_STRING

def _normalize_connection_string(connection_string: str) -> str:
    if connection_string.startswith("jdbc:sqlserver://"):
        rest = connection_string[len("jdbc:sqlserver://") :]
        host_port, _, params = rest.partition(";")
        host, _, port = host_port.partition(":")
        database = ""
        user = ""
        password = ""
        driver = "ODBC Driver 18 for SQL Server"
        for part in params.split(";"):
            if not part:
                continue
            key, _, value = part.partition("=")
            key = key.lower()
            if key == "databasename":
                database = value
            elif key == "user":
                user = value
            elif key == "password":
                password = value
            elif key == "driver":
                driver = value
        server_part = f"{host},{port}" if port else host
        odbc_parts = [
            f"DRIVER={driver}",
            f"SERVER={server_part}",
            f"DATABASE={database}",
            f"UID={user}",
            f"PWD={password}",
            "Encrypt=yes",
            "TrustServerCertificate=yes",
        ]
        return f"mssql+pyodbc:///?odbc_connect={quote_plus(';'.join(odbc_parts))}"
    return connection_string


def _ensure_async_postgres_driver(connection_string: str) -> str:
    try:
        url = make_url(connection_string)
    except Exception:  # pragma: no cover - best-effort parsing
        return connection_string

    drivername = url.drivername.lower()
    if drivername.startswith("postgresql") and "+asyncpg" not in drivername:
        # URL-encode username and password to handle special characters (e.g., @ : % in password)
        # This is required for asyncpg driver, which is stricter than psycopg about parsing credentials
        from urllib.parse import quote
        url_with_driver = url.set(drivername="postgresql+asyncpg")
        if url_with_driver.username or url_with_driver.password:
            # Rebuild with quoted credentials to avoid parsing errors in asyncpg
            user_part = quote(url_with_driver.username or "", safe="")
            pass_part = quote(url_with_driver.password or "", safe="")
            host_part = url_with_driver.host or "localhost"
            port_part = f":{url_with_driver.port}" if url_with_driver.port else ""
            db_part = f"/{url_with_driver.database}" if url_with_driver.database else ""
            rebuilt = f"postgresql+asyncpg://{user_part}:{pass_part}@{host_part}{port_part}{db_part}"
            return rebuilt
        return str(url_with_driver)
    return connection_string


def _normalize_async_connection_string(connection_string: str) -> str:
    normalized = _normalize_connection_string(connection_string)
    return _ensure_async_postgres_driver(normalized)


def _ensure_sync_postgres_driver(connection_string: str) -> str:
    """Return a sync-friendly PostgreSQL driver name (psycopg) for synchronous operations."""
    try:
        url = make_url(connection_string)
    except Exception:  # pragma: no cover - best-effort parsing
        return connection_string

    drivername = url.drivername.lower()
    if drivername.startswith("postgresql") and "+asyncpg" in drivername:
        return str(url.set(drivername="postgresql+psycopg"))
    return connection_string


def _normalize_sync_connection_string(connection_string: str) -> str:
    normalized = _normalize_connection_string(connection_string)
    return _ensure_sync_postgres_driver(normalized)


async def can_connect_async(connection_string: str) -> bool:
    """Attempt a light async connection check. Returns True if a simple select succeeds."""
    try:
        engine = get_async_engine(connection_string)
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:
        # Failure indicates async connectivity issues (authentication, driver mismatch, etc.)
        return False


def _create_sync_engine(connection_string: str) -> Engine:
    normalized = _normalize_sync_connection_string(connection_string)
    return create_engine(normalized, pool_pre_ping=True, pool_recycle=1800)



@lru_cache(maxsize=8)
def get_engine(connection_string: str) -> Engine:
    """Retrieve a cached SQLAlchemy engine for the connection string."""
    normalized = _normalize_connection_string(connection_string)
    engine = create_engine(normalized, pool_pre_ping=True, pool_recycle=1800)
    return engine


async def create_metadata_tables(connection_string: str) -> None:
    """Create the project metadata tables (idempotent) using async engine."""
    loop = asyncio.get_running_loop()
    def _create_tables_sync() -> None:
        engine = _create_sync_engine(connection_string)
        Base.metadata.create_all(engine)
    try:
        await loop.run_in_executor(None, _create_tables_sync)
    except Exception as exc:
        logger.warning("Could not create metadata tables: %s", exc)
        raise

@lru_cache(maxsize=16)
def get_sessionmaker(connection_string: str):
    """Return a cached sessionmaker for the connection string."""
    engine = get_engine(connection_string)
    return sessionmaker(bind=engine)

def get_session(connection_string: str) -> Session:
    """Get a new SQLAlchemy session for the connection string."""
    SessionLocal = get_sessionmaker(connection_string)
    return SessionLocal()

def get_connection(connection_string: str):
    """Get a raw connection from the engine."""
    engine = get_engine(connection_string)
    return engine.connect()


@lru_cache(maxsize=8)
def get_async_engine(connection_string: str) -> AsyncEngine:
    """Retrieve a cached SQLAlchemy async engine for the connection string."""
    normalized = _normalize_async_connection_string(connection_string)
    engine = create_async_engine(normalized, pool_pre_ping=True, pool_recycle=1800)
    return engine


@lru_cache(maxsize=16)
def get_async_sessionmaker(connection_string: str) -> async_sessionmaker[AsyncSession]:
    """Return a cached async sessionmaker for the connection string."""
    engine = get_async_engine(connection_string)
    return async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def get_project_db_session(connection_string: str) -> AsyncSession:
    """Context manager yielding an async session tied to the project metadata database."""
    session_factory = get_async_sessionmaker(connection_string)
    async with session_factory() as session:
        yield session
