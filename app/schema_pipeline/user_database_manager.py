"""Database connection helpers."""

from __future__ import annotations

from functools import lru_cache

from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def _normalize_jdbc_connection_string(connection_string: str) -> str:
	"""Convert JDBC SQL Server connection string to SQLAlchemy format."""
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

		# Build a robust ODBC connection string and pass via odbc_connect
		server_part = f"{host},{port}" if port else host
		odbc_kv = [
			f"DRIVER={driver}",
			f"SERVER={server_part}",
			f"DATABASE={database}",
			f"UID={user}",
			f"PWD={password}",
			"Encrypt=yes",
			"TrustServerCertificate=yes",
		]
		odbc_conn_str = ";".join(odbc_kv)
		return f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_conn_str)}"

	return connection_string


@lru_cache(maxsize=8)
def _engine_cache(connection_string: str) -> Engine:
	normalized = _normalize_jdbc_connection_string(connection_string)
	return create_engine(normalized, pool_pre_ping=True, pool_recycle=1800)


def get_engine(connection_string: str) -> Engine:
    """Retrieve a cached SQLAlchemy engine for the connection string."""

    return _engine_cache(connection_string)


def get_connection(connection_string: str):
    """Return a context manager for a SQLAlchemy connection."""

    engine = get_engine(connection_string)
    return engine.connect()