"""Utilities for extracting structured schema metadata from live databases."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from urllib.parse import quote_plus
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.utils.logger import setup_logging

logger = setup_logging(__name__)


@dataclass
class TableColumn:
    name: str
    type: str
    nullable: bool
    default: Optional[str]
    is_primary_key: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "nullable": self.nullable,
            "default": self.default,
            "is_primary_key": self.is_primary_key,
        }


@dataclass
class ForeignKey:
    constrained_columns: List[str]
    referred_table: str
    referred_columns: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "columns": self.constrained_columns,
            "references": {
                "table": self.referred_table,
                "columns": self.referred_columns,
            },
        }


class SchemaExtractor:
    """Extract schema metadata from a live SQL database using SQLAlchemy."""

    def __init__(self, connection_string: str) -> None:
        self.original_connection_string = connection_string
        self.connection_string = self._normalize_connection_string(connection_string)
        self.database_name = self._extract_database_name(connection_string)
        # self.connection_string = connection_string
        logger.debug("Initialising schema extractor for connection: %s", self.connection_string)
        self.engine: Engine = create_engine(self.connection_string, pool_pre_ping=True)
        self.inspector = inspect(self.engine)

    @staticmethod
    def _extract_database_name(connection_string: str) -> str:
        """Extract database name from connection string (JDBC or PostgreSQL format)."""
        if connection_string.startswith("jdbc:sqlserver://"):
            # JDBC format: jdbc:sqlserver://host:port;databaseName=DbName;...
            for part in connection_string.split(";"):
                if part.lower().startswith("databasename="):
                    return part.split("=", 1)[1]
        elif "postgresql://" in connection_string:
            # PostgreSQL format: postgresql://user:pass@host:port/dbname
            try:
                parts = connection_string.split("/")
                if parts:
                    return parts[-1]
            except (IndexError, ValueError):
                pass
        return "unknown"

    @staticmethod
    def _normalize_connection_string(connection_string: str) -> str:
        """Ensure connection string is compatible with SQLAlchemy."""

        if connection_string.startswith("jdbc:sqlserver://"):
            rest = connection_string[len("jdbc:sqlserver://") :]
            host_port, _, params = rest.partition(";")
            host, _, port = host_port.partition(":")
            database = ""
            user = ""
            password = ""
            # Prefer the installed and modern driver by default; will be overridden if specified
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

            # Build a robust ODBC connection string and pass via odbc_connect to avoid URL encoding issues
            server_part = f"{host},{port}" if port else host
            odbc_kv = [
                f"DRIVER={driver}",
                f"SERVER={server_part}",
                f"DATABASE={database}",
                f"UID={user}",
                f"PWD={password}",
                # Driver 18 defaults to encryption; include settings to work with typical on-prem installs
                "Encrypt=yes",
                "TrustServerCertificate=yes",
            ]
            odbc_conn_str = ";".join(odbc_kv)
            return f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_conn_str)}"

        return connection_string

    def extract_full_schema(self) -> Dict[str, Any]:
        """Collect metadata for all user tables in the database."""

        schema: Dict[str, Any] = {
            "database_name": self.database_name,
            "extracted_at": self._utc_timestamp(),
            "tables": {},
            "relationships": {},
            "statistics": {},
        }

        table_names = self._get_table_list()
        logger.info("Extracting schema for %d table(s)", len(table_names))

        for table_name in table_names:
            table_info = self._extract_table_info(table_name)
            schema["tables"][table_name] = table_info

        schema["relationships"] = self._extract_relationships(table_names)
        schema["statistics"] = self._calculate_statistics(schema)

        return schema

    def _get_table_list(self) -> List[str]:
        tables: List[str] = []
        try:
            default_schema = self.engine.dialect.default_schema_name or "dbo"
        except AttributeError:
            default_schema = "public"

        candidates = self.inspector.get_table_names(schema=default_schema)
        if not candidates:
            candidates = self.inspector.get_table_names()

        for name in candidates:
            if name.startswith("sys_") or name.lower().startswith("msreplication"):
                continue
            tables.append(name)
        return tables

    def _extract_table_info(self, table_name: str) -> Dict[str, Any]:
        columns_meta = self.inspector.get_columns(table_name)
        pk_info = self.inspector.get_pk_constraint(table_name)
        pk_columns = set(pk_info.get("constrained_columns", []) or [])

        columns: Dict[str, Any] = {}
        for column in columns_meta:
            col = TableColumn(
                name=column["name"],
                type=str(column.get("type")),
                nullable=bool(column.get("nullable", True)),
                default=self._stringify_default(column.get("default")),
                is_primary_key=column["name"] in pk_columns,
            )
            columns[col.name] = col.to_dict()

        return {
            "columns": columns,
            "primary_key": list(pk_columns),
            "row_count_estimate": self._estimate_row_count(table_name),
        }

    def _extract_relationships(self, table_names: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        relationships: Dict[str, List[Dict[str, Any]]] = {}
        for table_name in table_names:
            fks = self.inspector.get_foreign_keys(table_name) or []
            serialized: List[Dict[str, Any]] = []
            for fk in fks:
                try:
                    serialized.append(
                        ForeignKey(
                            constrained_columns=fk.get("constrained_columns", []),
                            referred_table=fk.get("referred_table", ""),
                            referred_columns=fk.get("referred_columns", []),
                        ).to_dict()
                    )
                except KeyError:
                    continue
            if serialized:
                relationships[table_name] = serialized
        return relationships

    def _estimate_row_count(self, table_name: str) -> Optional[int]:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row = result.fetchone()
                return int(row[0]) if row and row[0] is not None else None
        except SQLAlchemyError as exc:
            logger.debug("Row count estimate failed for %s: %s", table_name, exc)
            return None

    def _calculate_statistics(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        total_tables = len(schema["tables"])
        total_columns = sum(len(table["columns"]) for table in schema["tables"].values())
        total_relationships = sum(len(rel) for rel in schema["relationships"].values())
        return {
            "total_tables": total_tables,
            "total_columns": total_columns,
            "total_relationships": total_relationships,
        }

    @staticmethod
    def _stringify_default(default_val: Any) -> Optional[str]:
        if default_val is None:
            return None
        try:
            if isinstance(default_val, (dict, list)):
                return json.dumps(default_val)
            return str(default_val)
        except TypeError:
            return None

    @staticmethod
    def _utc_timestamp() -> str:
        from datetime import datetime, timezone

        return datetime.now(timezone.utc).isoformat()


def extract_schema(connection_string: str) -> Dict[str, Any]:
    """Convenience helper to extract schema metadata in one call."""

    extractor = SchemaExtractor(connection_string)
    return extractor.extract_full_schema()
