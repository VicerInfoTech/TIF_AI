"""Low-level SQL Server metadata extraction helpers."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.models import RawMetadata
from app.utils.logger import setup_logging

logger = setup_logging(__name__)


class SQLServerMetadataExtractor:
    """Extracts comprehensive metadata from SQL Server using system catalogs."""

    DEFAULT_EXCLUDE_SCHEMAS: Sequence[str] = (
        "sys",
        "INFORMATION_SCHEMA",
        "guest",
        "db_owner",
        "db_accessadmin",
        "db_securityadmin",
        "db_ddladmin",
        "db_backupoperator",
        "db_datareader",
        "db_datawriter",
        "db_denydatareader",
        "db_denydatawriter",
    )

    def __init__(
        self,
        connection_string: str,
        include_schemas: Optional[Iterable[str]] = None,
        exclude_schemas: Optional[Iterable[str]] = None,
    ) -> None:
        self.connection_string = self._normalize_connection_string(connection_string)
        self.include_schemas = {s.lower() for s in include_schemas or []}
        base_excludes = {s.lower() for s in self.DEFAULT_EXCLUDE_SCHEMAS}
        extra_excludes = {s.lower() for s in (exclude_schemas or [])}
        self.exclude_schemas = base_excludes | extra_excludes
        logger.debug("Initialised SQLServerMetadataExtractor include=%s exclude=%s", self.include_schemas, self.exclude_schemas)
        self.engine: Engine = create_engine(self.connection_string, pool_pre_ping=True)

    def extract(self) -> RawMetadata:
        """Fetch metadata for all requested schemas and return a structured payload."""

        with self.engine.begin() as connection:
            database_name = self._safe_scalar(connection, "SELECT DB_NAME()") or "unknown"
            logger.info("Extracting schema metadata from database '%s'", database_name)

            rows = {
                "schemas": self._filter_by_schema(self._fetch_rows(connection, self._schemas_sql())),
                "tables": self._filter_by_schema(self._fetch_rows(connection, self._tables_sql())),
                "columns": self._filter_by_schema(self._fetch_rows(connection, self._columns_sql())),
                "primary_keys": self._filter_by_schema(self._fetch_rows(connection, self._primary_keys_sql())),
                "foreign_keys": self._filter_by_schema(self._fetch_rows(connection, self._foreign_keys_sql())),
                "indexes": self._filter_by_schema(self._fetch_rows(connection, self._indexes_sql())),
                "unique_constraints": self._filter_by_schema(self._fetch_rows(connection, self._unique_constraints_sql())),
                "check_constraints": self._filter_by_schema(self._fetch_rows(connection, self._check_constraints_sql())),
                "views": self._filter_by_schema(self._fetch_rows(connection, self._views_sql())),
                "view_columns": self._filter_by_schema(self._fetch_rows(connection, self._view_columns_sql())),
                "procedures": self._filter_by_schema(self._fetch_rows(connection, self._procedures_sql())),
                "procedure_parameters": self._filter_by_schema(self._fetch_rows(connection, self._procedure_parameters_sql())),
                "functions": self._filter_by_schema(self._fetch_rows(connection, self._functions_sql())),
                "function_parameters": self._filter_by_schema(self._fetch_rows(connection, self._function_parameters_sql())),
            }

        logger.info(
            "Schema extraction complete: %d tables, %d views, %d procedures, %d functions",
            len(rows["tables"]),
            len(rows["views"]),
            len(rows["procedures"]),
            len(rows["functions"]),
        )

        return RawMetadata(database_name=database_name, **rows)  # type: ignore[arg-type]

    # ------------------------------------------------------------------
    # SQL query builders
    # ------------------------------------------------------------------

    def _schemas_sql(self) -> str:
        return (
            "SELECT schema_id, name AS schema_name "
            "FROM sys.schemas "
            "WHERE name NOT IN ('sys', 'INFORMATION_SCHEMA') "
            "ORDER BY name;"
        )

    def _tables_sql(self) -> str:
        return (
            "SELECT s.name AS schema_name, t.name AS table_name, t.object_id, "
            "       t.create_date, t.modify_date, t.type_desc, "
            "       CAST(ep.value AS NVARCHAR(MAX)) AS table_description "
            "FROM sys.tables t "
            "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "LEFT JOIN sys.extended_properties ep ON ep.major_id = t.object_id "
            "  AND ep.minor_id = 0 AND ep.name = 'MS_Description' "
            "WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA') "
            "ORDER BY s.name, t.name;"
        )

    def _columns_sql(self) -> str:
        return (
            "SELECT s.name AS schema_name, t.name AS table_name, t.object_id, c.name AS column_name, "
            "       c.column_id, ty.name AS data_type, c.max_length, c.precision, c.scale, c.is_nullable, "
            "       CASE WHEN ic.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_identity, "
            "       CAST(ic.seed_value AS NVARCHAR(128)) AS identity_seed_value, "
            "       CAST(ic.increment_value AS NVARCHAR(128)) AS identity_increment_value, c.is_computed, "
            "       CAST(cc.definition AS NVARCHAR(MAX)) AS computed_definition, "
            "       CAST(dc.definition AS NVARCHAR(MAX)) AS default_value, c.collation_name, "
            "       CAST(ep.value AS NVARCHAR(MAX)) AS column_description "
            "FROM sys.columns c "
            "INNER JOIN sys.tables t ON c.object_id = t.object_id "
            "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id "
            "LEFT JOIN sys.identity_columns ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
            "LEFT JOIN sys.computed_columns cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id "
            "LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id "
            "LEFT JOIN sys.extended_properties ep ON ep.major_id = c.object_id AND ep.minor_id = c.column_id AND ep.name = 'MS_Description' "
            "WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA') "
            "ORDER BY s.name, t.name, c.column_id;"
        )

    def _primary_keys_sql(self) -> str:
        return (
            "SELECT s.name AS schema_name, t.name AS table_name, i.name AS constraint_name, i.object_id, "
            "       COL_NAME(ic.object_id, ic.column_id) AS column_name, ic.key_ordinal, ic.is_descending_key, i.is_primary_key "
            "FROM sys.indexes i "
            "INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
            "INNER JOIN sys.tables t ON i.object_id = t.object_id "
            "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "WHERE i.is_primary_key = 1 "
            "ORDER BY s.name, t.name, ic.key_ordinal;"
        )

    def _foreign_keys_sql(self) -> str:
        return (
            "SELECT s.name AS schema_name, t.name AS table_name, fk.name AS constraint_name, fk.object_id, "
            "       COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name, "
            "       rs.name AS referenced_schema, rt.name AS referenced_table, "
            "       COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS referenced_column, "
            "       fk.delete_referential_action_desc AS on_delete, fk.update_referential_action_desc AS on_update, fk.is_disabled "
            "FROM sys.foreign_keys fk "
            "INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id "
            "INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id "
            "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "INNER JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id "
            "INNER JOIN sys.schemas rs ON rt.schema_id = rs.schema_id "
            "ORDER BY s.name, t.name, fk.name, fkc.constraint_column_id;"
        )

    def _indexes_sql(self) -> str:
        return (
            "SELECT s.name AS schema_name, t.name AS table_name, i.name AS index_name, i.object_id, i.is_unique, i.is_primary_key, i.is_unique_constraint, "
            "       i.type_desc, i.filter_definition, COL_NAME(ic.object_id, ic.column_id) AS column_name, ic.key_ordinal, ic.is_descending_key, ic.is_included_column "
            "FROM sys.indexes i "
            "INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
            "INNER JOIN sys.tables t ON i.object_id = t.object_id "
            "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "WHERE i.is_hypothetical = 0 AND i.is_disabled = 0 AND i.is_primary_key = 0 "
            "ORDER BY s.name, t.name, i.name, ic.key_ordinal;"
        )

    def _unique_constraints_sql(self) -> str:
        return (
            "SELECT s.name AS schema_name, t.name AS table_name, kc.name AS constraint_name, kc.object_id, "
            "       COL_NAME(ic.object_id, ic.column_id) AS column_name, ic.key_ordinal "
            "FROM sys.key_constraints kc "
            "INNER JOIN sys.tables t ON kc.parent_object_id = t.object_id "
            "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "INNER JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id "
            "WHERE kc.type = 'UQ' "
            "ORDER BY s.name, t.name, kc.name, ic.key_ordinal;"
        )

    def _check_constraints_sql(self) -> str:
        return (
            "SELECT s.name AS schema_name, t.name AS table_name, cc.name AS constraint_name, cc.object_id, cc.definition, cc.is_disabled "
            "FROM sys.check_constraints cc "
            "INNER JOIN sys.tables t ON cc.parent_object_id = t.object_id "
            "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "ORDER BY s.name, t.name, cc.name;"
        )

    def _views_sql(self) -> str:
        return (
            "SELECT s.name AS schema_name, v.name AS view_name, v.object_id, v.create_date, v.modify_date, "
            "       OBJECT_DEFINITION(v.object_id) AS definition, CAST(ep.value AS NVARCHAR(MAX)) AS view_description "
            "FROM sys.views v "
            "INNER JOIN sys.schemas s ON v.schema_id = s.schema_id "
            "LEFT JOIN sys.extended_properties ep ON ep.major_id = v.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description' "
            "WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA') "
            "ORDER BY s.name, v.name;"
        )

    def _view_columns_sql(self) -> str:
        return (
            "SELECT s.name AS schema_name, v.name AS view_name, c.name AS column_name, c.column_id, ty.name AS data_type, c.max_length, c.is_nullable, "
            "       CAST(ep.value AS NVARCHAR(MAX)) AS column_description "
            "FROM sys.columns c "
            "INNER JOIN sys.views v ON c.object_id = v.object_id "
            "INNER JOIN sys.schemas s ON v.schema_id = s.schema_id "
            "INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id "
            "LEFT JOIN sys.extended_properties ep ON ep.major_id = c.object_id AND ep.minor_id = c.column_id AND ep.name = 'MS_Description' "
            "WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA') "
            "ORDER BY s.name, v.name, c.column_id;"
        )

    def _procedures_sql(self) -> str:
        return (
            "SELECT s.name AS schema_name, p.name AS procedure_name, p.object_id, p.create_date, p.modify_date, "
            "       OBJECT_DEFINITION(p.object_id) AS definition, CAST(ep.value AS NVARCHAR(MAX)) AS procedure_description "
            "FROM sys.procedures p "
            "INNER JOIN sys.schemas s ON p.schema_id = s.schema_id "
            "LEFT JOIN sys.extended_properties ep ON ep.major_id = p.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description' "
            "WHERE p.is_ms_shipped = 0 AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA') "
            "ORDER BY s.name, p.name;"
        )

    def _procedure_parameters_sql(self) -> str:
        return (
            "SELECT s.name AS schema_name, p.name AS procedure_name, pm.name AS parameter_name, ty.name AS data_type, pm.max_length, pm.is_output, pm.has_default_value, pm.default_value "
            "FROM sys.parameters pm "
            "INNER JOIN sys.procedures p ON pm.object_id = p.object_id "
            "INNER JOIN sys.schemas s ON p.schema_id = s.schema_id "
            "INNER JOIN sys.types ty ON pm.user_type_id = ty.user_type_id "
            "WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA') "
            "ORDER BY s.name, p.name, pm.parameter_id;"
        )

    def _functions_sql(self) -> str:
        return (
            "SELECT s.name AS schema_name, o.name AS function_name, o.object_id, o.type_desc AS function_type, o.create_date, o.modify_date, "
            "       OBJECT_DEFINITION(o.object_id) AS definition, CAST(ep.value AS NVARCHAR(MAX)) AS function_description "
            "FROM sys.objects o "
            "INNER JOIN sys.schemas s ON o.schema_id = s.schema_id "
            "LEFT JOIN sys.extended_properties ep ON ep.major_id = o.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description' "
            "WHERE o.type IN ('FN', 'IF', 'TF') AND o.is_ms_shipped = 0 AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA') "
            "ORDER BY s.name, o.name;"
        )

    def _function_parameters_sql(self) -> str:
        return (
            "SELECT s.name AS schema_name, o.name AS function_name, pm.name AS parameter_name, ty.name AS data_type, pm.max_length, pm.has_default_value, pm.default_value "
            "FROM sys.parameters pm "
            "INNER JOIN sys.objects o ON pm.object_id = o.object_id "
            "INNER JOIN sys.schemas s ON o.schema_id = s.schema_id "
            "INNER JOIN sys.types ty ON pm.user_type_id = ty.user_type_id "
            "WHERE o.type IN ('FN', 'IF', 'TF') AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA') "
            "ORDER BY s.name, o.name, pm.parameter_id;"
        )

    # Row count SQL and related logic removed (not needed for read-only users)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _fetch_rows(self, connection, sql: str) -> List[Dict[str, Any]]:
        result = connection.execute(text(sql))
        return [dict(row._mapping) for row in result]

    # _safe_fetch_rows removed (only used for row count logic)

    def _filter_by_schema(self, rows: List[Dict[str, Any]], key: str = "schema_name") -> List[Dict[str, Any]]:
        if not rows:
            return []
        filtered: List[Dict[str, Any]] = []
        for row in rows:
            schema_name = str(row.get(key) or "").lower()
            if schema_name and schema_name in self.exclude_schemas:
                continue
            if self.include_schemas and schema_name not in self.include_schemas:
                continue
            filtered.append(row)
        return filtered

    @staticmethod
    def _safe_scalar(connection, sql: str) -> Optional[str]:
        try:
            result = connection.execute(text(sql))
            return result.scalar()
        except SQLAlchemyError:
            return None

    @staticmethod
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
            from urllib.parse import quote_plus

            return f"mssql+pyodbc:///?odbc_connect={quote_plus(';'.join(odbc_parts))}"
        return connection_string


__all__ = ["SQLServerMetadataExtractor"]
