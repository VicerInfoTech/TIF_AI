"""Preprocess extracted schema metadata for language model consumption."""

from __future__ import annotations

import re
from typing import Any, Dict

from app.core.keyword_mapper import generate_keyword_map
from app.utils.logger import setup_logging

logger = setup_logging(__name__)


class SchemaPreprocessor:
    """Clean and compress raw schema metadata extracted from a database."""

    GENERIC_SUFFIXES = {"master", "detail", "history", "log", "temp"}

    TYPE_MAP = {
        "INTEGER": "int",
        "INT": "int",
        "BIGINT": "bigint",
        "SMALLINT": "smallint",
        "TINYINT": "tinyint",
        "NUMERIC": "numeric",
        "DECIMAL": "decimal",
        "FLOAT": "float",
        "REAL": "float",
        "DOUBLE": "float",
        "CHAR": "char",
        "VARCHAR": "varchar",
        "TEXT": "text",
        "NVARCHAR": "varchar",
        "NTEXT": "text",
        "DATE": "date",
        "DATETIME": "datetime",
        "DATETIME2": "datetime",
        "TIMESTAMP": "timestamp",
        "BOOLEAN": "bool",
        "BIT": "bool",
    }

    def __init__(self, raw_schema: Dict[str, Any]) -> None:
        self.raw_schema = raw_schema

    def preprocess(self) -> Dict[str, Any]:
        """Return a compact, LLM-friendly schema dictionary."""

        metadata = {
            "database_name": self.raw_schema.get("database_name", "unknown"),
            "extracted_at": self.raw_schema.get("extracted_at"),
            "statistics": self.raw_schema.get("statistics", {}),
        }

        tables: Dict[str, Any] = {}
        for table_name, table_info in (self.raw_schema.get("tables") or {}).items():
            tables[table_name] = self._preprocess_table(table_name, table_info)

        relationships = self.raw_schema.get("relationships", {})

        keyword_map = generate_keyword_map(tables, relationships)

        logger.info("Preprocessed schema: %d tables, %d keywords", len(tables), len(keyword_map))

        return {
            "metadata": metadata,
            "tables": tables,
            "relationships": relationships,
            "keyword_map": keyword_map,
        }

    def _preprocess_table(self, table_name: str, table_info: Dict[str, Any]) -> Dict[str, Any]:
        columns: Dict[str, Any] = {}
        for col_name, col_info in (table_info.get("columns") or {}).items():
            columns[col_name] = {
                "type": self._normalize_type(col_info.get("type", "")),
                "nullable": bool(col_info.get("nullable", True)),
                "is_primary_key": bool(col_info.get("is_primary_key", False)),
                "default": col_info.get("default"),
            }

        description = self._infer_description(table_name)

        return {
            "columns": columns,
            "description": description,
            "row_count_estimate": table_info.get("row_count_estimate"),
        }

    def _normalize_type(self, sql_type: str) -> str:
        if not sql_type:
            return "unknown"
        base_match = re.match(r"^([A-Z]+)", sql_type.upper())
        base_type = base_match.group(1) if base_match else sql_type.upper()
        normalized = self.TYPE_MAP.get(base_type, base_type.lower())
        precision = re.search(r"\(([^)]+)\)", sql_type)
        if precision:
            normalized = f"{normalized}({precision.group(1)})"
        return normalized

    def _infer_description(self, table_name: str) -> str:
        words = re.findall(r"[A-Z][a-z]*", table_name) or [table_name]
        filtered = [w.lower() for w in words if w.lower() not in self.GENERIC_SUFFIXES]
        if not filtered:
            filtered = [table_name.lower()]
        pretty = " ".join(filtered)
        return pretty.capitalize()


def preprocess_schema(raw_schema: Dict[str, Any]) -> Dict[str, Any]:
    """Convenience helper to preprocess schema payloads."""

    return SchemaPreprocessor(raw_schema).preprocess()
