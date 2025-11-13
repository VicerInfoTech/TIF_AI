"""Schema selection utilities for reducing prompt context size."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Sequence, Set

from app.utils.logger import setup_logging

logger = setup_logging(__name__)


STOP_WORDS = {
    "how",
    "many",
    "what",
    "when",
    "where",
    "who",
    "which",
    "show",
    "list",
    "give",
    "fetch",
    "from",
    "with",
    "and",
    "for",
    "the",
    "all",
    "that",
    "have",
    "need",
    "want",
    "can",
    "could",
    "should",
    "would",
    "into",
    "about",
}


class SchemaSelector:
    """Select relevant tables by matching query tokens with keyword map data."""

    def __init__(self, schema_metadata: Dict[str, Any]) -> None:
        self.schema = schema_metadata
        self.tables = schema_metadata.get("tables", {})
        self.keyword_map: Dict[str, Sequence[str]] = schema_metadata.get("keyword_map", {})
        self.relationships = schema_metadata.get("relationships", {})
        self.last_tokens: List[str] = []

    def select_relevant_tables(self, query: str) -> List[str]:
        tokens = self._tokenize(query)
        self.last_tokens = tokens
        matched_tables: Set[str] = set()

        for token in tokens:
            if token in self.keyword_map:
                matched_tables.update(self.keyword_map[token])

        if matched_tables:
            matched_tables.update(self._expand_relationships(matched_tables))
        else:
            logger.debug("No keyword matches found; returning all tables")
            matched_tables = set(self.tables.keys())

        logger.info("Schema selector chose %d table(s)", len(matched_tables))
        return sorted(matched_tables)

    def _tokenize(self, query: str) -> List[str]:
        words = re.findall(r"\b\w+\b", query.lower())
        return [w for w in words if len(w) > 2 and w not in STOP_WORDS]

    def _expand_relationships(self, tables: Set[str]) -> Set[str]:
        related: Set[str] = set()
        for table in tables:
            for rel in self.relationships.get(table, []):
                target = (rel.get("references") or {}).get("table")
                if target:
                    related.add(target)
            for other_table, rels in self.relationships.items():
                for rel in rels:
                    target = (rel.get("references") or {}).get("table")
                    if target == table:
                        related.add(other_table)
        return related


def format_tables_for_llm(schema_metadata: Dict[str, Any], table_names: Sequence[str]) -> str:
    """Render selected tables into a concise textual representation for prompts."""

    tables = schema_metadata.get("tables", {})
    lines: List[str] = []

    for table_name in sorted(table_names):
        table = tables.get(table_name)
        if not table:
            continue

        description = table.get("description")
        if description:
            lines.append(f"Table: {table_name} — {description}")
        else:
            lines.append(f"Table: {table_name}")

        for column_name, column_info in table.get("columns", {}).items():
            col_type = column_info.get("type", "unknown")
            nullable = "nullable" if column_info.get("nullable", True) else "required"
            suffix = " [PK]" if column_info.get("is_primary_key") else ""
            default = column_info.get("default")
            default_text = f" default={default}" if default else ""
            lines.append(f"  - {column_name}: {col_type} ({nullable}){suffix}{default_text}")

        foreign_keys = schema_metadata.get("relationships", {}).get(table_name, [])
        for fk in foreign_keys:
            columns = ",".join(fk.get("columns", []))
            target_table = (fk.get("references") or {}).get("table")
            target_columns = ",".join((fk.get("references") or {}).get("columns", []))
            if target_table:
                lines.append(f"  > FK {columns} → {target_table}({target_columns})")

        lines.append("")

    return "\n".join(lines).strip()
