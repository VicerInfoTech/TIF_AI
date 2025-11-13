"""Keyword mapping helpers for schema-aware table selection."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Dict, List, Set

from app.utils.logger import setup_logging

logger = setup_logging(__name__)


class KeywordMapper:
    """Generate keyword-to-table mappings for downstream schema selection."""

    DEFAULT_SYNONYMS: Dict[str, List[str]] = {
        "dispense": ["order", "shipment", "delivery"],
        "patient": ["customer", "client"],
        "inventory": ["stock", "product"],
        "invoice": ["bill", "statement"],
        "payment": ["transaction", "settlement"],
    }

    STOP_WORDS = {"master", "detail", "history", "log", "temp"}

    def __init__(
        self,
    tables: Dict[str, Dict[str, Any]],
    relationships: Dict[str, List[Dict[str, Any]]],
        custom_synonyms: Dict[str, List[str]] | None = None,
    ) -> None:
        self.tables = tables
        self.relationships = relationships
        self.synonyms = dict(self.DEFAULT_SYNONYMS)
        if custom_synonyms:
            for key, values in custom_synonyms.items():
                base = key.lower()
                merged = set(self.synonyms.get(base, [])) | {v.lower() for v in values}
                self.synonyms[base] = sorted(merged)

    def generate(self) -> Dict[str, List[str]]:
        keyword_map: Dict[str, Set[str]] = defaultdict(set)

        for table_name, table in self.tables.items():
            table_tokens = self._tokenize(table_name)
            column_tokens = self._tokens_from_columns(table)
            tokens = table_tokens | column_tokens

            for token in tokens:
                keyword_map[token].add(table_name)
                for synonym in self.synonyms.get(token, []):
                    keyword_map[synonym].add(table_name)

        for source, rels in self.relationships.items():
            for rel in rels:
                target = (rel.get("references") or {}).get("table")
                if not target:
                    continue
                keyword_map[target.lower()].add(source)
                keyword_map[source.lower()].add(target)

        flattened = {key: sorted(list(values)) for key, values in keyword_map.items()}
        logger.debug("Generated keyword map with %d entries", len(flattened))
        return flattened

    def _tokenize(self, name: str) -> Set[str]:
        parts = re.findall(r"[A-Z][a-z]*", name) or [name]
        return {part.lower() for part in parts if self._is_meaningful(part)}

    def _tokens_from_columns(self, table: Dict[str, Any]) -> Set[str]:
        tokens: Set[str] = set()
        for column_name in table.get("columns", {}):
            for part in re.findall(r"[A-Z][a-z]*", column_name):
                if self._is_meaningful(part):
                    tokens.add(part.lower())
        return tokens

    def _is_meaningful(self, token: str) -> bool:
        token_lower = token.lower()
        return token_lower not in self.STOP_WORDS and len(token_lower) > 2


def generate_keyword_map(
    tables: Dict[str, Dict[str, Any]],
    relationships: Dict[str, List[Dict[str, Any]]],
    custom_synonyms: Dict[str, List[str]] | None = None,
) -> Dict[str, List[str]]:
    mapper = KeywordMapper(tables, relationships, custom_synonyms)
    return mapper.generate()
