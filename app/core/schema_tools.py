"""Utilities for loading and querying YAML-based schema artifacts."""

from __future__ import annotations

import re
from collections import Counter, deque
from itertools import combinations
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import yaml

from app.config import get_database_settings, PROJECT_ROOT
from app.models import (
    ColumnInfo,
    ForeignKeyInfo,
    JoinPath,
    JoinStep,
    TableDetail,
    TableMatch,
)
from app.utils.logger import setup_logging

logger = setup_logging(__name__)

STOP_WORDS = {
    "and",
    "the",
    "for",
    "with",
    "from",
    "this",
    "that",
    "into",
    "about",
    "show",
    "list",
    "give",
    "data",
    "info",
    "details",
}


def _tokenize(text: str) -> List[str]:
    words = re.findall(r"\b\w+\b", text.lower())
    return [w for w in words if len(w) > 2 and w not in STOP_WORDS]


class SchemaToolkit:
    """Caches schema metadata and exposes helper methods for planning."""

    def __init__(self, db_flag: str) -> None:
        self.db_flag = db_flag
        settings = get_database_settings(db_flag)
        self.settings = settings
        self.schema_root = self._derive_schema_root()
        self.schema_index = self._load_schema_index()
        self.table_details: Dict[str, TableDetail] = {}
        self.table_paths: Dict[str, Path] = {}
        self._load_table_artifacts()
        self.relationship_graph = self._build_relationship_graph()
        self.alias_map: Dict[str, str] = self._load_alias_map(settings.alias_map_file)
        if self.alias_map:
            logger.info("Loaded %d alias ontology entries", len(self.alias_map))

    def _load_alias_map(self, alias_map_file: Optional[str]) -> Dict[str, str]:
        if not alias_map_file:
            return {}
        path = Path(alias_map_file)
        if not path.exists():
            logger.warning("Alias map file not found: %s", path)
            return {}
        try:
            if path.suffix.lower() in {".yaml", ".yml"}:
                data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            elif path.suffix.lower() == ".json":
                import json
                data = json.loads(path.read_text(encoding="utf-8")) or {}
            else:
                logger.warning("Unsupported alias map format: %s", path)
                return {}
            normalized: Dict[str, str] = {}
            for k, v in data.items():
                if isinstance(k, str) and isinstance(v, str):
                    normalized[k.strip().lower()] = v.strip()
            return normalized
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to load alias map: %s", exc)
            return {}

    def _derive_schema_root(self) -> Path:
        """Resolve schema root using the standard `config/schemas/<db_flag>` layout.

        Previously the system relied on a `ddl_file` path in the database settings.
        The DDL path is removed; schema artifacts are expected under the
        repository `config/schemas/<db_flag>` directory.
        """
        candidate = PROJECT_ROOT / "config" / "schemas" / self.db_flag
        if not candidate.exists() or not candidate.is_dir():
            raise FileNotFoundError(
                f"Schema directory for '{self.db_flag}' not found at {candidate}."
            )
        return candidate

    def _load_schema_index(self) -> Dict[str, object]:
        index_path = self.schema_root / "schema_index.yaml"
        if not index_path.exists():
            raise FileNotFoundError(f"schema_index.yaml missing in {self.schema_root}")
        with index_path.open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}

    def _load_table_artifacts(self) -> None:
        yaml_files = list(self.schema_root.rglob("*.yaml"))
        for file_path in yaml_files:
            stem = file_path.stem.lower()
            if stem in {"schema_index", "metadata"}:
                continue
            with file_path.open("r", encoding="utf-8") as handle:
                raw = yaml.safe_load(handle) or {}
            table_name = raw.get("table_name") or raw.get("table") or file_path.stem
            detail = self._build_table_detail(raw, file_path)
            key = table_name.lower()
            self.table_details[key] = detail
            self.table_paths[key] = file_path
        logger.info(
            "Loaded %d table YAML artifacts for db_flag=%s",
            len(self.table_details),
            self.db_flag,
        )

    def _build_table_detail(self, data: Dict[str, object], file_path: Path) -> TableDetail:
        table_name = data.get("table_name") or data.get("table")
        if not table_name:
            table_name = file_path.stem
        schema = data.get("schema", "dbo")
        description = (data.get("description") or "") or None
        keywords = list(data.get("keywords") or [])

        pk_columns = set()
        pk_data = data.get("primary_key")
        if isinstance(pk_data, dict):
            pk_columns.update(pk_data.get("columns", []) or [])
        elif isinstance(pk_data, list):
            pk_columns.update(pk_data)

        fk_entries: List[ForeignKeyInfo] = []
        fk_column_map: Dict[str, str] = {}
        relationships_section = data.get("relationships") or {}
        relation_lookup: Dict[str, str] = {}
        for rel in relationships_section.get("outgoing", []):
            via_cols = tuple(rel.get("via_columns", []))
            if via_cols:
                relation_lookup["|".join(via_cols).lower()] = rel.get("relationship_type" or None)

        for fk in data.get("foreign_keys", []) or []:
            columns = fk.get("columns", []) or []
            referenced_table = fk.get("referenced_table") or fk.get("referenced_table_name")
            if not referenced_table:
                continue
            referenced_columns = fk.get("referenced_columns", []) or []
            rel_key = "|".join(col.lower() for col in columns)
            relationship_type = relation_lookup.get(rel_key)
            fk_entries.append(
                ForeignKeyInfo(
                    name=fk.get("constraint_name"),
                    columns=list(columns),
                    referenced_table=referenced_table,
                    referenced_columns=list(referenced_columns),
                    relationship_type=relationship_type,
                )
            )
            for col in columns:
                fk_column_map[col] = referenced_table

        columns: List[ColumnInfo] = []
        for col in data.get("columns", []) or []:
            col_name = col.get("name")
            if not col_name:
                continue
            sql_type = col.get("sql_type") or col.get("type")
            description = col.get("description") or None
            keywords_list = list(col.get("keywords") or [])
            columns.append(
                ColumnInfo(
                    name=col_name,
                    data_type=sql_type,
                    description=description,
                    keywords=keywords_list,
                    is_primary_key=col_name in pk_columns,
                    is_foreign_key=col_name in fk_column_map,
                    is_nullable=col.get("is_nullable"),
                    references=fk_column_map.get(col_name),
                )
            )

        return TableDetail(
            table_name=table_name,
            schema=schema,
            description=description,
            keywords=keywords,
            columns=columns,
            foreign_keys=fk_entries,
        )

    def _build_relationship_graph(self) -> Dict[str, List[JoinStep]]:
        graph: Dict[str, List[JoinStep]] = {}
        for table in self.table_details.values():
            for fk in table.foreign_keys:
                step = JoinStep(
                    from_table=table.table_name,
                    to_table=fk.referenced_table,
                    columns=fk.columns,
                    referenced_columns=fk.referenced_columns,
                    relationship_type=fk.relationship_type or "many_to_one",
                )
                graph.setdefault(table.table_name.lower(), []).append(step)
                reverse = JoinStep(
                    from_table=fk.referenced_table,
                    to_table=table.table_name,
                    columns=fk.referenced_columns,
                    referenced_columns=fk.columns,
                    relationship_type="one_to_many" if fk.relationship_type == "many_to_one" else fk.relationship_type,
                )
                graph.setdefault(fk.referenced_table.lower(), []).append(reverse)
        return graph


    def _score_table(
        self,
        detail: TableDetail,
        tokens: Sequence[str],
        include_column_matches: bool,
    ) -> Tuple[float, Counter]:
        reasons: Counter = Counter()
        score = 0.0
        searchable_parts: Dict[str, Iterable[str]] = {
            "name": [detail.table_name.lower()],
            "description": [detail.description.lower()] if detail.description else [],
            "keywords": [kw.lower() for kw in detail.keywords],
        }
        if include_column_matches:
            searchable_parts.update(
                {
                    "columns": [col.name.lower() for col in detail.columns],
                    "column_keywords": [keyword.lower() for col in detail.columns for keyword in col.keywords],
                }
            )
        for token in tokens:
            token_lower = token.lower()
            for field, values in searchable_parts.items():
                if any(token_lower in value for value in values):
                    weight = self._weight_for_field(field)
                    score += weight
                    reasons[token_lower] += 1
        return score, reasons

    def _weight_for_field(self, field: str) -> float:
        return {
            "name": 5.0,
            "description": 3.0,
            "keywords": 3.5,
            "columns": 2.5,
            "column_keywords": 2.0,
        }.get(field, 1.0)

    def find_join_paths(
        self,
        source_table: str,
        target_table: str,
        *,
        max_depth: int = 3,
        max_paths: int = 3,
    ) -> List[JoinPath]:
        source = source_table.lower()
        target = target_table.lower()
        if source == target:
            return [JoinPath(source=source_table, target=target_table, steps=[], length=0)]
        if source not in self.relationship_graph or target not in self.relationship_graph:
            return []

        queue = deque([(source, [])])
        results: List[JoinPath] = []
        visited_signatures: set[Tuple[str, ...]] = set()

        while queue and len(results) < max_paths:
            current, path = queue.popleft()
            if len(path) >= max_depth:
                continue
            for step in self.relationship_graph.get(current, []):
                next_table = step.to_table.lower()
                if any(s.from_table.lower() == step.to_table.lower() and s.to_table.lower() == step.from_table.lower() for s in path):
                    continue
                new_path = path + [step]
                if next_table == target:
                    signature = tuple(f"{seg.from_table}->{seg.to_table}" for seg in new_path)
                    if signature in visited_signatures:
                        continue
                    visited_signatures.add(signature)
                    results.append(
                        JoinPath(
                            source=source_table,
                            target=target_table,
                            steps=new_path,
                            length=len(new_path),
                        )
                    )
                    if len(results) >= max_paths:
                        break
                else:
                    queue.append((next_table, new_path))
        return results
