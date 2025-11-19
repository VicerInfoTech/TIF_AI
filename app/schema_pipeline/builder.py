"""Transforms raw SQL Server metadata into structured YAML-ready dictionaries."""

from __future__ import annotations

from collections import defaultdict
 
from datetime import datetime, timezone
from itertools import combinations
from typing import Any, DefaultDict, Dict, Iterable, List, Tuple

from app.models import DatabaseSchemaArtifacts, RawMetadata
from app.utils.logger import setup_logging

logger = setup_logging(__name__)


from app.models import BuilderSettings


class SchemaGraphBuilder:
    """Convert raw SQL Server metadata into structured dictionaries."""

    def __init__(self, settings: BuilderSettings | None = None) -> None:
        self.settings = settings or BuilderSettings()

    def build(self, raw: RawMetadata) -> DatabaseSchemaArtifacts:
        logger.debug("Building structured schema payload for database %s", raw.database_name)
        schemas: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(lambda: {
            "tables": {},
            "views": {},
            "procedures": {},
            "functions": {},
        })

        table_lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}
        # row_counts removed (row count logic not needed)
        columns = self._group_by_keys(raw.columns, ("schema_name", "table_name"))
        pk_rows = self._group_by_keys(raw.primary_keys, ("schema_name", "table_name"))
        fk_rows = self._group_by_multiple_keys(raw.foreign_keys)
        idx_rows = self._group_by_multiple_keys(raw.indexes, key_fields=("schema_name", "table_name", "index_name"))
        uq_rows = self._group_by_multiple_keys(raw.unique_constraints, key_fields=("schema_name", "table_name", "constraint_name"))
        ck_rows = self._group_by_keys(raw.check_constraints, ("schema_name", "table_name"))
        
        logger.debug("Processing %d tables for schema construction", len(raw.tables))
        
        for table_row in raw.tables:
            schema_name = table_row["schema_name"]
            table_name = table_row["table_name"]
            key = (schema_name, table_name)
            logger.debug("Building table payload for %s.%s", schema_name, table_name)
            column_entries = [self._build_column_dict(col) for col in columns.get(key, [])]
            pk_entry = self._build_pk(pk_rows.get(key, []))
            fk_entry = self._build_foreign_keys(fk_rows.get(key, {}))
            indexes_entry, indexed_column_names = self._build_indexes(idx_rows.get((schema_name, table_name), {}))
            uniques_entry = self._build_unique_constraints(uq_rows.get((schema_name, table_name), {}))
            checks_entry = [self._build_check_dict(row) for row in ck_rows.get(key, [])]

            relationships = self._build_outgoing_relationships(fk_entry)
            statistics = self._table_statistics(column_entries, indexed_column_names)

            table_payload = {
                "table_name": table_name,
                "schema": schema_name,
                "object_type": "table",
                "description": table_row.get("table_description") or "",
                "created_date": self._to_iso(table_row.get("create_date")),
                "modified_date": self._to_iso(table_row.get("modify_date")),
                # "row_count" removed
                "columns": column_entries,
                "primary_key": pk_entry,
                "foreign_keys": fk_entry,
                "unique_constraints": uniques_entry,
                "indexes": indexes_entry,
                "check_constraints": checks_entry,
                "keywords": [],
                "relationships": relationships,
                "statistics": statistics,
            }

            schemas[schema_name]["tables"][table_name] = table_payload
            table_lookup[key] = table_payload

        logger.debug("Build and Processing incoming relationships for %d tables", len(table_lookup))
        
        # Build incoming relationships        
        incoming_map: DefaultDict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
        for (schema_name, table_name), table_payload in table_lookup.items():
            for fk in table_payload["foreign_keys"]:
                target_key = (fk["referenced_schema"], fk["referenced_table"])
                incoming_map[target_key].append(
                    {
                        "from_schema": schema_name,
                        "from_table": table_name,
                        "via_columns": fk["columns"],
                        "relationship_type": "one_to_many",
                    }
                )
        
        logger.debug("Assigning incoming relationships to tables")
        # Assign incoming relationships
        for key, incoming in incoming_map.items():
            if key in table_lookup:
                table_lookup[key]["relationships"]["incoming"] = incoming

        relationship_summary = self._augment_many_to_many(table_lookup)
        logger.debug("Augmented many-to-many relationships, summary: %s", relationship_summary)
        
        self._build_views(raw, schemas)
        self._build_procedures(raw, schemas)
        self._build_functions(raw, schemas)

        schema_index = self._build_schema_index(raw.database_name, schemas, relationship_summary)
        logger.debug("Successfully built schema index for database: %s", raw.database_name)

        metadata_summary = {
            "database_name": raw.database_name,
            "extracted_at": self._utc_now(),
            "total_schemas": len(schemas),
            "total_tables": sum(len(bucket["tables"]) for bucket in schemas.values()),
            "total_views": sum(len(bucket["views"]) for bucket in schemas.values()),
            "total_procedures": sum(len(bucket["procedures"]) for bucket in schemas.values()),
            "total_functions": sum(len(bucket["functions"]) for bucket in schemas.values()),
        }
        logger.debug("Metadata summary: %s", metadata_summary)

        return DatabaseSchemaArtifacts(
            database_name=raw.database_name,
            extracted_at=metadata_summary["extracted_at"],
            schemas=schemas,  # type: ignore[arg-type]
            schema_index=schema_index,
            metadata_summary=metadata_summary,
        )

    # ------------------------------------------------------------------
    # Builders for tables
    # ------------------------------------------------------------------

    def _build_column_dict(self, row: Dict[str, Any]) -> Dict[str, Any]:
        '''Build a dictionary representing a column from a row of metadata.'''
        return {
            "name": row["column_name"],
            "type": (row["data_type"] or "").lower(),
            "sql_type": self._compose_sql_type(row),
            "max_length": self._safe_int(row.get("max_length")),
            "precision": self._safe_int(row.get("precision")),
            "scale": self._safe_int(row.get("scale")),
            "is_nullable": bool(row.get("is_nullable", True)),
            "is_identity": bool(row.get("is_identity")),
            "identity_seed": self._safe_int(row.get("identity_seed_value")),
            "identity_increment": self._safe_int(row.get("identity_increment_value")),
            "is_computed": bool(row.get("is_computed")),
            "computed_definition": row.get("computed_definition"),
            "default_value": row.get("default_value"),
            "collation": row.get("collation_name"),
            "description": row.get("column_description") or "",
        }

    def _compose_sql_type(self, row: Dict[str, Any]) -> str:
        '''Compose the SQL type string for a column based on its attributes.'''
        data_type = (row.get("data_type") or "").lower()
        precision = row.get("precision")
        scale = row.get("scale")
        length = row.get("max_length")
        if data_type in {"nvarchar", "nchar", "ntext"} and isinstance(length, int):
            length = length // 2
        if data_type in {"varchar", "nvarchar", "char", "nchar", "binary", "varbinary"}:
            if length == -1:
                return f"{data_type}(max)"
            if length:
                return f"{data_type}({int(length)})"
        if data_type in {"decimal", "numeric"}:
            if precision is not None and scale is not None:
                return f"{data_type}({int(precision)},{int(scale)})"
        if precision and scale is not None:
            return f"{data_type}({int(precision)},{int(scale)})"
        return data_type

    def _build_pk(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        '''Build primary key dictionary from rows of metadata.'''
        if not rows:
            return {}
        columns = [
            {
                "name": row["column_name"],
                "ordinal": self._safe_int(row.get("key_ordinal")),
                "is_descending": bool(row.get("is_descending_key")),
            }
            for row in sorted(rows, key=lambda r: r.get("key_ordinal", 0))
        ]
        return {
            "constraint_name": rows[0].get("constraint_name"),
            "columns": [col["name"] for col in columns],
            "column_details": columns,
        }

    def _build_foreign_keys(self, grouped: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        payload: List[Dict[str, Any]] = []
        for constraint_name, rows in grouped.items():
            sorted_rows = sorted(rows, key=lambda r: r.get("column_name"))
            payload.append(
                {
                    "constraint_name": constraint_name,
                    "columns": [row["column_name"] for row in sorted_rows],
                    "referenced_schema": rows[0]["referenced_schema"],
                    "referenced_table": rows[0]["referenced_table"],
                    "referenced_columns": [row["referenced_column"] for row in sorted_rows],
                    "on_delete": rows[0].get("on_delete"),
                    "on_update": rows[0].get("on_update"),
                    "is_disabled": bool(rows[0].get("is_disabled")),
                }
            )
        return payload

    def _build_indexes(
        self,
        grouped: Dict[str, List[Dict[str, Any]]],
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        payload: List[Dict[str, Any]] = []
        indexed_columns: List[str] = []
        for index_name, rows in grouped.items():
            sorted_rows = sorted(rows, key=lambda r: r.get("key_ordinal", 0) or 0)
            columns = []
            for row in sorted_rows:
                column_name = row["column_name"]
                if not row.get("is_included_column"):
                    indexed_columns.append(column_name)
                columns.append(
                    {
                        "column": column_name,
                        "is_descending": bool(row.get("is_descending_key")),
                        "is_included": bool(row.get("is_included_column")),
                    }
                )
            payload.append(
                {
                    "index_name": index_name,
                    "is_unique": bool(rows[0].get("is_unique")),
                    "is_clustered": "CLUSTERED" in (rows[0].get("type_desc") or ""),
                    "filter_definition": rows[0].get("filter_definition"),
                    "columns": columns,
                }
            )
        return payload, indexed_columns

    def _build_unique_constraints(self, grouped: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        payload: List[Dict[str, Any]] = []
        for constraint_name, rows in grouped.items():
            sorted_rows = sorted(rows, key=lambda r: r.get("key_ordinal", 0) or 0)
            payload.append(
                {
                    "constraint_name": constraint_name,
                    "columns": [row["column_name"] for row in sorted_rows],
                }
            )
        return payload

    def _build_check_dict(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "constraint_name": row["constraint_name"],
            "definition": row.get("definition"),
            "is_disabled": bool(row.get("is_disabled")),
        }

    def _build_outgoing_relationships(self, foreign_keys: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        outgoing = [
            {
                "to_schema": fk["referenced_schema"],
                "to_table": fk["referenced_table"],
                "via_columns": fk["columns"],
                "relationship_type": "many_to_one",
            }
            for fk in foreign_keys
        ]
        return {
            "outgoing": outgoing,
            "incoming": [],
            "many_to_many": [],
        }

    def _table_statistics(self, columns: List[Dict[str, Any]], indexed_columns: List[str]) -> Dict[str, Any]:
        return {
            "total_columns": len(columns),
            "nullable_columns": sum(1 for col in columns if col["is_nullable"]),
            "computed_columns": sum(1 for col in columns if col["is_computed"]),
            "indexed_columns": len(set(indexed_columns)),
        }

    def _augment_many_to_many(self, table_lookup: Dict[Tuple[str, str], Dict[str, Any]]) -> Dict[str, Any]:
        summary = {"many_to_many_patterns": []}
        processed_pairs: set[Tuple[Tuple[str, str], Tuple[str, str], str]] = set()
        for (schema_name, table_name), payload in table_lookup.items():
            foreign_keys = payload.get("foreign_keys", [])
            if len(foreign_keys) < 2:
                continue
            referenced = {(fk["referenced_schema"], fk["referenced_table"]) for fk in foreign_keys}
            if len(referenced) < 2:
                continue
            fk_column_names = {col for fk in foreign_keys for col in fk["columns"]}
            non_fk_columns = [col for col in payload["columns"] if col["name"] not in fk_column_names]
            if len(non_fk_columns) > 2:
                continue
            for fk_left, fk_right in combinations(foreign_keys, 2):
                left_key = (fk_left["referenced_schema"], fk_left["referenced_table"])
                right_key = (fk_right["referenced_schema"], fk_right["referenced_table"])
                if left_key == right_key:
                    continue
                pair_key = tuple(sorted([left_key, right_key])) + ((schema_name, table_name),)  # type: ignore[operator]
                if pair_key in processed_pairs:
                    continue
                processed_pairs.add(pair_key)
                pattern = {
                    "junction_table": table_name,
                    "junction_schema": schema_name,
                    "left_table": left_key[1],
                    "left_schema": left_key[0],
                    "right_table": right_key[1],
                    "right_schema": right_key[0],
                }
                summary["many_to_many_patterns"].append(pattern)
                self._attach_m2m_entry(table_lookup, left_key, right_key, schema_name, table_name, fk_left["columns"])
                self._attach_m2m_entry(table_lookup, right_key, left_key, schema_name, table_name, fk_right["columns"])
        return summary

    def _attach_m2m_entry(
        self,
        table_lookup: Dict[Tuple[str, str], Dict[str, Any]],
        base_key: Tuple[str, str],
        other_key: Tuple[str, str],
        junction_schema: str,
        junction_table: str,
        via_columns: List[str],
    ) -> None:
        if base_key not in table_lookup:
            return
        table_lookup[base_key]["relationships"].setdefault("many_to_many", []).append(
            {
                "via_table": junction_table,
                "via_schema": junction_schema,
                "to_table": other_key[1],
                "to_schema": other_key[0],
                "via_columns": via_columns,
                "relationship_type": "many_to_many",
            }
        )

    # ------------------------------------------------------------------
    # Builders for other object types
    # ------------------------------------------------------------------

    def _build_views(self, raw: RawMetadata, schemas: Dict[str, Dict[str, Dict[str, Any]]]) -> None:
        view_columns = self._group_by_keys(raw.view_columns, ("schema_name", "view_name"))
        for row in raw.views:
            schema_name = row["schema_name"]
            view_name = row["view_name"]
            columns = [
                {
                    "name": col["column_name"],
                    "type": (col["data_type"] or "").lower(),
                    "max_length": self._safe_int(col.get("max_length")),
                    "is_nullable": bool(col.get("is_nullable", True)),
                    "description": col.get("column_description") or "",
                }
                for col in view_columns.get((schema_name, view_name), [])
            ]
            schemas[schema_name]["views"][view_name] = {
                "view_name": view_name,
                "schema": schema_name,
                "object_type": "view",
                "description": row.get("view_description") or "",
                "created_date": self._to_iso(row.get("create_date")),
                "modified_date": self._to_iso(row.get("modify_date")),
                "definition": row.get("definition"),
                "columns": columns,
                "keywords": [],
            }

    def _build_procedures(self, raw: RawMetadata, schemas: Dict[str, Dict[str, Dict[str, Any]]]) -> None:
        parameters = self._group_by_keys(raw.procedure_parameters, ("schema_name", "procedure_name"))
        for row in raw.procedures:
            schema_name = row["schema_name"]
            proc_name = row["procedure_name"]
            params = [
                {
                    "name": param["parameter_name"],
                    "type": (param["data_type"] or "").lower(),
                    "max_length": self._safe_int(param.get("max_length")),
                    "is_output": bool(param.get("is_output")),
                    "has_default": bool(param.get("has_default_value")),
                    "default_value": param.get("default_value"),
                }
                for param in parameters.get((schema_name, proc_name), [])
            ]
            schemas[schema_name]["procedures"][proc_name] = {
                "procedure_name": proc_name,
                "schema": schema_name,
                "object_type": "procedure",
                "description": row.get("procedure_description") or "",
                "created_date": self._to_iso(row.get("create_date")),
                "modified_date": self._to_iso(row.get("modify_date")),
                "definition": row.get("definition"),
                "parameters": params,
                "keywords": [],
            }

    def _build_functions(self, raw: RawMetadata, schemas: Dict[str, Dict[str, Dict[str, Any]]]) -> None:
        parameters = self._group_by_keys(raw.function_parameters, ("schema_name", "function_name"))
        for row in raw.functions:
            schema_name = row["schema_name"]
            func_name = row["function_name"]
            params = [
                {
                    "name": param["parameter_name"],
                    "type": (param["data_type"] or "").lower(),
                    "max_length": self._safe_int(param.get("max_length")),
                    "has_default": bool(param.get("has_default_value")),
                    "default_value": param.get("default_value"),
                }
                for param in parameters.get((schema_name, func_name), [])
            ]
            schemas[schema_name]["functions"][func_name] = {
                "function_name": func_name,
                "schema": schema_name,
                "object_type": "function",
                "function_type": (row.get("function_type") or "").lower(),
                "description": row.get("function_description") or "",
                "created_date": self._to_iso(row.get("create_date")),
                "modified_date": self._to_iso(row.get("modify_date")),
                "definition": row.get("definition"),
                "parameters": params,
                "keywords": [],
            }

    # ------------------------------------------------------------------
    # Schema index + helpers
    # ------------------------------------------------------------------

    def _build_schema_index(
        self,
        database_name: str,
        schemas: Dict[str, Dict[str, Dict[str, Any]]],
        relationship_summary: Dict[str, Any],
    ) -> Dict[str, Any]:
        tables_payload: List[Dict[str, Any]] = []
        views_payload: List[Dict[str, Any]] = []
        procs_payload: List[Dict[str, Any]] = []
        funcs_payload: List[Dict[str, Any]] = []
        for schema_name, bucket in schemas.items():
            for table_name, table in bucket["tables"].items():
                tables_payload.append(
                    {
                        "table": table_name,
                        "schema": schema_name,
                        "object_type": "table",
                        "keywords": table["keywords"],
                        "column_names": [col["name"] for col in table["columns"]],
                        "primary_key": table.get("primary_key", {}).get("columns"),
                        # "row_count" removed
                        "has_foreign_keys": bool(table.get("foreign_keys")),
                        "short_description": table.get("description") or "",
                    }
                )
            for view_name, view in bucket["views"].items():
                views_payload.append(
                    {
                        "view": view_name,
                        "schema": schema_name,
                        "object_type": "view",
                        "keywords": view["keywords"],
                        "column_names": [col["name"] for col in view["columns"]],
                        "short_description": view.get("description") or "",
                    }
                )
            for proc_name, proc in bucket["procedures"].items():
                procs_payload.append(
                    {
                        "procedure": proc_name,
                        "schema": schema_name,
                        "object_type": "procedure",
                        "keywords": proc["keywords"],
                        "parameters": [param["name"] for param in proc["parameters"]],
                        "short_description": proc.get("description") or "",
                    }
                )
            for func_name, func in bucket["functions"].items():
                funcs_payload.append(
                    {
                        "function": func_name,
                        "schema": schema_name,
                        "object_type": "function",
                        "function_type": func.get("function_type"),
                        "keywords": func["keywords"],
                        "parameters": [param["name"] for param in func["parameters"]],
                        "short_description": func.get("description") or "",
                    }
                )
        return {
            "database_name": database_name,
            "extraction_date": self._utc_now(),
            "total_schemas": len(schemas),
            "total_tables": len(tables_payload),
            "total_views": len(views_payload),
            "total_procedures": len(procs_payload),
            "total_functions": len(funcs_payload),
            "schemas": [
                {
                    "name": schema_name,
                    "table_count": len(bucket["tables"]),
                    "view_count": len(bucket["views"]),
                    "procedure_count": len(bucket["procedures"]),
                    "function_count": len(bucket["functions"]),
                }
                for schema_name, bucket in schemas.items()
            ],
            "tables": tables_payload,
            "views": views_payload,
            "procedures": procs_payload,
            "functions": funcs_payload,
            "relationship_summary": relationship_summary,
        }

    # ------------------------------------------------------------------
    # Generic helpers
    # ------------------------------------------------------------------

    def _group_by_keys(
        self,
        rows: Iterable[Dict[str, Any]],
        keys: Tuple[str, str],
    ) -> DefaultDict[Tuple[str, str], List[Dict[str, Any]]]:
        grouped: DefaultDict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[(row[keys[0]], row[keys[1]])].append(row)
        return grouped

    def _group_by_multiple_keys(
        self,
        rows: Iterable[Dict[str, Any]],
        key_fields: Tuple[str, ...] = ("schema_name", "table_name", "constraint_name"),
    ) -> DefaultDict[Tuple[str, str], Dict[str, List[Dict[str, Any]]]]:
        grouped: DefaultDict[Tuple[str, str], Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
        for row in rows:
            table_key = (row[key_fields[0]], row[key_fields[1]])
            constraint_key = row[key_fields[2]]
            grouped[table_key][constraint_key].append(row)
        return grouped

    def _to_lookup(
        self,
        rows: Iterable[Dict[str, Any]],
        schema_key: str,
        name_key: str,
        value_key: str,
    ) -> Dict[Tuple[str, str], Any]:
        lookup: Dict[Tuple[str, str], Any] = {}
        for row in rows:
            lookup[(row[schema_key], row[name_key])] = row.get(value_key)
        return lookup


    def _safe_int(self, value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _to_iso(self, value: Any) -> str | None:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.isoformat()
        return str(value) if value else None

    def _utc_now(self) -> str:
        return datetime.now(timezone.utc).isoformat()


__all__ = ["SchemaGraphBuilder", "BuilderSettings"]
