"""Data models used across the schema extraction pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass(slots=True)
class RawMetadata:
    """Container for the raw metadata rows fetched from SQL Server."""

    database_name: str
    schemas: List[Dict[str, Any]]
    tables: List[Dict[str, Any]]
    columns: List[Dict[str, Any]]
    primary_keys: List[Dict[str, Any]]
    foreign_keys: List[Dict[str, Any]]
    indexes: List[Dict[str, Any]]
    unique_constraints: List[Dict[str, Any]]
    check_constraints: List[Dict[str, Any]]
    views: List[Dict[str, Any]]
    view_columns: List[Dict[str, Any]]
    procedures: List[Dict[str, Any]]
    procedure_parameters: List[Dict[str, Any]]
    functions: List[Dict[str, Any]]
    function_parameters: List[Dict[str, Any]]
    # row_counts removed (row count logic not needed)


@dataclass(slots=True)
class DatabaseSchemaArtifacts:
    """Structured payload ready for YAML/index generation."""

    database_name: str
    extracted_at: str
    schemas: Dict[str, Dict[str, Dict[str, Any]]]
    schema_index: Dict[str, Any]
    metadata_summary: Dict[str, Any] = field(default_factory=dict)


__all__ = ["RawMetadata", "DatabaseSchemaArtifacts"]
