"""Application data models used across the SQL insight agent."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Sequence

from pydantic import BaseModel, Field, field_validator


class DatabaseSettings(BaseModel):
    """Configuration options for a single database target."""
    connection_string: str = Field(..., min_length=1)
    ddl_file: Path = Field(..., description="Relative or absolute path to the DDL file")
    intro_template: str = Field(..., description="Path to the business intro template file")
    description: Optional[str] = None
    max_rows: int = Field(1000, ge=1, description="Maximum rows the agent should fetch")
    query_timeout: int = Field(30, ge=1, description="Query timeout in seconds")
    exclude_column_matches: bool = Field(False, description="Skip column name/keyword matches when searching tables")
    alias_map_file: Optional[str] = Field(None, description="Path to YAML/JSON file mapping business terms to canonical table names")

    @field_validator("ddl_file", mode="before")
    def _coerce_path(value: object) -> Path:
        if isinstance(value, Path):
            return value
        return Path(str(value))
    

class ApplicationConfig(BaseModel):
    """Top-level configuration for the agent runtime."""

    databases: Dict[str, DatabaseSettings]


class TimeRange(BaseModel):
    """Represents an optional time range filter for business intent."""

    start: Optional[str] = Field(None, description="ISO date or datetime string for the start of the range")
    end: Optional[str] = Field(None, description="ISO date or datetime string for the end of the range")
    grain: Optional[str] = Field(None, description="Optional grain such as day, week, month")


class MetricSpec(BaseModel):
    """Requested metric with optional aggregation and column mapping."""

    name: str = Field(..., description="Business-friendly metric name, e.g. 'total_invoices'")
    aggregation: str = Field(..., description="Aggregation function, e.g. COUNT, SUM, AVG")
    column: Optional[str] = Field(None, description="Concrete column to aggregate once resolved")
    description: Optional[str] = None


class DimensionSpec(BaseModel):
    """Dimension/grouping requested by the user."""

    name: str = Field(..., description="Business-friendly dimension name, e.g. 'client', 'state'")
    column: Optional[str] = Field(None, description="Concrete column once mapped")
    grain: Optional[str] = Field(None, description="Temporal grain if applicable")
    description: Optional[str] = None


class FilterSpec(BaseModel):
    """Structured filter definition produced by the business intent agent."""

    field: str = Field(..., description="Business field being filtered")
    operator: str = Field(..., description="Comparison operator, e.g. '=', 'between', 'in'")
    values: Optional[List[str]] = Field(None, description="Optional list of values for the comparison")
    column: Optional[str] = Field(None, description="Resolved physical column if known")
    free_text: Optional[str] = Field(None, description="Fallback natural language description of the filter")


class BusinessQuerySpec(BaseModel):
    """Normalized representation of the user's intent before SQL generation."""

    intent: str = Field(..., description="Brief summary of the analytical question")
    entities: List[str] = Field(default_factory=list, description="Key business entities mentioned")
    metrics: List[MetricSpec] = Field(default_factory=list)
    dimensions: List[DimensionSpec] = Field(default_factory=list)
    filters: List[FilterSpec] = Field(default_factory=list)
    time_range: Optional[TimeRange] = None
    limit: Optional[int] = Field(None, ge=1, description="Optional row limit requested")
    notes: Optional[str] = Field(None, description="Any clarifying notes produced by the intent agent")


class ColumnInfo(BaseModel):
    """Detailed metadata for a single table column sourced from YAML artifacts."""

    name: str
    data_type: Optional[str] = None
    description: Optional[str] = None
    keywords: List[str] = Field(default_factory=list)
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_nullable: Optional[bool] = None
    references: Optional[str] = Field(None, description="Target table if column is a foreign key")


class ForeignKeyInfo(BaseModel):
    """Describes a foreign key relationship for a table."""

    name: Optional[str] = None
    columns: List[str]
    referenced_table: str
    referenced_columns: List[str]
    relationship_type: Optional[str] = Field(None, description="many_to_one, one_to_many, etc.")


class TableDetail(BaseModel):
    """Complete table metadata used during planning and SQL generation."""

    table_name: str
    schema: str
    description: Optional[str] = None
    keywords: List[str] = Field(default_factory=list)
    columns: List[ColumnInfo] = Field(default_factory=list)
    foreign_keys: List[ForeignKeyInfo] = Field(default_factory=list)


class TableMatch(BaseModel):
    """Result returned by schema search utilities."""

    table_name: str
    score: float
    reason: Optional[str] = None
    description: Optional[str] = None
    columns: Sequence[str] = Field(default_factory=tuple)


class JoinStep(BaseModel):
    """One hop inside a join path between two tables."""

    from_table: str
    to_table: str
    columns: List[str]
    referenced_columns: List[str]
    relationship_type: Optional[str] = None


class JoinPath(BaseModel):
    """Represents a viable set of joins connecting two tables."""

    source: str
    target: str
    steps: List[JoinStep]
    length: int

class ColumnDocumentation(BaseModel):
    """Documentation for a single database column.
    
    This model ensures that each column has a business-friendly description
    and searchable keywords for non-technical users.
    """
    
    column_name: str = Field(description="Name of the database column")
    description: str = Field(
        description="Business-friendly description of the column (1-2 sentences)",
        min_length=10,
        max_length=500
    )
    keywords: List[str] = Field(
        description="Exactly 3 business-friendly keywords that non-technical users would use",
        min_length=3,
        max_length=3
    )


class TableDocumentation(BaseModel):
    """Documentation for all columns in a database table.
    
    Contains a list of column documentation objects generated by the LLM.
    """
    
    columns: List[ColumnDocumentation] = Field(
        description="List of column documentation objects",
        min_length=1
    )
