"""Application data models used across the SQL insight agent."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

from pydantic import BaseModel, Field, validator


class DatabaseSettings(BaseModel):
	"""Configuration options for a single database target."""

	connection_string: str = Field(..., min_length=1)
	ddl_file: Path = Field(..., description="Relative or absolute path to the DDL file")
	description: Optional[str] = None
	max_rows: int = Field(1000, ge=1, description="Maximum rows the agent should fetch")
	query_timeout: int = Field(30, ge=1, description="Query timeout in seconds")

	@validator("ddl_file", pre=True)
	def _coerce_path(cls, value: object) -> Path:
		if isinstance(value, Path):
			return value
		return Path(str(value))


class ApplicationConfig(BaseModel):
	"""Top-level configuration for the agent runtime."""

	databases: Dict[str, DatabaseSettings]

