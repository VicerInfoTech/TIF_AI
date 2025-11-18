"""DDL loading utilities with caching."""

from __future__ import annotations

from pathlib import Path


def load_schema(ddl_path: Path) -> str:
	"""Load and cache the DDL schema text."""

	with ddl_path.open("r", encoding="utf-8") as ddl_file:
		return ddl_file.read()

