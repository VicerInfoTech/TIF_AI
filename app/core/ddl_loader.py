"""DDL loading utilities with caching."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=32)
def load_schema(ddl_path: Path) -> str:
	"""Load and cache the DDL schema text."""

	with ddl_path.open("r", encoding="utf-8") as ddl_file:
		return ddl_file.read()

