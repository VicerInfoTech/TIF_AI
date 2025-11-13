"""SQL validation helpers to enforce read-only access."""

from __future__ import annotations

import re
from typing import Dict

READ_ONLY_PATTERN = re.compile(r"^\s*select\b", re.IGNORECASE)
FORBIDDEN_KEYWORDS = re.compile(
	r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|merge|call|replace)\b",
	re.IGNORECASE,
)


def validate_sql(sql: str) -> Dict[str, object]:
	"""Validate SQL is read-only and safe."""

	trimmed = sql.strip()
	if not trimmed:
		return {"valid": False, "reason": "Empty SQL statement"}

	if ";" in trimmed:
		return {"valid": False, "reason": "Semicolons are not permitted"}

	if not READ_ONLY_PATTERN.match(trimmed):
		return {"valid": False, "reason": "SQL must start with SELECT"}

	if FORBIDDEN_KEYWORDS.search(trimmed):
		return {"valid": False, "reason": "Detected forbidden keyword for read-only mode"}

	return {"valid": True, "reason": "SQL passed read-only validation"}

