"""Helpers to format SQL agent responses."""

from __future__ import annotations

from typing import Any, Dict

import pandas as pd


def format_results(
	dataframe: pd.DataFrame | None,
	sql: str,
	output_format: str,
	execution_time_ms: float | None = None,
) -> Dict[str, Any]:
	"""Format query execution output for the API response."""

	if dataframe is None:
		return {
			"status": "error",
			"data": {},
			"message": "No data returned",
		}

	if output_format == "table":
		payload = dataframe.to_string(index=False)
	elif output_format == "csv":
		payload = dataframe.to_csv(index=False)
	else:
		payload = dataframe.to_dict(orient="records")

	return {
		"status": "success",
		"data": {
			"results": payload,
			"sql": sql,
			"row_count": len(dataframe.index),
			"execution_time_ms": execution_time_ms,
		},
	}

