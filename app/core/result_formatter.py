"""Helpers to format SQL agent responses."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict

import numpy as np
import pandas as pd


def _serialize_value(value: Any) -> Any:
	if value is None:
		return None
	if isinstance(value, float) and np.isnan(value):
		return None
	if isinstance(value, (datetime, date)):
		return value.isoformat()
	if isinstance(value, np.generic):
		return value.item()
	if isinstance(value, (int, float, str, bool)):
		return value
	return str(value)


def format_results(
	dataframe: pd.DataFrame | None,
	sql: str,
	output_format: str,
	execution_time_ms: float | None = None,
) -> Dict[str, Any]:
	"""Format query execution output for the API response.
	
	Returns only the data in the requested format (json or csv).
	Defaults to json if format is not recognized.
	"""

	if dataframe is None:
		return {
			"status": False,
			"result": {},
			"message": "No data returned",
		}

	# Normalize output_format
	if output_format not in ("json", "csv"):
		output_format = "json"
	
	# Generate data in requested format
	if output_format == "csv":
		payload = dataframe.to_csv(index=False)
	else:  # json
		payload = dataframe.to_json(orient="records", date_format="iso")

	# Normalize format for response
	response_format = "csv" if output_format == "csv" else "json"

	return {
		"status": True,
		"result": {
			"content": payload,
			"row_count": int(len(dataframe.index)),
			"filetype": response_format,
		},
	}

