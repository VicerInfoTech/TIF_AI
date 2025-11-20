"""Configuration loading utilities for the SQL insight agent."""

from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Dict

from dotenv import load_dotenv

from app.models import ApplicationConfig, DatabaseSettings
from app.utils.logger import setup_logging

# Load environment variables from .env if present.
load_dotenv()

logger = setup_logging(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "database_config.json"


def _resolve_path(path: Path) -> Path:
	"""Resolve paths relative to the project root and ensure existence."""

	resolved = path if path.is_absolute() else (PROJECT_ROOT / path).resolve()
	if not resolved.exists():
		raise FileNotFoundError(f"Configuration referenced path does not exist: {resolved}")
	return resolved


def _override_with_env(db_key: str, settings: DatabaseSettings) -> DatabaseSettings:
	"""Allow environment variables to override connection-sensitive fields."""

	prefix = db_key.upper()
	overrides: Dict[str, object] = {}

	conn_override = os.getenv(f"{prefix}_CONNECTION_STRING")
	if conn_override:
		overrides["connection_string"] = conn_override

	max_rows_override = os.getenv(f"{prefix}_MAX_ROWS")
	if max_rows_override and max_rows_override.isdigit():
		overrides["max_rows"] = int(max_rows_override)

	timeout_override = os.getenv(f"{prefix}_QUERY_TIMEOUT")
	if timeout_override and timeout_override.isdigit():
		overrides["query_timeout"] = int(timeout_override)

	if overrides:
		settings = settings.model_copy(update=overrides)

	# Only resolve paths that are referenced in configuration JSON
	resolved = {
		"connection_string": os.path.expandvars(settings.connection_string),
		"intro_template": str(_resolve_path(Path(settings.intro_template))),
	}

	return settings.model_copy(update=resolved)


def _load_raw_config(config_path: Path) -> ApplicationConfig:
	with config_path.open("r", encoding="utf-8") as cfg_file:
		data = json.load(cfg_file)
	return ApplicationConfig.model_validate(data)


def load_database_config(config_path: Path = DEFAULT_CONFIG_PATH) -> ApplicationConfig:
	"""Load and cache database configuration from disk."""

	absolute_path = config_path if config_path.is_absolute() else (PROJECT_ROOT / config_path).resolve()
	if not absolute_path.exists():
		raise FileNotFoundError(f"Database configuration file not found: {absolute_path}")

	config = _load_raw_config(absolute_path)

	resolved_databases = {
		key: _override_with_env(key, settings)
		for key, settings in config.databases.items()
	}

	return ApplicationConfig(databases=resolved_databases)


def get_database_settings(db_flag: str, config_path: Path = DEFAULT_CONFIG_PATH) -> DatabaseSettings:
	"""Convenience helper to fetch settings for a specific database."""

	config = load_database_config(config_path)
	logger.info("Fetching database settings for db_flag= %s", db_flag)
	try:
		return config.databases[db_flag]
	except KeyError as exc:
		raise KeyError(f"Unknown database flag '{db_flag}'. Available: {list(config.databases)}") from exc

