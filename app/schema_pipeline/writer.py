"""YAML generation helpers for schema extraction pipeline."""

from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import yaml

from app.models import DatabaseSchemaArtifacts
from app.utils.logger import setup_logging

logger = setup_logging(__name__)


class YamlSchemaWriter:
    """Persist schema artifacts as YAML files on disk."""

    def __init__(self, output_dir: Path, backup_existing: bool = True) -> None:
        self.output_dir = output_dir
        self.backup_existing = backup_existing
        self._prepared = False

    def write(self, artifacts: DatabaseSchemaArtifacts) -> Path:
        if not self._prepared:
            self._prepare_output_dir()
            self._prepared = True

        for schema_name, bucket in artifacts.schemas.items():
            schema_dir = self.output_dir / schema_name
            (schema_dir / "_views").mkdir(parents=True, exist_ok=True)
            (schema_dir / "_procedures").mkdir(parents=True, exist_ok=True)
            (schema_dir / "_functions").mkdir(parents=True, exist_ok=True)

            for table_name, table_data in bucket["tables"].items():
                self._dump_yaml(schema_dir / f"{table_name}.yaml", table_data)
            for view_name, view_data in bucket["views"].items():
                self._dump_yaml(schema_dir / "_views" / f"{view_name}.yaml", view_data)
            for proc_name, proc_data in bucket["procedures"].items():
                self._dump_yaml(schema_dir / "_procedures" / f"{proc_name}.yaml", proc_data)
            for func_name, func_data in bucket["functions"].items():
                self._dump_yaml(schema_dir / "_functions" / f"{func_name}.yaml", func_data)

        self._dump_yaml(self.output_dir / "metadata.yaml", artifacts.metadata_summary)
        self._dump_yaml(self.output_dir / "schema_index.yaml", artifacts.schema_index)
        logger.info("Schema YAML written to %s", self.output_dir)
        return self.output_dir

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _prepare_output_dir(self) -> None:
        if self.output_dir.exists():
            if self.backup_existing:
                timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                backup_dir = self.output_dir.with_name(f"{self.output_dir.name}_backup_{timestamp}")
                shutil.move(self.output_dir, backup_dir)
                logger.info("Existing schema output moved to %s", backup_dir)
            else:
                shutil.rmtree(self.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _dump_yaml(self, path: Path, payload: Dict[str, object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            yaml.safe_dump(payload, handle, sort_keys=False, allow_unicode=True, default_flow_style=False)
        tmp_path.replace(path)


__all__ = ["YamlSchemaWriter"]
