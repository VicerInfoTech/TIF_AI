"""CLI entrypoint for running the schema extraction pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Optional

from app.config import get_database_settings
from app.schema_pipeline import SchemaExtractionPipeline
from app.utils.logger import setup_logging

logger = setup_logging(__name__)


def generate(
    db_flag: str,
    *,
    include_schemas: Optional[Iterable[str]] = None,
    exclude_schemas: Optional[Iterable[str]] = None,
    output_dir: Optional[Path] = None,
    backup_existing: bool = True,
) -> Path:
    """Run the schema extraction pipeline for a configured database."""

    settings = get_database_settings(db_flag)
    logger.info("Generating schema artifacts for %s", db_flag)

    target_dir = output_dir or _default_output_dir(settings.ddl_file)
    target_dir = target_dir.resolve()

    pipeline = SchemaExtractionPipeline(
        settings.connection_string,
        target_dir,
        include_schemas=include_schemas,
        exclude_schemas=exclude_schemas,
        backup_existing=backup_existing,
    )
    pipeline.run()

    return target_dir


def _default_output_dir(path: Path) -> Path:
    base = Path(path)
    if base.suffix:
        return base.with_suffix("")
    return base.with_name(base.name + "_schema")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate schema artifacts for a configured database")
    parser.add_argument("db_flag", help="Identifier from config/database_config.json")
    parser.add_argument("--schemas", nargs="*", help="Optional whitelist of schemas to include")
    parser.add_argument("--exclude-schemas", nargs="*", help="Optional schemas to exclude")
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Directory for YAML output (defaults to ddl_file stem)",
    )
    parser.add_argument("--no-backup", action="store_true", help="Do not backup existing YAML directory")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    output_path = generate(
        args.db_flag,
        include_schemas=args.schemas,
        exclude_schemas=args.exclude_schemas,
        output_dir=args.output_dir,
        backup_existing=not args.no_backup,
    )
    logger.info("Schema artifacts written to %s", output_path)


if __name__ == "__main__":
    main()