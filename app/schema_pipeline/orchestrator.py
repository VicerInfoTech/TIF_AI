"""Orchestrator that runs schema extraction, documentation, and embeddings in one flow."""

from __future__ import annotations

 
from pathlib import Path
from typing import Iterable, Optional

from app.user_db_config_loader import PROJECT_ROOT, get_user_database_settings
from app.schema_pipeline import SchemaExtractionPipeline
from app.schema_pipeline.embedding_pipeline import SchemaEmbeddingPipeline
from app.models import SchemaEmbeddingResult, SchemaEmbeddingSettings
from app.schema_pipeline.schema_documenting import document_database_schema
from app.models import SchemaDocumentationSummary
from app.utils.logger import setup_logging

logger = setup_logging(__name__)


from app.models import SchemaPipelineResult


class SchemaPipelineOrchestrator:
    """Runs extraction → documentation → embeddings and reports summarised data."""

    def __init__(
        self,
        db_flag: str,
        *,
        include_schemas: Iterable[str] | None = None,
        exclude_schemas: Iterable[str] | None = None,
        embedding_mode: str = "structured",
        run_documentation: bool = True,
        incremental_documentation: bool = True,
        run_embeddings: bool = True,
    ) -> None:
        from db.database_manager import get_engine
        self.db_flag = db_flag
        self.include_schemas = include_schemas
        self.exclude_schemas = exclude_schemas
        self.collection_name = f"{db_flag}_docs"
        self.chunk_size = 2000
        self.chunk_overlap = 100
        self.embedding_mode = embedding_mode
        self.run_documentation = run_documentation
        self.incremental_documentation = incremental_documentation
        self.run_embeddings = run_embeddings
        self.settings = get_user_database_settings(db_flag)
        self.extraction_output = PROJECT_ROOT / "config" / "schemas" / db_flag
        # Get the Postgres connection string from a central place (not user input)
        # This assumes you have a way to get the project-level Postgres connection string
        # For example, from an environment variable or a config file
        import os
        self.vector_connection_string = os.environ.get("POSTGRES_CONNECTION_STRING")

    def run(self) -> SchemaPipelineResult:
        logger.info("Starting schema pipeline for %s", self.db_flag)
        extraction_path = self._run_extraction()
        tables_exported = self._count_table_files(extraction_path)

        documentation_summary = None
        if self.run_documentation:
            documentation_summary = self._run_documentation(extraction_path)

        embedding_result = None
        if self.run_embeddings:
            embedding_result = self._run_embeddings()

        return SchemaPipelineResult(
            extraction_output=extraction_path,
            tables_exported=tables_exported,
            documentation_summary=documentation_summary,
            embedding_result=embedding_result,
        )

    def _run_extraction(self) -> Path:
        pipeline = SchemaExtractionPipeline(
            self.settings.connection_string,
            self.extraction_output,
            include_schemas=self.include_schemas,
            exclude_schemas=self.exclude_schemas,
            backup_existing=True,
        )
        pipeline.run()
        return self.extraction_output

    def _run_documentation(self, schema_dir: Path) -> SchemaDocumentationSummary:
        intro_path = Path(self.settings.intro_template)
        summary = document_database_schema(
            database_name=self.db_flag,
            schema_output_dir=schema_dir,
            intro_template_path=intro_path,
            incremental=self.incremental_documentation,
        )
        return summary

    def _run_embeddings(self) -> SchemaEmbeddingResult:
        connection = self.vector_connection_string
        if not connection:
            raise ValueError("POSTGRES_CONNECTION_STRING environment variable is required to embed schemas")

        settings = SchemaEmbeddingSettings(
            schema_root=SchemaEmbeddingPipeline.DEFAULT_SCHEMA_ROOT,
            minimal_output_root=SchemaEmbeddingPipeline.DEFAULT_OUTPUT_ROOT,
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap,
            collection_name=self.collection_name,
            embedding_mode=self.embedding_mode,
        )
        pipeline = SchemaEmbeddingPipeline(
            self.db_flag,
            connection,
            settings=settings,
        )
        return pipeline.run()

    def _count_table_files(self, directory: Path) -> int:
        excluded = {"schema_index.yaml", "metadata.yaml"}
        return sum(
            1
            for candidate in directory.rglob("*.yaml")
            if candidate.is_file() and candidate.name not in excluded
        )