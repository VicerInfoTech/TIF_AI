"""FastAPI application for SQL Insight Agent."""

from __future__ import annotations

# pylint: disable=duplicate-code
import re
from os import getenv
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware

from app.models import (
    QueryRequest,
    QueryResponse,
    ExecutionMetadata,
    HealthResponse,
    SchemaEmbeddingRequest,
    SchemaEmbeddingResponse,
    SchemaPipelineRequest,
    SchemaPipelineResponse,
    ExtractionStageSummary,
    DocumentationStageSummary,
    EmbeddingStageSummary,
)

from app.agent.chain import (
    agent_context,
    default_collection_name,
    get_cached_agent,
    get_collected_tables,
)
from app.config import get_database_settings
from app.core import query_executor, result_formatter, sql_validator
from app.schema_pipeline import SchemaPipelineOrchestrator
from app.schema_pipeline.embedding_pipeline import (
    SchemaEmbeddingPipeline,
    SchemaEmbeddingSettings,
)
from app.utils.logger import setup_logging

# Initialize logging
logger = setup_logging(__name__)

# Create FastAPI app
app = FastAPI(
    title="SQL Insight Agent",
    description="Natural Language to SQL query agent powered by LangChain with provider fallback",
    version="1.0.0",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Models moved to `app.models` for reusability and readability

_INTRO_CACHE: Dict[str, str] = {}


def _load_db_intro(db_flag: str, intro_path: str | None, fallback: str | None = None) -> str:
    """Load and cache the business introduction text for a database."""

    cached = _INTRO_CACHE.get(db_flag)
    if cached is not None:
        return cached

    intro_text = fallback or ""
    if intro_path:
        try:
            intro_text = Path(intro_path).read_text(encoding="utf-8").strip()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to read intro template for %s: %s", db_flag, exc)
    _INTRO_CACHE[db_flag] = intro_text
    return intro_text


def _sanitize_sql(sql_text: str) -> str:
    """Remove formatting fences and whitespace from the agent's SQL output."""

    if not sql_text:
        return ""
    cleaned = sql_text.strip()

    code_blocks = re.findall(r"```(?:sql)?\s*([\s\S]*?)```", cleaned, flags=re.IGNORECASE)
    if code_blocks:
        cleaned = code_blocks[-1].strip()

    if cleaned.lower().startswith("sql"):
        cleaned = cleaned[3:].lstrip(" :\n")

    select_match = re.search(r"select", cleaned, flags=re.IGNORECASE)
    if select_match:
        cleaned = cleaned[select_match.start():]

    return cleaned.strip()


def _extract_agent_output(agent_result: Any) -> str:
    """Normalize the agent response into a plain string."""

    def _stringify_segments(content: Any) -> str:
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict):
                    text_value = item.get("text")
                    if text_value:
                        parts.append(str(text_value))
                else:
                    text_attr = getattr(item, "text", None)
                    if text_attr:
                        parts.append(str(text_attr))
                    else:
                        parts.append(str(item))
            return "\n".join(part for part in parts if part)
        return str(content)

    if isinstance(agent_result, dict):
        messages = agent_result.get("messages")
        if isinstance(messages, list) and messages:
            final_message = messages[-1]
            content = getattr(final_message, "content", None)
            if content is not None:
                return _stringify_segments(content)
        for key in ("output", "content", "answer"):
            value = agent_result.get(key)
            if value:
                return _stringify_segments(value)
    return _stringify_segments(agent_result)


# Endpoints
@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    logger.debug("Health check requested")
    return HealthResponse()


@app.post("/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest) -> QueryResponse:
    """Execute a natural language SQL query.

    Args:
        request: QueryRequest with query, db_flag, and output_format

    Returns:
        QueryResponse with status, SQL, validation result, and formatted data

    Raises:
        HTTPException: If execution fails or database is unavailable
    """
    try:
        logger.info(
            "Received query request: query=%s, db_flag=%s, format=%s",
            request.query,
            request.db_flag,
            request.output_format,
        )

        db_settings = get_database_settings(request.db_flag)
        db_config = db_settings.model_dump()
        db_intro = _load_db_intro(
            request.db_flag,
            db_settings.intro_template,
            fallback=db_settings.description,
        )
        collection_name = default_collection_name(request.db_flag)

        providers = ("groq", "gemini")
        agent_output: Dict[str, Any] | None = None
        selected_tables: List[str] = []
        last_error: Exception | None = None

        for provider in providers:
            agent = get_cached_agent(provider, request.db_flag, db_intro)
            try:
                with agent_context(request.db_flag, collection_name):
                    agent_output = agent.invoke(
                        {
                            "messages": [
                                {
                                    "role": "user",
                                    "content": request.query,
                                }
                            ]
                        }
                    )
                    selected_tables = get_collected_tables()
                logger.info("Generated SQL using provider=%s", provider)
                break
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                logger.exception("Provider %s failed during SQL generation", provider)

        if agent_output is None:
            detail = (
                f"LLM providers unavailable: {last_error}" if last_error else "All LLM providers failed"
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=detail,
            )

        raw_output = _extract_agent_output(agent_output)
        sql_generated = _sanitize_sql(raw_output)
        if not sql_generated:
            logger.error("Agent returned empty SQL output")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Agent returned empty SQL output",
            )

        validation_result = sql_validator.validate_sql(sql_generated)
        validation_ok = validation_result.get("valid", False)
        if not validation_ok:
            logger.warning("SQL validation failed: %s", validation_result.get("reason"))
            return QueryResponse(
                status="error",
                sql=sql_generated,
                validation_passed=False,
                data=None,
                error=validation_result.get("reason"),
                selected_tables=selected_tables or None,
                keyword_matches=None,
                metadata=ExecutionMetadata(
                    execution_time_ms=None,
                    total_rows=None,
                    retry_count=0,
                ),
                token_usage=None,
            )

        exec_start = perf_counter()
        execution = query_executor.execute_query(sql_generated, db_config)
        elapsed_ms = (perf_counter() - exec_start) * 1000
        if not execution.get("success"):
            logger.error("SQL execution failed: %s", execution.get("error"))
            return QueryResponse(
                status="error",
                sql=sql_generated,
                validation_passed=True,
                data=None,
                error=execution.get("error"),
                selected_tables=selected_tables or None,
                keyword_matches=None,
                metadata=ExecutionMetadata(
                    execution_time_ms=elapsed_ms,
                    total_rows=None,
                    retry_count=0,
                ),
                token_usage=None,
            )

        dataframe = execution.get("dataframe")
        formatted = result_formatter.format_results(
            dataframe=dataframe,
            sql=sql_generated,
            output_format=request.output_format,
            execution_time_ms=elapsed_ms,
        )

        if formatted.get("status") != "success":
            logger.error("Result formatting failed: %s", formatted.get("message"))
            return QueryResponse(
                status="error",
                sql=sql_generated,
                validation_passed=True,
                data=None,
                error=formatted.get("message", "Failed to format results"),
                selected_tables=selected_tables or None,
                keyword_matches=None,
                metadata=ExecutionMetadata(
                    execution_time_ms=elapsed_ms,
                    total_rows=None,
                    retry_count=0,
                ),
                token_usage=None,
            )

        total_rows_raw = formatted.get("data", {}).get("row_count") if formatted.get("data") else None
        total_rows: int | None = None
        if total_rows_raw is not None:
            try:
                # Handle floats, strings, numpy types, etc.
                total_rows_int = int(float(total_rows_raw))
                if total_rows_int >= 0:
                    total_rows = total_rows_int
            except (TypeError, ValueError):
                logger.debug("Unable to coerce row_count=%r (%s) to int", total_rows_raw, type(total_rows_raw))
                total_rows = None

        logger.info(
            "Query execution completed: rows=%s elapsed_ms=%.1f",
            total_rows,
            elapsed_ms,
        )

        return QueryResponse(
            status="success",
            sql=sql_generated,
            validation_passed=True,
            data=formatted.get("data"),
            error=None,
            selected_tables=selected_tables or None,
            keyword_matches=None,
            metadata=ExecutionMetadata(
                execution_time_ms=elapsed_ms,
                total_rows=total_rows,
                retry_count=0,
            ),
            token_usage=None,
        )

    except ValueError as e:
        logger.error("Validation error: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request: {str(e)}",
        ) from e
    except KeyError as e:
        logger.error("Configuration error: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown database: {str(e)}",
        ) from e
    except Exception as e:
        logger.exception("Unexpected error during query execution: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        ) from e


@app.post("/schemas/embeddings", response_model=SchemaEmbeddingResponse)
async def generate_schema_embeddings(request: SchemaEmbeddingRequest) -> SchemaEmbeddingResponse:
    """Convert every schema YAML into embeddings stored in Postgres."""

    logger.info("Generating schema embeddings for db_flag=%s", request.db_flag)
    connection_string = getenv("POSTGRES_CONNECTION_STRING")
    if not connection_string:
        logger.error("Postgres connection string missing for embeddings")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Missing POSTGRES_CONNECTION_STRING",
        )

    try:
        settings = SchemaEmbeddingSettings(
            schema_root=SchemaEmbeddingPipeline.DEFAULT_SCHEMA_ROOT,
            minimal_output_root=SchemaEmbeddingPipeline.DEFAULT_OUTPUT_ROOT,
            collection_name=request.collection_name,
        )
        pipeline = SchemaEmbeddingPipeline(
            db_flag=request.db_flag,
            connection_string=connection_string,
            settings=settings,
        )
        result = pipeline.run()
        output_directory = pipeline.settings.minimal_output_root / request.db_flag

        message = (
            "Embeddings stored successfully"
            if result.document_chunks > 0
            else "No schema files were processed"
        )

        return SchemaEmbeddingResponse(
            db_flag=request.db_flag,
            output_directory=str(output_directory),
            processed_files=[path.name for path in result.minimal_files],
            message=message,
        )
    except HTTPException:
        raise
    except Exception as error:
        logger.exception("Schema embedding pipeline failed: %s", error)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Schema embedding pipeline failed: {error}",
        ) from error


@app.post("/schemas/pipeline", response_model=SchemaPipelineResponse)
async def run_schema_pipeline(request: SchemaPipelineRequest) -> SchemaPipelineResponse:
    """Run extraction → documentation → embeddings and return a reported summary."""

    logger.info("Running schema pipeline for db_flag=%s", request.db_flag)
    vector_connection = request.postgres_connection_string or getenv("POSTGRES_CONNECTION_STRING")
    if request.run_embeddings and not vector_connection:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="POSTGRES_CONNECTION_STRING is required to generate embeddings",
        )

    try:
        orchestrator = SchemaPipelineOrchestrator(
            request.db_flag,
            include_schemas=request.include_schemas,
            exclude_schemas=request.exclude_schemas,
            collection_name=request.collection_name,
            chunk_size=request.embedding_chunk_size,
            chunk_overlap=request.embedding_chunk_overlap,
            run_documentation=request.run_documentation,
            run_embeddings=request.run_embeddings,
            vector_connection_string=vector_connection,
        )
        outcome = orchestrator.run()

        extraction_summary = ExtractionStageSummary(
            status="success",
            output_directory=str(outcome.extraction_output),
            tables_exported=outcome.tables_exported,
            message="Schema extraction completed",
        )

        if request.run_documentation:
            doc_summary = outcome.documentation_summary
            if doc_summary is None:
                documentation_stage = DocumentationStageSummary(
                    status="failed",
                    tables_total=0,
                    documented=0,
                    failed=0,
                    message="Documentation stage did not produce a summary",
                )
            else:
                documentation_stage = DocumentationStageSummary(
                    status="success",
                    tables_total=doc_summary.tables_total,
                    documented=doc_summary.documented,
                    failed=doc_summary.failed,
                    message="Documentation completed",
                )
        else:
            documentation_stage = DocumentationStageSummary(
                status="skipped",
                tables_total=0,
                documented=0,
                failed=0,
                message="Documentation stage was skipped",
            )

        embeddings_output_dir = SchemaEmbeddingPipeline.DEFAULT_OUTPUT_ROOT / request.db_flag
        if request.run_embeddings:
            embedding_result = outcome.embedding_result
            if embedding_result is None:
                embeddings_stage = EmbeddingStageSummary(
                    status="failed",
                    minimal_files=0,
                    document_chunks=0,
                    output_directory=str(embeddings_output_dir),
                    message="Embedding stage did not produce results",
                )
            else:
                embeddings_stage = EmbeddingStageSummary(
                    status="success",
                    minimal_files=len(embedding_result.minimal_files),
                    document_chunks=embedding_result.document_chunks,
                    output_directory=str(embeddings_output_dir),
                    message="Embedding stage completed",
                )
        else:
            embeddings_stage = EmbeddingStageSummary(
                status="skipped",
                minimal_files=0,
                document_chunks=0,
                output_directory=str(embeddings_output_dir),
                message="Embedding stage was skipped",
            )

        return SchemaPipelineResponse(
            db_flag=request.db_flag,
            extraction=extraction_summary,
            documentation=documentation_stage,
            embeddings=embeddings_stage,
        )
    except HTTPException:
        raise
    except Exception as error:
        logger.exception("Schema pipeline failed: %s", error)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Schema pipeline failed: {error}",
        ) from error


@app.get("/")
async def root():
    """Root endpoint with API documentation link."""
    return {
        "message": "SQL Insight Agent API",
        "docs": "/docs",
        "health": "/health",
        "endpoints": {
            "POST /query": "Execute natural language SQL query",
            "POST /schemas/embeddings": "Convert schema YAML definitions to embeddings",
            "POST /schemas/pipeline": "Run schema extraction, documentation, and embedding",
            "GET /health": "Health check",
        },
    }
    
if __name__ == "__main__":
    import uvicorn

    logger.info("Starting SQL Insight Agent API server")
    uvicorn.run(
        "app.main:app",
        host="127.0.0.1",
        port=8000,
        reload=False,
        log_level="info",
    )
