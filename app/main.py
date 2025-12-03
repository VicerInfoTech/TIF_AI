"""FastAPI application for AQ Insight Agent."""

from __future__ import annotations

# pylint: disable=duplicate-code
import re
import os
from datetime import datetime
from os import getenv
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException, status
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import uuid

from app.models import (
    QueryRequest,
    QueryResponse,
    ExecutionMetadata,
    ErrorCode,
    HealthResponse,
    SchemaEmbeddingRequest,
    SchemaEmbeddingResponse,
    SchemaPipelineRequest,
    SchemaPipelineResponse,
    SchemaPipelineReport,
    ExtractionStageSummary,
    DocumentationStageSummary,
    EmbeddingStageSummary,
)

from app.agent.chain import (
    agent_context,
    default_collection_name,
    get_available_providers,
    get_cached_agent,
    get_cached_agent_with_context,
    get_collected_tables,
    parse_structured_response,
    summarize_query_results,
)
from typing import Tuple
from app.user_db_config_loader import get_user_database_settings, PROJECT_ROOT
from db.model import DatabaseConfig
from db.database_manager import (
    create_metadata_tables,
    get_project_db_connection_string,
    get_project_db_session,
    get_session,
)
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from app.core import query_executor, result_formatter, sql_validator
from app.schema_pipeline import SchemaPipelineOrchestrator
from app.schema_pipeline.embedding_pipeline import (
    SchemaEmbeddingPipeline,
    SchemaEmbeddingSettings,
)
from app.utils.logger import setup_logging
from db.conversation_memory import (
    store_query_context,
    get_query_history,
    get_session_summary,
    update_or_create_session_summary,
)
from app.agent.tools import get_tool_call_counts
from dotenv import load_dotenv
load_dotenv()

# Initialize logging
logger = setup_logging(__name__, level="INFO")

# Create FastAPI app
app = FastAPI(
    title="AQ Insight Agent",
    description="Natural Language to SQL query agent powered by LangChain with provider fallback",
    version="1.0.0",
)

# Dev-only static UI (chat page)
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    # mount under /static so files are available -- dev convenience only
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/chat")
async def chat_ui():
    """Dev UI: serve a small chat HTML page for manual testing.

    This route intentionally returns the static chat UI that posts to /query.
    It should be considered a development convenience and not a production feature.
    """
    f = static_dir / "chat.html"
    if f.exists():
        return FileResponse(f)
    # if static not present, redirect to OpenAPI docs as fallback
    return RedirectResponse(url="/docs")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Models moved to `app.models` for reusability and readability

def _short_provider_error(exc: Exception) -> tuple[str, int | str]:
    """Return a short, single-line error message and optional code for provider exceptions.

    Returns (message, code_or_type) where message is a short string and code_or_type is
    either an integer HTTP-like status code or provider-specific error type.
    """
    if exc is None:
        return ("Unknown provider error", 502)
    # Prefer structured provider response message if available
    try:
        if hasattr(exc, "response") and hasattr(exc.response, "json"):
            err_json = exc.response.json()
            if isinstance(err_json, dict):
                # Handle common "error" nesting
                msg = err_json.get("error", {}).get("message") or err_json.get("message")
                err_type = err_json.get("error", {}).get("type") or err_json.get("type")
                if msg:
                    return (msg, err_type or getattr(exc, "status_code", 502))
    except Exception:
        pass
    class_name = exc.__class__.__name__
    short_msg = str(exc).splitlines()[0]
    return (f"{class_name}: {short_msg}", getattr(exc, "status_code", 502))


ERROR_STATUS_MAP: dict[ErrorCode, int] = {
    ErrorCode.UNKNOWN_DATABASE: status.HTTP_400_BAD_REQUEST,
    ErrorCode.INVALID_REQUEST: status.HTTP_400_BAD_REQUEST,
    ErrorCode.PROVIDER_UNAVAILABLE: status.HTTP_503_SERVICE_UNAVAILABLE,
    ErrorCode.SQL_VALIDATION_FAILED: status.HTTP_422_UNPROCESSABLE_ENTITY,
    ErrorCode.QUERY_EXECUTION_FAILED: status.HTTP_500_INTERNAL_SERVER_ERROR,
    ErrorCode.RESULT_FORMATTING_FAILED: status.HTTP_500_INTERNAL_SERVER_ERROR,
    ErrorCode.INTERNAL_ERROR: status.HTTP_500_INTERNAL_SERVER_ERROR,
}


def _create_error_response(
    *,
    message: str,
    error_code: ErrorCode,
    request_id: str | None,
    validation_passed: bool | None,
    follow_up_questions: List[str] | None = None,
    execution_time_ms: float | None = None,
    retry_count: int = 0,
    status_code: int | None = None,
) -> JSONResponse:
    """Serialize a standardized error payload with consistent envelope and status code."""

    resolved_status = status_code or ERROR_STATUS_MAP.get(error_code, status.HTTP_500_INTERNAL_SERVER_ERROR)
    payload = QueryResponse(
        status=False,
        validation_passed=validation_passed,
        result=None,
        error=message,
        error_code=error_code,
        follow_up_questions=follow_up_questions,
        metadata=ExecutionMetadata(
            execution_time_ms=execution_time_ms,
            retry_count=retry_count,
            request_id=request_id,
        ),
    ).model_dump()
    return JSONResponse(status_code=resolved_status, content=payload)

def _invoke_providers(
    providers: List[str],
    request: QueryRequest,
    collection_name: str,
    request_id: str | None = None,
) -> Tuple[Dict[str, Any] | None, str | None, List[str], Exception | None]:
    """Try providers in order and return (agent_output, successful_provider, selected_tables, last_error).

    The function maintains the same behavior: fall back to the next provider on error, log short messages
    at ERROR/WARNING and debug tracebacks only.
    """
    agent_output: Dict[str, Any] | None = None
    last_error: Exception | None = None
    successful_provider: str | None = None
    selected_tables: List[str] = []

    for provider_idx, provider in enumerate(providers):
        try:
            if request.user_id and request.session_id:
                agent = get_cached_agent_with_context(
                    provider,
                    request.db_flag,
                    user_id=request.user_id,
                    session_id=request.session_id,
                )
                logger.debug(
                    "Using context-aware agent for user=%s, session=%s",
                    request.user_id,
                    request.session_id,
                )
            else:
                agent = get_cached_agent(provider, request.db_flag)
                logger.debug("Using stateless agent (no user/session context)")

            with agent_context(request.db_flag, collection_name, user_id=request.user_id, session_id=request.session_id):
                agent_output = agent.invoke({"messages": [{"role": "user", "content": request.query}]})
                selected_tables = get_collected_tables()
            logger.info("Generated SQL using provider=%s", provider)
            try:
                counts = get_tool_call_counts()
                schema_count = counts.get("get_database_schema", 0)
                logger.info("Tool usage counts for this run: get_database_schema=%s", schema_count)
            except Exception:
                logger.debug("Failed to obtain tool call counts for logging", exc_info=True)
            successful_provider = provider
            break
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            error_str = str(exc).lower()
            is_rate_limit = "429" in error_str or ("rate" in error_str and "limit" in error_str)
            is_provider_error = "provider returned error" in error_str
            short_msg, err_code = _short_provider_error(exc)
            if is_rate_limit or is_provider_error:
                logger.warning(
                    "Provider %s hit rate limit or temporary error (attempt %d/%d): %s",
                    provider,
                    provider_idx + 1,
                    len(providers),
                    short_msg,
                )
            else:
                logger.error("Provider %s failed during SQL generation: %s", provider, short_msg)
                logger.debug("Full provider exception (request=%s): %s", request_id, str(exc))
            if provider_idx < len(providers) - 1:
                logger.info("Falling back to next provider: %s (request=%s)", providers[provider_idx + 1], request_id)
                continue
    return agent_output, successful_provider, selected_tables, last_error


def _build_provider_error_response(last_error: Exception | None, request_id: str | None = None) -> JSONResponse:
    """Return a standardized error response for provider failures."""

    short_msg, provider_hint = _short_provider_error(last_error) if last_error else ("All LLM providers failed", 502)
    message = f"LLM provider failed: {short_msg}"
    if request_id:
        message = f"{message} [ref:{request_id}]"

    resolved_status = status.HTTP_503_SERVICE_UNAVAILABLE
    if isinstance(provider_hint, int):
        if provider_hint == 429:
            resolved_status = status.HTTP_429_TOO_MANY_REQUESTS
        elif 400 <= provider_hint < 600:
            resolved_status = provider_hint

    logger.error("Provider error (request=%s): %s", request_id, short_msg)
    if last_error:
        logger.debug("Full provider exception (request=%s): %s", request_id, str(last_error))

    return _create_error_response(
        message=message,
        error_code=ErrorCode.PROVIDER_UNAVAILABLE,
        request_id=request_id,
        validation_passed=False,
        follow_up_questions=None,
        status_code=resolved_status,
    )

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


def _mask_sql_for_logs(sql_text: str, max_len: int = 200) -> str:
    """Return a masked/shortened version of SQL suitable for logs.

    - Redacts string literals inside single or double quotes
    - Truncates to max_len and appends '...[REDACTED]' if longer
    """
    if not sql_text:
        return ""
    # redact single/double quoted strings
    masked = re.sub(r"'([^']*)'", "'<REDACTED>'", sql_text)
    masked = re.sub(r'\"([^\"]*)\"', '"<REDACTED>"', masked)
    # collapse whitespace
    masked = re.sub(r"\s+", " ", masked).strip()
    if len(masked) > max_len:
        return masked[:max_len] + " ... [TRUNCATED]"
    return masked


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
async def execute_query(request: QueryRequest) -> QueryResponse | JSONResponse:
    """Execute a natural language SQL query and return structured results or errors."""

    request_id = uuid.uuid4().hex
    follow_up_questions: List[str] | None = None
    contextual_insights: str | None = None

    try:
        logger.info(
            "Received query request: query=%s, db_flag=%s, format=%s",
            request.query,
            request.db_flag,
            request.output_format,
        )
        logger.debug("Request ID: %s", request_id)
        logger.debug(
            "Conversation identifiers user_id=%s session_id=%s",
            request.user_id,
            request.session_id,
        )

        try:
            db_settings = await get_user_database_settings(request.db_flag)
        except KeyError as exc:  # pragma: no cover - handled explicitly
            logger.error("Configuration error loading db_flag=%s: %s", request.db_flag, str(exc))
            return _create_error_response(
                message=f"Unknown database: {request.db_flag} [ref:{request_id}]",
                error_code=ErrorCode.UNKNOWN_DATABASE,
                request_id=request_id,
                validation_passed=False,
            )

        db_config = db_settings.model_dump()
        collection_name = default_collection_name(request.db_flag)
        logger.info("Using collection name: %s", collection_name)

        providers = get_available_providers()
        logger.debug("Provider order determined by environment: %s", providers)

        agent_output, successful_provider, selected_tables, last_error = _invoke_providers(
            providers,
            request,
            collection_name,
            request_id=request_id,
        )

        if agent_output is None:
            return _build_provider_error_response(last_error, request_id=request_id)

        structured_llm_response = parse_structured_response(agent_output)
        if structured_llm_response:
            raw_output = structured_llm_response.sql_query
            contextual_insights = structured_llm_response.query_context
            follow_up_questions = structured_llm_response.follow_up_questions
        else:
            raw_output = _extract_agent_output(agent_output)

        sql_generated = _sanitize_sql(raw_output)
        logger.info("Generated SQL (masked): %s", _mask_sql_for_logs(sql_generated))
        if not sql_generated:
            logger.error("Agent returned empty SQL output")
            return _create_error_response(
                message=f"Agent returned empty SQL output [ref:{request_id}]",
                error_code=ErrorCode.INTERNAL_ERROR,
                request_id=request_id,
                validation_passed=False,
            )

        validation_result = sql_validator.validate_sql(sql_generated)
        validation_ok = validation_result.get("valid", False)
        logger.info(
            "Validated SQL (mask): %s (valid=%s, reason=%s, request=%s)",
            _mask_sql_for_logs(sql_generated),
            validation_ok,
            validation_result.get("reason"),
            request_id,
        )
        if not validation_ok:
            client_err = f"{validation_result.get('reason')} [ref:{request_id}]"
            logger.warning("SQL validation failed: %s", client_err)
            return _create_error_response(
                message=client_err,
                error_code=ErrorCode.SQL_VALIDATION_FAILED,
                request_id=request_id,
                validation_passed=False,
                follow_up_questions=follow_up_questions,
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            )

        exec_start = perf_counter()
        execution = await query_executor.execute_query_async(sql_generated, db_config)
        elapsed_ms = (perf_counter() - exec_start) * 1000
        if not execution.get("success"):
            client_err = f"{execution.get('error')} [ref:{request_id}]"
            logger.error("SQL execution failed: %s", client_err)
            return _create_error_response(
                message=client_err,
                error_code=ErrorCode.QUERY_EXECUTION_FAILED,
                request_id=request_id,
                validation_passed=True,
                follow_up_questions=follow_up_questions,
                execution_time_ms=elapsed_ms,
            )

        dataframe = execution.get("dataframe")
        formatted = result_formatter.format_results(
            dataframe=dataframe,
            sql=sql_generated,
            output_format=request.output_format,
            execution_time_ms=elapsed_ms,
        )

        if not formatted.get("status"):
            client_err = f"{formatted.get('message', 'Failed to format results')} [ref:{request_id}]"
            logger.error("Result formatting failed: %s", client_err)
            return _create_error_response(
                message=client_err,
                error_code=ErrorCode.RESULT_FORMATTING_FAILED,
                request_id=request_id,
                validation_passed=True,
                follow_up_questions=follow_up_questions,
                execution_time_ms=elapsed_ms,
            )

        result_payload = formatted.get("result") or {}
        row_count_raw = result_payload.get("row_count")
        row_count: int | None = None
        if row_count_raw is not None:
            try:
                row_count = int(float(row_count_raw)) if float(row_count_raw) >= 0 else None
            except (TypeError, ValueError):
                logger.debug(
                    "Unable to coerce row_count=%r (%s) to int",
                    row_count_raw,
                    type(row_count_raw),
                )
                row_count = None

        logger.info(
            "Query execution completed: rows=%s elapsed_ms=%.1f",
            row_count,
            elapsed_ms,
        )

        if request.user_id and request.session_id:
            try:
                store_query_context(
                    user_id=request.user_id,
                    session_id=request.session_id,
                    db_flag=request.db_flag,
                    query_text=request.query,
                    sql_generated=sql_generated,
                    tables_used=selected_tables or [],
                    follow_up_questions=follow_up_questions or [],
                    contextual_insights=contextual_insights,
                    execution_time=elapsed_ms / 1000.0 if elapsed_ms else None,
                )
                update_or_create_session_summary(
                    user_id=request.user_id,
                    session_id=request.session_id,
                    db_flag=request.db_flag,
                )
                latest_history = get_query_history(
                    request.user_id,
                    request.session_id,
                    request.db_flag,
                    limit=1,
                )
                if latest_history:
                    latest = latest_history[0]
                    logger.debug(
                        "Session summary updated for user=%s, session=%s: latest_query=%s sql=%s",
                        request.user_id,
                        request.session_id,
                        latest.get("query_text"),
                        _mask_sql_for_logs(latest.get("sql_generated") or ""),
                    )
                else:
                    session_summary = get_session_summary(
                        user_id=request.user_id,
                        session_id=request.session_id,
                        db_flag=request.db_flag,
                    )
                    if session_summary:
                        logger.debug(
                            "Session summary updated for user=%s, session=%s: total_queries=%s",
                            request.user_id,
                            request.session_id,
                            session_summary.get("total_queries"),
                        )
                logger.debug(
                    "Stored query context for user=%s, session=%s",
                    request.user_id,
                    request.session_id,
                )
            except Exception as exc:  # pragma: no cover - persistence best effort
                logger.warning("Failed to store conversation history: %s", exc)
        else:
            logger.debug(
                "Skipping conversation persistence (missing identifiers) user_id=%s session_id=%s",
                request.user_id,
                request.session_id,
            )

        return QueryResponse(
            status=True,
            validation_passed=True,
            result=result_payload,
            error=None,
            error_code=None,
            follow_up_questions=follow_up_questions,
            metadata=ExecutionMetadata(
                execution_time_ms=elapsed_ms,
                retry_count=0,
                request_id=request_id,
            ),
        )

    except ValueError as exc:
        logger.error("Validation error: %s", exc)
        return _create_error_response(
            message=f"Invalid request: {exc}",
            error_code=ErrorCode.INVALID_REQUEST,
            request_id=request_id,
            validation_passed=False,
        )
    except Exception as exc:  # noqa: BLE001
        short_err = (
            query_executor._short_error_message(exc)  # type: ignore[attr-defined]
            if hasattr(query_executor, "_short_error_message")
            else str(exc)
        )
        logger.exception("Unexpected error during query execution: %s", short_err)
        return _create_error_response(
            message=f"Internal server error: {short_err}",
            error_code=ErrorCode.INTERNAL_ERROR,
            request_id=request_id,
            validation_passed=False,
        )


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


@app.post("/schemas/enroll", response_model=SchemaPipelineResponse)
async def enroll_database(request: SchemaPipelineRequest) -> SchemaPipelineResponse:
    """Enroll and extract a database schema, run documentation and embeddings."""
    logger.info("Running schema pipeline for db_flag=%s", request.db_flag)
    # POSTGRES_CONNECTION_STRING is now handled internally by the orchestrator

    try:
        project_connection = get_project_db_connection_string()
        await create_metadata_tables(project_connection)
        db_row = await _fetch_or_create_database_config(request, project_connection)
    except SQLAlchemyError as err:
        logger.error("DatabaseConfig check/insert failed: %s", err)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"DatabaseConfig check/insert failed: {err}",
        )

    if db_row.schema_extracted and not request.incremental_documentation:
        logger.info("Schema already extracted for db_flag=%s and incremental=False. Skipping.", request.db_flag)
        extraction_output = PROJECT_ROOT / "database_schemas" / request.db_flag / "schema"
        extraction_summary = ExtractionStageSummary(
            status="success",
            output_directory=str(extraction_output),
            tables_exported=0,
            message="Database already enrolled and schema extraction is up to date",
        )
        documentation_stage = DocumentationStageSummary(
            status="skipped",
            tables_total=0,
            documented=0,
            failed=0,
            message="Documentation skipped because schema already exists",
        )
        embeddings_stage = EmbeddingStageSummary(
            status="skipped",
            minimal_files=0,
            document_chunks=0,
            output_directory=str(SchemaEmbeddingPipeline.DEFAULT_OUTPUT_ROOT / request.db_flag),
            message="Embedding skipped because schema already exists",
        )
        report = _build_pipeline_report(extraction_summary, documentation_stage, embeddings_stage)
        return SchemaPipelineResponse(
            db_flag=request.db_flag,
            extraction=extraction_summary,
            documentation=documentation_stage,
            embeddings=embeddings_stage,
            report=report,
        )
    
    if db_row.schema_extracted:
        logger.info("Database %s already enrolled. Proceeding with update/refresh (incremental=True).", request.db_flag)

    # Now run the pipeline as before
    try:
        db_settings = await get_user_database_settings(request.db_flag)
        orchestrator = SchemaPipelineOrchestrator(
            request.db_flag,
            settings=db_settings,
            include_schemas=request.include_schemas,
            exclude_schemas=request.exclude_schemas,
            run_documentation=request.run_documentation,
            incremental_documentation=request.incremental_documentation,
            run_embeddings=request.run_embeddings,
        )
        outcome = orchestrator.run()

        extraction_summary = ExtractionStageSummary(
            status="success",
            output_directory=str(outcome.extraction_output),
            tables_exported=outcome.tables_exported,
            message="Schema extraction completed",
        )

        logger.info("Schema extraction completed: tables_exported=%d", outcome.tables_exported)
        
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

            await _mark_schema_extracted(request.db_flag)

        report = _build_pipeline_report(extraction_summary, documentation_stage, embeddings_stage)
        return SchemaPipelineResponse(
            db_flag=request.db_flag,
            extraction=extraction_summary,
            documentation=documentation_stage,
            embeddings=embeddings_stage,
            report=report,
        )
    except HTTPException:
        raise
    except Exception as error:
        logger.exception("Schema pipeline failed: %s", error)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Schema pipeline failed: {error}",
        ) from error


async def _fetch_or_create_database_config(request: SchemaPipelineRequest, project_connection: str) -> DatabaseConfig:
    try:
        async with get_project_db_session(project_connection) as session:
            result = await session.execute(select(DatabaseConfig).filter_by(db_flag=request.db_flag))
            db_row = result.scalar_one_or_none()
            if db_row:
                return db_row

            db_row = DatabaseConfig(
                db_flag=request.db_flag,
                db_type=request.db_type,
                connection_string=request.connection_string,
                description=request.description,
                intro_template=request.intro_template,
                exclude_column_matches=request.exclude_column_matches,
                # Set defaults internally for removed fields
                max_rows=10000,
                query_timeout=30,
            )
            session.add(db_row)
            try:
                await session.commit()
                await session.refresh(db_row)
                logger.info("Inserted new DatabaseConfig for db_flag=%s", request.db_flag)
            except IntegrityError:
                await session.rollback()
                fallback = await session.execute(select(DatabaseConfig).filter_by(db_flag=request.db_flag))
                db_row = fallback.scalar_one()
            return db_row
    except SQLAlchemyError as exc:
        logger.warning("Project DB async operation failed (%s). Falling back to sync path.", type(exc).__name__)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _fetch_or_create_database_config_sync, request, project_connection)


def _fetch_or_create_database_config_sync(request: SchemaPipelineRequest, project_connection: str) -> DatabaseConfig:
    session = get_session(project_connection)
    try:
        result = session.query(DatabaseConfig).filter_by(db_flag=request.db_flag).first()
        db_row = result
        if db_row:
            return db_row

        db_row = DatabaseConfig(
            db_flag=request.db_flag,
            db_type=request.db_type,
            connection_string=request.connection_string,
            description=request.description,
            intro_template=request.intro_template,
            exclude_column_matches=request.exclude_column_matches,
            # Set defaults internally for removed fields
            max_rows=10000,
            query_timeout=30,
        )
        session.add(db_row)
        try:
            session.commit()
            session.refresh(db_row)
            logger.info("Inserted new DatabaseConfig for db_flag=%s", request.db_flag)
        except IntegrityError:
            session.rollback()
            db_row = session.query(DatabaseConfig).filter_by(db_flag=request.db_flag).first()
        return db_row
    finally:
        session.close()


async def _mark_schema_extracted(db_flag: str) -> None:
    project_connection = get_project_db_connection_string()
    try:
        async with get_project_db_session(project_connection) as session:
            result = await session.execute(select(DatabaseConfig).filter_by(db_flag=db_flag))
            db_row = result.scalar_one_or_none()
            if not db_row:
                return
            db_row.schema_extracted = True
            db_row.schema_extraction_date = datetime.utcnow()
            await session.commit()
    except SQLAlchemyError as exc:
        logger.warning("Project DB async mark_schema_extracted failed (%s). Falling back to sync path.", type(exc).__name__)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _mark_schema_extracted_sync, db_flag, project_connection)


def _mark_schema_extracted_sync(db_flag: str, project_connection: str) -> None:
    session = get_session(project_connection)
    try:
        db_row = session.query(DatabaseConfig).filter_by(db_flag=db_flag).first()
        if not db_row:
            return
        db_row.schema_extracted = True
        db_row.schema_extraction_date = datetime.utcnow()
        session.commit()
    finally:
        session.close()



def _build_pipeline_report(
    extraction_summary: ExtractionStageSummary,
    documentation_stage: DocumentationStageSummary,
    embeddings_stage: EmbeddingStageSummary,
) -> SchemaPipelineReport:
    documentation_skipped = max(
        0,
        documentation_stage.tables_total - documentation_stage.documented - documentation_stage.failed,
    )
    return SchemaPipelineReport(
        extracted_files=extraction_summary.tables_exported,
        documentation_tables_total=documentation_stage.tables_total,
        documentation_documented=documentation_stage.documented,
        documentation_failed=documentation_stage.failed,
        documentation_skipped=documentation_skipped,
        embeddings_minimal_files=embeddings_stage.minimal_files,
        embeddings_document_chunks=embeddings_stage.document_chunks,
    )


@app.get("/")
async def root():
    """Root endpoint with API documentation link."""
    return {
        "message": "AQ Insight Agent API",
        "docs": "/docs",
        "health": "/health",
        "endpoints": {
            "POST /query": "Execute natural language SQL query",
            "POST /schemas/embeddings": "Convert schema YAML definitions to embeddings",
            "POST /schemas/enroll": "Enroll a database, extract schema, document, and embed",
            "GET /health": "Health check",
        },
    }
    
if __name__ == "__main__":
    import uvicorn

    logger.info("Starting AQ Insight Agent API server")
    uvicorn.run(
        "app.main:app",
        host="127.0.0.1",
        port=8000,
        reload=False,
        log_level="info",
    )


@app.on_event("startup")
async def warm_postgres_in_background():
    """Warm up the LangGraph Postgres connections in background to avoid
    a long first-request delay while keeping the server startup fast.
    """
    try:
        # Import lazily so we don't force initialization at module import time
        from db.langchain_memory import get_store, get_checkpointer
        do_warm = os.getenv("WARM_LANGGRAPH", "true").lower() in ("1", "true", "yes")
        if not do_warm:
            logger.info("LangGraph warm-up disabled via WARM_LANGGRAPH env var")
            return

        async def _warm():
            loop = asyncio.get_event_loop()
            # Run potentially blocking initialization in threadpool to avoid blocking the event loop
            await loop.run_in_executor(None, lambda: get_store())
            await loop.run_in_executor(None, lambda: get_checkpointer())
        # Schedule the warming task, do not await to avoid blocking startup
        asyncio.create_task(_warm())
        logger.info("Scheduled background warm-up for LangGraph Postgres resources")
    except Exception as exc:
        logger.warning("Failed to schedule LangGraph warm-up: %s", exc)


@app.on_event("startup")
async def warm_project_db_in_background():
    """Warm up the project DB (create metadata tables) in the background.

    This avoids slow startup due to metadata table creation while ensuring
    the tables are created before clients attempt write operations.
    """
    try:
        do_warm = os.getenv("WARM_PROJECT_DB", "true").lower() in ("1", "true", "yes")
        if not do_warm:
            logger.info("Project DB warm-up disabled via WARM_PROJECT_DB env var")
            return
        project_connection = get_project_db_connection_string()

        async def _warm():
            try:
                await create_metadata_tables(project_connection)
                logger.info("Warm-up: project metadata tables created or verified")
            except Exception as exc:  # pragma: no cover - best-effort background warming
                logger.warning("Project DB warm-up failed: %s", exc)

        asyncio.create_task(_warm())
        logger.info("Scheduled background warm-up for project metadata DB")
    except Exception as exc:
        logger.warning("Failed to schedule project DB warm-up: %s", exc)
