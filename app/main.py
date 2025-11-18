"""FastAPI application for SQL Insight Agent."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from app.agent.graph import create_sql_agent_graph
from app.agent.state import initialise_state
from app.utils.logger import setup_logging

# Initialize logging
logger = setup_logging(__name__)

# Create FastAPI app
app = FastAPI(
    title="SQL Insight Agent",
    description="Natural Language to SQL query agent powered by LangGraph and Groq",
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

# Compile the LangGraph workflow once at startup
_graph = None


def get_graph():
    """Get or create the compiled LangGraph workflow."""
    global _graph
    if _graph is None:
        logger.info("Initializing LangGraph workflow")
        _graph = create_sql_agent_graph()
    return _graph


# Request/Response Models
class QueryRequest(BaseModel):
    """Request model for natural language SQL query."""

    query: str = Field(..., min_length=1, description="Natural language query")
    db_flag: str = Field(
        ..., min_length=1, description="Target database (e.g., 'medical_db_prod', 'inventory_db')"
    )
    output_format: str = Field(
        default="json",
        description="Output format: json, csv, or table",
        pattern="^(json|csv|table)$",
    )


class ExecutionMetadata(BaseModel):
    """Metadata about query execution."""

    execution_time_ms: Optional[float] = Field(None, description="Execution time in milliseconds")
    total_rows: Optional[int] = Field(None, description="Total rows returned")
    retry_count: int = Field(0, description="Number of retries performed")


class QueryResponse(BaseModel):
    """Response model for query execution."""

    status: str = Field(..., description="Response status (success or error)")
    sql: Optional[str] = Field(None, description="Generated SQL query")
    validation_passed: Optional[bool] = Field(None, description="Whether SQL passed validation")
    data: Optional[Dict[str, Any]] = Field(None, description="Query results in requested format")
    error: Optional[str] = Field(None, description="Error message if status is error")
    selected_tables: Optional[List[str]] = Field(None, description="Tables selected for this query")
    keyword_matches: Optional[List[str]] = Field(None, description="Tokens used for schema selection")
    metadata: ExecutionMetadata = Field(default_factory=ExecutionMetadata)
    token_usage: Optional[Dict[str, Any]] = Field(None, description="Token usage metrics")


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = Field("healthy")
    message: str = Field("SQL Insight Agent is running")
    version: str = Field("1.0.0")


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
        logger.info("Received query request: query=%s, db_flag=%s, format=%s",
                   request.query, request.db_flag, request.output_format)

        # Initialize state
        initial_state = initialise_state(
            query=request.query,
            db_flag=request.db_flag,
            output_format=request.output_format,
        )

        # Execute workflow
        graph = get_graph()
        logger.debug("Invoking LangGraph workflow")
        final_state = graph.invoke(initial_state)
        if final_state is None:
            logger.error("LangGraph workflow returned no state")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to execute the LangGraph workflow",
            )

        # Extract response data
        status_val = "success" if final_state.get("execution_result") is not None else "error"
        sql_generated = final_state.get("generated_sql")
        validation_result = final_state.get("validation_result") or {}
        validation_ok = validation_result.get("valid", False)
        error_msg = validation_result.get("reason") or final_state.get("error_message")
        formatted_data = final_state.get("final_response", {})
        execution_time = final_state.get("execution_time_ms")
        total_rows = final_state.get("total_rows")
        retry_count = final_state.get("retry_count", 0)

        logger.info("Query execution completed: status=%s, sql=%s, validation=%s",
                   status_val, sql_generated, validation_ok)

        return QueryResponse(
            status=status_val,
            sql=sql_generated,
            validation_passed=validation_ok,
            data=formatted_data if status_val == "success" else None,
            error=error_msg if status_val == "error" else None,
            selected_tables=final_state.get("selected_tables"),
            keyword_matches=final_state.get("keyword_matches"),
            metadata=ExecutionMetadata(
                execution_time_ms=execution_time,
                total_rows=total_rows,
                retry_count=retry_count,
            ),
            token_usage=final_state.get("token_usage"),
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


@app.get("/")
async def root():
    """Root endpoint with API documentation link."""
    return {
        "message": "SQL Insight Agent API",
        "docs": "/docs",
        "health": "/health",
        "endpoints": {
            "POST /query": "Execute natural language SQL query",
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
