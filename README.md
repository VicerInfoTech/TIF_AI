# SQL Insight Agent

LangGraph-based agentic SQL query system for MySQL.

## Features
- Natural language to SQL
- Multi-DB support
- Secure validation
- FastAPI API
- Groq LLM integration
- Dual-agent pipeline (business intent + SQL execution)
- YAML-driven schema intelligence with deterministic join planning

## Architecture Overview

```
User NL Query
	│
	▼
+----------------------+      +------------------------------+
| Business Intent Agent|      | Schema Toolkit (YAML loader) |
| (Groq + Pydantic)    |◀────▶| search/describe/join helpers |
+----------------------+      +------------------------------+
				│ structured BusinessQuerySpec
				▼
+----------------------+      +-------------------+
| LangGraph SQL Agent  |─────▶| SQL Validator/DB  |
| (planner + generator)|◀────▶| Execution/Results |
+----------------------+      +-------------------+
```

- **Business Intent Agent**: single-shot Groq workflow that converts natural language into a `BusinessQuerySpec` (entities, metrics, dimensions, filters, time range).
- **Schema Toolkit**: loads `config/schemas/<db>` YAML artifacts, exposes table search, descriptions, and automatic join-path discovery.
- **SQL Agent**: the existing LangGraph with new nodes: `load_config → analyze_intent → select_schema → plan_sql → generate_sql → validate_sql → execute_query → format_results`.
- Both agents share the schema toolkit so LLMs never hallucinate tables/columns outside the documented metadata.

## Schema Pipeline API

The schema pipeline exposes a single API that performs extraction, documentation,
and embedding generation in one request. This replaces the previous CLI helpers
and ensures the FastAPI server is the single source of truth for schema
workflows. All YAML artifacts remain under `config/schemas/<db_flag>`, just like
before, but the API now orchestrates every stage for you.

```json
POST /schemas/pipeline
{
  "db_flag": "avamed_db",
  "collection_name": "boxmaster_docs",
  "run_documentation": true,
  "run_embeddings": true
}
```

- **Behavior**: generates the YAML schema artifacts, enriches them via the
  Groq-based documentation agent, converts each table to the ultra-minimal text
  format, and persists embeddings for later retrieval.
- **Control**: skip documentation or embedding by setting `run_documentation`
  or `run_embeddings` to `false`. Provide `postgres_connection_string` in the
  payload or via `POSTGRES_CONNECTION_STRING` so the embedding stage can access
  the PGVector-backed Postgres database.
- **Result**: the endpoint returns a structured summary of each stage (tables
  exported, documented, minimal files written, and the number of document
  chunks stored), enabling quick sanity checks before running downstream agents.

All schema artifacts are still grouped per schema (tables, views, functions) and
include `schema_index.yaml`, but you no longer need separate scripts to keep
them in sync.

## Logging and traceability

The agent uses `app/utils/logger.py` to emit structured logs to both console and
a daily file (`Log/app_YYYY-MM-DD.log`). The logger already sets `DEBUG` level,
so you can follow every node's progress when the server is running:

- `load_config` logs when the YAML schema toolkit is initialized and how many
  tables were loaded.
- `business_intent_node` logs how many schema summaries were provided and a
  high-level summary of the `BusinessQuerySpec` that was produced.
- `select_schema_node` reports the chosen table files and join summary size.
- `plan_sql_node` and `generate_sql_node` log prompt lengths, plan sizes, and
  token usage.
- `execute_query_node` reports execution success, row counts, and elapsed time,
  while `format_results_node` logs the final response status.

If you need more verbose output, adjust the formatter in `app/utils/logger.py`
or add additional `logger.debug` statements in the nodes as required.

## Schema embedding API

- Endpoint: `POST /schemas/embeddings`
- Payload: `{ "db_flag": "avamed_db", "collection_name": "boxmaster_docs" }`
- Environment: Set `POSTGRES_CONNECTION_STRING` to point at your PgVector-backed Postgres instance.
- Behavior: iterates the YAML files directly under `config/schemas/<db_flag>`, converts each to the ultra-minimal `temp_output/minimal/<db_flag>/<table>_minimal.txt` format, chunks the text, computes embeddings using `jinaai/jina-embeddings-v3`, and stores the results into the specified PGVector collection.
- Response: describe the processed files, output directory, and a short message.

Use this route whenever you regenerate schema YAMLs and want the matching embeddings refreshed for downstream LangGraph workflows.

## Unified schema pipeline API

- Endpoint: `POST /schemas/pipeline`
- Payload example:
  ```json
  {
    "db_flag": "avamed_db",
    "collection_name": "boxmaster_docs",
    "run_documentation": true,
    "run_embeddings": true
  }
  ```
- Environment: ensure `POSTGRES_CONNECTION_STRING` (or the optional `postgres_connection_string` payload field) points to a Postgres instance configured with PGVector.
- Behavior: runs the full extraction, documentation, and embedding flow and returns a structured report summarizing each stage (tables exported, documentation stats, embedding counts).
- Response: details per stage (`extraction`, `documentation`, `embeddings`) including success/skip state, counts, and output directories so you can quickly verify what changed.

Use this comprehensive route when you want to regenerate everything from scratch and immediately refresh the embeddings used by downstream agents.
