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

## Schema Extraction Pipeline

Use the `generate_schema.py` helper to extract SQL Server metadata and emit the
YAML artifacts consumed by the LangGraph agents. The runtime now relies
exclusively on the YAML schema directory (`config/schemas/<db_flag>`), so
regenerate the artifacts whenever the database schema changes:

```bash
python generate_schema.py avamed_db --format yaml
```

### Common options

- `--schemas <schema1> <schema2>` – only include the listed schemas.
- `--exclude-schemas <schema>` – skip specific schemas.
- `--output-dir <path>` – override the default output location (defaults to the
  stem of the configured `ddl_file`).
- `--no-backup` – overwrite the existing YAML output directory instead of
  rotating it with a timestamped backup.
- `--format yaml` – emit YAML files only (preferred).
- `--format both` – still available if you need the legacy compact JSON
  artifact for external tooling, but the SQL agent ignores that file.

All YAML files are grouped per schema, with dedicated folders for views,
procedures, and functions alongside the master `schema_index.yaml` file.

### Column documentation

The schema YAML files capture basic metadata, but you can run
`python document_schema.py <db_flag>` to refresh column descriptions,
keywords, and narrative summaries via the documentation pipeline. Save the
results under `config/schemas/<db_flag>` and the LangGraph nodes will
automatically surface the updated descriptions when finding tables or
columns.

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
