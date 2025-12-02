# AQ Insight Agent

A LangGraph-based, agentic SQL query system for SQL Server and MySQL, with natural language to SQL, schema-driven intelligence, and secure validation. Built with FastAPI, multiple LLM providers (OpenAI, Anthropic, Groq, Gemini, DeepSeek), and YAML-based schema introspection.

---

## Features

- Natural language to SQL (NL2SQL) API
- Multi-database support (add new DBs via API or config table)
- Secure, read-only SQL validation (prevents unsafe queries)
- FastAPI HTTP API with OpenAPI docs
- **Multi-LLM Support**: OpenAI, OpenRouter, DeepSeek, Groq, Anthropic, Gemini
- Dual-agent pipeline: business intent → SQL generation
- YAML-driven schema intelligence (automatic join planning, table/column search)
- Schema extraction, documentation, and embedding pipeline
- PGVector/PGVector-backed Postgres for schema embeddings
- Structured logging to file and console

---

## Quickstart

### 1. Install `uv` (Windows)

This project uses `uv` for dependency management. Install it via PowerShell:

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 2. Clone & Sync

```sh
git clone <your-repo-url>
cd sql-insight-agent
uv sync
```

### 2. Environment Variables

Create a `.env` file in the project root. The following environment variables are supported:

#### LLM Providers (Set at least one)

- `OPENAI_API_KEY` — OpenAI
- `OPENROUTER_API_KEY` — OpenRouter
- `DEEPSEEK_API_KEY` — DeepSeek
- `GROQ_API_KEY` — Groq
- `ANTHROPIC_API_KEY` — Anthropic
- `GOOGLE_API_KEY` — Gemini/Google

#### Database/Vector Store

- `POSTGRES_CONNECTION_STRING` — Connection string for your PGVector-enabled Postgres instance (required for schema embeddings and agent checkpoints)

Example `.env`:

```
GROQ_API_KEY=your-groq-api-key
OPENAI_API_KEY=your-openai-api-key
POSTGRES_CONNECTION_STRING=postgresql://user:password@host:5432/dbname
```

### 3. Database Configuration

Databases are configured via the `DatabaseConfig` table in the project database. You can enroll a new database using the API.

#### Enroll a Database via API

Use the `POST /schemas/enroll` endpoint to register a database, extract its schema, and generate embeddings.

```json
POST /schemas/enroll
```json
{
  "db_flag": "your_db_flag",
  "connection_string": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=your_server;DATABASE=your_database;UID=your_user;PWD=your_password",
  "db_type": "mssql",
  "description": "Description of your database",
  "run_documentation": true,
  "run_embeddings": true
}
```

```

### 4. Run the API
```sh
uv run ./run.py
# or
python -m app.main
```

API will be available at: <http://127.0.0.1:8000>

---

## API Endpoints

### Query Endpoint

- `POST /query`
- Request body:

```json
{
  "query": "Show me all orders for September 2025",
  "db_flag": "your_database_flag"
}
```

```
- Returns: SQL, results (CSV, JSON, describe), and a natural-language summary.

### Schema Enrollment & Pipeline
- `POST /schemas/enroll`
- Enrolls a database, extracts schema, documents tables, and generates embeddings in one step.

### Schema Embedding (Standalone)
- `POST /schemas/embeddings`
- Refreshes embeddings for an existing schema.

## Dev Chat UI (temporary)

This repository includes a developer-facing chat UI for manual testing. It is intentionally lightweight and meant for local/dev use only.

- Open the UI in your browser after starting the server:
  - http://127.0.0.1:8000/chat
  - or http://127.0.0.1:8000/static/chat.html
- The UI sends requests to `POST /query` and displays the agent SQL plus results (CSV/JSON). It also allows entering `user_id` and `session_id` to exercise persistent conversation memory.
- WARNING: This UI is unauthenticated and meant for local dev only. Don't expose it in production without authentication & CORS restrictions.

---

## Schema Management
- Extracted schema YAMLs live under `config/schemas/<db_flag>/`.
- The pipeline API will extract, document, and embed all tables automatically.
- No manual scripts needed—just call the API.

---

## Logging
- Logs are written to `Log/app_YYYY-MM-DD.log` and the console.
- Log level is `DEBUG` by default for full traceability.

---

## Security
- Only SELECT queries are allowed (no DML/DDL).
- Semicolons are only allowed as a single trailing character.
- All SQL is validated before execution.

---

## Troubleshooting
- If you see Pydantic path warnings, ensure all paths in config are strings, not Path objects.
- If LLM queries fail, check your API keys and network access.
- For embedding, ensure your Postgres instance is running and accessible.

---

## Project Structure
```

app/
  main.py           # FastAPI entrypoint
  user_db_config_loader.py # DB config loader
  models.py         # Pydantic models
  agent/            # LLM agent logic, prompts, tools
  core/             # SQL, result formatting, retriever
  schema_pipeline/  # Schema extraction, embedding, docs
  utils/            # Logging, token tracking
config/
  schemas/<db_flag>/
  db_intro/
db/
  model.py          # DatabaseConfig SQLAlchemy model
Log/
tests/
run.py
README.md

```

---

## License
MIT
