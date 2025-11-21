# SQL Insight Agent

A LangGraph-based, agentic SQL query system for SQL Server and MySQL, with natural language to SQL, schema-driven intelligence, and secure validation. Built with FastAPI, Groq/Gemini LLMs, and YAML-based schema introspection.

---

## Features
- Natural language to SQL (NL2SQL) API
- Multi-database support (add new DBs via config)
- Secure, read-only SQL validation (prevents unsafe queries)
- FastAPI HTTP API with OpenAPI docs
- Groq and Gemini LLM integration (provider fallback)
- Dual-agent pipeline: business intent → SQL generation
- YAML-driven schema intelligence (automatic join planning, table/column search)
- Schema extraction, documentation, and embedding pipeline
- PGVector/PGVector-backed Postgres for schema embeddings
- Structured logging to file and console

---

## Quickstart

### 1. Clone & Install
```sh
git clone <your-repo-url>
cd sql-insight-agent
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -r requirements.txt
```


### 2. Environment Variables
Create a `.env` file in the project root. The following environment variables are supported:

#### LLM Providers
- `GROQ_API_KEY` — API key for Groq LLM (required for Groq-based agent)
- `GOOGLE_API_KEY` — API key for Gemini/Google LLM (required for Gemini-based agent)

#### Database/Vector Store
- `POSTGRES_CONNECTION_STRING` — Connection string for your PGVector-enabled Postgres instance (required for schema embeddings)

#### Optional/Advanced
- `<DB_FLAG>_CONNECTION_STRING` — Override the connection string for a specific database (e.g., `AVAMED_DB_CONNECTION_STRING`)
- `<DB_FLAG>_MAX_ROWS` — Override the max rows for a specific database
- `<DB_FLAG>_QUERY_TIMEOUT` — Override the query timeout for a specific database

Example `.env`:
```
GROQ_API_KEY=your-groq-api-key
GOOGLE_API_KEY=your-google-api-key
POSTGRES_CONNECTION_STRING=postgresql://user:password@host:5432/dbname
AVAMED_DB_CONNECTION_STRING=DRIVER={ODBC Driver 17 for SQL Server};SERVER=host;DATABASE=avamed_db;UID=user;PWD=pass
```

### 3. Database Configuration
Edit `config/database_config.json` to add your database(s):
```json
{
  "databases": {
    "avamed_db": {
      "connection_string": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=host;DATABASE=avamed_db;UID=user;PWD=pass",
      "intro_template": "config/db_intro/avamed_db_intro.txt",
      "description": "AvasMed DME management database",
      "max_rows": 1000,
      "query_timeout": 30,
      "exclude_column_matches": false
    }
  }
}
```
- Add a new block for each DB you want to support.
- `intro_template` is a path to a text file describing the business context for the DB.

### 4. Run the API
```sh
uv run ./run.py
# or
python -m app.main
```

API will be available at: http://127.0.0.1:8000

---

## API Endpoints

### Query Endpoint
- `POST /query`
- Request body:
```json
{
  "query": "Show me all orders for September 2025",
  "db_flag": "avamed_db"
}
```
- Returns: SQL, results (CSV, JSON, describe), and a natural-language summary.

### Schema Embedding
- `POST /schemas/embeddings`
- Request body:
```json
{
  "db_flag": "avamed_db",
  "collection_name": "avamed_db_docs"
}
```
- Stores schema embeddings in PGVector.

### Unified Schema Pipeline
- `POST /schemas/pipeline`
- Request body:
```json
{
  "db_flag": "avamed_db",
  "collection_name": "avamed_db_docs",
  "run_documentation": true,
  "run_embeddings": true
}
```
- Runs extraction, documentation, and embedding in one call.

## Dev Chat UI (temporary)

This repository includes a developer-facing chat UI for manual testing. It is intentionally lightweight and meant for local/dev use only.

- Open the UI in your browser after starting the server:
  - http://127.0.0.1:8000/chat
  - or http://127.0.0.1:8000/static/chat.html
- The UI sends requests to `POST /query` and displays the agent SQL plus results (CSV/JSON). It also allows entering `user_id` and `session_id` to exercise persistent conversation memory.
- WARNING: This UI is unauthenticated and meant for local dev only. Don't expose it in production without authentication & CORS restrictions.

---

## Schema Management
- All schema YAMLs live under `config/schemas/<db_flag>/`.
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

## Adding a New Database
1. Add a new entry in `config/database_config.json`.
2. Place a business intro text file in `config/db_intro/`.
3. Place schema YAMLs in `config/schemas/<db_flag>/` (or use the pipeline API to generate them).
4. Restart the API server.

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
  config.py         # Config loader
  models.py         # Pydantic models
  agent/            # LLM agent logic, prompts, tools
  core/             # SQL, result formatting, retriever
  schema_pipeline/  # Schema extraction, embedding, docs
  utils/            # Logging, token tracking
config/
  database_config.json
  schemas/<db_flag>/
  db_intro/
Log/
tests/
run.py
README.md
```

---

## License
MIT
