# SQL Insight Agent - Complete Flow Diagram & Explanation

## 1. HIGH-LEVEL ARCHITECTURE FLOW

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          USER REQUEST FLOW                                      │
└─────────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────────┐
    │  User Query      │
    │ (Natural Lang)   │
    └────────┬─────────┘
             │
             ▼
    ┌──────────────────────────────────────────────────────┐
    │  FastAPI POST /query Endpoint (app/main.py)          │
    │  ✓ Validate request schema                           │
    │  ✓ Load database configuration & intro template      │
    │  ✓ Resolve collection name for vector store          │
    └────────────┬─────────────────────────────────────────┘
             │
             ▼
    ┌──────────────────────────────────────────────────────┐
    │  LLM Provider Loop (Fallback Strategy)                │
    │  1. Try: ChatGroq (Primary)                           │
    │  2. Fallback: ChatGoogleGenerativeAI (Gemini)        │
    └────────────┬─────────────────────────────────────────┘
             │
             ▼
    ┌──────────────────────────────────────────────────────┐
    │  LangChain Agent Executor (create_agent)             │
    │  ✓ Receives: messages + system_prompt + tools        │
    │  ✓ Runs ReAct loop: Plan → Tool → Observe → Repeat   │
    └────────────┬─────────────────────────────────────────┘
             │
             ├─────────────┬──────────────────┬─────────────────┐
             │             │                  │                 │
             ▼             ▼                  ▼                 ▼
    ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
    │search_tables │ │fetch_table_  │ │fetch_table_  │ │validate_sql  │
    │              │ │summary       │ │section       │ │              │
    │ (Tool 1)     │ │ (Tool 2)     │ │ (Tool 3)     │ │ (Tool 4)     │
    └──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
           │                │                │                │
           └────────────────┴────────────────┴────────────────┘
                            │
                            ▼
                ┌──────────────────────────────┐
                │  PGVector Vector Store       │
                │  (Retrieves schema docs)     │
                │  - Collections               │
                │  - Metadata filters          │
                │  - Embeddings                │
                └──────────────────────────────┘
                            │
                            ▼
                ┌──────────────────────────────┐
                │  Agent Generates SQL         │
                │  (Based on context + tools)  │
                └────────────┬─────────────────┘
                             │
                             ▼
                ┌──────────────────────────────┐
                │  Sanitize SQL Output         │
                │  (Extract from markdown)     │
                └────────────┬─────────────────┘
                             │
                             ▼
                ┌──────────────────────────────┐
                │  Validate SQL                │
                │  (Read-only, no injections)  │
                └────────────┬─────────────────┘
                             │
        ┌────────────────────┴────────────────────┐
        │                                         │
        ▼ (Valid)                          ▼ (Invalid)
    ┌──────────────┐                  ┌──────────────┐
    │Execute Query │                  │Return Error  │
    │on Database   │                  │Response      │
    └────────┬─────┘                  └──────────────┘
             │
             ▼
    ┌──────────────────────────┐
    │Format Results            │
    │(JSON/CSV/Table)          │
    └────────┬─────────────────┘
             │
             ▼
    ┌──────────────────────────┐
    │Build QueryResponse       │
    │- status                  │
    │- sql                     │
    │- data (results)          │
    │- metadata                │
    │- error (if any)          │
    └────────┬─────────────────┘
             │
             ▼
    ┌──────────────────────────┐
    │Return to User (JSON)     │
    └──────────────────────────┘
```

---

## 2. DETAILED STEP-BY-STEP BREAKDOWN

### **STEP 1: Request Reception & Configuration Loading**

**Location:** `app/main.py` - `execute_query()` endpoint

**What happens:**
```python
1. Receive QueryRequest:
   - query: "I want to know from which company I receive how many return order in the October 2024"
   - db_flag: "avamed_db"
   - output_format: "json"

2. Load database settings for 'avamed_db':
   - Fetch connection string (MSSQL server details)
  - Load intro_template path (e.g., D:/sql-insight-agent/database_schemas/avamed_db/db_intro/avamed_db_intro.txt)
   - Load description

3. Resolve collection name:
   - default_collection_name("avamed_db") → "avamed_db_docs"
   - How it's selected: Environment variable lookup chain:
     a. Check: PGVECTOR_COLLECTION_NAME_AVAMED_DB (specific override)
     b. Check: PGVECTOR_COLLECTION_NAME (global default)
     c. Fallback: "{db_flag}_docs" = "avamed_db_docs"
```

**Log Output:**
```
2025-11-19 19:18:33 - main:167 - INFO - Received query request: query=..., db_flag=avamed_db, format=json
2025-11-19 19:18:33 - config:98 - INFO - Fetching database settings for db_flag= avamed_db
```

---

### **STEP 2: LLM Initialization with Provider Fallback**

**Location:** `app/agent/chain.py` - `get_cached_agent()` + `get_llm()`

**What happens:**
```python
Loop through providers:
  Provider 1: "groq"
    ✓ Initialize: ChatGroq(
        model="meta-llama/llama-4-scout-17b-16e-instruct",
        temperature=0.1
      )
    ✓ This is the PRIMARY provider
    
  IF Groq fails → Provider 2: "gemini"
    Initialize: ChatGoogleGenerativeAI(
      model="gemini-2.5-pro",
      temperature=0.1
    )
    This is the FALLBACK provider

For each provider:
- Cached via @lru_cache(maxsize=None) per (provider, db_flag, db_intro) tuple
- Reused across requests to avoid re-initialization overhead
```

**Log Output:**
```
2025-11-19 19:18:33 - chain:107 - DEBUG - Initializing ChatGroq model for provider=groq
2025-11-19 19:18:34 - chain:97 - INFO - Created LangChain SQL agent using model meta-llama/llama-4-scout-17b-16e-instruct
```

---

### **STEP 3: Agent Context Setup**

**Location:** `app/agent/tools.py` - `agent_context()` context manager

**What happens:**
```python
with agent_context(db_flag="avamed_db", collection_name="avamed_db_docs"):
    # Inside this block:
    # - _current_db_flag context variable = "avamed_db"
    # - _current_collection context variable = "avamed_db_docs"
    # - _accessed_tables context variable = empty set (tracks used tables)
    
    # These context vars are thread-local and accessible to all tools
    # This ensures metadata filters in retrieval include the correct db_flag
```

**Log Output:**
```
2025-11-19 19:18:34 - tools:60 - DEBUG - Agent context set db_flag=avamed_db collection=avamed_db_docs
```

---

### **STEP 4: Agent Invocation (LangChain ReAct Loop)**

**Location:** `app/agent/chain.py` - `create_agent()` creates runnable, then invoked in `app/main.py`

**What happens:**

```
Agent receives input message:
{
  "messages": [
    {"role": "user", "content": "I want to know from which company I receive..."}
  ]
}

System Prompt (from chain.py):
- Defines agent as "SQL Agent for AvasMed (DME management system)"
- Lists all available tables by category (PRODUCTS, ORDERS, FINANCIAL, etc.)
- Defines tool usage policy:
  1. Call search_tables first to find candidate tables
  2. For each table, fetch_table_summary
  3. If needed, fetch_table_section for columns/relationships
  4. Generate SQL only from confirmed schema
  5. Validate SQL before final output
  6. Output must be plain SELECT statement (no markdown)

ReAct Loop:
┌─────────────────────────────────────┐
│ 1. LLM analyzes user query          │
│    → Identifies intent: aggregate   │
│      return orders by company       │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│ 2. LLM decides which tools to call  │
│    Plan:                            │
│    - search_tables("return orders") │
│    - fetch_table_summary(...)       │
│    - fetch_table_section(...)       │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│ 3. Execute tool calls (4 tools)     │
│    Tool 1: search_tables            │
│    Tool 2: fetch_table_summary      │
│    Tool 3: fetch_table_section      │
│    Tool 4: validate_sql             │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│ 4. LLM observes results             │
│    → Gets confirmations of columns  │
│    → Gets relationship paths        │
│    → Gets validation OK             │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│ 5. LLM generates SQL                │
│    SELECT cm.Name, COUNT(...) ...   │
│    FROM ReturnDispense rd ...       │
│    WHERE MONTH(rd.ReceivedDate) ...│
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│ 6. Agent returns final response     │
│    (message with SQL content)       │
└─────────────────────────────────────┘
```

---

### **STEP 5: Tool Executions (Vector Retrieval)**

**Location:** `app/agent/tools.py` - tool functions call `app/core/retriever.py`

#### **Tool 1: search_tables()**
```
Input: "I want to know from which company I receive how many return order..."

Action:
1. Build query embedding using HuggingFace (jinaai/jina-embeddings-v3)
2. Search PGVector collection with filters:
   {
     "section": "summary",
     "db_flag": "avamed_db"
   }
3. Return top 4 documents (summaries of candidate tables)

Output: Descriptions of:
- ReturnDispense
- ClientInvoiceReturnDispense
- ClientInvoice
- CompanyMaster
```

**Log Output:**
```
2025-11-19 19:18:34 - retriever:59 - DEBUG - Creating PGVector client for collection=avamed_db_docs
2025-11-19 19:18:34 - retriever:44 - DEBUG - Initializing HuggingFace embeddings model=jinaai/jina-embeddings-v3
2025-11-19 19:18:48 - retriever:78 - DEBUG - vector_search collection=avamed_db_docs filters={'section': 'summary', 'db_flag': 'avamed_db'} hits=4
```

#### **Tool 2 & 3: fetch_table_summary() & fetch_table_section()**
```
For each identified table, agent calls:

fetch_table_summary(table_name="ReturnDispense")
  Filters: {"section": "summary", "table_name": "ReturnDispense", "db_flag": "avamed_db"}
  Returns: Full description of ReturnDispense table

fetch_table_section(table_name="ReturnDispense", section="columns")
  Filters: {"section": "columns", "table_name": "ReturnDispense", "db_flag": "avamed_db"}
  Returns: List of all columns with data types

fetch_table_section(table_name="ClientInvoice", section="relationships")
  Filters: {"section": "relationships", "table_name": "ClientInvoice", "db_flag": "avamed_db"}
  Returns: Foreign key relationships to other tables
```

**Log Output:**
```
2025-11-19 19:18:49 - retriever:78 - DEBUG - vector_search collection=avamed_db_docs filters={'section': 'summary', 'table_name': 'ReturnDispense', 'db_flag': 'avamed_db', 'schema': 'dbo'} hits=1
2025-11-19 19:18:49 - retriever:78 - DEBUG - vector_search collection=avamed_db_docs filters={'section': 'summary', 'table_name': 'ClientInvoice', 'db_flag': 'avamed_db', 'schema': 'dbo'} hits=1
2025-11-19 19:18:49 - retriever:78 - DEBUG - vector_search collection=avamed_db_docs filters={'section': 'summary', 'table_name': 'ClientInvoiceReturnDispense', 'db_flag': 'avamed_db', 'schema': 'dbo'} hits=1
2025-11-19 19:18:49 - retriever:78 - DEBUG - vector_search collection=avamed_db_docs filters={'section': 'summary', 'table_name': 'CompanyMaster', 'db_flag': 'avamed_db', 'schema': 'dbo'} hits=1
```

---

### **STEP 6: SQL Sanitization**

**Location:** `app/main.py` - `_sanitize_sql()`

**What happens:**
```python
Raw agent output (may include narration):
"""
To get the number of return orders for each company in October 2024, 
we need to join ReturnDispense, ClientInvoiceReturnDispense, ClientInvoice, 
and CompanyMaster tables.

```sql
SELECT cm.Name, 
       COUNT(rd.ReturnDispenseId) AS ReturnOrderCount
FROM ReturnDispense rd
INNER JOIN ClientInvoiceReturnDispense cir ON ...
WHERE MONTH(rd.ReceivedDate) = 10 AND YEAR(rd.ReceivedDate) = 2024
GROUP BY cm.Name
```
"""

Sanitization steps:
1. Strip code fences (```sql ... ```)
2. Remove language tags ("sql")
3. Find first SELECT keyword (case-insensitive)
4. Return everything from SELECT onwards

Final output:
"""
SELECT cm.Name, 
       COUNT(rd.ReturnDispenseId) AS ReturnOrderCount
FROM ReturnDispense rd
INNER JOIN ClientInvoiceReturnDispense cir ON ...
WHERE MONTH(rd.ReceivedDate) = 10 AND YEAR(rd.ReceivedDate) = 2024
GROUP BY cm.Name
"""
```

---

### **STEP 7: SQL Validation**

**Location:** `app/core/sql_validator.py`

**What happens:**
```python
validate_sql(sql_text):
1. Check if SQL starts with SELECT (read-only)
2. Reject INSERT, UPDATE, DELETE, DROP, ALTER (DML/DDL)
3. Reject common SQL injection patterns
4. Return: {"valid": true, "reason": null}  OR  {"valid": false, "reason": "..."}
```

---

### **STEP 8: Query Execution**

**Location:** `app/core/query_executor.py`

**What happens:**
```python
execute_query(sql_text, db_config):
1. Connect to MSSQL database using SQLAlchemy
2. Execute validated SQL query
3. Fetch results into pandas DataFrame
4. Return: {
     "success": true,
     "dataframe": <DataFrame>,
     "error": null,
     "row_count": 10
   }

Results:
┌──────────────────────────┬─────────────────────┐
│ Name                     │ ReturnOrderCount    │
├──────────────────────────┼─────────────────────┤
│ Addicks Medical Supply   │ 85                  │
│ Delta Medical Group Inc  │ 133                 │
│ Dune Medical Supply LLC  │ 9                   │
│ ...                      │ ...                 │
└──────────────────────────┴─────────────────────┘
```

---

### **STEP 9: Result Formatting**

**Location:** `app/core/result_formatter.py`

**What happens:**
```python
format_results(dataframe, output_format="json", ...):
1. Convert DataFrame to requested format (json/csv/table)
2. Add metadata:
   - row_count: 10
   - execution_time_ms: 85.49
   - columns: ["Name", "ReturnOrderCount"]

Output:
{
  "status": "success",
  "results": [
    {"Name": "Addicks Medical Supply LLC", "ReturnOrderCount": 85},
    {"Name": "Delta Medical Group Inc", "ReturnOrderCount": 133},
    ...
  ],
  "row_count": 10,
  "sql": "SELECT ...",
  "execution_time_ms": 85.49
}
```

---

### **STEP 10: Build Final Response**

**Location:** `app/main.py` - Build `QueryResponse` model

**QueryResponse Structure:**
```python
{
  "status": "success",                    # success or error
  "sql": "SELECT ...",                    # Generated T-SQL
  "validation_passed": true,              # SQL passed validation?
  "data": {                               # Results in requested format
    "results": [...],
    "row_count": 10,
    "execution_time_ms": 85.49
  },
  "error": null,                          # Error message (if any)
  "selected_tables": [                    # Tables used in query
    "ClientInvoice",
    "ClientInvoiceReturnDispense",
    "CompanyMaster",
    "ReturnDispense"
  ],
  "keyword_matches": null,                # Schema search keywords (future)
  "metadata": {                           # Execution metadata
    "execution_time_ms": 85.49,
    "total_rows": 10,
    "retry_count": 0
  },
  "token_usage": null                     # LLM token usage (future)
}
```

---

## 3. ANSWERING YOUR SPECIFIC QUESTIONS

### **Q1: What is `token_usage` and why is it `null`?**

**Answer:**
- `token_usage` tracks LLM API consumption (input tokens + output tokens)
- It's currently `null` because:
  - ChatGroq and ChatGoogleGenerativeAI don't expose token counts in responses (would need to call separate API)
  - Implementation is deferred for future optimization
  - LLM providers charge per-token, so this would help track costs
  
**Future Implementation:**
```python
# Could be implemented like:
{
  "token_usage": {
    "llm_input_tokens": 2048,
    "llm_output_tokens": 156,
    "total_tokens": 2204,
    "estimated_cost_usd": 0.032
  }
}
```

---

### **Q2: How is `collection_name` selected?**

**Answer:**

**Hierarchy (First match wins):**

1. **Per-database environment override:**
   ```bash
   PGVECTOR_COLLECTION_NAME_AVAMED_DB="my_custom_collection"  # ← Highest priority
   ```

2. **Global collection override:**
   ```bash
   PGVECTOR_COLLECTION_NAME="global_default_docs"  # ← Second priority
   ```

3. **Auto-generated fallback:**
   ```python
   f"{db_flag}_docs"  # ← Lowest priority
   # For db_flag="avamed_db" → "avamed_db_docs"
   # For db_flag="medical_db_prod" → "medical_db_prod_docs"
   ```

**Code Implementation:**
```python
# app/core/retriever.py
def default_collection_name(db_flag: str) -> str:
    """Resolve the PGVector collection name for a database flag."""
    
    normalized_flag = (db_flag or "").strip()
    
    # 1. Per-database override
    env_key = f"PGVECTOR_COLLECTION_NAME_{normalized_flag.upper()}"
    per_db = os.getenv(env_key)
    if per_db:
        return per_db
    
    # 2. Global default
    global_default = os.getenv("PGVECTOR_COLLECTION_NAME")
    if global_default:
        return global_default
    
    # 3. Auto-generated
    return f"{normalized_flag}_docs"
```

**Log Trace:**
```
app/main.py calls:
  collection_name = default_collection_name("avamed_db")

default_collection_name() returns:
  "avamed_db_docs"  (because no env overrides are set)

This collection is then used in:
  agent_context(db_flag="avamed_db", collection_name="avamed_db_docs")
```

---

### **Q3: What does metadata filter do?**

**Answer:**

**Purpose:** Ensure vector search only returns documents for the correct database and schema section.

**Metadata Filters Used:**

1. **Always included:**
   ```python
   {"db_flag": "avamed_db"}  # Only docs from this database
   ```

2. **By search type:**
   ```python
   # For searching candidate tables
   {"section": "summary", "db_flag": "avamed_db"}
   
   # For fetching specific table info
   {"section": "columns", "table_name": "ReturnDispense", "db_flag": "avamed_db"}
   
   # For relationships
   {"section": "relationships", "table_name": "ClientInvoice", "db_flag": "avamed_db"}
   ```

**Log Example:**
```
vector_search collection=avamed_db_docs 
  filters={'section': 'summary', 'table_name': 'CompanyMaster', 'db_flag': 'avamed_db', 'schema': 'dbo'} 
  hits=1
```

---

## 4. DATA FLOW DIAGRAM (SIMPLIFIED)

```
┌──────────────┐
│ User Query   │
└──────┬───────┘
       │
       ▼
┌──────────────────────────────────────┐
│ 1. Load Config & Resolve Collection  │
│    collection_name = "avamed_db_docs"│
└──────┬───────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────┐
│ 2. Initialize LLM (ChatGroq Primary) │
│    + System Prompt + Tools           │
└──────┬───────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────┐
│ 3. LangChain Agent Loop              │
│    ├─ search_tables()                │
│    ├─ fetch_table_summary()          │
│    ├─ fetch_table_section()          │
│    └─ validate_sql()                 │
│       ↓ (retrieves from PGVector)    │
└──────┬───────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────┐
│ 4. Generate SQL from Schema Context  │
└──────┬───────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────┐
│ 5. Sanitize (Extract from Markdown) │
└──────┬───────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────┐
│ 6. Validate SQL (Read-only Check)   │
└──────┬───────────────────────────────┘
       │
       ├─ INVALID ──────┐
       │                ▼
       │          Return Error
       │
       ▼ (VALID)
┌──────────────────────────────────────┐
│ 7. Execute on MSSQL Database         │
└──────┬───────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────┐
│ 8. Format Results (JSON/CSV/Table)   │
└──────┬───────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────┐
│ 9. Build QueryResponse (with metadata)
│    - status: "success"               │
│    - sql: "SELECT ..."               │
│    - data: results                   │
│    - metadata: execution_time, rows  │
└──────┬───────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────┐
│ Return JSON to User                  │
└──────────────────────────────────────┘
```

---

## 5. KEY COMPONENTS SUMMARY

| Component | Purpose | Technology |
|-----------|---------|-----------|
| **LangChain create_agent()** | Orchestrate ReAct loop | LangChain 1.0 |
| **ChatGroq / ChatGoogleGenerativeAI** | LLM inference | Groq + Google APIs |
| **PGVector** | Vector embeddings storage | PostgreSQL + pgvector extension |
| **HuggingFace Embeddings** | Generate query embeddings | jinaai/jina-embeddings-v3 |
| **SQLAlchemy** | Database connectivity | MSSQL dialect |
| **FastAPI** | HTTP API server | Uvicorn ASGI |
| **Pydantic Models** | Request/response validation | Type-safe schemas |

---

## 6. PROVIDER FALLBACK MECHANISM

```
Provider Loop Execution:

Attempt 1: ChatGroq
  ├─ Initialize model
  ├─ Run agent
  ├─ IF success → Use results, log success, break
  └─ IF error → Log error, continue to next

Attempt 2: ChatGoogleGenerativeAI (Gemini)
  ├─ Initialize model
  ├─ Run agent
  ├─ IF success → Use results, log success, break
  └─ IF error → Raise HTTPException(502 Bad Gateway)

Result:
  - If all providers fail → Return 502 error with last exception message
  - If any provider succeeds → Return 200 with results
```

**Log Example:**
```
2025-11-19 19:18:33 - chain:107 - DEBUG - Initializing ChatGroq model for provider=groq
2025-11-19 19:18:34 - chain:97 - INFO - Created LangChain SQL agent using model meta-llama/llama-4-scout-17b-16e-instruct
2025-11-19 19:18:49 - main:203 - INFO - Generated SQL using provider=groq
```

---

## 7. COLLECTION_NAME IN CONTEXT

```
Request Flow:

1. FastAPI receives: db_flag="avamed_db"
   │
   ├─ Load config for "avamed_db"
   │
   ├─ Call: default_collection_name("avamed_db")
   │  ├─ Check: PGVECTOR_COLLECTION_NAME_AVAMED_DB env var → Not set
   │  ├─ Check: PGVECTOR_COLLECTION_NAME env var → Not set
   │  └─ Return: "avamed_db_docs" (fallback)
   │
   ├─ Create agent_context(db_flag="avamed_db", collection_name="avamed_db_docs")
   │
   ├─ Agent calls tools:
   │  ├─ search_tables() → Uses collection "avamed_db_docs" + filter db_flag="avamed_db"
   │  ├─ fetch_table_summary() → Uses collection "avamed_db_docs" + filters
   │  ├─ fetch_table_section() → Uses collection "avamed_db_docs" + filters
   │  └─ validate_sql() → No collection used
   │
   └─ All retrieval calls query PGVector collection "avamed_db_docs"
      with metadata filters ensuring only correct database docs returned

Final Result:
  Only schema documents tagged with:
    - collection: "avamed_db_docs"
    - metadata: {"db_flag": "avamed_db", ...}
  are retrieved and used by the agent.
```

---

## 8. FUTURE ENHANCEMENTS

| Field | Current | Future |
|-------|---------|--------|
| `token_usage` | `null` | Track input/output token counts |
| `keyword_matches` | `null` | Log which keywords matched tables |
| Provider selection | Hardcoded loop | Config-driven priorities |
| Collection override | Env vars | Per-request override in API |
| SQL explain plans | Not captured | Analyze query performance |
| Retry logic | Implicit in agent | Explicit retry counts per tool |

---

**This completes the end-to-end flow. Each step is logged and traceable for debugging.**
