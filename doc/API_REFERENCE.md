# SQL Insight Agent - API Quick Reference

## Base URL
```
http://127.0.0.1:8000
```

## Authentication
Currently no authentication required (development mode)

---

## Endpoints Summary

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/` | API information |
| POST | `/query` | Execute natural language query |
| POST | `/schemas/enroll` | Enroll new database |
| POST | `/schemas/embeddings` | Refresh embeddings |
| GET | `/chat` | Developer UI |

---

## 1. POST /query

Execute a natural language SQL query with optional conversation context.

### Request Body

```json
{
  "query": "string (required) - Natural language question",
  "db_flag": "string (required) - Database identifier",
  "output_format": "json|csv|table (optional, default: json)",
  "user_id": "string (optional) - For conversation tracking",
  "session_id": "string (optional) - For conversation tracking"
}
```

### Example Request

```bash
curl -X POST "http://127.0.0.1:8000/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Show me top 10 customers by total order value",
    "db_flag": "sales_prod",
    "output_format": "json",
    "user_id": "analyst@company.com",
    "session_id": "session-001"
  }'
```

### Response (Success)

```json
{
  "status": "success",
  "sql": "SELECT TOP 10 CustomerID, CustomerName, SUM(OrderTotal) as TotalValue FROM Orders...",
  "validation_passed": true,
  "data": {
    "results": [...],
    "row_count": 10,
    "execution_time_ms": 245.67,
    "csv": "CustomerID,CustomerName,TotalValue\n...",
    "raw_json": "[{...}]"
  },
  "selected_tables": ["Orders", "Customers"],
  "follow_up_questions": [
    "Would you like to see this by product category?",
    "Should I compare to last quarter?"
  ],
  "natural_summary": "The top 10 customers have an average order value of...",
  "metadata": {
    "execution_time_ms": 245.67,
    "total_rows": 10,
    "retry_count": 0
  }
}
```

### Response (Error)

```json
{
  "status": "error",
  "sql": "SELECT * FROM NonExistentTable",
  "validation_passed": true,
  "error": "Table 'NonExistentTable' does not exist",
  "data": null,
  "metadata": {
    "execution_time_ms": 123.45,
    "total_rows": null,
    "retry_count": 0
  }
}
```

---

## 2. POST /schemas/enroll

Enroll a new database: extract schema, generate documentation, create embeddings.

### Request Body

```json
{
  "db_flag": "string (required) - Unique database identifier",
  "db_type": "mssql|mysql|postgresql (required)",
  "connection_string": "string (required) - Database connection string",
  "description": "string (optional) - Human-readable description",
  "intro_template": "string (optional) - Path to business context file",
  "exclude_column_matches": "boolean (optional, default: false)",
  "include_schemas": ["string"] (optional) - Schema whitelist",
  "exclude_schemas": ["string"] (optional) - Schema blacklist",
  "run_documentation": "boolean (optional, default: true)",
  "incremental_documentation": "boolean (optional, default: true)",
  "run_embeddings": "boolean (optional, default: true)"
}
```

### Connection String Formats

**⚠️ SECURITY REQUIREMENT**: Always use READ-ONLY database credentials in connection strings. The SQL Insight Agent only needs SELECT permissions.

**SQL Server (with read-only user):**
```
DRIVER={ODBC Driver 17 for SQL Server};SERVER=host:port;DATABASE=dbname;UID=readonly_user;PWD=pass
```

**MySQL (with read-only user):**
```
mysql+pymysql://readonly_user:password@host:port/dbname
```

**PostgreSQL (with read-only user):**
```
postgresql://readonly_user:password@host:port/dbname
```

**How to create read-only users**: See `POSTGRESQL_SETUP.md` → Security Configuration section

### Example Request

```bash
curl -X POST "http://127.0.0.1:8000/schemas/enroll" \
  -H "Content-Type: application/json" \
  -d '{
    "db_flag": "sales_prod",
    "db_type": "mssql",
    "connection_string": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=sql-server:1433;DATABASE=SalesDB;UID=readonly;PWD=SecurePass123",
    "description": "Production sales database",
    "intro_template": "database_schemas/sales_prod/db_intro/sales_context.txt",
    "exclude_schemas": ["sys", "tempdb"],
    "run_documentation": true,
    "run_embeddings": true
  }'
```

### Response

```json
{
  "db_flag": "sales_prod",
  "extraction": {
    "status": "success",
    "output_directory": "d:\\sql-insight-agent\\database_schemas\\sales_prod\\schema",
    "tables_exported": 45,
    "message": "Schema extraction completed"
  },
  "documentation": {
    "status": "success",
    "tables_total": 45,
    "documented": 45,
    "failed": 0,
    "message": "Documentation completed"
  },
  "embeddings": {
    "status": "success",
    "minimal_files": 45,
    "document_chunks": 312,
    "output_directory": "d:\\sql-insight-agent\\temp_output\\minimal\\sales_prod",
    "message": "Embedding stage completed"
  },
  "report": {
    "extracted_files": 45,
    "documentation_tables_total": 45,
    "documentation_documented": 45,
    "documentation_failed": 0,
    "documentation_skipped": 0,
    "embeddings_minimal_files": 45,
    "embeddings_document_chunks": 312
  }
}
```

---

## 3. POST /schemas/embeddings

Refresh vector embeddings for an enrolled database.

### Request Body

```json
{
  "db_flag": "string (required) - Database identifier",
  "collection_name": "string (optional, default: '{db_flag}_docs')"
}
```

### Example Request

```bash
curl -X POST "http://127.0.0.1:8000/schemas/embeddings" \
  -H "Content-Type: application/json" \
  -d '{
    "db_flag": "sales_prod",
    "collection_name": "sales_prod_docs"
  }'
```

### Response

```json
{
  "db_flag": "sales_prod",
  "output_directory": "d:\\sql-insight-agent\\config\\minimal_schemas\\sales_prod",
  "processed_files": [
    "Orders_minimal.txt",
    "Customers_minimal.txt",
    "Products_minimal.txt"
  ],
  "message": "Embeddings stored successfully"
}
```

---

## 4. GET /health

Health check endpoint for monitoring.

### Example Request

```bash
curl -X GET "http://127.0.0.1:8000/health"
```

### Response

```json
{
  "status": "healthy",
  "message": "SQL Insight Agent is running",
  "version": "1.0.0"
}
```

---

## 5. GET /

API information and endpoint directory.

### Example Request

```bash
curl -X GET "http://127.0.0.1:8000/"
```

### Response

```json
{
  "message": "SQL Insight Agent API",
  "docs": "/docs",
  "health": "/health",
  "endpoints": {
    "POST /query": "Execute natural language SQL query",
    "POST /schemas/embeddings": "Convert schema YAML definitions to embeddings",
    "POST /schemas/enroll": "Enroll a database, extract schema, document, and embed",
    "GET /health": "Health check"
  }
}
```

---

## Error Responses

All endpoints return errors in this format:

### HTTP 400 - Bad Request
```json
{
  "detail": "Invalid request: query parameter is required"
}
```

### HTTP 500 - Internal Server Error
```json
{
  "detail": "Internal server error: Database connection failed"
}
```

### HTTP 502 - Bad Gateway
```json
{
  "detail": "LLM providers unavailable: All configured providers failed"
}
```

---

## Python SDK Usage Examples

### Basic Query

```python
import requests

response = requests.post('http://127.0.0.1:8000/query', json={
    "query": "Show me all orders from last week",
    "db_flag": "sales_prod"
})

result = response.json()
if result['status'] == 'success':
    print(f"SQL: {result['sql']}")
    print(f"Rows: {result['metadata']['total_rows']}")
    print(f"Results: {result['data']['results']}")
else:
    print(f"Error: {result['error']}")
```

### Query with Conversation Context

```python
import requests

session_id = "session-2024-11-25"
user_id = "analyst@company.com"

# First query
response1 = requests.post('http://127.0.0.1:8000/query', json={
    "query": "Show me top products by revenue",
    "db_flag": "sales_prod",
    "user_id": user_id,
    "session_id": session_id
})

# Follow-up query (uses context)
response2 = requests.post('http://127.0.0.1:8000/query', json={
    "query": "Now show me the customers who bought these products",
    "db_flag": "sales_prod",
    "user_id": user_id,
    "session_id": session_id
})
```

### Database Enrollment

```python
import requests

# ⚠️ Always use read-only credentials
response = requests.post('http://127.0.0.1:8000/schemas/enroll', json={
    "db_flag": "inventory_db",
    "db_type": "mysql",
    "connection_string": "mysql+pymysql://readonly_user:pass@localhost:3306/inventory",
    "description": "Warehouse inventory database",
    "run_documentation": True,
    "run_embeddings": True
})

report = response.json()
print(f"Tables extracted: {report['report']['extracted_files']}")
print(f"Tables documented: {report['report']['documentation_documented']}")
print(f"Document chunks: {report['report']['embeddings_document_chunks']}")
```

### Export Results to CSV

```python
import requests
import pandas as pd

response = requests.post('http://127.0.0.1:8000/query', json={
    "query": "List all products with inventory below 10 units",
    "db_flag": "inventory_db",
    "output_format": "json"
})

result = response.json()
if result['status'] == 'success':
    # Option 1: Use provided CSV
    with open('low_inventory.csv', 'w') as f:
        f.write(result['data']['csv'])
    
    # Option 2: Convert results to DataFrame
    df = pd.DataFrame(result['data']['results'])
    df.to_csv('low_inventory.csv', index=False)
```

---

## JavaScript/TypeScript Examples

### Using Fetch API

```javascript
async function queryDatabase(question, dbFlag) {
  const response = await fetch('http://127.0.0.1:8000/query', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      query: question,
      db_flag: dbFlag,
      output_format: 'json'
    })
  });
  
  const result = await response.json();
  
  if (result.status === 'success') {
    console.log('SQL:', result.sql);
    console.log('Rows:', result.metadata.total_rows);
    console.log('Data:', result.data.results);
    
    // Display natural summary
    if (result.natural_summary) {
      console.log('Summary:', result.natural_summary);
    }
    
    return result.data.results;
  } else {
    throw new Error(result.error);
  }
}

// Usage
queryDatabase("Show me recent customer orders", "sales_prod")
  .then(data => console.table(data))
  .catch(err => console.error(err));
```

### Using Axios

```javascript
import axios from 'axios';

const API_BASE = 'http://127.0.0.1:8000';

class SQLInsightClient {
  constructor(baseURL = API_BASE) {
    this.client = axios.create({
      baseURL: baseURL,
      headers: {
        'Content-Type': 'application/json'
      }
    });
  }
  
  async query(question, dbFlag, userId = null, sessionId = null) {
    const response = await this.client.post('/query', {
      query: question,
      db_flag: dbFlag,
      output_format: 'json',
      user_id: userId,
      session_id: sessionId
    });
    
    return response.data;
  }
  
  async enrollDatabase(config) {
    const response = await this.client.post('/schemas/enroll', config);
    return response.data;
  }
  
  async refreshEmbeddings(dbFlag) {
    const response = await this.client.post('/schemas/embeddings', {
      db_flag: dbFlag
    });
    return response.data;
  }
  
  async healthCheck() {
    const response = await this.client.get('/health');
    return response.data;
  }
}

// Usage
const client = new SQLInsightClient();

// Simple query
const result = await client.query(
  "Show me top customers",
  "sales_prod"
);

// Conversational query
const conversationResult = await client.query(
  "Now show me their recent orders",
  "sales_prod",
  "user@company.com",
  "session-001"
);
```

---

## cURL Examples

### Query Execution

```bash
# Simple query
curl -X POST "http://127.0.0.1:8000/query" \
  -H "Content-Type: application/json" \
  -d '{"query":"Show me all customers","db_flag":"sales_prod"}'

# Query with conversation context
curl -X POST "http://127.0.0.1:8000/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Show me top 10 products by revenue",
    "db_flag": "sales_prod",
    "output_format": "json",
    "user_id": "analyst@company.com",
    "session_id": "session-001"
  }'

# CSV output format
curl -X POST "http://127.0.0.1:8000/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "List all employees",
    "db_flag": "hr_db",
    "output_format": "csv"
  }'
```

### Database Enrollment

```bash
# Enroll SQL Server database (⚠️ using read-only user)
curl -X POST "http://127.0.0.1:8000/schemas/enroll" \
  -H "Content-Type: application/json" \
  -d '{
    "db_flag": "production_db",
    "db_type": "mssql",
    "connection_string": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ProductionDB;UID=readonly_user;PWD=Password123",
    "description": "Main production database",
    "run_documentation": true,
    "run_embeddings": true
  }'

# Enroll MySQL database (⚠️ using read-only user, NOT root)
curl -X POST "http://127.0.0.1:8000/schemas/enroll" \
  -H "Content-Type: application/json" \
  -d '{
    "db_flag": "inventory_mysql",
    "db_type": "mysql",
    "connection_string": "mysql+pymysql://readonly_user:password@localhost:3306/inventory",
    "description": "Inventory management database",
    "exclude_schemas": ["mysql", "sys", "performance_schema"]
  }'
```

---

## Postman Collection

Import this JSON into Postman:

```json
{
  "info": {
    "name": "SQL Insight Agent API",
    "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
  },
  "item": [
    {
      "name": "Query Database",
      "request": {
        "method": "POST",
        "header": [
          {
            "key": "Content-Type",
            "value": "application/json"
          }
        ],
        "body": {
          "mode": "raw",
          "raw": "{\n  \"query\": \"Show me top customers by revenue\",\n  \"db_flag\": \"sales_prod\",\n  \"output_format\": \"json\"\n}"
        },
        "url": {
          "raw": "http://127.0.0.1:8000/query",
          "protocol": "http",
          "host": ["127", "0", "0", "1"],
          "port": "8000",
          "path": ["query"]
        }
      }
    },
    {
      "name": "Enroll Database",
      "request": {
        "method": "POST",
        "header": [
          {
            "key": "Content-Type",
            "value": "application/json"
          }
        ],
        "body": {
          "mode": "raw",
          "raw": "{\n  \"db_flag\": \"test_db\",\n  \"db_type\": \"mssql\",\n  \"connection_string\": \"DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=TestDB;UID=readonly_user;PWD=Password123\",\n  \"description\": \"Test database\",\n  \"run_documentation\": true,\n  \"run_embeddings\": true\n}"
        },
        "url": {
          "raw": "http://127.0.0.1:8000/schemas/enroll",
          "protocol": "http",
          "host": ["127", "0", "0", "1"],
          "port": "8000",
          "path": ["schemas", "enroll"]
        }
      }
    },
    {
      "name": "Health Check",
      "request": {
        "method": "GET",
        "url": {
          "raw": "http://127.0.0.1:8000/health",
          "protocol": "http",
          "host": ["127", "0", "0", "1"],
          "port": "8000",
          "path": ["health"]
        }
      }
    }
  ]
}
```

---

## Rate Limits

Currently no rate limits enforced. Recommended limits for production:

- **Query endpoint**: 100 requests/minute per user
- **Enrollment endpoint**: 5 requests/hour per database
- **Embedding refresh**: 10 requests/hour per database

---

## Best Practices

### 🔒 Security (CRITICAL)

1. **ALWAYS use read-only database credentials** when enrolling databases
   - ⚠️ **Never use `sa`, `root`, or admin accounts**
   - Create dedicated `readonly_user` or `sql_insight_readonly` accounts
   - Grant ONLY SELECT permissions
   - See `POSTGRESQL_SETUP.md` → Security Configuration for setup scripts
   
2. **Why this matters**:
   - SQL Insight Agent only needs to READ data, never write
   - Prevents accidental data modification
   - Protects against SQL injection or malicious queries
   - Follows principle of least privilege

### 📋 General Best Practices

3. **Use conversation context** for follow-up queries by providing `user_id` and `session_id`
4. **Start new sessions** for unrelated query workflows
5. **Enroll databases once** during initial setup, not per query
6. **Use `incremental_documentation: true`** to avoid re-documenting existing tables
7. **Set appropriate `max_rows`** in database config to prevent memory issues
8. **Monitor logs** in `Log/app_YYYY-MM-DD.log` for debugging

---

## Support

- Interactive API Docs: http://127.0.0.1:8000/docs
- Full Documentation: `README_DETAILED.md`
- Logs: `Log/app_YYYY-MM-DD.log`
