"""API Integration Tests for SQL Insight Agent FastAPI Endpoint."""

from __future__ import annotations

import json
import time
from datetime import datetime

import requests

# Configuration
BASE_URL = "http://localhost:8000"
HEADERS = {"Content-Type": "application/json"}

# Color codes for terminal output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"
BOLD = "\033[1m"


def print_header(title: str):
    """Print a formatted header."""
    print(f"\n{CYAN}{BOLD}{'='*70}{RESET}")
    print(f"{CYAN}{BOLD}{title:^70}{RESET}")
    print(f"{CYAN}{BOLD}{'='*70}{RESET}\n")


def print_success(msg: str):
    """Print success message."""
    print(f"{GREEN}✅ {msg}{RESET}")


def print_error(msg: str):
    """Print error message."""
    print(f"{RED}❌ {msg}{RESET}")


def print_info(msg: str):
    """Print info message."""
    print(f"{CYAN}ℹ️  {msg}{RESET}")


def print_warning(msg: str):
    """Print warning message."""
    print(f"{YELLOW}⚠️  {msg}{RESET}")


def test_health_check():
    """Test the health check endpoint."""
    print_header("TEST 1: Health Check Endpoint")
    
    try:
        response = requests.get(f"{BASE_URL}/health", headers=HEADERS, timeout=5)
        response.raise_for_status()
        
        data = response.json()
        print_success(f"Health check passed: {data['status']}")
        print(f"  Message: {data['message']}")
        print(f"  Version: {data['version']}")
        
        return True
    except requests.exceptions.ConnectionError:
        print_error("Could not connect to server. Is it running on http://localhost:8000?")
        return False
    except Exception as e:
        print_error(f"Health check failed: {str(e)}")
        return False


def test_root_endpoint():
    """Test the root endpoint."""
    print_header("TEST 2: Root Endpoint")
    
    try:
        response = requests.get(f"{BASE_URL}/", headers=HEADERS, timeout=5)
        response.raise_for_status()
        
        data = response.json()
        print_success(f"Root endpoint accessible: {data['message']}")
        print(f"  API Docs: {data['docs']}")
        print(f"  Available endpoints:")
        for endpoint, desc in data["endpoints"].items():
            print(f"    - {endpoint}: {desc}")
        
        return True
    except Exception as e:
        print_error(f"Root endpoint failed: {str(e)}")
        return False


def test_query(query: str, db_flag: str = "medical_db_prod", output_format: str = "json"):
    """Execute a single query via API."""
    print_info(f"Query: {query}")
    print_info(f"Database: {db_flag} | Format: {output_format}")
    
    try:
        payload = {
            "query": query,
            "db_flag": db_flag,
            "output_format": output_format,
        }
        
        start_time = time.time()
        response = requests.post(
            f"{BASE_URL}/query",
            json=payload,
            headers=HEADERS,
            timeout=30,
        )
        elapsed = time.time() - start_time
        
        response.raise_for_status()
        result = response.json()
        
        # Print results
        print(f"  ⏱️  Response time: {elapsed:.2f}s")
        print(f"  Status: {result['status']}")
        
        if result.get("sql"):
            print(f"  Generated SQL: {result['sql']}")
        
        if result.get("validation_passed") is not None:
            validation_status = "✅ VALID" if result["validation_passed"] else "❌ INVALID"
            print(f"  Validation: {validation_status}")
        
        if result.get("error"):
            print_error(f"Error: {result['error']}")
            return False
        
        # Print metadata
        metadata = result.get("metadata", {})
        if metadata.get("execution_time_ms"):
            print(f"  Execution time: {metadata['execution_time_ms']:.2f}ms")
        if metadata.get("total_rows"):
            print(f"  Total rows: {metadata['total_rows']}")
        if metadata.get("retry_count"):
            print(f"  Retries: {metadata['retry_count']}")
        
        # Print data preview
        if result.get("data"):
            data = result["data"]
            if output_format == "json" and "data" in data:
                rows = data["data"]
                if isinstance(rows, list) and rows:
                    print(f"  Sample row: {rows[0]}")
            else:
                data_str = str(data)[:100]
                print(f"  Data preview: {data_str}...")
        
        print_success("Query executed successfully")
        return True
        
    except requests.exceptions.Timeout:
        print_error("Request timeout (30s)")
        return False
    except requests.exceptions.HTTPError as e:
        error_detail = "Unknown error"
        try:
            error_detail = e.response.json().get("detail", str(e))
        except Exception:
            error_detail = str(e)
        print_error(f"HTTP {e.response.status_code}: {error_detail}")
        return False
    except Exception as e:
        print_error(f"Query failed: {str(e)}")
        return False


def test_single_queries():
    """Test individual queries."""
    print_header("TEST 3: Single Query Executions")
    
    test_cases = [
        {
            "query": "Show me all employees",
            "db_flag": "medical_db_prod",
            "output_format": "json",
        },
        {
            "query": "How many employees are there?",
            "db_flag": "medical_db_prod",
            "output_format": "json",
        },
        {
            "query": "Get attendance logs from today",
            "db_flag": "medical_db_prod",
            "output_format": "csv",
        },
    ]
    
    passed = 0
    for i, test_case in enumerate(test_cases, 1):
        print(f"\nQuery {i}/3:")
        if test_query(**test_case):
            passed += 1
        print()
    
    print(f"Single queries: {passed}/{len(test_cases)} passed")
    return passed == len(test_cases)


def test_output_formats():
    """Test different output formats."""
    print_header("TEST 4: Output Format Support")
    
    query = "Show me top employees"
    formats = ["json", "csv", "table"]
    
    passed = 0
    for fmt in formats:
        print(f"\nFormat: {fmt.upper()}")
        if test_query(query, output_format=fmt):
            passed += 1
    
    print(f"\nOutput formats: {passed}/{len(formats)} passed")
    return passed == len(formats)


def test_invalid_requests():
    """Test error handling with invalid requests."""
    print_header("TEST 5: Error Handling & Validation")
    
    test_cases = [
        {
            "name": "Empty query",
            "payload": {"query": "", "db_flag": "medical_db_prod"},
            "expect_error": True,
        },
        {
            "name": "Unknown database",
            "payload": {"query": "SELECT 1", "db_flag": "unknown_db"},
            "expect_error": True,
        },
        {
            "name": "Invalid format",
            "payload": {
                "query": "Show employees",
                "db_flag": "medical_db_prod",
                "output_format": "invalid",
            },
            "expect_error": True,
        },
    ]
    
    passed = 0
    for test_case in test_cases:
        print(f"\n{test_case['name']}:")
        try:
            response = requests.post(
                f"{BASE_URL}/query",
                json=test_case["payload"],
                headers=HEADERS,
                timeout=10,
            )
            
            if test_case["expect_error"]:
                if response.status_code >= 400:
                    print_success(f"Correctly rejected with status {response.status_code}")
                    error_detail = response.json().get("detail", "")
                    if error_detail:
                        print(f"  Reason: {error_detail}")
                    passed += 1
                else:
                    print_error(f"Expected error but got success (status {response.status_code})")
            else:
                if response.status_code == 200:
                    print_success("Correctly accepted")
                    passed += 1
                else:
                    print_error(f"Expected success but got error (status {response.status_code})")
        except Exception as e:
            print_error(f"Request failed: {str(e)}")
    
    print(f"\nError handling: {passed}/{len(test_cases)} passed")
    return passed == len(test_cases)


def test_database_switching():
    """Test switching between databases."""
    print_header("TEST 6: Database Switching")
    
    databases = ["medical_db_prod", "inventory_db"]
    passed = 0
    
    for db in databases:
        print(f"\nTesting database: {db}")
        if test_query("Show me all records", db_flag=db):
            passed += 1
    
    print(f"\nDatabase switching: {passed}/{len(databases)} passed")
    return passed == len(databases)


def test_concurrent_requests():
    """Test handling of rapid consecutive requests."""
    print_header("TEST 7: Rapid Consecutive Requests")
    
    num_requests = 3
    queries = [
        "Show employees",
        "How many employees?",
        "Get attendance logs",
    ]
    
    print(f"Sending {num_requests} rapid requests...")
    
    passed = 0
    start_time = time.time()
    
    for i, query in enumerate(queries, 1):
        print(f"\nRequest {i}/{num_requests}: {query}")
        if test_query(query):
            passed += 1
    
    total_time = time.time() - start_time
    
    print(f"\nTotal time: {total_time:.2f}s")
    print(f"Requests: {passed}/{num_requests} passed")
    return passed == num_requests


def main():
    """Run all API tests."""
    print(f"\n{BOLD}{'='*70}{RESET}")
    print(f"{BOLD}{'SQL INSIGHT AGENT - API INTEGRATION TESTS':^70}{RESET}")
    print(f"{BOLD}{'='*70}{RESET}\n")
    
    print_info(f"Target API: {BASE_URL}")
    print_info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Run tests
    results = {
        "Health Check": test_health_check(),
        "Root Endpoint": test_root_endpoint(),
    }
    
    # Only run query tests if health check passed
    if results["Health Check"]:
        results["Single Queries"] = test_single_queries()
        results["Output Formats"] = test_output_formats()
        results["Error Handling"] = test_invalid_requests()
        results["Database Switching"] = test_database_switching()
        results["Rapid Requests"] = test_concurrent_requests()
    else:
        print_warning("Skipping query tests due to health check failure")
        print_warning("Make sure to start the server with: python -m uvicorn app.main:app --reload\n")
    
    # Summary
    print_header("TEST SUMMARY")
    passed_count = sum(1 for v in results.values() if v)
    total_count = len(results)
    
    for test_name, passed in results.items():
        status = f"{GREEN}✅ PASS{RESET}" if passed else f"{RED}❌ FAIL{RESET}"
        print(f"{status} - {test_name}")
    
    print(f"\n{BOLD}Total: {passed_count}/{total_count} test groups passed{RESET}\n")
    
    if passed_count == total_count:
        print(f"{GREEN}{BOLD}🎉 ALL TESTS PASSED!{RESET}\n")
    else:
        print(f"{RED}{BOLD}⚠️  SOME TESTS FAILED{RESET}\n")


if __name__ == "__main__":
    main()
