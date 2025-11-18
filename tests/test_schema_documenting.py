"""Test the schema documenting agent."""

import json
from pathlib import Path

from app.schema_pipeline.schema_documenting import SchemaDocumentingAgent


def test_column_documentation():
    """Test documenting a sample table."""
    
    # Sample table data
    table_name = "Patients"
    schema_name = "dbo"
    table_description = "Stores patient demographic and contact information"
    columns = [
        {"name": "PatientID", "sql_type": "int", "is_nullable": False, "is_identity": True},
        {"name": "FirstName", "sql_type": "nvarchar(50)", "is_nullable": False, "is_identity": False},
        {"name": "LastName", "sql_type": "nvarchar(50)", "is_nullable": False, "is_identity": False},
        {"name": "DateOfBirth", "sql_type": "date", "is_nullable": True, "is_identity": False},
        {"name": "SSN", "sql_type": "varchar(11)", "is_nullable": True, "is_identity": False},
        {"name": "PhoneNumber", "sql_type": "varchar(15)", "is_nullable": True, "is_identity": False},
        {"name": "Email", "sql_type": "nvarchar(100)", "is_nullable": True, "is_identity": False},
    ]
    
    business_intro = """
AvaMed is a healthcare management system that handles patient records, appointments, and medical billing.
Key terms: Patient (person receiving care), Provider (doctor), Visit (appointment), Claim (insurance request).
    """.strip()
    
    # Initialize agent
    agent = SchemaDocumentingAgent(model="llama-3.3-70b-versatile", temperature=0.2)
    
    # Generate documentation
    print(f"\n{'='*80}")
    print(f"Testing Schema Documentation Agent")
    print(f"{'='*80}\n")
    print(f"Table: {schema_name}.{table_name}")
    print(f"Description: {table_description}")
    print(f"\nColumns to document: {len(columns)}")
    print(f"\n{'-'*80}\n")
    
    doc_map = agent.document_table(
        table_name=table_name,
        schema_name=schema_name,
        table_description=table_description,
        columns=columns,
        business_intro=business_intro,
    )
    
    # Display results
    if doc_map:
        print("✅ Documentation Generated:\n")
        for col_name, doc in doc_map.items():
            print(f"Column: {col_name}")
            print(f"  Description: {doc.description}")
            print(f"  Keywords: {', '.join(doc.keywords)}")
            print()
    else:
        print("❌ Failed to generate documentation")
    
    print(f"\n{'='*80}\n")


if __name__ == "__main__":
    test_column_documentation()
