"""Agent that generates business-friendly column descriptions and keywords using Groq LLM.

This module uses LangChain's structured output capabilities to generate documentation
for database schemas using LLMs with guaranteed schema validation.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Tuple
from app.models import ColumnDocumentation, TableDocumentation
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import Runnable
from langchain_groq import ChatGroq
import yaml
from dotenv import load_dotenv

load_dotenv()

from app.utils.logger import setup_logging

logger = setup_logging(__name__)


class SchemaDocumentingAgent:
    """Generate column descriptions and keywords using LLM with business context."""

    def __init__(
        self,
        model: str = "llama-3.3-70b-versatile",
        temperature: float = 0.2,
    ) -> None:
        """Initialize the schema documenting agent.

        Args:
            model: Groq model to use for generation. Defaults to llama-3.3-70b-versatile
                which provides good balance of speed and quality.
            temperature: LLM temperature (0.2 for more consistent documentation).
                Lower values produce more deterministic outputs.
        """
        self.llm: ChatGroq = ChatGroq(model=model, temperature=temperature)
        self.prompt: ChatPromptTemplate = self._build_prompt()
        # Use with_structured_output for guaranteed schema compliance
        # This is the modern LangChain approach for structured generation
        self.chain: Runnable = self.prompt | self.llm.with_structured_output(
            TableDocumentation,
            strict=False
        )

    def _build_prompt(self) -> ChatPromptTemplate:
        """Build the prompt template for column documentation.

        Returns:
            ChatPromptTemplate configured for generating column documentation.
        """
        return ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    """You are a database documentation expert specializing in creating business-friendly 
documentation that helps non-technical users understand database schemas.

Business Context:
{business_intro}

Your Task:
Generate clear, business-oriented documentation for database columns.

Guidelines:
1. **Descriptions**: Write 1-2 sentence descriptions that explain what the data represents in business terms
   - Avoid technical jargon and implementation details
   - Focus on the business value and use case
   - Make it understandable to non-technical stakeholders

2. **Keywords**: Provide exactly 3 keywords per column
   - Use simple, common terms business users would naturally search for
   - Think about how someone would ask for this data in plain language
   - Avoid database-specific terminology (no "varchar", "foreign_key", etc.)

3. **Context**: Use the table name and description to understand the column's purpose within the larger data model
""",
                ),
                (
                    "human",
                    """Table: {table_name}
Schema: {schema_name}
Table Description: {table_description}

Columns to document:
{columns_json}

Generate business-friendly descriptions and exactly 3 keywords for each column.""",
                ),
            ]
        )

    def document_table(
        self,
        table_name: str,
        schema_name: str,
        table_description: str,
        columns: List[Dict[str, Any]],
        business_intro: str,
    ) -> Dict[str, ColumnDocumentation]:
        """Generate documentation for all columns in a table using LLM.

        This method uses LangChain's structured output to ensure the LLM response
        conforms to our Pydantic schema for validated documentation.

        Args:
            table_name: Name of the table to document
            schema_name: Schema/database the table belongs to
            table_description: Human-readable description of the table's purpose
            columns: List of column dictionaries with keys: 'name', 'type', etc.
            business_intro: Business context text to guide documentation style

        Returns:
            Dictionary mapping column_name to ColumnDocumentation object.
            Returns empty dict if documentation generation fails.

        Example:
            >>> agent = SchemaDocumentingAgent()
            >>> columns = [{"name": "customer_id", "type": "int"}]
            >>> docs = agent.document_table(
            ...     "orders", "sales", "Customer orders", columns, "E-commerce platform"
            ... )
        """
        logger.info(
            "Starting documentation for table %s.%s with %d columns",
            schema_name,
            table_name,
            len(columns),
        )

        if not columns:
            logger.warning(
                "No columns provided for table %s.%s", schema_name, table_name
            )
            return {}

        # Prepare column summary for LLM with relevant metadata
        columns_json = json.dumps(
            [
                {
                    "name": col["name"],
                    "type": col.get("sql_type", col.get("type", "unknown")),
                    "is_nullable": col.get("is_nullable", True),
                    "is_identity": col.get("is_identity", False),
                }
                for col in columns
            ],
            indent=2,
        )

        try:
            # Invoke the chain - structured output ensures type safety
            result: TableDocumentation = self.chain.invoke(
                {
                    "business_intro": business_intro,
                    "table_name": table_name,
                    "schema_name": schema_name,
                    "table_description": table_description
                    or "No description available",
                    "columns_json": columns_json,
                }
            )

            # Convert list to dict for efficient lookup
            doc_map = {doc.column_name: doc for doc in result.columns}

            logger.info(
                "Successfully documented %d/%d columns for %s.%s",
                len(doc_map),
                len(columns),
                schema_name,
                table_name,
            )

            # Warn if we didn't get documentation for all columns
            if len(doc_map) != len(columns):
                missing = set(col["name"] for col in columns) - set(doc_map.keys())
                logger.warning(
                    "Missing documentation for columns in %s.%s: %s",
                    schema_name,
                    table_name,
                    missing,
                )

            return doc_map

        except Exception as exc:
            logger.error(
                "Failed to document table %s.%s: %s",
                schema_name,
                table_name,
                exc,
                exc_info=True,
            )
            # Return empty dict as fallback to allow pipeline to continue
            return {}

    def document_schema(
        self,
        schema_yaml_dir: Path,
        business_intro_path: Path,
    ) -> None:
        """Process all table YAML files and add column documentation.

        This method reads existing schema YAML files, generates documentation for each
        table's columns using the LLM, and writes the updated YAML files back to disk.

        Args:
            schema_yaml_dir: Directory containing schema YAML files (e.g. /avamed_db)
            business_intro_path: Path to business intro text file for context

        Raises:
            FileNotFoundError: If schema_yaml_dir doesn't exist
        """
        if not schema_yaml_dir.exists():
            raise FileNotFoundError(f"Schema directory not found: {schema_yaml_dir}")

        # Load business context
        if not business_intro_path.exists():
            logger.warning("Business intro file not found: %s", business_intro_path)
            business_intro = "No business context provided."
        else:
            business_intro = business_intro_path.read_text(encoding="utf-8").strip()

        if not business_intro:
            business_intro = "No business context provided."
            logger.warning("Empty business intro file, using default context")

        logger.info(
            "Starting schema documentation with business intro from: %s",
            business_intro_path,
        )

        schema_index = self._load_schema_index(schema_yaml_dir)
        intro_snippet = self._intro_snippet(business_intro)

        # Find all table YAML files (exclude metadata files)
        yaml_files = list(schema_yaml_dir.rglob("*.yaml"))
        yaml_files = [
            f for f in yaml_files if f.stem not in ("schema_index", "metadata")
        ]

        if not yaml_files:
            logger.warning("No YAML files found in %s", schema_yaml_dir)
            return

        logger.info("Found %d table YAML files to document", len(yaml_files))

        # Track progress
        successful = 0
        failed = 0

        for idx, yaml_file in enumerate(yaml_files, 1):
            logger.info(
                "Processing file %d/%d: %s", idx, len(yaml_files), yaml_file.name
            )

            try:
                # Load existing table data
                with yaml_file.open("r", encoding="utf-8") as handle:
                    table_data = yaml.safe_load(handle)

                # Validate table data structure
                if not table_data or "columns" not in table_data:
                    logger.warning("Skipping invalid table file: %s", yaml_file)
                    failed += 1
                    continue

                table_name = table_data.get("table_name", yaml_file.stem)
                schema_name = table_data.get("schema", "dbo")
                table_description = self._table_description_from_index(
                    schema_index,
                    schema_name,
                    table_name,
                    table_data.get("description", ""),
                )
                table_description = self._combine_with_intro(
                    table_description,
                    intro_snippet,
                )
                columns = table_data.get("columns", [])

                table_data["description"] = table_description or table_data.get("description", "")

                if not columns:
                    logger.info("No columns to document in %s", yaml_file)
                    successful += 1
                    continue

                # Generate documentation using LLM
                doc_map = self.document_table(
                    table_name=table_name,
                    schema_name=schema_name,
                    table_description=table_description,
                    columns=columns,
                    business_intro=business_intro,
                )

                if not doc_map:
                    logger.warning("No documentation generated for %s", yaml_file)
                    failed += 1
                    continue

                # Update column descriptions and keywords
                for col in columns:
                    col_name = col["name"]
                    if col_name in doc_map:
                        col["description"] = doc_map[col_name].description
                        col["keywords"] = doc_map[col_name].keywords
                    else:
                        # Ensure keywords field exists even if not documented
                        col.setdefault("keywords", [])
                        logger.debug(
                            "No documentation for column %s in %s", col_name, yaml_file
                        )

                # Update table-level keywords if empty
                if not table_data.get("keywords"):
                    # Aggregate unique keywords from all columns
                    all_keywords = set()
                    for col in columns:
                        all_keywords.update(col.get("keywords", []))
                    # Take top 10 most common keywords
                    table_data["keywords"] = sorted(all_keywords)[:10]

                # Write updated YAML back to file
                with yaml_file.open("w", encoding="utf-8") as handle:
                    yaml.dump(
                        table_data,
                        handle,
                        default_flow_style=False,
                        allow_unicode=True,
                        sort_keys=False,
                    )

                logger.info("✅ Updated documentation in %s", yaml_file)
                successful += 1

            except Exception as exc:
                logger.error("Failed to process %s: %s", yaml_file, exc, exc_info=True)
                failed += 1
                continue

        # Summary logging
        logger.info(
            "Schema documentation complete: %d successful, %d failed out of %d total",
            successful,
            failed,
            len(yaml_files),
        )

    def _load_schema_index(self, schema_yaml_dir: Path) -> Mapping[Tuple[str, str], Dict[str, Any]]:
        index_path = schema_yaml_dir / "schema_index.yaml"
        if not index_path.exists():
            logger.warning("schema_index.yaml missing at %s", index_path)
            return {}

        try:
            with index_path.open("r", encoding="utf-8") as handle:
                index_data = yaml.safe_load(handle) or {}
        except Exception as exc:
            logger.error("Failed to load schema index: %s", exc)
            return {}

        table_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for table_entry in index_data.get("tables", []):
            schema = table_entry.get("schema", "").lower()
            table = table_entry.get("table", "").lower()
            if schema and table:
                table_map[(schema, table)] = table_entry

        return table_map

    def _table_description_from_index(
        self,
        schema_index: Mapping[Tuple[str, str], Dict[str, Any]],
        schema_name: str,
        table_name: str,
        fallback: str,
    ) -> str:
        lookup_key = (schema_name.lower(), table_name.lower())
        entry = schema_index.get(lookup_key)
        if entry:
            desc = entry.get("short_description")
            if desc:
                return desc
        return fallback

    def _intro_snippet(self, business_intro: str) -> str:
        if not business_intro:
            return ""
        for line in business_intro.splitlines():
            clean = line.strip()
            if clean:
                return clean[:300]
        return business_intro.strip()[:300]

    def _combine_with_intro(self, description: str, intro_snippet: str) -> str:
        parts = []
        if description:
            parts.append(description.strip())
        if intro_snippet:
            parts.append(f"Context: {intro_snippet}")
        return "\n".join(parts)


def document_database_schema(
    database_name: str,
    schema_output_dir: Path,
    intro_template_path: Path,
    model: str = "llama-3.3-70b-versatile",
    temperature: float = 0.2,
) -> None:
    """Main entry point to document a database schema using LLM-generated descriptions.

    This function creates a SchemaDocumentingAgent and processes all table YAML files
    in the specified directory, generating business-friendly documentation for columns.

    Args:
        database_name: Name of the database (e.g., "avamed_db"). Used for logging.
        schema_output_dir: Directory with generated schema YAML files to document
        intro_template_path: Path to business intro text file providing context
        model: Groq model to use for generation. Defaults to llama-3.3-70b-versatile.
        temperature: LLM temperature for generation. Lower = more consistent.

    Raises:
        FileNotFoundError: If schema_output_dir doesn't exist

    Example:
        >>> from pathlib import Path
        >>> document_database_schema(
        ...     database_name="avamed_db",
        ...     schema_output_dir=Path("output/avamed_db"),
        ...     intro_template_path=Path("config/prompts/avamed_db_intro.txt"),
        ...     model="llama-3.3-70b-versatile",
        ...     temperature=0.2
        ... )
    """
    logger.info("Starting schema documentation for database: %s", database_name)
    logger.info("Using model: %s with temperature: %.2f", model, temperature)

    try:
        # Initialize agent with specified configuration
        agent = SchemaDocumentingAgent(model=model, temperature=temperature)

        # Process all schema files
        agent.document_schema(schema_output_dir, intro_template_path)

        logger.info("✅ Schema documentation complete for %s", database_name)

    except Exception as exc:
        logger.error(
            "Schema documentation failed for %s: %s", database_name, exc, exc_info=True
        )
        raise


__all__ = [
    "SchemaDocumentingAgent",
    "document_database_schema",
    "ColumnDocumentation",
    "TableDocumentation",
]
