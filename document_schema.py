"""CLI script to document database schema using LLM."""

import sys
from pathlib import Path

from app.schema_pipeline.schema_documenting import document_database_schema
from app.config import get_database_settings
from app.utils.logger import setup_logging

logger = setup_logging(__name__)


def main() -> None:
    """Run schema documentation agent."""
    if len(sys.argv) < 2:
        print("Usage: python document_schema.py <database_name>")
        print("\nExample: python document_schema.py avamed_db")
        sys.exit(1)
    
    db_name = sys.argv[1]
    
    try:
        # Load database config
        settings = get_database_settings(db_name)
        
        # Paths
        schema_output_dir = Path("config/schemas") / db_name
        intro_template_path = Path(settings.intro_template)
        
        if not schema_output_dir.exists():
            logger.error("Schema output directory not found: %s", schema_output_dir)
            logger.info("Run: python generate_schema.py %s --format yaml", db_name)
            sys.exit(1)
        
        logger.info("Starting schema documentation for %s", db_name)
        logger.info("Schema directory: %s", schema_output_dir)
        logger.info("Business intro: %s", intro_template_path)
        
        document_database_schema(
            database_name=db_name,
            schema_output_dir=schema_output_dir,
            intro_template_path=intro_template_path,
        )
        
        logger.info("✅ Schema documentation complete!")
        
    except Exception as exc:
        logger.error("Failed to document schema: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
