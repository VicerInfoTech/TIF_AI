from app.config import get_database_settings
from app.core.schema_extractor import SchemaExtractor
from app.core.schema_preprocessor import SchemaPreprocessor
from app.utils.logger import setup_logging
import json
from pathlib import Path

logger = setup_logging(__name__)

def generate(db_flag):
    logger.info(f"Generating schema for {db_flag}")
    settings = get_database_settings(db_flag)
    
    # 1. Extract from live DB
    extractor = SchemaExtractor(settings.connection_string)
    raw = extractor.extract_full_schema()
    
    # 2. Preprocess
    compact = SchemaPreprocessor(raw).preprocess()
    
    # 3. Save
    output = Path(settings.ddl_file).with_suffix('.json')
    with output.open('w') as f:
        json.dump(compact, f, indent=2)
    
    logger.info(f"✅ Saved to {output}")

if __name__ == "__main__":
    import sys
    generate(sys.argv[1])  