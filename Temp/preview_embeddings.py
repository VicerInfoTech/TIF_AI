"""
Script to preview the text content that will be used for embeddings.
Generates a single text file containing minimal and structured text for all tables.
"""

import sys
from pathlib import Path

# Add project root to path so we can import app modules
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from app.schema_pipeline.minimal_text import yaml_to_minimal_text
from app.schema_pipeline.structured_docs import yaml_to_structured_sections

# Configuration
DB_FLAG = "your_db_flag"  # Default placeholder: use your_db_flag (update as needed)
SCHEMA_DIR = PROJECT_ROOT / "config" / "schemas" / DB_FLAG
OUTPUT_FILE = Path(__file__).parent / "embedding_preview.txt"

def main():
    print(f"Scanning schema directory: {SCHEMA_DIR}")
    if not SCHEMA_DIR.exists():
        print(f"Error: Directory {SCHEMA_DIR} does not exist.")
        return

    # Find all table YAML files
    yaml_files = list(SCHEMA_DIR.rglob("*.yaml"))
    yaml_files = [
        f for f in yaml_files 
        if f.stem not in ("schema_index", "metadata")
    ]
    
    print(f"Found {len(yaml_files)} table files.")
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        out.write(f"EMBEDDING TEXT PREVIEW FOR {DB_FLAG}\n")
        out.write("=" * 50 + "\n\n")
        
        for i, yaml_file in enumerate(sorted(yaml_files), 1):
            table_name = yaml_file.stem
            print(f"[{i}/{len(yaml_files)}] Processing {table_name}...")
            
            try:
                # Generate Minimal Text
                minimal = yaml_to_minimal_text(yaml_file)
                
                # Generate Structured Text
                structured_data = yaml_to_structured_sections(yaml_file)
                
                out.write(f"TABLE: {table_name}\n")
                out.write("-" * 30 + "\n")
                
                out.write(">>> MINIMAL TEXT (Compact representation):\n")
                out.write(minimal + "\n\n")
                
                out.write(">>> STRUCTURED SECTIONS (Detailed representation):\n")
                for section in structured_data["sections"]:
                    out.write(f"--- Section: {section['name']} ---\n")
                    out.write(section['text'] + "\n")
                
                out.write("\n" + "=" * 50 + "\n\n")
                
            except Exception as e:
                print(f"Error processing {table_name}: {e}")
                out.write(f"ERROR processing {table_name}: {e}\n\n")

    print(f"\nPreview generated successfully at: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
