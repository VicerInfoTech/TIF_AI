import os
from importlib import util
from pathlib import Path
import yaml


def _load_minimal_text() -> util.module_from_spec:  # type: ignore[valid-type]
    module_path = Path(__file__).resolve().parents[1] / "app" / "schema_pipeline" / "minimal_text.py"
    spec = util.spec_from_file_location("minimal_text", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Could not load minimal_text helper")
    module = util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


yaml_to_minimal_text = _load_minimal_text().yaml_to_minimal_text


def yaml_to_structured_sections(file_path: str) -> dict:
    """Return structured sections along with the minimal summary for a schema YAML."""

    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    with open(file_path, 'r', encoding='utf-8') as file:
        data = yaml.safe_load(file) or {}

    table_name = data.get('table_name', os.path.splitext(os.path.basename(file_path))[0])
    schema_name = data.get('schema', 'dbo')
    minimal_summary = yaml_to_minimal_text(file_path)

    header_lines = [
        f"Table: {table_name}",
        f"Schema: {schema_name}",
        f"Type: {data.get('object_type', 'table')}",
        f"Description: {data.get('description', '').strip()}",
    ]

    columns = data.get('columns', [])
    columns_lines = []
    if columns:
        columns_lines.append("COLUMNS:")
        for column in columns:
            nullable = "NULL" if column.get('is_nullable') else "NOT NULL"
            identity = "IDENTITY" if column.get('is_identity') else ""
            columns_lines.append(f"- {column.get('name')}: {column.get('type')} {nullable} {identity}".strip())
            columns_lines.append(f"  Desc: {column.get('description', '').strip()}")
            if column.get('keywords'):
                columns_lines.append(f"  Keywords: {', '.join(column.get('keywords', []))}")
    else:
        columns_lines.append("COLUMNS: None")

    keys_lines = []
    pk = data.get('primary_key', {})
    if pk:
        keys_lines.append(f"PRIMARY KEY: {pk.get('constraint_name')} ({', '.join(pk.get('columns', []))})")

    foreign_keys = data.get('foreign_keys', [])
    if foreign_keys:
        keys_lines.append("FOREIGN KEYS:")
        for fk in foreign_keys:
            cols = ', '.join(fk.get('columns', []))
            ref_cols = ', '.join(fk.get('referenced_columns', []))
            keys_lines.append(f"- {fk.get('constraint_name')}: {cols} -> {fk.get('referenced_table')}({ref_cols})")

    indexes = data.get('indexes', [])
    if indexes:
        keys_lines.append("INDEXES:")
        for idx in indexes:
            unique = "UNIQUE " if idx.get('is_unique') else ""
            clustered = "CLUSTERED " if idx.get('is_clustered') else ""
            cols = []
            for col in idx.get('columns', []):
                direction = "DESC" if col.get('is_descending') else "ASC"
                cols.append(f"{col.get('column')} {direction}")
            keys_lines.append(f"- {idx.get('index_name')}: {unique}{clustered}{', '.join(cols)}")

    relationships = data.get('relationships', {})
    relations_lines = []
    if relationships.get('outgoing'):
        relations_lines.append("OUTGOING RELATIONS:")
        for rel in relationships.get('outgoing', []):
            relations_lines.append(f"- {rel.get('to_table')} ({rel.get('relationship_type')})")
    if relationships.get('incoming'):
        relations_lines.append("INCOMING RELATIONS:")
        for rel in relationships.get('incoming', []):
            relations_lines.append(f"- {rel.get('from_table')} ({rel.get('relationship_type')})")

    stats = data.get('statistics', {})
    stats_lines = []
    if stats:
        stats_lines.append(
            f"STATS: Columns={stats.get('total_columns')}, Nullable={stats.get('nullable_columns')}, Indexed={stats.get('indexed_columns')}"
        )

    return {
        "table_name": table_name,
        "schema": schema_name,
        "minimal_summary": minimal_summary,
        "sections": [
            {"name": "header", "text": "\n".join(header_lines).strip()},
            {"name": "columns", "text": "\n".join(columns_lines).strip()},
            {"name": "keys", "text": "\n".join(keys_lines).strip()},
            {"name": "relationships", "text": "\n".join(relations_lines).strip()},
            {"name": "stats", "text": "\n".join(stats_lines).strip()},
        ],
    }

# Example usage:
if __name__ == "__main__":
    file_path = r"config\schemas\avamed_db\dbo\Dispense.yaml"
    result = yaml_to_structured_sections(file_path)
    minimal = result["minimal_summary"]
    sections = result["sections"]

    print("MINIMAL SUMMARY:")
    print(minimal)
    print("\n" + "="*50 + "\n")
    print("STRUCTURED SECTIONS:")
    for section in sections:
        print(f"[{section['name'].upper()}]")
        print(section['text'])
        print()

    structured_output = "\n\n".join(
        f"[{section['name']}]\n{section['text']}"
        for section in sections
        if section['text']
    )

    with open("BoxMaster_structured.txt", "w", encoding="utf-8") as f:
        f.write(structured_output)

    with open("BoxMaster_minimal.txt", "w", encoding="utf-8") as f:
        f.write(minimal)