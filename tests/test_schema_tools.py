from pathlib import Path

from app.core.schema_tools import SchemaToolkit


def test_schema_toolkit_loads_tables(tmp_path, monkeypatch):
    schema_root = tmp_path / "schemas" / "demo"
    schema_root.mkdir(parents=True, exist_ok=True)
    (schema_root / "schema_index.yaml").write_text(
        "database_name: demo\ntotal_tables: 1\n",
        encoding="utf-8",
    )
    table_dir = schema_root / "dbo"
    table_dir.mkdir(parents=True, exist_ok=True)
    (table_dir / "Users.yaml").write_text(
        """
        table_name: Users
        schema: dbo
        description: demo table
        columns:
          - name: UserId
            type: int
            is_nullable: false
          - name: UserName
            type: nvarchar
            is_nullable: false
        primary_key:
          columns: [UserId]
        """,
        encoding="utf-8",
    )

    ddl_file = (schema_root.parent / (schema_root.name + ".sql"))
    ddl_file.write_text("-- demo ddl", encoding="utf-8")

    def _fake_settings(_: str):
        return type("obj", (), {"ddl_file": Path(ddl_file)})()

    monkeypatch.setattr("app.core.schema_tools.get_database_settings", _fake_settings)

    toolkit = SchemaToolkit("demo")
    tables = toolkit.list_tables()
    assert "Users" in tables
    detail = toolkit.describe_table("Users")
    assert detail and detail.table_name == "Users"
