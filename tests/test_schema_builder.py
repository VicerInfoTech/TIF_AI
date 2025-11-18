from pathlib import Path

from app.schema_pipeline.builder import SchemaGraphBuilder
from app.schema_pipeline.models import RawMetadata
from app.schema_pipeline.writer import YamlSchemaWriter


def _sample_raw_metadata() -> RawMetadata:
    tables = [
        {
            "schema_name": "dbo",
            "table_name": "Patients",
            "object_id": 1,
            "create_date": None,
            "modify_date": None,
            "type_desc": "USER_TABLE",
            "table_description": None,
        },
        {
            "schema_name": "dbo",
            "table_name": "Doctors",
            "object_id": 2,
            "create_date": None,
            "modify_date": None,
            "type_desc": "USER_TABLE",
            "table_description": None,
        },
        {
            "schema_name": "dbo",
            "table_name": "PatientDoctors",
            "object_id": 3,
            "create_date": None,
            "modify_date": None,
            "type_desc": "USER_TABLE",
            "table_description": None,
        },
    ]

    columns = [
        # Patients
        {
            "schema_name": "dbo",
            "table_name": "Patients",
            "object_id": 1,
            "column_name": "PatientID",
            "column_id": 1,
            "data_type": "int",
            "max_length": 4,
            "precision": 10,
            "scale": 0,
            "is_nullable": 0,
            "is_identity": 1,
            "seed_value": 1,
            "increment_value": 1,
            "is_computed": 0,
            "computed_definition": None,
            "default_value": None,
            "collation_name": None,
            "column_description": None,
        },
        {
            "schema_name": "dbo",
            "table_name": "Doctors",
            "object_id": 2,
            "column_name": "DoctorID",
            "column_id": 1,
            "data_type": "int",
            "max_length": 4,
            "precision": 10,
            "scale": 0,
            "is_nullable": 0,
            "is_identity": 1,
            "seed_value": 1,
            "increment_value": 1,
            "is_computed": 0,
            "computed_definition": None,
            "default_value": None,
            "collation_name": None,
            "column_description": None,
        },
        # Junction FKs
        {
            "schema_name": "dbo",
            "table_name": "PatientDoctors",
            "object_id": 3,
            "column_name": "PatientID",
            "column_id": 1,
            "data_type": "int",
            "max_length": 4,
            "precision": 10,
            "scale": 0,
            "is_nullable": 0,
            "is_identity": 0,
            "seed_value": None,
            "increment_value": None,
            "is_computed": 0,
            "computed_definition": None,
            "default_value": None,
            "collation_name": None,
            "column_description": None,
        },
        {
            "schema_name": "dbo",
            "table_name": "PatientDoctors",
            "object_id": 3,
            "column_name": "DoctorID",
            "column_id": 2,
            "data_type": "int",
            "max_length": 4,
            "precision": 10,
            "scale": 0,
            "is_nullable": 0,
            "is_identity": 0,
            "seed_value": None,
            "increment_value": None,
            "is_computed": 0,
            "computed_definition": None,
            "default_value": None,
            "collation_name": None,
            "column_description": None,
        },
    ]

    primary_keys = [
        {
            "schema_name": "dbo",
            "table_name": "Patients",
            "constraint_name": "PK_Patients",
            "column_name": "PatientID",
            "key_ordinal": 1,
            "is_descending_key": 0,
        },
        {
            "schema_name": "dbo",
            "table_name": "Doctors",
            "constraint_name": "PK_Doctors",
            "column_name": "DoctorID",
            "key_ordinal": 1,
            "is_descending_key": 0,
        },
    ]

    foreign_keys = [
        {
            "schema_name": "dbo",
            "table_name": "PatientDoctors",
            "constraint_name": "FK_PatientDoctors_Patients",
            "column_name": "PatientID",
            "referenced_schema": "dbo",
            "referenced_table": "Patients",
            "referenced_column": "PatientID",
            "on_delete": "NO_ACTION",
            "on_update": "NO_ACTION",
            "is_disabled": 0,
        },
        {
            "schema_name": "dbo",
            "table_name": "PatientDoctors",
            "constraint_name": "FK_PatientDoctors_Doctors",
            "column_name": "DoctorID",
            "referenced_schema": "dbo",
            "referenced_table": "Doctors",
            "referenced_column": "DoctorID",
            "on_delete": "NO_ACTION",
            "on_update": "NO_ACTION",
            "is_disabled": 0,
        },
    ]

    return RawMetadata(
        database_name="TestDB",
        schemas=[{"schema_id": 1, "schema_name": "dbo"}],
        tables=tables,
        columns=columns,
        primary_keys=primary_keys,
        foreign_keys=foreign_keys,
        indexes=[],
        unique_constraints=[],
        check_constraints=[],
        views=[],
        view_columns=[],
        procedures=[],
        procedure_parameters=[],
        functions=[],
        function_parameters=[],
    )


def test_builder_detects_many_to_many(tmp_path: Path) -> None:
    raw = _sample_raw_metadata()
    builder = SchemaGraphBuilder()
    artifacts = builder.build(raw)

    patients = artifacts.schemas["dbo"]["tables"]["Patients"]
    m2m = patients["relationships"]["many_to_many"]
    assert any(entry["to_table"] == "Doctors" for entry in m2m)

    writer = YamlSchemaWriter(tmp_path)
    writer.write(artifacts)

    assert (tmp_path / "dbo" / "Patients.yaml").exists()
    assert (tmp_path / "schema_index.yaml").exists()