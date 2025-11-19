"""Compatibility re-export module for schema pipeline models.

The models are now consolidated in `app.models`. This module remains for
backwards compatibility and simply re-exports the dataclasses so that
older import paths continue to work.
"""

from app.models import (
    RawMetadata,
    DatabaseSchemaArtifacts,
    SectionContent,
    StructuredSchemaData,
)

__all__ = ["RawMetadata", "DatabaseSchemaArtifacts", "SectionContent", "StructuredSchemaData"]
