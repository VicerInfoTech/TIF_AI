"""
Parse db_intro.txt file with embedded deprecation section.
Extracts business context and deprecation information.
"""

from typing import Tuple, List, Optional, Dict
from pathlib import Path
import re
from pydantic import BaseModel, Field


class DeprecationInfo(BaseModel):
    """Parsed deprecation for a single column."""
    table_name: str = Field(description="Table where deprecated column exists")
    column_name: str = Field(description="Name of deprecated column")
    reason: str = Field(description="Why it's deprecated")
    migrate_to_table: Optional[str] = Field(default=None, description="Target table")
    migrate_to_column: Optional[str] = Field(default=None, description="Target column")
    join_key: Optional[str] = Field(default=None, description="Foreign key to join")
    deprecated_since: str = Field(default="2025-01-01", description="When deprecated")


class DbIntroParser:
    """
    Parse db_intro.txt file that contains:
    1. Context (before [DEPRECIATION SCHEMA INFORMATION])
    2. Deprecations (after [DEPRECIATION SCHEMA INFORMATION])
    """
    
    DEPRECATION_MARKER = "[DEPRECIATION SCHEMA INFORMATION]"
    
    @staticmethod
    def read_and_parse(file_path: Path) -> Tuple[str, str, List[DeprecationInfo]]:
        """
        Read db_intro file and split into context + deprecations.
        
        Returns:
            (db_intro_context, deprecation_section, deprecations_list)
        """
        
        if not file_path.exists():
            print(f"[ERROR] File not found: {file_path}")
            return "", "", []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print(f"\n[READ] Reading db_intro from: {file_path.name}")
        print(f"   File size: {len(content)} characters")
        
        # Split by deprecation marker
        if DbIntroParser.DEPRECATION_MARKER in content:
            parts = content.split(DbIntroParser.DEPRECATION_MARKER)
            db_intro_context = parts[0].strip()
            deprecation_section = parts[1].strip() if len(parts) > 1 else ""
            
            print(f"   [OK] Found deprecation section")
        else:
            db_intro_context = content.strip()
            deprecation_section = ""
            print(f"   [INFO] No deprecation section found")
        
        # Parse deprecations
        deprecations = DbIntroParser._parse_deprecation_section(deprecation_section)
        
        if deprecations:
            print(f"   [OK] Parsed {len(deprecations)} deprecations")
            for dep in deprecations:
                print(f"      - {dep.table_name}.{dep.column_name}")
        
        return db_intro_context, deprecation_section, deprecations
    
    @staticmethod
    def _parse_deprecation_section(section: str) -> List[DeprecationInfo]:
        """
        Parse deprecation section text into structured data.
        
        Supports formats like:
        Table: ClientInvoice
            - PaymentAmount column is deprecated Moved to ClientInvoicePayment...
            - PaymentMode column is deprecated Moved to ClientInvoicePayment...
        """
        
        if not section or len(section.strip()) < 10:
            return []
        
        deprecations = []
        
        # Pattern: "Table: TableName"
        table_pattern = r'Table:\s*(\w+)'
        current_table = None
        
        lines = section.split('\n')
        
        for line in lines:
            line = line.strip()
            
            if not line:
                continue
            
            # Check for table declaration
            table_match = re.search(table_pattern, line)
            if table_match:
                current_table = table_match.group(1)
                continue
            
            # Check for column deprecation
            # Pattern: "- ColumnName column is deprecated Moved to ..."
            if current_table and line.startswith('-'):
                dep = DbIntroParser._parse_deprecation_line(line, current_table)
                if dep:
                    deprecations.append(dep)
        
        return deprecations
    
    @staticmethod
    def _parse_deprecation_line(line: str, table_name: str) -> Optional[DeprecationInfo]:
        """
        Parse a single deprecation line.
        
        Format:
        - ColumnName column is deprecated/depricated Moved to TargetTable in column name TargetColumn. Join via JoinKey.
        """
        
        # Remove leading dash and spaces
        line = line.lstrip('- ').strip()
        
        # Extract column name (before "column")
        col_pattern = r'^(\w+)\s+column\s+is\s+deprica'
        col_match = re.search(col_pattern, line, re.IGNORECASE)
        if not col_match:
            return None
        
        column_name = col_match.group(1)
        
        # Extract reason - everything from "Moved to" onwards
        reason = "Moved to another table"
        if "Moved to" in line or "moved to" in line:
            moved_idx = line.lower().find("moved to")
            reason = line[moved_idx:].split('.')[0].strip()
        
        # Extract target table: "Moved to TargetTable"
        target_table_pattern = r'(?:Moved to|moved to)\s+(\w+)'
        target_table_match = re.search(target_table_pattern, line)
        migrate_to_table = target_table_match.group(1) if target_table_match else None
        
        # Extract target column: "in column name TargetColumn"
        target_col_pattern = r'(?:in|In)\s+column\s+name\s+(\w+)'
        target_col_match = re.search(target_col_pattern, line)
        migrate_to_column = target_col_match.group(1) if target_col_match else column_name
        
        # Extract join key: "Join via JoinKey"
        join_key_pattern = r'(?:Join via|join via)\s+(\w+)'
        join_key_match = re.search(join_key_pattern, line)
        join_key = join_key_match.group(1) if join_key_match else None
        
        # Auto-generate join key if not provided
        if not join_key:
            join_key = f"{table_name}Id"
        
        return DeprecationInfo(
            table_name=table_name,
            column_name=column_name,
            reason=reason,
            migrate_to_table=migrate_to_table,
            migrate_to_column=migrate_to_column,
            join_key=join_key
        )
