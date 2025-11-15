#!/usr/bin/env python3
"""Run the full-text search migration script."""
import sys
from pathlib import Path

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from app.database import engine

def run_migration():
    """Execute the migration SQL script."""
    migration_sql = Path(__file__).parent / "add_fulltext_search.sql"
    
    with open(migration_sql, 'r') as f:
        sql_commands = f.read()
    
    # Split by semicolons and filter out comments and empty statements
    statements = []
    current_statement = []
    
    for line in sql_commands.split('\n'):
        line = line.strip()
        # Skip comments and empty lines
        if not line or line.startswith('--'):
            continue
        current_statement.append(line)
        if line.endswith(';'):
            statement = ' '.join(current_statement)
            if statement.strip() and not statement.strip().startswith('--'):
                statements.append(statement)
            current_statement = []
    
    # Execute each statement
    with engine.begin() as conn:
        for statement in statements:
            if statement.strip():
                print(f"Executing: {statement[:80]}...")
                conn.execute(text(statement))
        print("Migration completed successfully!")

if __name__ == "__main__":
    try:
        run_migration()
    except Exception as e:
        print(f"Migration failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

