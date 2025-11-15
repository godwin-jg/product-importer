#!/usr/bin/env python3
"""Run the trigram index migration script."""
import sys
from pathlib import Path

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from app.database import engine

def run_migration():
    """Execute the trigram index migration SQL script."""
    migration_sql = Path(__file__).parent / "add_trigram_index.sql"
    
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
    # Note: CONCURRENTLY indexes cannot be created in a transaction
    # So we use autocommit mode for index creation
    with engine.connect() as conn:
        # First, enable the extension (can be in transaction)
        extension_stmt = [s for s in statements if 'CREATE EXTENSION' in s]
        index_stmts = [s for s in statements if 'CREATE INDEX' in s]
        
        # Execute extension creation in transaction
        if extension_stmt:
            with conn.begin():
                print(f"Executing: {extension_stmt[0][:80]}...")
                conn.execute(text(extension_stmt[0]))
        
        # Execute index creation outside transaction (CONCURRENTLY requires autocommit)
        # Also increase statement timeout for large tables
        # Note: CONCURRENTLY doesn't support IF NOT EXISTS, so we check first
        for statement in index_stmts:
            if statement.strip():
                # Extract index name from statement (only one index now: trgm_idx_products_sku)
                index_name = 'trgm_idx_products_sku'
                
                # Check if index already exists
                check_query = text("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_indexes 
                        WHERE indexname = :index_name
                    )
                """)
                result = conn.execute(check_query, {"index_name": index_name}).scalar()
                if result:
                    print(f"Index {index_name} already exists, skipping...")
                    continue
                
                # Remove IF NOT EXISTS from statement (CONCURRENTLY doesn't support it)
                statement_clean = statement.replace('IF NOT EXISTS', '').strip()
                
                print(f"Executing (this may take several minutes for large tables): {statement_clean[:80]}...")
                # Use autocommit connection for CONCURRENTLY index creation
                # CONCURRENTLY cannot run inside a transaction
                # Create a new connection with autocommit from the start
                import psycopg2
                from app.core.config import settings
                # Parse DATABASE_URL to get connection params
                db_url = settings.DATABASE_URL
                # Create a direct psycopg2 connection with autocommit
                pg_conn = psycopg2.connect(
                    db_url,
                    connect_timeout=10
                )
                pg_conn.autocommit = True
                try:
                    cursor = pg_conn.cursor()
                    # Set a longer timeout for index creation (5 minutes)
                    cursor.execute("SET statement_timeout = '300000'")
                    cursor.execute(statement_clean)
                    cursor.close()
                    print(f"Index {index_name} created successfully!")
                except Exception as e:
                    if 'already exists' in str(e).lower():
                        print(f"Index {index_name} already exists, skipping...")
                    else:
                        raise
                finally:
                    pg_conn.close()
        
        print("Trigram index migration completed successfully!")

if __name__ == "__main__":
    try:
        run_migration()
    except Exception as e:
        print(f"Migration failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

