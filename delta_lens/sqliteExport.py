import duckdb
import logging
from pathlib import Path
import os

def export_to_sqlite(conn, sqlite_db_path: str, sample_threshold = 10000, table_filter = None, mismatches_only = True):  
    """
    Export tables from DuckDB to SQLite database with optional sampling.
    This function exports selected tables from a DuckDB connection to a SQLite database. It supports filtering
    tables and sampling large tables to reduce export size.
    Args:
        conn: DuckDB connection object
        sqlite_db_path (str): Path to the SQLite database file to export to
        sample_threshold (int, optional): Maximum number of rows to sample from large tables. 
            If <= 0, no sampling is performed. Defaults to 10000.
        table_filter (callable, optional): Function to filter which tables to export.
            Takes table name as input and returns boolean. If None, exports tables ending with 
            '_compare_field_summary', 'entity_compare_results', or '_compare'. Defaults to None.
    Returns:
        str: Path to the created SQLite database file
    Notes:
        - Function will create tables in SQLite database matching the source tables in DuckDB
        - For tables with more rows than sample_threshold, random sampling is used
    """
    logger = logging.getLogger(__name__)
    # Check if SQLite file exists and raise error
    if os.path.exists(sqlite_db_path):
        raise FileExistsError(f"SQLite database file '{sqlite_db_path}' already exists")

    if table_filter is None:
        table_filter = lambda table: table.endswith("_compare_field_summary") or table.endswith("entity_compare_results") or  table.endswith("_compare")
    
    duck_con = conn
    try:   
            # Attach SQLite database
        duck_con.execute(f"ATTACH '{sqlite_db_path}' AS sqlite_db (TYPE SQLITE);")
        
        # Get list of tables and their primary keys from DuckDB
        tables_info = duck_con.execute("""
            SELECT 
                t.table_name,
                ARRAY_AGG(c.column_name) FILTER (WHERE c.is_key = true) as primary_keys
            FROM information_schema.tables t
            LEFT JOIN (
                SELECT tc.table_name, cc.column_name, 
                        CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN true ELSE false END as is_key
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage cc 
                    ON tc.constraint_name = cc.constraint_name
                    AND tc.table_name = cc.table_name
            ) c ON t.table_name = c.table_name
            WHERE t.table_schema = 'main'
            AND t.table_type IN ('BASE TABLE', 'VIEW')
            GROUP BY t.table_name
        """).fetchall()
        
        tables = [(name, pks) for name, pks in tables_info if (table_filter is None or table_filter(name))]
        logger.info(f"Found {len(tables)} tables to export")
        
        for table_name, primary_keys in tables:
            
                logger.info(f"Exporting table: {table_name}")
                
                # Get column definitions including types
                columns = duck_con.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """).fetchall()
                
                plain_create_statement = f"CREATE TABLE sqlite_db.{table_name} AS SELECT * FROM main.{table_name}"

                if sample_threshold <= 0:
                    duck_con.execute(plain_create_statement)
                else:
                    row_count = duck_con.execute(f"SELECT COUNT(*) FROM main.{table_name}").fetchone()[0]
                    if row_count > sample_threshold:
                        logger.info(f"sampling {sample_threshold} rows from {table_name}")

                        if table_name.endswith("_compare") and mismatches_only:
                            duck_con.execute(f"CREATE TABLE sqlite_db.{table_name} AS SELECT * FROM main.{table_name} WHERE _full_match=0  USING SAMPLE {sample_threshold};")
                        else:
                            duck_con.execute(f"CREATE TABLE sqlite_db.{table_name} AS SELECT * FROM main.{table_name} USING SAMPLE {sample_threshold};")
                    else:
                        duck_con.execute(plain_create_statement)
            
            
                row_count = duck_con.execute(f"SELECT COUNT(*) FROM sqlite_db.{table_name}").fetchone()[0]
                logger.info(f"Exported {row_count} rows from {table_name}")
    finally:      
        duck_con.execute("DETACH sqlite_db")

    return sqlite_db_path
    