import duckdb
import logging
from pathlib import Path
import os
import gzip
import tempfile
import tarfile
import io

def export_to_csv_archive(conn, archive_path: str,  table_filter = None, mismatches_only = True):
    """
    Export tables from DuckDB to a gzipped tar archive containing CSV files.
    
    Args:
        conn: DuckDB connection object
        archive_path (str): Path to the output tar.gz archive file
        sample_threshold (int, optional): Maximum number of rows to sample from large tables. 
            If <= 0, no sampling is performed. Defaults to 10000.
        table_filter (callable, optional): Function to filter which tables to export.
            Takes table name as input and returns boolean. If None, exports tables ending with 
            '_compare_field_summary', 'entity_compare_results', or '_compare'. Defaults to None.
    
    Returns:
        str: Path to the created archive file
    """
    logger = logging.getLogger(__name__)
    
    # Check if archive file exists
    if os.path.exists(archive_path):
        raise FileExistsError(f"Archive file '{archive_path}' already exists")

    if table_filter is None:
        table_filter = lambda table: table.endswith("_compare_field_summary") or \
                                   table.endswith("entity_compare_results") or \
                                   table.endswith("_compare")
    
    duck_con = conn
    temp_tar = archive_path.replace('.gz', '')
    
    # Create temporary directory for CSV files
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # Get list of tables
            tables = duck_con.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                AND table_type IN ('BASE TABLE', 'VIEW')
            """).fetchall()
            
            # Filter tables
            tables = [name[0] for name in tables if table_filter(name[0])]
            logger.info(f"Found {len(tables)} tables to export")
            
            # Create uncompressed tar archive
            with tarfile.open(temp_tar, "w") as tar:
                for table_name in tables:
                    logger.info(f"Exporting table: {table_name} mismatches_only={mismatches_only}")
                    csv_path = Path(temp_dir) / f"{table_name}.csv"
                    
                    if table_name.endswith("_compare") and mismatches_only:

                        query = f"""
                                    COPY (SELECT * FROM {table_name} WHERE _full_match=0)
                                    TO '{csv_path}'
                                    WITH (HEADER TRUE, DELIMITER ',')
                                """
                    else:       
                        query = f"""
                                    COPY (SELECT * FROM {table_name})
                                    TO '{csv_path}'
                                    WITH (HEADER TRUE, DELIMITER ',')
                                """
                
                    # Export to CSV
                    duck_con.execute(query)
                    
                    # Add CSV to tar archive
                    tar.add(csv_path, arcname=f"{table_name}.csv")
                    
                    # Log row count
                    row_count = sum(1 for _ in open(csv_path)) - 1  # Subtract header row
                    logger.info(f"Exported {row_count} rows from {table_name}")
            
            # Compress the tar file
            with open(temp_tar, 'rb') as f_in:
                with gzip.open(archive_path, 'wb') as f_out:
                    f_out.write(f_in.read())
            
            # Clean up temporary tar file
            os.remove(temp_tar)
            
            logger.info(f"Archive created successfully at: {archive_path}")
            return archive_path
            
        except Exception as e:
            logger.error(f"Error during export: {str(e)}")
            if os.path.exists(archive_path):
                os.remove(archive_path)
            if os.path.exists(temp_tar):
                os.remove(temp_tar)
            raise