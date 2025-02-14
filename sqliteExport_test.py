import pytest
import os
import sqlite3
import duckdb
from pathlib import Path
from delta_lens.sqliteExport import export_to_sqlite
import logging

@pytest.fixture
def setup_logger():
    """Configure logging for tests"""
    logging.basicConfig(level=logging.INFO)
    yield
    logging.getLogger().handlers.clear()

@pytest.fixture
def sample_duckdb_connection():
    """Create a sample DuckDB database with test data"""
    con = duckdb.connect(':memory:')
    
    # Create comparison results table
    con.execute("""
        CREATE TABLE entity_compare_results (
            entity VARCHAR PRIMARY KEY,
            rows_left INTEGER,
            rows_right INTEGER,
            rows_fully_matched INTEGER,
            error_text VARCHAR,
            success INTEGER
        )
    """)
    
    # Create field summary table
    con.execute("""
        CREATE TABLE trade_compare_field_summary (
            field VARCHAR,
            total INTEGER,
            matches INTEGER,
            match_percentage DOUBLE
        )
    """)
    
    # Create comparison table with composite primary key
    con.execute("""
        CREATE TABLE trade_compare (
            trade_id INTEGER,
            timestamp VARCHAR,
            price_left DOUBLE,
            price_right DOUBLE,
            price_match BOOLEAN,
            _exists_left BOOLEAN,
            _exists_right BOOLEAN,
            _full_match BOOLEAN,
            PRIMARY KEY (trade_id)
        )
    """)
    
    # Insert sample data
    con.execute("""
        INSERT INTO entity_compare_results VALUES
        ('trade', 100, 100, 95, NULL, 1)
    """)
    
    con.execute("""
        INSERT INTO trade_compare_field_summary VALUES
        ('price', 100, 95, 95.0)
    """)
    
    con.execute("""
        INSERT INTO trade_compare VALUES
        (1, '2024-01-01', 100.0, 100.0, true, true, true, true),
        (2, '2024-01-01', 101.0, 102.0, false, true, true, false)
    """)
    
    yield con
    con.close()

def test_basic_export(sample_duckdb_connection, setup_logger, tmp_path):
    """Test basic export functionality"""
    sqlite_path = str(tmp_path / "test.sqlite")
    exported_path = export_to_sqlite(sample_duckdb_connection, sqlite_path)
    
    assert os.path.exists(exported_path)
    
    # Verify exported data
    con = sqlite3.connect(sqlite_path)
    cursor = con.cursor()
    
    # Check tables exist
    tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    table_names = {t[0] for t in tables}
    assert 'entity_compare_results' in table_names
    assert 'trade_compare_field_summary' in table_names
    assert 'trade_compare' in table_names
    
    # Check data content
    results = cursor.execute("SELECT * FROM entity_compare_results").fetchall()
    assert len(results) == 1
    assert results[0][0] == 'trade'
    
    con.close()

def test_table_filter(sample_duckdb_connection, setup_logger, tmp_path):
    """Test custom table filter"""
    sqlite_path = str(tmp_path / "test_filter.sqlite")
    
    # Custom filter to only export _compare tables
    filter_func = lambda name: name.endswith('_compare')
    exported_path = export_to_sqlite(sample_duckdb_connection, sqlite_path, table_filter=filter_func)
    
    con = sqlite3.connect(exported_path)
    cursor = con.cursor()
    
    tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    table_names = {t[0] for t in tables}
    
    assert 'trade_compare' in table_names
    assert 'entity_compare_results' not in table_names
    assert len(table_names) == 1
    
    con.close()

def test_sample_threshold(sample_duckdb_connection, setup_logger, tmp_path):
    """Test sample threshold functionality"""
    sqlite_path = str(tmp_path / "test_sample.sqlite")
    
    # Set sample threshold to 1
    exported_path = export_to_sqlite(sample_duckdb_connection, sqlite_path, sample_threshold=1)
    
    con = sqlite3.connect(exported_path)
    cursor = con.cursor()
    
    # Check sampled data
    count = cursor.execute("SELECT COUNT(*) FROM trade_compare").fetchone()[0]
    assert count == 1
    
    con.close()


def test_error_handling(setup_logger, tmp_path):
    """Test error handling"""
    sqlite_path = str(tmp_path / "test_error.sqlite")
    
    # Try to export from closed connection
    con = duckdb.connect(':memory:')
    con.close()
    
    with pytest.raises(Exception):
        export_to_sqlite(con, sqlite_path)

if __name__ == '__main__':
    pytest.main([__file__, '-v'])