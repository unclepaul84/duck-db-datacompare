import unittest
from pathlib import Path
import duckdb
import tarfile
import gzip
import os
from delta_lens.csvExport import export_to_csv_archive

class TestCsvExport(unittest.TestCase):
    def setUp(self):
        # Create test database connection
        self.conn = duckdb.connect(':memory:')
        
        # Create test tables
        self.conn.execute("""
            CREATE TABLE test_compare (
                id INTEGER,
                name VARCHAR,
                _full_match INTEGER
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE test_compare_field_summary (
                field VARCHAR,
                mismatch_count INTEGER
            )
        """)

        # Insert test data
        self.conn.execute("""
            INSERT INTO test_compare VALUES 
                (1, 'Match', 1),
                (2, 'Mismatch', 0),
                (3, 'Another Mismatch', 0)
        """)
        
        self.conn.execute("""
            INSERT INTO test_compare_field_summary VALUES
                ('name', 2)
        """)

    def tearDown(self):
        self.conn.close()
        # Cleanup any test archives
        for f in Path('.').glob('test_export*.tar.gz'):
            f.unlink()

    def test_export_with_mismatches_only(self):
        archive_path = 'test_export_mismatches.tar.gz'
        export_to_csv_archive(self.conn, archive_path, mismatches_only=True)
        
        self.assertTrue(os.path.exists(archive_path))
        
        # Check archive contents
        with tarfile.open(archive_path, 'r:gz') as tar:
            # Check if expected files exist
            files = tar.getnames()
            self.assertIn('test_compare.csv', files)
            self.assertIn('test_compare_field_summary.csv', files)
            
            # Extract and check content
            compare_file = tar.extractfile('test_compare.csv')
            lines = compare_file.read().decode().splitlines()
            self.assertEqual(len(lines), 3)  # Header + 2 mismatched rows

    def test_export_all_records(self):
        archive_path = 'test_export_all.tar.gz'
        export_to_csv_archive(self.conn, archive_path, mismatches_only=False)
        
        with tarfile.open(archive_path, 'r:gz') as tar:
            compare_file = tar.extractfile('test_compare.csv')
            lines = compare_file.read().decode().splitlines()
            self.assertEqual(len(lines), 4)  # Header + 3 rows

    def test_custom_table_filter(self):
        archive_path = 'test_export_filtered.tar.gz'
        table_filter = lambda table: table == 'test_compare'
        export_to_csv_archive(self.conn, archive_path, table_filter=table_filter)
        
        with tarfile.open(archive_path, 'r:gz') as tar:
            files = tar.getnames()
            self.assertEqual(len(files), 1)
            self.assertIn('test_compare.csv', files)

    def test_file_exists_error(self):
        archive_path = 'test_export_exists.tar.gz'
        # Create empty file
        with open(archive_path, 'w') as f:
            pass
            
        with self.assertRaises(FileExistsError):
            export_to_csv_archive(self.conn, archive_path)

if __name__ == '__main__':
    unittest.main()