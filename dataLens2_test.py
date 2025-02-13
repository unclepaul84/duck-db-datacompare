import pytest
import tempfile
import csv
import os
from delta_lens.deltaLens import DeltaLens, EntityComparer
from delta_lens.config import Config, Entity, Side, Transform, Defaults, ReferenceDataset
import duckdb
import logging

@pytest.fixture
def sample_csv_files():
    """Create temporary test files with known data"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='_1.csv') as f1:
        writer = csv.writer(f1)
        writer.writerow(['trade_id', 'timestamp', 'symbol', 'price', 'quantity'])
        writer.writerow(['1', '2024-01-01', 'AAPL', '150.0', '100'])
        writer.writerow(['2', '2024-01-01', 'GOOGL', '2500.0', '50'])
        file1 = f1.name

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='_2.csv') as f2:
        writer = csv.writer(f2)
        writer.writerow(['trade_id', 'timestamp', 'symbol', 'price', 'quantity'])
        writer.writerow(['1', '2024-01-01', 'AAPL', '151.0', '100'])  # Price mismatch
        writer.writerow(['3', '2024-01-01', 'MSFT', '200.0', '75'])   # New record
        file2 = f2.name

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='_ref.csv') as f3:
        writer = csv.writer(f3)
        writer.writerow(['symbol', 'sector'])
        writer.writerow(['AAPL', 'Technology'])
        file3 = f3.name

    yield {'left': file1, 'right': file2, 'ref': f3.name}
    
    # Cleanup
    for f in [file1, file2, f3.name]:
        if os.path.exists(f):
            os.unlink(f)

@pytest.fixture
def basic_config(sample_csv_files):
    """Create basic valid configuration"""
    return Config(

        entities=[
            Entity(
                entityName="trade",
                leftSide=Side(
                    title="system_1",
                    inputFile=sample_csv_files['left']
                ),
                rightSide=Side(
                    title="system_2",
                    inputFile=sample_csv_files['right']
                ),
                primaryKeys=["trade_id"]
            )
        ]
    )

@pytest.fixture
def config_with_transform(sample_csv_files):
    """Create configuration with transforms"""
    return Config(
        entities=[
            Entity(
                entityName="trade",
                leftSide=Side(
                    title="system_1",
                    inputFile=sample_csv_files['left'],
                    transform=Transform(
                        query="SELECT * FROM trade_system_1 WHERE symbol = 'AAPL'",
                        cached=True
                    )
                ),
                rightSide=Side(
                    title="system_2",
                    inputFile=sample_csv_files['right'],
                    transform=Transform(
                        query="SELECT * FROM trade_system_2 WHERE symbol = 'AAPL'",
                        cached=True
                    )
                ),
                primaryKeys=["trade_id"]
            )
        ]
    )

def test_successful_comparison(basic_config):
    """Test basic successful comparison"""
    lens = DeltaLens("test_run", basic_config)
    lens.execute(continue_on_error=True)
    
    results = lens.con.execute("SELECT * FROM entity_compare_results").fetchdf()
    
    assert len(results) == 1
    assert results.iloc[0]['success'] == 1
    assert results.iloc[0]['rows_left'] > 0
    assert results.iloc[0]['rows_right'] > 0

def test_comparison_with_transform(config_with_transform):
    """Test comparison with transformation applied"""
    lens = DeltaLens("test_transform", config_with_transform)
    lens.execute(continue_on_error=False)
    
    results = lens.con.execute("SELECT * FROM trade_compare").fetchdf()
    assert all(row['symbol_left'] == 'AAPL' for _, row in results.iterrows())

def test_missing_input_file():
    """Test handling of missing input file"""
    config = Config(
        entities=[
            Entity(
                entityName="trade",
                leftSide=Side(title="s1", inputFile="nonexistent.csv"),
                rightSide=Side(title="s2", inputFile="nonexistent.csv"),
                primaryKeys=["trade_id"]
            )
        ]
    )
    
    lens = DeltaLens("test_missing", config)
    with pytest.raises(FileNotFoundError):
        lens.execute(continue_on_error=False)

def test_mismatched_schemas(sample_csv_files):
    """Test handling of mismatched schemas"""
    # Create file with different schema
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        writer = csv.writer(f)
        writer.writerow(['trade_id', 'different_column'])
        writer.writerow(['1', 'value'])
        
    config = Config(
        entities=[
            Entity(
                entityName="trade",
                leftSide=Side(title="s1", inputFile=sample_csv_files['left']),
                rightSide=Side(title="s2", inputFile=f.name),
                primaryKeys=["trade_id"]
            )
        ]
    )
    
    lens = DeltaLens("test_schema", config)
    with pytest.raises(ValueError, match="Column.*not found in both tables"):
        lens.execute(continue_on_error=False)
    
    os.unlink(f.name)

def test_invalid_transform_query(sample_csv_files):
    """Test handling of invalid transform query"""
    config = Config(
        entities=[
            Entity(
                entityName="trade",
                leftSide=Side(
                    title="s1",
                    inputFile=sample_csv_files['left'],
                    transform=Transform(query="INVALID SQL")
                ),
                rightSide=Side(
                    title="s2",
                    inputFile=sample_csv_files['right']
                ),
                primaryKeys=["trade_id"]
            )
        ]
    )
    
    lens = DeltaLens("test_invalid_query", config)
    with pytest.raises(duckdb.Error):
        lens.execute(continue_on_error=False)

def test_duplicate_execution(basic_config):
    """Test that execute() can only be called once"""
    lens = DeltaLens("test_duplicate", basic_config)
    lens.execute(continue_on_error=True)
    
    with pytest.raises(ValueError, match="Execute method has already been called"):
        lens.execute(continue_on_error=False)

def test_continue_on_error(sample_csv_files):
    """Test continue_on_error behavior"""
    config = Config(
        entities=[
            Entity(  # This one will fail
                entityName="bad_trade",
                leftSide=Side(title="s1", inputFile="nonexistent.csv"),
                rightSide=Side(title="s2", inputFile="nonexistent.csv"),
                primaryKeys=["trade_id"]
            ),
            Entity(  # This one would succeed
                entityName="good_trade",
                leftSide=Side(title="s1", inputFile=sample_csv_files['left']),
                rightSide=Side(title="s2", inputFile=sample_csv_files['right']),
                primaryKeys=["trade_id"]
            )
        ]
    )
    
    # Test with continue_on_error=True
    lens = DeltaLens("test_continue", config)
    lens.execute()
    
    results = lens.con.execute("SELECT * FROM entity_compare_results ORDER BY entity").fetchdf()
    assert len(results) == 2
    assert results.iloc[0]['success'] == 0  # bad_trade
    assert results.iloc[1]['success'] == 1  # good_trade

def test_field_summary_generation(basic_config):
    """Test field summary statistics generation"""
    lens = DeltaLens("test_summary", basic_config)
    lens.execute(continue_on_error=False)
    
    summary = lens.con.execute("SELECT * FROM trade_compare_field_summary").fetchdf()
    assert len(summary) > 0
    assert all(col in summary.columns for col in ['field', 'total', 'matches', 'match_percentage'])

if __name__ == "__main__":
    pytest.main([__file__, "-v"])