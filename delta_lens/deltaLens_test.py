import pytest
import os
import tempfile
import csv
from deltaLens import DeltaLens, EntityComparer
from config import Config, Entity, Side, Transform, Defaults
import duckdb
import logging

@pytest.fixture
def sample_config():
    return Config(
        defaults=Defaults(
            leftSideTitle="system_1",
            rightSideTitle="system_2",
            filePattenGlobTemplate="{entityName}_{title}.csv"
        ),
        entities=[
            Entity(
                entityName="trade",
                leftSide=Side(
                    title="system_1",
                    inputFile="test_trades_1.csv",
                    transform=Transform(
                        query="SELECT * FROM trade WHERE symbol = 'AAPL'",
                        cached=True
                    )
                ),
                rightSide=Side(
                    title="system_2",
                    inputFile="test_trades_2.csv",
                    transform=Transform(
                        query="SELECT * FROM trade WHERE symbol = 'AAPL'",
                        cached=True
                    )
                ),
                primaryKeys=["trade_id", "trade_date"],
                excludeColumns=["price"]
            )
        ]
    )

@pytest.fixture
def sample_csv_files():
    # Create temporary CSV files for testing
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='trades_1.csv') as f1:
        writer = csv.writer(f1)
        writer.writerow(['trade_id', 'trade_date', 'symbol', 'price', 'quantity'])
        writer.writerow(['1', '2021-01-01', 'AAPL', '150.0', '100'])
        writer.writerow(['2', '2021-01-01', 'GOOGL', '2500.0', '50'])
        writer.writerow(['9', '2021-01-01', 'GOOGL', '2500.0', '50'])
        file1 = f1.name

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='trades_2.csv') as f2:
        writer = csv.writer(f2)
        writer.writerow(['trade_id', 'trade_date', 'symbol', 'price', 'quantity'])
        writer.writerow(['1', '2021-01-01', 'AAPL', '151.0', '101'])
        writer.writerow(['2', '2021-01-01','GOOGL', '2500.0', '50'])
        writer.writerow(['7', '2021-01-01','GOOGL', '2500.0', '50'])
        file2 = f2.name

    yield {'left': file1, 'right': file2}
    
    # Cleanup
    os.unlink(file1)
    os.unlink(file2)

@pytest.fixture
def delta_lens(sample_config):
    # Delete the test database file if it exists
    if os.path.exists("test_run.duckdb"):
        os.remove("test_run.duckdb")
    return DeltaLens("test_run", sample_config)


def test_runcompare_with_sample_data(delta_lens, sample_config, sample_csv_files):
    # Update config with temp file paths
    sample_config.entities[0].leftSide.inputFile = sample_csv_files['left']
    sample_config.entities[0].rightSide.inputFile = sample_csv_files['right']
    
    # Run comparison
    delta_lens.execute()
    
    # Verify tables were created
    tables = delta_lens.con.execute("SELECT table_name FROM information_schema.tables").fetchall()

    table_names = [t[0] for t in tables]
    
    assert "trade_system_1" in table_names
    assert "trade_system_2" in table_names


if __name__ == "__main__":
    pytest.main([__file__,'-o','log_cli=true'])