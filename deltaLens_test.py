import pytest
import os
import tempfile
import csv
from delta_lens.deltaLens import DeltaLens, EntityComparer
from delta_lens.config import Config, Entity, Side, Transform, Defaults,ReferenceDataset
import duckdb
import logging

@pytest.fixture
def sample_config():
    return Config(
        reference_datasets=[ReferenceDataset(datasetName="id_map", inputFile="" ) ],
        entities=[
             Entity(
                entityName="trade_2",
                leftSide=Side(
                    title="system_1",
                    inputFile="test_trades_2.csv",
                    transform=Transform(
                        query="ELECT i.trade_id_system_2 as trade_id, trade_date, symbol , price , quantity  FROM trade_system_1 t INNER JOIN id_map i ON t.trade_id = i.trade_id_system_1",
                        cached=True
                    )
                ),
                rightSide=Side(
                    title="system_2",
                    inputFile="test_trades_1.csv"
                    
                ),
                primaryKeys=["trade_id", "trade_date"],
                excludeColumns=[]
            ),
            Entity(
                entityName="trade",
                leftSide=Side(
                    title="system_1",
                    inputFile="test_trades_2.csv",
                    transform=Transform(
                        query="SELECT i.trade_id_system_2 as trade_id, trade_date, symbol , price , quantity  FROM trade_system_1 t INNER JOIN id_map i ON t.trade_id = i.trade_id_system_1",
                        cached=True
                    )
                ),
                rightSide=Side(
                    title="system_2",
                    inputFile="test_trades_1.csv"
                    
                ),
                primaryKeys=["trade_id", "trade_date"],
                excludeColumns=[]
            )
           
        ]
    )

@pytest.fixture
def sample_csv_files():
    # Create temporary CSV files for testing
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='trades_system_1.csv') as f1:
        writer = csv.writer(f1)
        writer.writerow(['trade_id', 'trade_date', 'symbol', 'price', 'quantity'])
        writer.writerow(['1', '2021-01-01', 'AAPL', '150.0', '100'])
        writer.writerow(['2', '2021-01-01', 'GOOGL', '2500.0', '50'])
        writer.writerow(['9', '2021-01-01', 'GOOGL', '2500.0', '50'])
        file1 = f1.name

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='trades_system_2.csv') as f2:
        writer = csv.writer(f2)
        writer.writerow(['trade_id', 'trade_date', 'symbol', 'price', 'quantity'])
        writer.writerow(['1', '2021-01-01', 'AAPL', '151.0', '101'])
        writer.writerow(['2', '2021-01-01','GOOGL', '2500.0', '50'])
        writer.writerow(['7', '2021-01-01','GOOGL', '2500.0', '50'])
        file2 = f2.name

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='id_map.csv') as f3:
        writer = csv.writer(f3)
        writer.writerow(['trade_id_system_1', 'trade_id_system_2'])
        writer.writerow(['1', '10' ])
        writer.writerow(['2', '20' ])
        writer.writerow(['9', '90' ])
        file3 = f3.name


    yield {'left': file1, 'right': file2, 'id_map': file3}
    
    # Cleanup
    os.unlink(file1)
    os.unlink(file2)
    os.unlink(file3)

@pytest.fixture
def delta_lens(sample_config):
    # Delete the test database file if it exists
    if os.path.exists("test_run.duckdb"):
        os.remove("test_run.duckdb")
    return DeltaLens("test_run", sample_config, persistent=True)


def test_runcompare_with_sample_data(delta_lens, sample_config, sample_csv_files):
    # Update config with temp file paths
    sample_config.entities[1].leftSide.inputFile = sample_csv_files['left']
    sample_config.entities[1].rightSide.inputFile = sample_csv_files['right']
    sample_config.reference_datasets[0].inputFile = sample_csv_files['id_map']
    
    # Run comparison
    delta_lens.execute()
    

    result_statement = f"SELECT * FROM {sample_config.entities[1].entityName}_compare"

    comparison_result = delta_lens.con.execute(result_statement).fetchdf()

    summary_results = delta_lens.con.execute(f"select * from {sample_config.entities[1].entityName}_compare_field_summary").fetchdf()
        
    logging.info(summary_results)  
    logging.info(comparison_result)


    # Verify tables were created
    tables = delta_lens.con.execute("SELECT table_name FROM information_schema.tables").fetchall()

    table_names = [t[0] for t in tables]
    
    assert "trade_system_1" in table_names
    assert "trade_system_2" in table_names
    assert "id_map" in table_names
    logging.info(table_names)

if __name__ == "__main__":
    pytest.main([__file__])