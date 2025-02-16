import pytest
import os
import tempfile
import csv
from delta_lens.deltaLens import DeltaLens, EntityComparer
from delta_lens.config import Config, Entity, Side, Transform, Defaults,ReferenceDataset, load_config
import duckdb
import logging
from delta_lens.sqliteExport import *




def test_runcompare_with_sample_data():

    if os.path.exists("test_run.duckdb"):
        os.remove("test_run.duckdb")
    if os.path.exists("test_run.sqlite"):
        os.remove("test_run.sqlite")

    config = load_config("data/legislators.compare.config.json")
    delta_lens = DeltaLens("test_run", config, persistent=True)
    # Run comparison
    delta_lens.execute(continue_on_error=False)
    


if __name__ == "__main__":
    pytest.main([__file__])