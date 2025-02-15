import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
import logging
from delta_lens.cli import main

@pytest.fixture
def mock_args():
    args = MagicMock()
    args.config = "test_config.json"
    args.run_name = "test_run"
    args.output_dir = "test_output"
    args.persistent = False
    args.continue_on_error = True
    args.export_sqlite = True
    args.export_csv = True
    args.export_sampling_threshold = 1000
    args.export_mismatches_only = True
    args.log_level = "INFO"
    return args

@pytest.fixture
def mock_config():
    return {"test": "config"}

@pytest.fixture
def mock_lens():
    lens = MagicMock()
    lens.con = MagicMock()
    return lens

def test_main_success(mock_args, mock_config, mock_lens):
    with patch('delta_lens.cli.parse_args', return_value=mock_args), \
         patch('delta_lens.cli.setup_logging') as mock_setup_logging, \
         patch('delta_lens.cli.Path.mkdir') as mock_mkdir, \
         patch('delta_lens.cli.load_config', return_value=mock_config) as mock_load_config, \
         patch('delta_lens.cli.DeltaLens', return_value=mock_lens) as mock_delta_lens, \
         patch('delta_lens.cli.export_to_sqlite') as mock_export_sqlite, \
         patch('delta_lens.cli.export_to_csv_archive') as mock_export_csv:

        main()

        mock_setup_logging.assert_called_once_with(mock_args.log_level)
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_load_config.assert_called_once_with(mock_args.config)
        mock_delta_lens.assert_called_once_with(
            mock_args.run_name,
            mock_config,
            persistent=mock_args.persistent,
            persist_path=str(Path(mock_args.output_dir))
        )
        mock_lens.execute.assert_called_once_with(continue_on_error=mock_args.continue_on_error)
        mock_export_sqlite.assert_called_once()
        mock_export_csv.assert_called_once()

def test_main_error(mock_args):
    with patch('delta_lens.cli.parse_args', return_value=mock_args), \
         patch('delta_lens.cli.setup_logging'), \
         patch('delta_lens.cli.load_config', side_effect=Exception("Test error")), \
         patch('logging.getLogger') as mock_logger:

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 1
        mock_logger.return_value.error.assert_called_once()

def test_main_without_exports(mock_args, mock_config, mock_lens):
    mock_args.export_sqlite = False
    mock_args.export_csv = False
    
    with patch('delta_lens.cli.parse_args', return_value=mock_args), \
         patch('delta_lens.cli.setup_logging'), \
         patch('delta_lens.cli.Path.mkdir'), \
         patch('delta_lens.cli.load_config', return_value=mock_config), \
         patch('delta_lens.cli.DeltaLens', return_value=mock_lens), \
         patch('delta_lens.cli.export_to_sqlite') as mock_export_sqlite, \
         patch('delta_lens.cli.export_to_csv_archive') as mock_export_csv:

        main()

        mock_export_sqlite.assert_not_called()
        mock_export_csv.assert_not_called()