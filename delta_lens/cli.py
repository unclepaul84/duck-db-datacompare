"""
DeltaLens CLI - A command-line interface for comparing datasets and generating comparison reports.
This module provides the command-line interface for DeltaLens, allowing users to:
- Configure and run dataset comparisons
- Generate comparison reports
- Export results to SQLite databases
- Control logging and error handling

The CLI can be configured through command-line arguments or environment variables:
- DELTALENS_CONFIG: Path to config file
- DELTALENS_RUN_NAME: Name for the comparison run
- DELTALENS_OUTPUT_DIR: Directory for output files
- DELTALENS_PERSISTENT: Enable persistent storage
- DELTALENS_CONTINUE_ON_ERROR: Continue processing on errors
- DELTALENS_EXPORT_SQLITE: Export results to SQLite
- DELTALENS_EXPORT_SAMPLING_THRESHOLD: Sample size for  export
- DELTALENS_EXPORT_CSV: export to csv archive
- DELTALENS_EXPORT_MISMATCHES_ONLY: export only mismatched rows
- DELTALENS_LOG_LEVEL: Logging level


Example using command line:
    python cli.py --config compare.json --run-name my_comparison --output-dir ./results

Example using environment variables:
    export DELTALENS_CONFIG=compare.json
    export DELTALENS_RUN_NAME=my_comparison
    export DELTALENS_OUTPUT_DIR=./results
    export DELTALENS_PERSISTENT=true
    export DELTALENS_EXPORT_SQLITE=true
    export DELTALENS_EXPORT_CSV=true
    export DELTALENS_EXPORT_SAMPLING_THRESHOLD=10000
    export DELTALENS_EXPORT_MISMATCHES_ONLY=true
    python cli.py

Dependencies:
    - argparse
    - logging
    - pathlib
    - delta_lens.deltaLens
    - delta_lens.config
    - delta_lens.sqliteExport

Author: Paul
License: MIT
"""
import logging
import os
from datetime import date
from pathlib import Path
from delta_lens.deltaLens import DeltaLens
from delta_lens.config import load_config
from delta_lens.sqliteExport import export_to_sqlite
from delta_lens.csvExport import export_to_csv_archive
import argparse

def setup_logging(log_level: str = "INFO") -> None:
    """Configure logging"""
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='DeltaLens - Compare datasets and generate comparison reports')
    
    # Config file
    parser.add_argument(
        '--config', '-c',
        type=str,
        default=os.getenv('DELTALENS_CONFIG'),
        help='Path to config file (env: DELTALENS_CONFIG)'
    )
    
    # Run name
    parser.add_argument(
        '--run-name', '-n',
        type=str,
        default=os.getenv('DELTALENS_RUN_NAME', f'compare_{date.today().strftime("%Y-%m-%d")}'),
        help='Name for this comparison run (env: DELTALENS_RUN_NAME)'
    )
    
    # Output directory
    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        default=os.getenv('DELTALENS_OUTPUT_DIR', '.'),
        help='Directory for output files (env: DELTALENS_OUTPUT_DIR)'
    )
    
    # Persistence options
    parser.add_argument(
        '--persistent',
        action='store_true',
        default=os.getenv('DELTALENS_PERSISTENT', 'false').lower() in ('true', '1', 'yes'),
        help='Use persistent storage (env: DELTALENS_PERSISTENT)'
    )
    
    # Continue on error
    parser.add_argument(
        '--continue-on-error',
        action='store_true',
        default=os.getenv('DELTALENS_CONTINUE_ON_ERROR', 'true').lower() in ('true', '1', 'yes'),
        help='Continue processing entities if error occurs (env: DELTALENS_CONTINUE_ON_ERROR)'
    )
    
    # Export to SQLite
    parser.add_argument(
        '--export-sqlite',
        action='store_true',
        default=os.getenv('DELTALENS_EXPORT_SQLITE', 'true').lower() in ('true', '1', 'yes'),
        help='Export results to SQLite (env: DELTALENS_EXPORT_SQLITE)'
    )
    
        # Export to SQLite
    parser.add_argument(
        '--export-csv',
        action='store_true',
        default=os.getenv('DELTALENS_EXPORT_CSV', 'true').lower() in ('true', '1', 'yes'),
        help='Export results to CSV (gziped) (env: DELTALENS_EXPORT_CSV)'
    )

    parser.add_argument(
        '--export-mismatches-only',
        action='store_true',
        default=os.getenv('DELTALENS_EXPORT_MISMATCHES_ONLY', 'true').lower() in ('true', '1', 'yes'),
        help='Export mismatched rows only (env: DELTALENS_EXPORT_MISMATCHES_ONLY)'
    )
    # SQLite sample threshold
    parser.add_argument(
        '--export-sampling-threshold',
        type=int,
        default=int(os.getenv('DELTALENS_EXPORT_SAMPLING_THRESHOLD', '10000')),
        help='Sampling size for  export (env: DELTALENS_EXPORT_SAMPLING_THRESHOLD)'
    )
    
    # Log level
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default=os.getenv('DELTALENS_LOG_LEVEL', 'INFO'),
        help='Logging level (env: DELTALENS_LOG_LEVEL)'
    )
    
    return parser.parse_args()

def main():
    """Main entry point for CLI"""
    args = parse_args()
    setup_logging(args.log_level)
    logger = logging.getLogger("DeltaLens-CLI")
    
    try:
        # Ensure output directory exists
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load configuration
        logger.info(f"Loading configuration from: {args.config}")
        config = load_config(args.config)
        
        # Initialize DeltaLens
        lens = DeltaLens(
            args.run_name,
            config,
            persistent=args.persistent,
            persist_path=str(output_dir)
        )
        
        # Execute comparison
        logger.info("Starting comparison")
        lens.execute(continue_on_error=args.continue_on_error)
        
        # Export to SQLite if requested
        if args.export_sqlite:

            sqlite_path = output_dir / f"{args.run_name}.sqlite"
            logger.info(f"Exporting results to SQLite: {sqlite_path}")
            export_to_sqlite(
                lens.con,
                str(sqlite_path),
                sample_threshold=args.export_sampling_threshold,
                mismatches_only=args.export_mismatches_only
            )

         # Export to SQLite if requested
        if args.export_csv:
            tar_path = output_dir / f"{args.run_name}.tar.gz"
            logger.info(f"Exporting results to CSV: {tar_path}")
            export_to_csv_archive(
                lens.con,
                str(tar_path),
                mismatches_only=args.export_mismatches_only
            )
        
        
        logger.info("Comparison completed successfully")
        
    except Exception as e:
        logger.error(f"Error during comparison: {str(e)}", exc_info=True)
        raise SystemExit(1)

if __name__ == "__main__":
    main()