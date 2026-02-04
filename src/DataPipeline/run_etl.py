"""
Run ETL Pipeline

Command-line interface for executing the Pre-Kafka ETL pipeline.

Usage:
    # Process all files in raw directory
    python run_etl.py
    
    # Process specific file
    python run_etl.py --file Monday_WorkingHours_ISCX.csv
    
    # Use custom config
    python run_etl.py --config /path/to/config.yaml
    
    # Enable debug logging
    python run_etl.py --debug
"""

import sys
import argparse
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.DataPipeline.etl_pipeline import ETLPipeline
from utils.config_loader import ConfigLoader


def setup_logging(log_level: str = "INFO", log_to_file: bool = False, log_file: str = None):
    """
    Setup logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_to_file: Whether to log to file
        log_file: Path to log file
    """
    log_format = '%(asctime)s | %(name)s | %(levelname)s | %(message)s'
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[]
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    logging.getLogger().addHandler(console_handler)
    
    # File handler
    if log_to_file and log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(log_format))
        logging.getLogger().addHandler(file_handler)


def main():
    """Main entry point for ETL pipeline CLI."""
    parser = argparse.ArgumentParser(
        description='Run Pre-Kafka ETL Pipeline for CICIDS 2017 dataset',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--file',
        type=str,
        help='Process specific CSV file (filename only, not full path)'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        help='Path to custom config file'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    parser.add_argument(
        '--no-log-file',
        action='store_true',
        help='Disable logging to file'
    )
    
    args = parser.parse_args()
    
    # Load config for logging settings
    config_loader = ConfigLoader(args.config)
    config_loader.load_config()
    log_config = config_loader.get_logging_config()
    
    # Setup logging
    log_level = "DEBUG" if args.debug else log_config.get('level', 'INFO')
    log_to_file = not args.no_log_file and log_config.get('log_to_file', True)
    log_file = log_config.get('log_file', 'logs/etl_pipeline.log')
    
    # Resolve log file path
    if log_to_file:
        project_root = Path(__file__).parent.parent.parent
        log_file = project_root / log_file
    
    setup_logging(log_level, log_to_file, str(log_file) if log_to_file else None)
    
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize pipeline
        pipeline = ETLPipeline(config_path=args.config)
        
        if args.file:
            # Process single file
            logger.info(f"Processing single file: {args.file}")
            file_path = pipeline.raw_dir / args.file
            
            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                sys.exit(1)
            
            success = pipeline.process_file(file_path)
            pipeline.generate_summary_report()
            
            sys.exit(0 if success else 1)
        else:
            # Process all files
            logger.info("Processing all files in raw directory")
            stats = pipeline.run()
            
            # Exit with error code if any files failed
            sys.exit(0 if stats['files_failed'] == 0 else 1)
            
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
