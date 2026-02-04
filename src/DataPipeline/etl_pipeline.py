"""
ETL Pipeline Module

Main orchestrator for the Pre-Kafka ETL process.
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import Union, Dict, Any, List
from datetime import datetime
import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.config_loader import ConfigLoader
from src.DataPipeline.data_reader import DataReader
from src.DataPipeline.data_cleaner import DataCleaner
from src.DataPipeline.data_validator import DataValidator

logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    Main ETL Pipeline orchestrator for cleaning CICIDS 2017 dataset.
    
    Workflow:
        1. Read raw CSV files
        2. Clean data (Inf/NaN/column names)
        3. Validate cleaned data
        4. Save to processed directory
        5. Generate summary report
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize ETL Pipeline.
        
        Args:
            config_path: Path to config file (optional)
        """
        # Load configuration
        self.config_loader = ConfigLoader(config_path)
        self.config_loader.load_config()
        
        self.config = self.config_loader.get_config()
        self.etl_config = self.config_loader.get_etl_config()
        self.data_paths = self.config_loader.get_data_paths()
        
        # Initialize components
        reader_config = self.etl_config.get('reader', {})
        self.reader = DataReader(
            backend=reader_config.get('backend', 'polars'),
            chunk_size=reader_config.get('chunk_size', 100000),
            encoding=reader_config.get('encoding', 'utf-8')
        )
        
        cleaning_config = self.etl_config.get('cleaning', {})
        self.cleaner = DataCleaner(
            infinity_strategy=cleaning_config.get('infinity', {}).get('strategy', 'max'),
            missing_strategy=cleaning_config.get('missing', {}).get('strategy', 'drop'),
            large_int_value=cleaning_config.get('infinity', {}).get('large_int_value', 1000000000),
            column_config=cleaning_config.get('columns', {})
        )
        
        validation_config = self.etl_config.get('validation', {})
        self.validator = DataValidator(
            check_infinity=validation_config.get('check_infinity', True),
            check_missing=validation_config.get('check_missing', True),
            check_schema=validation_config.get('check_schema', True),
            check_duplicates=validation_config.get('check_duplicates', True)
        )
        
        # Resolve paths
        self.raw_dir = Path(self.config_loader.resolve_path(self.data_paths['raw']))
        self.processed_dir = Path(self.config_loader.resolve_path(self.data_paths['processed']))
        self.reports_dir = Path(self.config_loader.resolve_path(self.data_paths.get('reports', 'artifacts/reports')))
        
        # Create directories if they don't exist
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        
        # Pipeline statistics
        self.pipeline_stats = {
            'start_time': None,
            'end_time': None,
            'files_processed': 0,
            'files_failed': 0,
            'total_rows_input': 0,
            'total_rows_output': 0,
            'files': []
        }
        
        logger.info("ETL Pipeline initialized")
        logger.info(f"Raw directory: {self.raw_dir}")
        logger.info(f"Processed directory: {self.processed_dir}")
    
    def process_file(self, file_path: Union[str, Path], expected_columns: List[str] = None) -> bool:
        """
        Process a single CSV file through the ETL pipeline.
        
        Args:
            file_path: Path to the CSV file
            expected_columns: Expected column names for validation (optional)
            
        Returns:
            True if successful, False otherwise
        """
        file_path = Path(file_path)
        filename = file_path.name
        
        logger.info(f"\n{'='*70}")
        logger.info(f"Processing: {filename}")
        logger.info(f"{'='*70}")
        
        file_stats = {
            'filename': filename,
            'status': 'processing',
            'start_time': datetime.now().isoformat()
        }
        
        try:
            # Step 1: Read data
            logger.info("Step 1/4: Reading data...")
            df = self.reader.read_csv(file_path)
            file_stats['original_rows'] = len(df)
            file_stats['original_columns'] = len(df.columns)
            self.pipeline_stats['total_rows_input'] += len(df)
            
            # Step 2: Clean data
            logger.info("Step 2/4: Cleaning data...")
            df = self.cleaner.clean_dataframe(df)
            cleaning_stats = self.cleaner.get_cleaning_stats()
            file_stats['cleaning'] = cleaning_stats
            file_stats['final_rows'] = len(df)
            self.pipeline_stats['total_rows_output'] += len(df)
            
            # Step 3: Validate data
            logger.info("Step 3/4: Validating data...")
            is_valid, validation_report = self.validator.validate_all(df, expected_columns)
            file_stats['validation'] = validation_report
            
            if not is_valid:
                logger.warning(f"Validation warnings for {filename}")
            
            # Step 4: Save processed data
            logger.info("Step 4/4: Saving processed data...")
            output_path = self.processed_dir / filename
            self.save_processed_data(df, output_path)
            file_stats['output_path'] = str(output_path)
            
            file_stats['status'] = 'success'
            file_stats['end_time'] = datetime.now().isoformat()
            
            logger.info(f"Successfully processed {filename}")
            logger.info(f"  Rows: {file_stats['original_rows']:,} -> {file_stats['final_rows']:,}")
            
            self.pipeline_stats['files_processed'] += 1
            self.pipeline_stats['files'].append(file_stats)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to process {filename}: {e}", exc_info=True)
            file_stats['status'] = 'failed'
            file_stats['error'] = str(e)
            file_stats['end_time'] = datetime.now().isoformat()
            
            self.pipeline_stats['files_failed'] += 1
            self.pipeline_stats['files'].append(file_stats)
            
            return False
    
    def process_all_files(self) -> Dict[str, Any]:
        """
        Process all CSV files in the raw directory.
        
        Returns:
            Pipeline statistics dictionary
        """
        logger.info("\n" + "="*70)
        logger.info("STARTING ETL PIPELINE")
        logger.info("="*70)
        
        self.pipeline_stats['start_time'] = datetime.now().isoformat()
        
        # Get all CSV files
        csv_files = self.reader.get_csv_files(self.raw_dir)
        
        if not csv_files:
            logger.error("No CSV files found in raw directory")
            return self.pipeline_stats
        
        logger.info(f"Found {len(csv_files)} CSV files to process\n")
        
        # Get expected columns from first file (for schema validation)
        expected_columns = None
        if csv_files and self.etl_config.get('validation', {}).get('check_schema', True):
            try:
                first_df = self.reader.read_csv(csv_files[0])
                # Clean column names to get expected schema
                first_df = self.cleaner.clean_column_names(first_df)
                expected_columns = first_df.columns
                logger.info(f"Using schema from {csv_files[0].name} ({len(expected_columns)} columns)\n")
            except Exception as e:
                logger.warning(f"Could not extract schema from first file: {e}")
        
        # Process each file
        for i, csv_file in enumerate(csv_files, 1):
            logger.info(f"\n[{i}/{len(csv_files)}] Processing {csv_file.name}")
            self.process_file(csv_file, expected_columns)
        
        self.pipeline_stats['end_time'] = datetime.now().isoformat()
        
        # Generate and save summary report
        self.generate_summary_report()
        
        return self.pipeline_stats
    
    def save_processed_data(self, df: pd.DataFrame, output_path: Union[str, Path]):
        """
        Save processed dataframe to file.
        
        Args:
            df: Processed dataframe
            output_path: Output file path
        """
        output_path = Path(output_path)
        output_config = self.etl_config.get('output', {})
        format_type = output_config.get('format', 'csv')
        compression = output_config.get('compression')
        
        if format_type == 'csv':
            df.to_csv(output_path, index=False)
        elif format_type == 'parquet':
            df.to_parquet(output_path, compression=compression or 'snappy')
        
        logger.info(f"Saved to: {output_path}")
    
    def _convert_to_serializable(self, obj):
        """Convert numpy types to native Python types for JSON serialization."""
        import numpy as np
        
        if isinstance(obj, dict):
            return {k: self._convert_to_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_to_serializable(item) for item in obj]
        elif isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return obj
    
    def generate_summary_report(self):
        """Generate and save pipeline summary report."""
        logger.info("\n" + "="*70)
        logger.info("ETL PIPELINE SUMMARY")
        logger.info("="*70)
        
        stats = self.pipeline_stats
        
        print(f"\nTotal Files: {len(stats['files'])}")
        print(f"  Successful: {stats['files_processed']}")
        print(f"  Failed: {stats['files_failed']}")
        print(f"\nTotal Rows:")
        print(f"  Input:  {stats['total_rows_input']:,}")
        print(f"  Output: {stats['total_rows_output']:,}")
        print(f"  Dropped: {stats['total_rows_input'] - stats['total_rows_output']:,}")
        
        # Convert numpy types to native Python types for JSON serialization
        serializable_stats = self._convert_to_serializable(stats)
        
        # Save detailed report to JSON
        report_path = self.reports_dir / f"etl_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w') as f:
            json.dump(serializable_stats, f, indent=2)
        
        logger.info(f"\nDetailed report saved to: {report_path}")
        logger.info("="*70 + "\n")
    
    def run(self) -> Dict[str, Any]:
        """
        Run the complete ETL pipeline.
        
        Returns:
            Pipeline statistics
        """
        return self.process_all_files()
