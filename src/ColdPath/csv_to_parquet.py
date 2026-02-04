"""
CSV to Parquet Converter

Converts cleaned CSV files to Parquet format for efficient ML training.
Parquet provides columnar storage with compression for faster read performance.
"""

import os
import sys
import logging
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class CSVToParquetConverter:
    """
    Convert CSV files to Parquet format with compression.
    
    Parquet benefits:
    - Columnar storage (faster for ML feature selection)
    - Better compression (50-80% smaller than CSV)
    - Schema preservation
    - Fast read performance
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize converter.
        
        Args:
            config_path: Path to config file (optional)
        """
        # Load configuration
        self.config_loader = ConfigLoader(config_path)
        self.config_loader.load_config()
        
        self.data_paths = self.config_loader.get_data_paths()
        self.parquet_config = self.config_loader.get_config().get('parquet', {})
        
        # Resolve paths
        self.processed_dir = Path(self.config_loader.resolve_path(self.data_paths['processed']))
        self.parquet_dir = Path(self.config_loader.resolve_path(self.data_paths.get('parquet', 'data/parquet')))
        
        # Create output directory
        self.parquet_dir.mkdir(parents=True, exist_ok=True)
        
        # Parquet settings
        self.compression = self.parquet_config.get('compression', 'snappy')
        self.row_group_size_mb = self.parquet_config.get('row_group_size', 128)
        
        # Statistics
        self.stats = {
            'files_converted': 0,
            'files_failed': 0,
            'total_csv_size': 0,
            'total_parquet_size': 0,
            'files': []
        }
        
        logger.info("CSVToParquetConverter initialized")
        logger.info(f"Input: {self.processed_dir}")
        logger.info(f"Output: {self.parquet_dir}")
        logger.info(f"Compression: {self.compression}")
    
    def convert_file(self, csv_path: Path) -> bool:
        """
        Convert a single CSV file to Parquet.
        
        Args:
            csv_path: Path to CSV file
            
        Returns:
            True if successful, False otherwise
        """
        filename = csv_path.stem
        logger.info(f"Converting {csv_path.name} to Parquet...")
        
        file_stats = {
            'filename': csv_path.name,
            'status': 'processing'
        }
        
        try:
            # Read CSV
            df = pd.read_csv(csv_path)
            csv_size = csv_path.stat().st_size
            file_stats['csv_size_mb'] = csv_size / (1024 * 1024)
            file_stats['rows'] = len(df)
            file_stats['columns'] = len(df.columns)
            
            logger.info(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")
            
            # Output path
            parquet_path = self.parquet_dir / f"{filename}.parquet"
            
            # Convert to Parquet with PyArrow
            # Using PyArrow table for better control
            table = pa.Table.from_pandas(df)
            
            # Write Parquet with compression
            pq.write_table(
                table,
                parquet_path,
                compression=self.compression,
                row_group_size=self.row_group_size_mb * 1024 * 1024,  # Convert MB to bytes
                use_dictionary=True,  # Enable dictionary encoding
                version='2.6'  # Modern Parquet version
            )
            
            # Get Parquet file size
            parquet_size = parquet_path.stat().st_size
            file_stats['parquet_size_mb'] = parquet_size / (1024 * 1024)
            
            # Calculate compression ratio
            compression_ratio = (1 - parquet_size / csv_size) * 100
            file_stats['compression_ratio'] = compression_ratio
            file_stats['output_path'] = str(parquet_path)
            file_stats['status'] = 'success'
            
            logger.info(f"  CSV size: {file_stats['csv_size_mb']:.2f} MB")
            logger.info(f"  Parquet size: {file_stats['parquet_size_mb']:.2f} MB")
            logger.info(f"  Compression: {compression_ratio:.1f}% reduction")
            logger.info(f"  Saved to: {parquet_path}")
            
            # Update global stats
            self.stats['files_converted'] += 1
            self.stats['total_csv_size'] += csv_size
            self.stats['total_parquet_size'] += parquet_size
            self.stats['files'].append(file_stats)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to convert {csv_path.name}: {e}", exc_info=True)
            file_stats['status'] = 'failed'
            file_stats['error'] = str(e)
            self.stats['files_failed'] += 1
            self.stats['files'].append(file_stats)
            return False
    
    def convert_all(self) -> Dict[str, Any]:
        """
        Convert all CSV files in processed directory to Parquet.
        
        Returns:
            Statistics dictionary
        """
        logger.info("\n" + "="*70)
        logger.info("CSV TO PARQUET CONVERSION")
        logger.info("="*70)
        
        # Get all CSV files
        csv_files = sorted(self.processed_dir.glob("*.csv"))
        
        if not csv_files:
            logger.error("No CSV files found in processed directory")
            return self.stats
        
        logger.info(f"Found {len(csv_files)} CSV files to convert\n")
        
        # Convert each file
        for i, csv_file in enumerate(csv_files, 1):
            logger.info(f"[{i}/{len(csv_files)}] {csv_file.name}")
            self.convert_file(csv_file)
            print()  # Blank line between files
        
        # Print summary
        self.print_summary()
        
        return self.stats
    
    def print_summary(self):
        """Print conversion summary."""
        logger.info("\n" + "="*70)
        logger.info("CONVERSION SUMMARY")
        logger.info("="*70)
        
        total_csv_mb = self.stats['total_csv_size'] / (1024 * 1024)
        total_parquet_mb = self.stats['total_parquet_size'] / (1024 * 1024)
        
        if self.stats['total_csv_size'] > 0:
            overall_compression = (1 - self.stats['total_parquet_size'] / self.stats['total_csv_size']) * 100
        else:
            overall_compression = 0
        
        print(f"\nFiles Converted: {self.stats['files_converted']}")
        print(f"Files Failed: {self.stats['files_failed']}")
        print(f"\nTotal CSV Size: {total_csv_mb:.2f} MB")
        print(f"Total Parquet Size: {total_parquet_mb:.2f} MB")
        print(f"Overall Compression: {overall_compression:.1f}% reduction")
        print(f"\nParquet files saved to: {self.parquet_dir}")
        
        logger.info("="*70 + "\n")
    
    def validate_parquet(self, parquet_path: Path) -> bool:
        """
        Validate a Parquet file.
        
        Args:
            parquet_path: Path to Parquet file
            
        Returns:
            True if valid, False otherwise
        """
        try:
            # Read Parquet file
            table = pq.read_table(parquet_path)
            
            logger.info(f"Validating {parquet_path.name}")
            logger.info(f"  Rows: {table.num_rows:,}")
            logger.info(f"  Columns: {table.num_columns}")
            logger.info(f"  Schema: {table.schema}")
            
            # Check for valid data
            if table.num_rows == 0:
                logger.warning("  WARNING: Parquet file has 0 rows")
                return False
            
            logger.info("  Validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Validation failed for {parquet_path.name}: {e}")
            return False
    
    def validate_all(self) -> bool:
        """
        Validate all Parquet files.
        
        Returns:
            True if all files valid, False otherwise
        """
        parquet_files = sorted(self.parquet_dir.glob("*.parquet"))
        
        if not parquet_files:
            logger.error("No Parquet files found")
            return False
        
        logger.info(f"Validating {len(parquet_files)} Parquet files\n")
        
        all_valid = True
        for parquet_file in parquet_files:
            if not self.validate_parquet(parquet_file):
                all_valid = False
            print()
        
        if all_valid:
            logger.info("All Parquet files validated successfully")
        else:
            logger.error("Some Parquet files failed validation")
        
        return all_valid


def main():
    """Main entry point for CSV to Parquet conversion."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Convert CSV files to Parquet')
    parser.add_argument('--validate', action='store_true', help='Validate Parquet files')
    parser.add_argument('--config', type=str, help='Path to config file')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
    )
    
    try:
        converter = CSVToParquetConverter(config_path=args.config)
        
        if args.validate:
            # Just validate existing Parquet files
            success = converter.validate_all()
            sys.exit(0 if success else 1)
        else:
            # Convert CSV to Parquet
            stats = converter.convert_all()
            sys.exit(0 if stats['files_failed'] == 0 else 1)
            
    except Exception as e:
        logger.error(f"Conversion failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
