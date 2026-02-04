"""
Data Reader Module

Handles CSV file ingestion using pandas.
"""

import os
import logging
from pathlib import Path
from typing import List, Union, Optional
import pandas as pd

logger = logging.getLogger(__name__)


class DataReader:
    """
    Handle CSV file reading using pandas.
    
    Attributes:
        chunk_size (int): Chunk size for reading large files
        encoding (str): File encoding
    """
    
    def __init__(self, backend: str = "pandas", chunk_size: int = 100000, encoding: str = "utf-8"):
        """
        Initialize DataReader.
        
        Args:
            backend: Backend to use (only 'pandas' supported)
            chunk_size: Chunk size for large file processing
            encoding: File encoding
        """
        self.backend = "pandas"
        self.chunk_size = chunk_size
        self.encoding = encoding
        
        logger.info(f"DataReader initialized with pandas backend")
    
    def validate_file_exists(self, file_path: Union[str, Path]) -> bool:
        """
        Validate that a file exists.
        
        Args:
            file_path: Path to the file
            
        Returns:
            True if file exists, False otherwise
        """
        file_path = Path(file_path)
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return False
        if not file_path.is_file():
            logger.error(f"Path is not a file: {file_path}")
            return False
        return True
    
    def read_csv(self, file_path: Union[str, Path]) -> pd.DataFrame:
        """
        Read a single CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            pandas DataFrame
            
        Raises:
            FileNotFoundError: If file doesn't exist
            Exception: For other reading errors
        """
        file_path = Path(file_path)
        
        if not self.validate_file_exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        logger.info(f"Reading file: {file_path.name}")
        
        try:
            df = pd.read_csv(file_path, encoding=self.encoding)
            logger.info(f"Read {len(df):,} rows with {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"Error reading {file_path.name}: {e}")
            raise
    
    def get_csv_files(self, directory: Union[str, Path]) -> List[Path]:
        """
        Get all CSV files in a directory.
        
        Args:
            directory: Directory path
            
        Returns:
            List of CSV file paths
        """
        directory = Path(directory)
        
        if not directory.exists():
            logger.error(f"Directory not found: {directory}")
            return []
        
        csv_files = list(directory.glob("*.csv"))
        logger.info(f"Found {len(csv_files)} CSV files in {directory}")
        
        return sorted(csv_files)
    
    def read_all_csvs(self, directory: Union[str, Path]) -> List[tuple]:
        """
        Read all CSV files in a directory.
        
        Args:
            directory: Directory path
            
        Returns:
            List of tuples (filename, dataframe)
        """
        csv_files = self.get_csv_files(directory)
        
        if not csv_files:
            logger.warning(f"No CSV files found in {directory}")
            return []
        
        results = []
        for csv_file in csv_files:
            try:
                df = self.read_csv(csv_file)
                results.append((csv_file.name, df))
            except Exception as e:
                logger.error(f"Failed to read {csv_file.name}: {e}")
                continue
        
        logger.info(f"Successfully read {len(results)}/{len(csv_files)} files")
        return results
