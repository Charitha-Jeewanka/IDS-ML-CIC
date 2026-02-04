"""
Data Cleaner Module

Handles data cleaning operations including Infinity/NaN handling and column normalization.
"""

import logging
import re
from typing import Dict, Any
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class DataCleaner:
    """
    Clean data by handling Infinity values, NaN values, and normalizing column names.
    
    Attributes:
        infinity_strategy (str): Strategy for handling Infinity values
        missing_strategy (str): Strategy for handling missing values
        large_int_value (int): Value to use when infinity_strategy is 'large_int'
        stats (dict): Statistics about cleaning operations
    """
    
    def __init__(
        self,
        infinity_strategy: str = "max",
        missing_strategy: str = "drop",
        large_int_value: int = 1000000000,
        column_config: Dict[str, Any] = None
    ):
        """
        Initialize DataCleaner.
        
        Args:
            infinity_strategy: How to handle Inf ('max', 'large_int', 'drop')
            missing_strategy: How to handle NaN ('drop', 'median', 'mean', 'forward_fill')
            large_int_value: Value to use when infinity_strategy is 'large_int'
            column_config: Configuration for column name cleaning
        """
        self.infinity_strategy = infinity_strategy
        self.missing_strategy = missing_strategy
        self.large_int_value = large_int_value
        self.column_config = column_config or {
            'strip_whitespace': True,
            'lowercase': False,
            'replace_spaces': '_',
            'remove_special_chars': False
        }
        self.stats = {}
        
        logger.info(f"DataCleaner initialized (Inf: {infinity_strategy}, NaN: {missing_strategy})")
    
    def clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and normalize column names.
        
        Args:
            df: Input dataframe
            
        Returns:
            Dataframe with cleaned column names
        """
        original_columns = df.columns.tolist()
        new_columns = []
        
        for col in original_columns:
            new_col = col
            
            # Strip whitespace
            if self.column_config.get('strip_whitespace', True):
                new_col = new_col.strip()
            
            # Replace spaces
            if self.column_config.get('replace_spaces'):
                new_col = new_col.replace(' ', self.column_config['replace_spaces'])
            
            # Convert to lowercase
            if self.column_config.get('lowercase', False):
                new_col = new_col.lower()
            
            # Remove special characters (keep alphanumeric and underscores)
            if self.column_config.get('remove_special_chars', False):
                new_col = re.sub(r'[^a-zA-Z0-9_]', '', new_col)
            
            new_columns.append(new_col)
        
        # Rename columns
        df.columns = new_columns
        
        changed = sum(1 for old, new in zip(original_columns, new_columns) if old != new)
        logger.info(f"Column names cleaned: {changed}/{len(original_columns)} columns modified")
        self.stats['columns_renamed'] = changed
        
        return df
    
    def handle_infinity(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle Infinity values in pandas DataFrame.
        
        Args:
            df: Input pandas DataFrame
            
        Returns:
            DataFrame with Infinity values handled
        """
        inf_count = np.isinf(df.select_dtypes(include=[np.number]).values).sum()
        
        if inf_count == 0:
            logger.info("No Infinity values found")
            self.stats['infinity_values'] = 0
            return df
        
        logger.info(f"Found {inf_count} Infinity values")
        
        if self.infinity_strategy == "drop":
            original_len = len(df)
            df = df.replace([np.inf, -np.inf], np.nan).dropna()
            dropped = original_len - len(df)
            logger.info(f"Dropped {dropped} rows containing Infinity")
            self.stats['rows_dropped_inf'] = dropped
            
        elif self.infinity_strategy == "max":
            for col in df.select_dtypes(include=[np.number]).columns:
                if np.isinf(df[col]).any():
                    # Get max of finite values
                    finite_values = df[col][np.isfinite(df[col])]
                    if len(finite_values) > 0:
                        col_max = finite_values.max()
                        df[col] = df[col].replace([np.inf, -np.inf], col_max)
                    else:
                        # If all values are inf, use large_int_value
                        df[col] = df[col].replace([np.inf, -np.inf], self.large_int_value)
            logger.info(f"Replaced Infinity values with column maximums")
            
        elif self.infinity_strategy == "large_int":
            df = df.replace([np.inf, -np.inf], self.large_int_value)
            logger.info(f"Replaced Infinity values with {self.large_int_value}")
        
        self.stats['infinity_values'] = inf_count
        return df
    
    def handle_missing(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing (NaN) values in pandas DataFrame.
        
        Args:
            df: Input pandas DataFrame
            
        Returns:
            DataFrame with missing values handled
        """
        missing_count = df.isna().sum().sum()
        
        if missing_count == 0:
            logger.info("No missing values found")
            self.stats['missing_values'] = 0
            return df
        
        logger.info(f"Found {missing_count} missing values")
        
        if self.missing_strategy == "drop":
            original_len = len(df)
            df = df.dropna()
            dropped = original_len - len(df)
            logger.info(f"Dropped {dropped} rows with missing values")
            self.stats['rows_dropped_nan'] = dropped
            
        elif self.missing_strategy == "median":
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())
            logger.info("Filled missing values with median")
            
        elif self.missing_strategy == "mean":
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean())
            logger.info("Filled missing values with mean")
            
        elif self.missing_strategy == "forward_fill":
            df = df.fillna(method='ffill')
            logger.info("Forward filled missing values")
        
        self.stats['missing_values'] = missing_count
        return df
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all cleaning operations to a dataframe.
        
        Args:
            df: Input dataframe
            
        Returns:
            Cleaned dataframe
        """
        original_rows = len(df)
        
        logger.info(f"Starting data cleaning on {original_rows:,} rows")
        self.stats = {'original_rows': original_rows}
        
        # Clean column names
        df = self.clean_column_names(df)
        
        # Handle infinity and missing values
        df = self.handle_infinity(df)
        df = self.handle_missing(df)
        
        final_rows = len(df)
        self.stats['final_rows'] = final_rows
        self.stats['total_rows_dropped'] = original_rows - final_rows
        
        logger.info(f"Cleaning complete: {final_rows:,} rows ({original_rows - final_rows:,} dropped)")
        
        return df
    
    def get_cleaning_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the cleaning operations.
        
        Returns:
            Dictionary with cleaning statistics
        """
        return self.stats.copy()
