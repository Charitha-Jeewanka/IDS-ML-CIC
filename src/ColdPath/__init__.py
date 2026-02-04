"""
ColdPath Package

Components for converting and storing data in Parquet format for ML training.
"""

__version__ = "0.1.0"

from .csv_to_parquet import CSVToParquetConverter

__all__ = ["CSVToParquetConverter"]
