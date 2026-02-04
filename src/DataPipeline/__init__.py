"""
DataPipeline Package

Pre-Kafka ETL pipeline for cleaning CICIDS 2017 dataset.
Handles Infinity values, NaN values, and column name normalization.
"""

__version__ = "0.1.0"
__author__ = "IDS-ML-CIC Project"

from .data_reader import DataReader
from .data_cleaner import DataCleaner
from .data_validator import DataValidator
from .etl_pipeline import ETLPipeline

__all__ = [
    "DataReader",
    "DataCleaner",
    "DataValidator",
    "ETLPipeline",
]
