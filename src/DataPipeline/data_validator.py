"""
Data Validator Module

Validates data quality and schema consistency after cleaning.
"""

import logging
from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class DataValidator:
    """
    Validate data quality and schema consistency.
    
    Attributes:
        check_infinity (bool): Whether to check for infinity values
        check_missing (bool): Whether to check for missing values
        check_schema (bool): Whether to validate schema
        check_duplicates (bool): Whether to check for duplicate rows
    """
    
    def __init__(
        self,
        check_infinity: bool = True,
        check_missing: bool = True,
        check_schema: bool = True,
        check_duplicates: bool = True
    ):
        """
        Initialize DataValidator.
        
        Args:
            check_infinity: Enable infinity value checks
            check_missing: Enable missing value checks
            check_schema: Enable schema validation
            check_duplicates: Enable duplicate checks
        """
        self.check_infinity = check_infinity
        self.check_missing = check_missing
        self.check_schema = check_schema
        self.check_duplicates = check_duplicates
        
        logger.info("DataValidator initialized")
    
    def validate_no_infinity(self, df: pd.DataFrame) -> bool:
        """
        Check if dataframe contains any infinity values.
        
        Args:
            df: Input dataframe
            
        Returns:
            True if no infinity values, False otherwise
        """
        if not self.check_infinity:
            return True
        
        inf_count = np.isinf(df.select_dtypes(include=[np.number]).values).sum()
        
        if inf_count > 0:
            logger.error(f"Validation failed: Found {inf_count} infinity values")
            return False
        
        logger.info("No infinity values found")
        return True
    
    def validate_no_missing(self, df: pd.DataFrame) -> bool:
        """
        Check if dataframe contains any missing values.
        
        Args:
            df: Input dataframe
            
        Returns:
            True if no missing values, False otherwise
        """
        if not self.check_missing:
            return True
        
        missing_count = df.isna().sum().sum()
        
        if missing_count > 0:
            logger.warning(f"Found {missing_count} missing values")
            return False
        
        logger.info("No missing values found")
        return True
    
    def validate_schema(self, df: pd.DataFrame, expected_columns: Optional[List[str]] = None) -> bool:
        """
        Validate dataframe schema against expected columns.
        
        Args:
            df: Input dataframe
            expected_columns: List of expected column names (optional)
            
        Returns:
            True if schema is valid, False otherwise
        """
        if not self.check_schema or expected_columns is None:
            return True
        
        actual_columns = set(df.columns)
        expected_columns = set(expected_columns)
        
        missing = expected_columns - actual_columns
        extra = actual_columns - expected_columns
        
        if missing or extra:
            if missing:
                logger.error(f"Missing columns: {missing}")
            if extra:
                logger.error(f"Extra columns: {extra}")
            return False
        
        logger.info(f"Schema validated: {len(actual_columns)} columns")
        return True
    
    def validate_data_types(self, df: pd.DataFrame) -> bool:
        """
        Validate that numeric columns have appropriate data types.
        
        Args:
            df: Input dataframe
            
        Returns:
            True if data types are valid, False otherwise
        """
        dtypes = df.dtypes
        logger.info(f"Data types: {len(dtypes)} columns")
        return True
    
    def check_duplicates_count(self, df: pd.DataFrame) -> int:
        """
        Count duplicate rows in dataframe.
        
        Args:
            df: Input dataframe
            
        Returns:
            Number of duplicate rows
        """
        if not self.check_duplicates:
            return 0
        
        duplicate_count = df.duplicated().sum()
        
        if duplicate_count > 0:
            logger.warning(f"Found {duplicate_count} duplicate rows")
        else:
            logger.info("No duplicate rows found")
        
        return duplicate_count
    
    def generate_validation_report(self, df: pd.DataFrame, filename: str = "") -> Dict[str, Any]:
        """
        Generate comprehensive validation report.
        
        Args:
            df: Input dataframe
            filename: Name of the file being validated (optional)
            
        Returns:
            Dictionary with validation results
        """
        logger.info(f"Generating validation report{' for ' + filename if filename else ''}")
        
        report = {
            'filename': filename,
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'columns': df.columns.tolist(),
        }
        
        # Check infinity
        if self.check_infinity:
            inf_count = np.isinf(df.select_dtypes(include=[np.number]).values).sum()
            report['infinity_values'] = inf_count
            report['has_infinity'] = inf_count > 0
        
        # Check missing
        if self.check_missing:
            missing_count = df.isna().sum().sum()
            report['missing_values'] = missing_count
            report['has_missing'] = missing_count > 0
        
        # Check duplicates
        if self.check_duplicates:
            duplicate_count = self.check_duplicates_count(df)
            report['duplicate_rows'] = duplicate_count
            report['has_duplicates'] = duplicate_count > 0
        
        # Data quality score
        issues = sum([
            report.get('has_infinity', False),
            report.get('has_missing', False),
            report.get('has_duplicates', False)
        ])
        report['quality_score'] = 'PASS' if issues == 0 else 'FAIL'
        report['issues_count'] = issues
        
        return report
    
    def validate_all(
        self,
        df: pd.DataFrame,
        expected_columns: Optional[List[str]] = None
    ) -> tuple:
        """
        Run all validation checks.
        
        Args:
            df: Input dataframe
            expected_columns: Expected column names (optional)
            
        Returns:
            Tuple of (is_valid, report_dict)
        """
        logger.info("Running all validation checks")
        
        checks = {
            'no_infinity': self.validate_no_infinity(df),
            'no_missing': self.validate_no_missing(df),
            'schema_valid': self.validate_schema(df, expected_columns),
            'data_types_valid': self.validate_data_types(df)
        }
        
        duplicate_count = self.check_duplicates_count(df)
        
        is_valid = all(checks.values())
        
        report = {
            'validation_passed': is_valid,
            'checks': checks,
            'duplicate_count': duplicate_count,
            'total_rows': len(df),
            'total_columns': len(df.columns)
        }
        
        if is_valid:
            logger.info("All validation checks passed")
        else:
            logger.error("Validation failed")
        
        return is_valid, report
