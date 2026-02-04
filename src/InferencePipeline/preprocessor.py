"""
Data Preprocessing Module

Handles:
- Loading Parquet files with temporal split
- Scaling/normalization
- Class imbalance handling (SMOTE)
- Data preparation for training
"""

import sys
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Tuple, List, Optional
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler
from imblearn.over_sampling import SMOTE

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.config_loader import ConfigLoader
from utils.logger import get_logger

logger = get_logger(__name__)


class DataPreprocessor:
    """
    Data preprocessing for IDS model training.
    
    Loads Parquet files, applies scaling, handles class imbalance.
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize preprocessor.
        
        Args:
            config_path: Path to config file (optional)
        """
        self.config_loader = ConfigLoader(config_path)
        self.config_loader.load_config()
        
        ml_config = self.config_loader.get_config().get('ml', {})
        preproc_config = ml_config.get('preprocessing', {})
        imbalance_config = ml_config.get('class_imbalance', {})
        
        self.data_dir = Path(self.config_loader.resolve_path(ml_config.get('data_dir', 'data/parquet')))
        self.train_files = ml_config.get('train_files', [])
        self.test_files = ml_config.get('test_files', [])
        self.target_column = ml_config.get('target_column', 'Label')
        self.features_to_drop = ml_config.get('features_to_drop', [])
        
        self.scaling_method = preproc_config.get('scaling_method', 'standard')
        self.handle_inf = preproc_config.get('handle_inf', 'max')
        self.handle_nan = preproc_config.get('handle_nan', 'drop')
        
        self.use_smote = imbalance_config.get('use_smote', True)
        self.smote_strategy = imbalance_config.get('smote_strategy', 'auto')
        self.smote_k_neighbors = imbalance_config.get('smote_k_neighbors', 5)
        self.random_state = imbalance_config.get('random_state', 42)
        
        self.scaler = None
        self.smote = None
        
        logger.info("DataPreprocessor initialized")
        logger.info(f"Data directory: {self.data_dir}")
        logger.info(f"Scaling method: {self.scaling_method}")
        logger.info(f"SMOTE enabled: {self.use_smote}")
    
    def load_parquet_files(
        self, 
        file_list: List[str],
        selected_features: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Load and concatenate Parquet files.
        
        Args:
            file_list: List of Parquet filenames
            selected_features: Optional list of features to load (plus target)
            
        Returns:
            Concatenated DataFrame
        """
        logger.info(f"Loading {len(file_list)} Parquet files...")
        
        dfs = []
        total_rows = 0
        
        for filename in file_list:
            file_path = self.data_dir / filename
            
            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                raise FileNotFoundError(f"File not found: {file_path}")
            
            logger.info(f"  Loading: {filename}")
            
            # Load Parquet
            if selected_features:
                # Load only selected columns + target
                cols_to_load = selected_features + [self.target_column]
                df = pd.read_parquet(file_path, columns=cols_to_load)
            else:
                df = pd.read_parquet(file_path)
            
            logger.info(f"    Rows: {len(df):,}, Columns: {len(df.columns)}")
            
            dfs.append(df)
            total_rows += len(df)
        
        # Concatenate
        logger.info(f"Concatenating {len(dfs)} DataFrames...")
        combined_df = pd.concat(dfs, ignore_index=True)
        
        logger.info(f"Total rows loaded: {total_rows:,}")
        
        return combined_df
    
    def load_temporal_split(
        self,
        selected_features: Optional[List[str]] = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load data with temporal split (Mon-Thu train, Fri test).
        
        Args:
            selected_features: Optional list of features to load
            
        Returns:
            Tuple of (train_df, test_df)
        """
        logger.info("\n" + "="*70)
        logger.info("LOADING DATA WITH TEMPORAL SPLIT")
        logger.info("="*70)
        
        # Load training data (Monday-Thursday)
        logger.info("\nTraining Set (Monday-Thursday):")
        train_df = self.load_parquet_files(self.train_files, selected_features)
        
        # Load test data (Friday)
        logger.info("\nTest Set (Friday):")
        test_df = self.load_parquet_files(self.test_files, selected_features)
        
        logger.info("\n" + "="*70)
        logger.info(f"Training samples: {len(train_df):,}")
        logger.info(f"Test samples: {len(test_df):,}")
        logger.info("="*70 + "\n")
        
        return train_df, test_df
    
    def handle_infinite_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle infinite values in DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with infinities handled
        """
        # Count infinities
        inf_count = np.isinf(df.select_dtypes(include=[np.number])).sum().sum()
        
        if inf_count > 0:
            logger.info(f"Handling {inf_count} infinite values...")
            
            if self.handle_inf == 'max':
                # Replace with column max (excluding inf)
                for col in df.select_dtypes(include=[np.number]).columns:
                    mask = np.isinf(df[col])
                    if mask.any():
                        finite_max = df[col][~mask].max()
                        df.loc[mask, col] = finite_max
            elif self.handle_inf == 'drop':
                df = df.replace([np.inf, -np.inf], np.nan)
        
        return df
    
    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing (NaN) values.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with missing values handled
        """
        nan_count = df.isna().sum().sum()
        
        if nan_count > 0:
            logger.info(f"Handling {nan_count} missing values...")
            
            if self.handle_nan == 'drop':
                before = len(df)
                df = df.dropna()
                after = len(df)
                logger.info(f"  Dropped {before - after} rows with NaN")
            elif self.handle_nan == 'median':
                df = df.fillna(df.median(numeric_only=True))
            elif self.handle_nan == 'mean':
                df = df.fillna(df.mean(numeric_only=True))
        
        return df
    
    def prepare_features_target(
        self,
        df: pd.DataFrame,
        feature_columns: Optional[List[str]] = None
    ) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Separate features and target.
        
        Args:
            df: Input DataFrame
            feature_columns: Optional list of feature column names
            
        Returns:
            Tuple of (X, y)
        """
        # Get feature columns
        if feature_columns is None:
            # Use all columns except target and features_to_drop
            feature_columns = [col for col in df.columns 
                             if col != self.target_column and col not in self.features_to_drop]
        
        X = df[feature_columns].copy()
        y = df[self.target_column].copy()
        
        logger.info(f"Features: {len(feature_columns)}, Samples: {len(df)}")
        
        return X, y
    
    def apply_scaling(self, X_train: pd.DataFrame, X_test: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Apply feature scaling.
        
        Args:
            X_train: Training features
            X_test: Test features
            
        Returns:
            Tuple of (X_train_scaled, X_test_scaled)
        """
        logger.info(f"\nApplying {self.scaling_method} scaling...")
        
        # Initialize scaler
        if self.scaling_method == 'standard':
            self.scaler = StandardScaler()
        elif self.scaling_method == 'minmax':
            self.scaler = MinMaxScaler()
        elif self.scaling_method == 'robust':
            self.scaler = RobustScaler()
        else:
            logger.warning(f"Unknown scaling method: {self.scaling_method}, using StandardScaler")
            self.scaler = StandardScaler()
        
        # Fit on training data only
        X_train_scaled = pd.DataFrame(
            self.scaler.fit_transform(X_train),
            columns=X_train.columns,
            index=X_train.index
        )
        
        # Transform test data
        X_test_scaled = pd.DataFrame(
            self.scaler.transform(X_test),
            columns=X_test.columns,
            index=X_test.index
        )
        
        logger.info("  Scaling complete")
        
        return X_train_scaled, X_test_scaled
    
    def apply_smote(self, X: pd.DataFrame, y: pd.Series) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Apply SMOTE for class imbalance.
        
        Args:
            X: Features
            y: Target
            
        Returns:
            Tuple of (X_resampled, y_resampled)
        """
        if not self.use_smote:
            logger.info("SMOTE disabled, skipping...")
            return X, y
        
        logger.info(f"\nApplying SMOTE (strategy={self.smote_strategy})...")
        
        # Show class distribution before
        logger.info("Class distribution BEFORE SMOTE:")
        for label, count in y.value_counts().items():
            logger.info(f"  {label}: {count:,} ({count/len(y)*100:.1f}%)")
        
        # Apply SMOTE
        self.smote = SMOTE(
            sampling_strategy=self.smote_strategy,
            k_neighbors=self.smote_k_neighbors,
            random_state=self.random_state,
            n_jobs=-1
        )
        
        X_resampled, y_resampled = self.smote.fit_resample(X, y)
        
        # Convert back to DataFrame/Series
        X_resampled = pd.DataFrame(X_resampled, columns=X.columns)
        y_resampled = pd.Series(y_resampled, name=y.name)
        
        # Show class distribution after
        logger.info("\nClass distribution AFTER SMOTE:")
        for label, count in y_resampled.value_counts().items():
            logger.info(f"  {label}: {count:,} ({count/len(y_resampled)*100:.1f}%)")
        
        logger.info(f"\nTotal samples: {len(y):,} → {len(y_resampled):,}")
        
        return X_resampled, y_resampled


def main():
    """Test preprocessor."""
    logging.basicConfig(level=logging.INFO)
    
    print("DataPreprocessor - Test requires Parquet files")
    print("Use run_feature_selection.py or run_training.py for full pipeline")


if __name__ == "__main__":
    main()
