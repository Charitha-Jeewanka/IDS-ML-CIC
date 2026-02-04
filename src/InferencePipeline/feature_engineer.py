"""
Feature Engineering Module

Performs feature selection through:
- Correlation analysis
- Variance thresholding  
- Domain-driven selection
- Preliminary importance ranking
"""

import sys
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Tuple, Any
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import VarianceThreshold

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.config_loader import ConfigLoader
from utils.logger import get_logger

logger = get_logger(__name__)


class FeatureEngineer:
    """
    Feature selection and engineering for IDS models.
    
    Removes redundant, low-variance, and identity-based features.
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize feature engineer.
        
        Args:
            config_path: Path to config file (optional)
        """
        self.config_loader = ConfigLoader(config_path)
        self.config_loader.load_config()
        
        ml_config = self.config_loader.get_config().get('ml', {})
        fe_config = ml_config.get('feature_engineering', {})
        
        self.target_column = ml_config.get('target_column', 'Label')
        self.features_to_drop = ml_config.get('features_to_drop', [])
        
        self.correlation_threshold = fe_config.get('correlation_threshold', 0.95)
        self.variance_threshold = fe_config.get('variance_threshold', 0.01)
        self.max_features = fe_config.get('max_features', 30)
        self.importance_method = fe_config.get('feature_importance_method', 'random_forest')
        
        self.selected_features = None
        self.feature_importance = None
        self.correlation_matrix = None
        
        logger.info("FeatureEngineer initialized")
        logger.info(f"Correlation threshold: {self.correlation_threshold}")
        logger.info(f"Variance threshold: {self.variance_threshold}")
        logger.info(f"Max features: {self.max_features}")
    
    def analyze_correlations(self, df: pd.DataFrame, output_dir: str = "artifacts/features") -> List[str]:
        """
        Analyze feature correlations and remove highly correlated features.
        
        Args:
            df: DataFrame with features
            output_dir: Directory to save correlation heatmap
            
        Returns:
            List of features to drop (highly correlated)
        """
        logger.info(f"\nAnalyzing correlations (threshold={self.correlation_threshold})...")
        
        # Get numeric columns only
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        # Remove target if present
        if self.target_column in numeric_cols:
            numeric_cols.remove(self.target_column)
        
        logger.info(f"  Numeric features: {len(numeric_cols)}")
        
        # Calculate correlation matrix
        self.correlation_matrix = df[numeric_cols].corr().abs()
        
        # Find highly correlated pairs
        upper_triangle = np.triu(np.ones(self.correlation_matrix.shape), k=1).astype(bool)
        upper_corr = self.correlation_matrix.where(upper_triangle)
        
        # Features to drop (keep first occurrence)
        to_drop = [col for col in upper_corr.columns if any(upper_corr[col] > self.correlation_threshold)]
        
        logger.info(f"  Highly correlated features to drop: {len(to_drop)}")
        
        # Save heatmap (top 30 features for readability)
        self._save_correlation_heatmap(
            self.correlation_matrix.iloc[:30, :30],
            output_dir
        )
        
        return to_drop
    
    def _save_correlation_heatmap(self, corr_matrix: pd.DataFrame, output_dir: str):
        """Save correlation heatmap."""
        try:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            
            plt.figure(figsize=(14, 12))
            sns.heatmap(
                corr_matrix,
                cmap='coolwarm',
                center=0,
                square=True,
                linewidths=0.5,
                cbar_kws={"shrink": 0.8},
                vmin=-1,
                vmax=1
            )
            plt.title('Feature Correlation Matrix (Top 30 Features)', fontsize=14, fontweight='bold')
            plt.tight_layout()
            
            save_path = output_path / 'correlation_matrix.png'
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"  Saved correlation heatmap: {save_path}")
            
        except Exception as e:
            logger.warning(f"Could not save correlation heatmap: {e}")
    
    def remove_low_variance_features(self, df: pd.DataFrame) -> List[str]:
        """
        Remove features with low variance (quasi-constant).
        
        Args:
            df: DataFrame with features
            
        Returns:
            List of low-variance features to drop
        """
        logger.info(f"\nRemoving low-variance features (threshold={self.variance_threshold})...")
        
        # Get numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if self.target_column in numeric_cols:
            numeric_cols.remove(self.target_column)
        
        # Apply variance threshold
        selector = VarianceThreshold(threshold=self.variance_threshold)
        selector.fit(df[numeric_cols])
        
        # Get low-variance features
        low_var_features = [col for col, var in zip(numeric_cols, selector.variances_) 
                           if var <= self.variance_threshold]
        
        logger.info(f"  Low-variance features: {len(low_var_features)}")
        
        return low_var_features
    
    def get_domain_features(self, all_features: List[str]) -> Tuple[List[str], List[str]]:
        """
        Separate behavior features from identity features.
        
        Args:
            all_features: List of all feature names
            
        Returns:
            Tuple of (behavior_features, identity_features)
        """
        logger.info("\nSeparating behavior vs identity features...")
        
        # Identity features (already in features_to_drop from config)
        identity_keywords = ['ip', 'port', 'timestamp', 'time', 'id', 'index']
        
        identity_features = []
        for feature in all_features:
            if any(keyword in feature.lower() for keyword in identity_keywords):
                identity_features.append(feature)
        
        # Behavior features (everything else)
        behavior_features = [f for f in all_features if f not in identity_features and f != self.target_column]
        
        logger.info(f"  Behavior features: {len(behavior_features)}")
        logger.info(f"  Identity features (to drop): {len(identity_features)}")
        
        return behavior_features, identity_features
    
    def rank_features_preliminary(
        self, 
        X: pd.DataFrame, 
        y: pd.Series,
        n_estimators: int = 100
    ) -> pd.DataFrame:
        """
        Preliminary feature importance ranking using Random Forest.
        
        Args:
            X: Feature DataFrame
            y: Target Series
            n_estimators: Number of trees
            
        Returns:
            DataFrame with feature importance scores
        """
        logger.info(f"\nRanking features using {self.importance_method}...")
        
        # Sample data for speed (use 100K rows if >100K)
        if len(X) > 100000:
            logger.info("  Sampling 100K rows for preliminary ranking...")
            sample_idx = np.random.choice(len(X), 100000, replace=False)
            X_sample = X.iloc[sample_idx]
            y_sample = y.iloc[sample_idx]
        else:
            X_sample = X
            y_sample = y
        
        # Train Random Forest
        logger.info(f"  Training Random Forest ({n_estimators} trees)...")
        rf = RandomForestClassifier(
            n_estimators=n_estimators,
            max_depth=10,
            random_state=42,
            n_jobs=-1,
            verbose=0
        )
        
        rf.fit(X_sample, y_sample)
        
        # Get feature importance
        self.feature_importance = pd.DataFrame({
            'feature': X.columns,
            'importance': rf.feature_importances_
        }).sort_values('importance', ascending=False).reset_index(drop=True)
        
        logger.info(f"  Top 10 features by importance:")
        for idx, row in self.feature_importance.head(10).iterrows():
            logger.info(f"    {idx+1}. {row['feature']}: {row['importance']:.4f}")
        
        return self.feature_importance
    
    def select_top_features(self, importance_df: pd.DataFrame = None) -> List[str]:
        """
        Select top N features based on importance.
        
        Args:
            importance_df: Feature importance DataFrame (uses self.feature_importance if None)
            
        Returns:
            List of top feature names
        """
        if importance_df is None:
            importance_df = self.feature_importance
        
        if importance_df is None:
            logger.error("No feature importance data available")
            return []
        
        top_features = importance_df.head(self.max_features)['feature'].tolist()
        
        logger.info(f"\nSelected top {len(top_features)} features")
        
        self.selected_features = top_features
        
        return top_features
    
    def save_feature_selection_report(self, output_dir: str = "artifacts/features"):
        """
        Save feature selection results.
        
        Args:
            output_dir: Output directory
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Save feature importance CSV
        if self.feature_importance is not None:
            csv_path = output_path / 'feature_importance.csv'
            self.feature_importance.to_csv(csv_path, index=False)
            logger.info(f"\nSaved feature importance: {csv_path}")
        
        # Save selected features JSON
        if self.selected_features is not None:
            import json
            json_path = output_path / 'selected_features.json'
            with open(json_path, 'w') as f:
                json.dump({
                    'selected_features': self.selected_features,
                    'count': len(self.selected_features)
                }, f, indent=2)
            logger.info(f"Saved selected features: {json_path}")
        
        # Save summary report
        report_path = output_path / 'feature_selection_report.txt'
        with open(report_path, 'w') as f:
            f.write("FEATURE SELECTION REPORT\n")
            f.write("="*70 + "\n\n")
            
            if self.selected_features:
                f.write(f"Total features selected: {len(self.selected_features)}\n\n")
                f.write("Top 20 Features:\n")
                for idx, feat in enumerate(self.selected_features[:20], 1):
                    importance = self.feature_importance[
                        self.feature_importance['feature'] == feat
                    ]['importance'].values[0]
                    f.write(f"{idx:2d}. {feat:50s} {importance:.4f}\n")
        
        logger.info(f"Saved feature selection report: {report_path}")


def main():
    """Test feature engineer."""
    logging.basicConfig(level=logging.INFO)
    
    print("FeatureEngineer - Test requires loaded Parquet data")
    print("Use run_feature_selection.py for full pipeline")


if __name__ == "__main__":
    main()
