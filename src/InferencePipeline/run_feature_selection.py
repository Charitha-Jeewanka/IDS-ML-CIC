"""
Feature Selection Pipeline Runner

CLI for running feature selection and analysis on CICIDS-2017 data.
"""

import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.InferencePipeline.feature_engineer import FeatureEngineer
from src.InferencePipeline.preprocessor import DataPreprocessor
from utils.logger import get_logger

# Setup logging
logger = get_logger(__name__, log_file='logs/feature_selection.log')


def main():
    """Run feature selection pipeline."""
    parser = argparse.ArgumentParser(
        description='Run feature selection on CICIDS-2017 data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full feature selection
  python run_feature_selection.py
  
  # Custom output directory
  python run_feature_selection.py --output artifacts/custom_features
  
  # Skip SMOTE during preliminary ranking
  python run_feature_selection.py --no-smote
        """
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default='artifacts/features',
        help='Output directory for feature selection results'
    )
    
    parser.add_argument(
        '--no-smote',
        action='store_true',
        help='Skip SMOTE during preliminary ranking (faster)'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        help='Path to custom config file'
    )
    
    args = parser.parse_args()
    
    try:
        logger.info("\n" + "="*70)
        logger.info("FEATURE SELECTION PIPELINE")
        logger.info("="*70)
        logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # Step 1: Load data
        logger.info("Step 1/5: Loading data...")
        preprocessor = DataPreprocessor(config_path=args.config)
        
        train_df, test_df = preprocessor.load_temporal_split()
        
        print()
        
        # Step 2: Clean data
        logger.info("Step 2/5: Cleaning data...")
        
        logger.info("Training set:")
        train_df = preprocessor.handle_infinite_values(train_df)
        train_df = preprocessor.handle_missing_values(train_df)
        
        logger.info("\nTest set:")
        test_df = preprocessor.handle_infinite_values(test_df)
        test_df = preprocessor.handle_missing_values(test_df)
        
        print()
        
        # Step 3: Feature engineering analysis
        logger.info("Step 3/5: Analyzing features...")
        engineer = FeatureEngineer(config_path=args.config)
        
        # 3a. Correlation analysis
        highly_correlated = engineer.analyze_correlations(train_df, output_dir=args.output)
        
        # 3b. Low variance features
        low_variance = engineer.remove_low_variance_features(train_df)
        
        # 3c. Domain-driven separation
        all_features = [col for col in train_df.columns if col != preprocessor.target_column]
        behavior_features, identity_features = engineer.get_domain_features(all_features)
        
        # Combine features to drop
        features_to_drop = set(
            highly_correlated + 
            low_variance + 
            identity_features +
            preprocessor.features_to_drop
        )
        
        # Get clean feature set
        clean_features = [f for f in all_features 
                         if f not in features_to_drop and f != preprocessor.target_column]
        
        logger.info(f"\nFeature filtering summary:")
        logger.info(f"  Original features: {len(all_features)}")
        logger.info(f"  Highly correlated: {len(highly_correlated)}")
        logger.info(f"  Low variance: {len(low_variance)}")
        logger.info(f"  Identity features: {len(identity_features)}")
        logger.info(f"  Clean features remaining: {len(clean_features)}")
        
        print()
        
        # Step 4: Preliminary importance ranking
        logger.info("Step 4/5: Ranking features by importance...")
        
        # Prepare data
        X_train, y_train = preprocessor.prepare_features_target(train_df, feature_columns=clean_features)
        
        # Use binary target for faster preliminary ranking
        y_binary = (y_train != 'BENIGN').astype(int)
        
        # Rank features
        importance_df = engineer.rank_features_preliminary(X_train, y_binary, n_estimators=100)
        
        # Select top features
        selected_features = engineer.select_top_features(importance_df)
        
        print()
        
        # Step 5: Save results
        logger.info("Step 5/5: Saving feature selection results...")
        engineer.save_feature_selection_report(output_dir=args.output)
        
        # Print summary
        logger.info("\n" + "="*70)
        logger.info("FEATURE SELECTION COMPLETE")
        logger.info("="*70)
        logger.info(f"\nSelected Features: {len(selected_features)}")
        logger.info(f"\nTop 10 Features:")
        for idx, feat in enumerate(selected_features[:10], 1):
            logger.info(f"  {idx}. {feat}")
        logger.info(f"\nResults saved to: {args.output}")
        logger.info("  - correlation_matrix.png")
        logger.info("  - feature_importance.csv")
        logger.info("  - selected_features.json")
        logger.info("  - feature_selection_report.txt")
        logger.info("\n" + "="*70 + "\n")
        
        sys.exit(0)
        
    except KeyboardInterrupt:
        logger.info("\nFeature selection interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Feature selection failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
