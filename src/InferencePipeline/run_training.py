"""
H2O AutoML Training Pipeline Runner

End-to-end training pipeline with ML flow tracking and explainability.
"""

import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.InferencePipeline.h2o_config import H2OConfig
from src.InferencePipeline.data_loader import H2ODataLoader
from src.InferencePipeline.h2o_trainer import H2OAutoMLTrainer
from src.InferencePipeline.mlflow_tracker import MLflowTracker
from src.InferencePipeline.explainability import ModelExplainer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('logs/training.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Run end-to-end training pipeline."""
    parser = argparse.ArgumentParser(
        description='Train H2O AutoML models for intrusion detection',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Binary classification (BENIGN vs ATTACK) - 1 hour
  python run_training.py --mode binary --max-runtime 3600
  
  # Multi-class classification - 2 hours  
  python run_training.py --mode multiclass --max-runtime 7200
  
  # Quick test - 5 minutes
  python run_training.py --mode binary --max-runtime 300 --max-models 5
  
  # Custom experiment name
  python run_training.py --experiment "IDS-Binary-v2" --mode binary
        """
    )
    
    parser.add_argument(
        '--mode',
        type=str,
        choices=['binary', 'multiclass'],
        default='binary',
        help='Classification mode (default: binary)'
    )
    
    parser.add_argument(
        '--max-runtime',
        type=int,
        help='Max training time in seconds (default: from config)'
    )
    
    parser.add_argument(
        '--max-models',
        type=int,
        help='Max number of models to train (default: from config)'
    )
    
    parser.add_argument(
        '--experiment',
        type=str,
        help='MLflow experiment name (default: from config)'
    )
    
    parser.add_argument(
        '--no-explainability',
        action='store_true',
        help='Skip explainability report generation'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        help='Path to custom config file'
    )
    
    args = parser.parse_args()
    
    try:
        logger.info("\n" + "="*70)
        logger.info("H2O AUTOML TRAINING PIPELINE")
        logger.info("="*70)
        logger.info(f"\nMode: {args.mode.upper()}")
        logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # Step 1: Initialize H2O
        logger.info("Step 1/7: Initializing H2O cluster...")
        h2o_config = H2OConfig(config_path=args.config)
        h2o_config.initialize()
        
        if not h2o_config.health_check():
            logger.error("H2O cluster health check failed")
            sys.exit(1)
        
        print()
        
        # Step 2: Load data
        logger.info("Step 2/7: Loading data with temporal split...")
        data_loader = H2ODataLoader(config_path=args.config)
        train_frame, test_frame = data_loader.load_data()
        
        print()
        
        # Step 3: Setup MLflow
        logger.info("Step 3/7: Setting up MLflow tracking...")
        mlflow_tracker = MLflowTracker(config_path=args.config)
        
        run_name = f"{args.mode}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        mlflow_tracker.start_run(
            run_name=run_name,
            tags={'mode': args.mode, 'type': 'automl'}
        )
        
        # Log parameters
        mlflow_tracker.log_params({
            'mode': args.mode,
            'max_runtime_secs': args.max_runtime or 'config',
            'max_models': args.max_models or 'config',
            'train_rows': int(train_frame.nrow),
            'test_rows': int(test_frame.nrow)
        })
        
        print()
        
        # Step 4: Train AutoML
        logger.info("Step 4/7: Training AutoML models...")
        trainer = H2OAutoMLTrainer(train_frame, test_frame, config_path=args.config)
        
        trainer.train(
            mode=args.mode,
            max_runtime_secs=args.max_runtime,
            max_models=args.max_models
        )
        
        # Get leaderboard
        leaderboard = trainer.get_leaderboard(n=10)
        
        print()
        
        # Step 5: Evaluate best model
        logger.info("Step 5/7: Evaluating best model...")
        metrics = trainer.evaluate()
        
        # Log metrics to MLflow
        mlflow_tracker.log_metrics(metrics)
        
        print()
        
        # Step 6: Generate explainability reports
        if not args.no_explainability:
            logger.info("Step 6/7: Generating explainability reports...")
            
            explainer = ModelExplainer(
                model=trainer.best_model,
                test_frame=test_frame,
                output_dir=f"artifacts/explainability/{run_name}"
            )
            
            reports = explainer.generate_all_reports()
            
            # Log artifacts to MLflow
            if reports:
                for report_name, report_path in reports.items():
                    mlflow_tracker.log_artifact(report_path, artifact_path="explainability")
        else:
            logger.info("Step 6/7: Skipping explainability (--no-explainability)")
        
        print()
        
        # Step 7: Save model
        logger.info("Step 7/7: Saving model...")
        
        # Save to disk
        model_path = trainer.save_model(
            path=f"models/h2o/{run_name}"
        )
        
        # Log model to MLflow
        mlflow_tracker.log_h2o_model(trainer.best_model, artifact_path="model")
        
        # Get training summary
        summary = trainer.get_training_summary()
        
        logger.info("\n" + "="*70)
        logger.info("TRAINING COMPLETE!")
        logger.info("="*70)
        logger.info(f"\nBest Model: {summary['best_model_id']}")
        logger.info(f"Total Models: {summary['total_models']}")
        logger.info(f"\nTest Performance:")
        for metric, value in metrics.items():
            logger.info(f"  {metric.upper()}: {value:.4f}")
        logger.info(f"\nModel saved to: {model_path}")
        logger.info(f"\nMLflow Run: {run_name}")
        logger.info(f"View results: mlflow ui")
        logger.info("="*70 + "\n")
        
        # End MLflow run
        mlflow_tracker.end_run(status="FINISHED")
        
        # Shutdown H2O
        logger.info("Shutting down H2O cluster...")
        h2o_config.shutdown()
        
        logger.info("\nAll done! 🎉\n")
        
        sys.exit(0)
        
    except KeyboardInterrupt:
        logger.info("\nTraining interrupted by user")
        if 'mlflow_tracker' in locals():
            mlflow_tracker.end_run(status="KILLED")
        if 'h2o_config' in locals():
            h2o_config.shutdown()
        sys.exit(1)
    except Exception as e:
        logger.error(f"Training failed: {e}", exc_info=True)
        if 'mlflow_tracker' in locals():
            mlflow_tracker.end_run(status="FAILED")
        if 'h2o_config' in locals():
            h2o_config.shutdown()
        sys.exit(1)


if __name__ == "__main__":
    main()
