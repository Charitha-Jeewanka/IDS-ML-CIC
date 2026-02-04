"""
MLflow Experiment Tracking Module

Handles MLflow integration for H2O AutoML experiments.
"""

import sys
import logging
import mlflow
import mlflow.sklearn
from pathlib import Path
from typing import Dict, Any, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class MLflowTracker:
    """
    MLflow experiment tracking for H2O AutoML.
    
    Tracks experiments, metrics, parameters, and artifacts.
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize MLflow tracker.
        
        Args:
            config_path: Path to config file (optional)
        """
        self.config_loader = ConfigLoader(config_path)
        self.config_loader.load_config()
        
        mlflow_config = self.config_loader.get_config().get('mlflow', {})
        
        self.tracking_uri = mlflow_config.get('tracking_uri', 'file:./mlruns')
        self.experiment_name = mlflow_config.get('experiment_name', 'CICIDS-IDS-AutoML')
        self.artifact_location = mlflow_config.get('artifact_location', 'artifacts/mlflow')
        
        self.autolog_config = mlflow_config.get('autolog', {})
        
        self.active_run = None
        self.experiment_id = None
        
        # Setup MLflow
        self._setup_mlflow()
        
        logger.info("MLflowTracker initialized")
        logger.info(f"Tracking URI: {self.tracking_uri}")
        logger.info(f"Experiment: {self.experiment_name}")
    
    def _setup_mlflow(self):
        """Setup MLflow tracking."""
        try:
            # Set tracking URI
            mlflow.set_tracking_uri(self.tracking_uri)
            
            # Create or get experiment
            experiment = mlflow.get_experiment_by_name(self.experiment_name)
            
            if experiment is None:
                self.experiment_id = mlflow.create_experiment(
                    name=self.experiment_name,
                    artifact_location=self.artifact_location
                )
                logger.info(f"Created MLflow experiment: {self.experiment_name}")
            else:
                self.experiment_id = experiment.experiment_id
                logger.info(f"Using existing MLflow experiment: {self.experiment_name}")
            
            # Set experiment
            mlflow.set_experiment(self.experiment_name)
            
            # Configure autologging if enabled
            if self.autolog_config.get('enabled', True):
                mlflow.autolog(
                    log_models=self.autolog_config.get('log_models', True),
                    log_input_examples=self.autolog_config.get('log_input_examples', True),
                    log_model_signatures=self.autolog_config.get('log_model_signatures', True),
                    disable=False
                )
                logger.info("MLflow autologging enabled")
                
        except Exception as e:
            logger.error(f"Failed to setup MLflow: {e}")
            raise
    
    def start_run(self, run_name: str = None, tags: Dict[str, str] = None):
        """
        Start MLflow run.
        
        Args:
            run_name: Name for the run
            tags: Tags to add to the run
        """
        try:
            self.active_run = mlflow.start_run(
                run_name=run_name,
                tags=tags
            )
            
            logger.info(f"Started MLflow run: {run_name or 'unnamed'}")
            logger.info(f"Run ID: {self.active_run.info.run_id}")
            
            return self.active_run
            
        except Exception as e:
            logger.error(f"Failed to start run: {e}")
            raise
    
    def log_params(self, params: Dict[str, Any]):
        """
        Log parameters to MLflow.
        
        Args:
            params: Dictionary of parameters
        """
        try:
            mlflow.log_params(params)
            logger.debug(f"Logged {len(params)} parameters")
            
        except Exception as e:
            logger.error(f"Failed to log parameters: {e}")
    
    def log_metrics(self, metrics: Dict[str, float], step: int = None):
        """
        Log metrics to MLflow.
        
        Args:
            metrics: Dictionary of metrics
            step: Step number (optional)
        """
        try:
            mlflow.log_metrics(metrics, step=step)
            logger.info(f"Logged metrics: {metrics}")
            
        except Exception as e:
            logger.error(f"Failed to log metrics: {e}")
    
    def log_metric(self, key: str, value: float, step: int = None):
        """
        Log single metric to MLflow.
        
        Args:
            key: Metric name
            value: Metric value
            step: Step number (optional)
        """
        try:
            mlflow.log_metric(key, value, step=step)
            logger.debug(f"Logged metric: {key}={value}")
            
        except Exception as e:
            logger.error(f"Failed to log metric: {e}")
    
    def log_h2o_model(self, model, artifact_path: str = "model"):
        """
        Log H2O model to MLflow.
        
        Args:
            model: H2O model object
            artifact_path: Path within artifacts directory
        """
        try:
            mlflow.h2o.log_model(
                h2o_model=model,
                artifact_path=artifact_path
            )
            logger.info(f"Logged H2O model to: {artifact_path}")
            
        except Exception as e:
            logger.error(f"Failed to log model: {e}")
    
    def log_artifact(self, local_path: str, artifact_path: str = None):
        """
        Log artifact file to MLflow.
        
        Args:
            local_path: Path to local file
            artifact_path: Path within artifacts directory
        """
        try:
            mlflow.log_artifact(local_path, artifact_path=artifact_path)
            logger.debug(f"Logged artifact: {local_path}")
            
        except Exception as e:
            logger.error(f"Failed to log artifact: {e}")
    
    def log_artifacts(self, local_dir: str, artifact_path: str = None):
        """
        Log entire directory of artifacts to MLflow.
        
        Args:
            local_dir: Path to local directory
            artifact_path: Path within artifacts directory
        """
        try:
            mlflow.log_artifacts(local_dir, artifact_path=artifact_path)
            logger.info(f"Logged artifacts from: {local_dir}")
            
        except Exception as e:
            logger.error(f"Failed to log artifacts: {e}")
    
    def set_tags(self, tags: Dict[str, str]):
        """
        Set tags for current run.
        
        Args:
            tags: Dictionary of tags
        """
        try:
            mlflow.set_tags(tags)
            logger.debug(f"Set {len(tags)} tags")
            
        except Exception as e:
            logger.error(f"Failed to set tags: {e}")
    
    def end_run(self, status: str = "FINISHED"):
        """
        End MLflow run.
        
        Args:
            status: Run status (FINISHED, FAILED, KILLED)
        """
        try:
            mlflow.end_run(status=status)
            logger.info(f"Ended MLflow run with status: {status}")
            self.active_run = None
            
        except Exception as e:
            logger.error(f"Failed to end run: {e}")
    
    def get_run_info(self) -> Dict[str, Any]:
        """
        Get information about active run.
        
        Returns:
            Dictionary with run information
        """
        if self.active_run is None:
            return {}
        
        return {
            'run_id': self.active_run.info.run_id,
            'run_name': self.active_run.data.tags.get('mlflow.runName'),
            'experiment_id': self.active_run.info.experiment_id,
            'status': self.active_run.info.status,
            'artifact_uri': self.active_run.info.artifact_uri
        }


def main():
    """Test MLflow tracker."""
    logging.basicConfig(level=logging.INFO)
    
    tracker = MLflowTracker()
    
    # Start run
    tracker.start_run("test-run", tags={'test': 'true'})
    
    # Log params and metrics
    tracker.log_params({'param1': 'value1', 'param2': 42})
    tracker.log_metrics({'metric1': 0.95, 'metric2': 0.87})
    
    # Get run info
    info = tracker.get_run_info()
    print(f"\nRun Info: {info}")
    
    # End run
    tracker.end_run()
    
    print("\nMLflow tracker test complete!")
    print(f"View experiments at: {tracker.tracking_uri}")


if __name__ == "__main__":
    main()
