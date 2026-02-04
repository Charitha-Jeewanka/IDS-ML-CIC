"""
Model Explainability Module

Generates model explanations and visualizations for management presentations.
"""

import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
import json

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logger = logging.getLogger(__name__)


class ModelExplainer:
    """
    Generate explainability reports for H2O models.
    
    Outputs for management presentations:
    - Feature importance plots
    - Confusion matrices
    - ROC/PR curves
    - Performance metrics
    - Model comparison reports
    """
    
    def __init__(self, model, test_frame, output_dir: str = "artifacts/explainability"):
        """
        Initialize explainer.
        
        Args:
            model: H2O model
            test_frame: H2O test frame
            output_dir: Directory for outputs
        """
        self.model = model
        self.test_frame = test_frame
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.performance = None
        
        logger.info("ModelExplainer initialized")
        logger.info(f"Output directory: {self.output_dir}")
    
    def generate_all_reports(self) -> Dict[str, str]:
        """
        Generate all explainability reports.
        
        Returns:
            Dictionary of report paths
        """
        logger.info("\n" + "="*70)
        logger.info("GENERATING EXPLAINABILITY REPORTS")
        logger.info("="*70 + "\n")
        
        reports = {}
        
        # 1. Feature importance
        logger.info("1. Generating feature importance plot...")
        fi_path = self.plot_feature_importance()
        if fi_path:
            reports['feature_importance'] = str(fi_path)
        
        # 2. Confusion matrix
        logger.info("2. Generating confusion matrix...")
        cm_path = self.plot_confusion_matrix()
        if cm_path:
            reports['confusion_matrix'] = str(cm_path)
        
        # 3. ROC curve
        logger.info("3. Generating ROC curve...")
        roc_path = self.plot_roc_curve()
        if roc_path:
            reports['roc_curve'] = str(roc_path)
        
        # 4. Performance metrics
        logger.info("4. Generating performance report...")
        perf_path = self.create_performance_report()
        if perf_path:
            reports['performance_report'] = str(perf_path)
        
        logger.info("\n" + "="*70)
        logger.info("EXPLAINABILITY REPORTS COMPLETE")
        logger.info("="*70)
        logger.info(f"\nReports saved to: {self.output_dir}")
        for name, path in reports.items():
            logger.info(f"  {name}: {Path(path).name}")
        logger.info("")
        
        return reports
    
    def plot_feature_importance(self, n: int = 20) -> Optional[Path]:
        """
        Plot feature importance.
        
        Args:
            n: Number of top features to plot
            
        Returns:
            Path to saved plot
        """
        try:
            import matplotlib.pyplot as plt
            import pandas as pd
            
            # Get variable importance
            varimp = self.model.varimp(use_pandas=True)
            
            if varimp is None or len(varimp) == 0:
                logger.warning("Feature importance not available")
                return None
            
            # Take top N
            varimp_top = varimp.head(n)
            
            # Plot
            fig, ax = plt.subplots(figsize=(10, 8))
            
            ax.barh(varimp_top['variable'], varimp_top['scaled_importance'])
            ax.set_xlabel('Scaled Importance', fontsize=12)
            ax.set_ylabel('Feature', fontsize=12)
            ax.set_title(f'Top {n} Feature Importance', fontsize=14, fontweight='bold')
            ax.invert_yaxis()
            
            plt.tight_layout()
            
            # Save
            output_path = self.output_dir / 'feature_importance.png'
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"  Saved: {output_path.name}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error plotting feature importance: {e}")
            return None
    
    def plot_confusion_matrix(self) -> Optional[Path]:
        """
        Plot confusion matrix.
        
        Returns:
            Path to saved plot
        """
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
            import numpy as np
            
            # Get performance
            if self.performance is None:
                self.performance = self.model.model_performance(self.test_frame)
            
            # Get confusion matrix
            cm = self.performance.confusion_matrix()
            
            if cm is None:
                logger.warning("Confusion matrix not available")
                return None
            
            # Convert to pandas DataFrame
            cm_df = cm.as_data_frame()
            
            # Extract matrix values (remove totals row/col)
            matrix = cm_df.iloc[:-1, 1:-1].values.astype(float)
            labels = cm_df.columns[1:-1].tolist()
            
            # Plot
            fig, ax = plt.subplots(figsize=(10, 8))
            
            sns.heatmap(
                matrix,
                annot=True,
                fmt='.0f',
                cmap='Blues',
                xticklabels=labels,
                yticklabels=labels,
                ax=ax,
                cbar_kws={'label': 'Count'}
            )
            
            ax.set_xlabel('Predicted', fontsize=12)
            ax.set_ylabel('Actual', fontsize=12)
            ax.set_title('Confusion Matrix', fontsize=14, fontweight='bold')
            
            plt.tight_layout()
            
            # Save
            output_path = self.output_dir / 'confusion_matrix.png'
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"  Saved: {output_path.name}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error plotting confusion matrix: {e}")
            return None
    
    def plot_roc_curve(self) -> Optional[Path]:
        """
        Plot ROC curve.
        
        Returns:
            Path to saved plot
        """
        try:
            import matplotlib.pyplot as plt
            
            # Get performance
            if self.performance is None:
                self.performance = self.model.model_performance(self.test_frame)
            
            # Plot ROC
            fig, ax = plt.subplots(figsize=(8, 8))
            
            # H2O provides plot_roc method
            roc = self.performance.plot(type='roc', server=False)
            
            # Extract data for matplotlib (if H2O plot fails, use metrics)
            try:
                fpr = self.performance.fprs
                tpr = self.performance.tprs
                auc = self.performance.auc()
                
                ax.plot(fpr, tpr, label=f'ROC (AUC = {auc:.3f})', linewidth=2)
                ax.plot([0, 1], [0, 1], 'k--', label='Random', linewidth=1)
                
                ax.set_xlabel('False Positive Rate', fontsize=12)
                ax.set_ylabel('True Positive Rate', fontsize=12)
                ax.set_title('ROC Curve', fontsize=14, fontweight='bold')
                ax.legend(loc='lower right', fontsize=10)
                ax.grid(True, alpha=0.3)
                
                plt.tight_layout()
                
                # Save
                output_path = self.output_dir / 'roc_curve.png'
                plt.savefig(output_path, dpi=300, bbox_inches='tight')
                plt.close()
                
                logger.info(f"  Saved: {output_path.name}")
                return output_path
                
            except:
                logger.warning("Could not extract ROC data")
                plt.close()
                return None
            
        except Exception as e:
            logger.error(f"Error plotting ROC curve: {e}")
            return None
    
    def create_performance_report(self) -> Optional[Path]:
        """
        Create performance metrics report.
        
        Returns:
            Path to saved report
        """
        try:
            # Get performance
            if self.performance is None:
                self.performance = self.model.model_performance(self.test_frame)
            
            # Extract metrics
            metrics = {
                'model_id': self.model.model_id,
                'auc': float(self.performance.auc()),
                'logloss': float(self.performance.logloss()),
            }
            
            # Add accuracy, precision, recall, F1 if available
            try:
                metrics['accuracy'] = float(self.performance.accuracy()[0][1])
                metrics['precision'] = float(self.performance.precision()[0][1])
                metrics['recall'] = float(self.performance.recall()[0][1])
                metrics['f1'] = float(self.performance.F1()[0][1])
            except:
                pass
            
            # Save as JSON
            json_path = self.output_dir / 'performance_metrics.json'
            with open(json_path, 'w') as f:
                json.dump(metrics, f, indent=2)
            
            # Create Markdown report
            md_path = self.output_dir / 'model_report.md'
            with open(md_path, 'w') as f:
                f.write(f"# Model Performance Report\n\n")
                f.write(f"**Model**: {metrics['model_id']}\n\n")
                f.write(f"## Test Set Metrics\n\n")
                f.write(f"| Metric | Value |\n")
                f.write(f"|--------|-------|\n")
                for metric, value in metrics.items():
                    if metric != 'model_id':
                        f.write(f"| {metric.upper()} | {value:.4f} |\n")
                f.write(f"\n## Visualizations\n\n")
                f.write(f"- ![Feature Importance](feature_importance.png)\n")
                f.write(f"- ![Confusion Matrix](confusion_matrix.png)\n")
                f.write(f"- ![ROC Curve](roc_curve.png)\n")
            
            logger.info(f"  Saved: {json_path.name}, {md_path.name}")
            return md_path
            
        except Exception as e:
            logger.error(f"Error creating performance report: {e}")
            return None


def main():
    """Test explainability module."""
    logging.basicConfig(level=logging.INFO)
    
    print("ModelExplainer - Test requires trained H2O model")
    print("Use run_training.py for full pipeline with explainability")


if __name__ == "__main__":
    main()
