import os
import yaml
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)


class ConfigLoader:
    def __init__(self, config_path: str = None):
        if config_path:
            self.config_path = config_path
        else:
            # Resolve config.yaml relative to this python file
            base_path = os.path.dirname(os.path.abspath(__file__))
            self.config_path = os.path.join(base_path, "config.yaml")

        self.config = None

    def load_config(self):
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            logger.info("Config loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise

    def get_config(self):
        return self.config
    
    def get_etl_config(self):
        """Get ETL pipeline configuration."""
        if not self.config:
            self.load_config()
        return self.config.get('etl', {})
    
    def get_data_paths(self):
        """Get data paths configuration."""
        if not self.config:
            self.load_config()
        return self.config.get('data_paths', {})
    
    def get_cleaning_strategy(self):
        """Get data cleaning strategies."""
        etl_config = self.get_etl_config()
        return etl_config.get('cleaning', {})
    
    def get_logging_config(self):
        """Get logging configuration."""
        if not self.config:
            self.load_config()
        return self.config.get('logging', {})
    
    def resolve_path(self, relative_path: str):
        """
        Resolve a relative path to absolute path from project root.
        
        Args:
            relative_path: Path relative to project root
            
        Returns:
            Absolute path
        """
        # Get project root (parent of utils directory)
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(base_path, relative_path)


