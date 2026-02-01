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


