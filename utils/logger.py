"""
Centralized Logging Utility

Provides consistent logging setup across all modules using config.yaml.
"""

import logging
from pathlib import Path
from typing import Optional
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.config_loader import ConfigLoader


def setup_logger(
    name: str,
    log_file: Optional[str] = None,
    config_path: Optional[str] = None
) -> logging.Logger:
    """
    Setup logger with configuration from config.yaml.
    
    Args:
        name: Logger name (usually __name__)
        log_file: Optional specific log file (overrides config)
        config_path: Optional path to config file
        
    Returns:
        Configured logger
    """
    # Load config
    config_loader = ConfigLoader(config_path)
    config_loader.load_config()
    
    logging_config = config_loader.get_config().get('logging', {})
    
    # Get logging settings
    level_str = logging_config.get('level', 'INFO')
    log_format = logging_config.get('format', '%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    log_to_file = logging_config.get('log_to_file', True)
    default_log_file = logging_config.get('log_file', 'logs/app.log')
    
    # Use provided log file or default
    if log_file is None:
        log_file = default_log_file
    
    # Convert level string to logging constant
    level = getattr(logging, level_str.upper(), logging.INFO)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(log_format)
    
    # Console handler (always)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if enabled)
    if log_to_file and log_file:
        # Ensure log directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, mode='a')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger


def get_logger(name: str, log_file: Optional[str] = None) -> logging.Logger:
    """
    Get or create logger.
    
    Convenience wrapper around setup_logger.
    
    Args:
        name: Logger name
        log_file: Optional log file path
        
    Returns:
        Logger instance
    """
    return setup_logger(name, log_file=log_file)


# Example usage
if __name__ == "__main__":
    # Test logger
    logger = get_logger(__name__, log_file='logs/test.log')
    
    logger.debug("Debug message")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message")
    
    print("\nLogger configured successfully!")
    print(f"Check logs/test.log for file output")
