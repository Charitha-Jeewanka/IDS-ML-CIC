"""
Simple validation script to test ETL components without running the full pipeline.
Tests individual components with mock data.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)


def test_config_loader():
    """Test configuration loading."""
    logger.info("\n" + "="*70)
    logger.info("Testing ConfigLoader")
    logger.info("="*70)
    
    try:
        from utils.config_loader import ConfigLoader
        
        config_loader = ConfigLoader()
        config_loader.load_config()
        
        # Test basic config access
        config = config_loader.get_config()
        assert config is not None, "Config is None"
        logger.info("Config loaded successfully")
        
        # Test ETL config access
        etl_config = config_loader.get_etl_config()
        assert 'reader' in etl_config, "ETL config missing 'reader'"
        assert 'cleaning' in etl_config, "ETL config missing 'cleaning'"
        logger.info("ETL config accessible")
        
        # Test data paths
        data_paths = config_loader.get_data_paths()
        assert 'raw' in data_paths, "Data paths missing 'raw'"
        assert 'processed' in data_paths, "Data paths missing 'processed'"
        logger.info("Data paths configured")
        
        # Test path resolution
        raw_path = config_loader.resolve_path(data_paths['raw'])
        assert raw_path is not None, "Path resolution failed"
        logger.info(f"Path resolution works: {raw_path}")
        
        logger.info("\nConfigLoader: ALL TESTS PASSED\n")
        return True
        
    except Exception as e:
        logger.error(f"ConfigLoader test failed: {e}")
        return False


def test_imports():
    """Test that all components can be imported."""
    logger.info("\n" + "="*70)
    logger.info("Testing Component Imports")
    logger.info("="*70)
    
    try:
        # Test if pandas is available
        try:
            import pandas as pd
            logger.info("pandas imported")
        except ImportError:
            logger.warning("pandas not installed (run: pip install pandas)")
            return False
        
        # Try importing DataPipeline components
        from src.DataPipeline.data_reader import DataReader
        logger.info("DataReader imported")
        
        from src.DataPipeline.data_cleaner import DataCleaner
        logger.info("DataCleaner imported")
        
        from src.DataPipeline.data_validator import DataValidator
        logger.info("DataValidator imported")
        
        from src.DataPipeline.etl_pipeline import ETLPipeline
        logger.info("ETLPipeline imported")
        
        logger.info("\nComponent Imports: ALL TESTS PASSED\n")
        return True
        
    except Exception as e:
        logger.error(f"Import test failed: {e}")
        return False


def test_component_initialization():
    """Test that components can be initialized."""
    logger.info("\n" + "="*70)
    logger.info("Testing Component Initialization")
    logger.info("="*70)
    
    try:
        from src.DataPipeline.data_reader import DataReader
        from src.DataPipeline.data_cleaner import DataCleaner
        from src.DataPipeline.data_validator import DataValidator
        
        # Test DataReader
        reader = DataReader(backend="pandas")
        assert reader.backend == "pandas", "DataReader backend not set"
        logger.info("DataReader initialized")
        
        # Test DataCleaner
        cleaner = DataCleaner(infinity_strategy="max", missing_strategy="drop")
        assert cleaner.infinity_strategy == "max", "DataCleaner strategy not set"
        logger.info("DataCleaner initialized")
        
        # Test DataValidator
        validator = DataValidator(check_infinity=True, check_missing=True)
        assert validator.check_infinity == True, "DataValidator checks not set"
        logger.info("DataValidator initialized")
        
        logger.info("\nComponent Initialization: ALL TESTS PASSED\n")
        return True
        
    except Exception as e:
        logger.error(f"Initialization test failed: {e}")
        return False


def test_directory_structure():
    """Test that required directories exist."""
    logger.info("\n" + "="*70)
    logger.info("Testing Directory Structure")
    logger.info("="*70)
    
    project_root = Path(__file__).parent.parent.parent
    
    required_dirs = {
        'data/raw': 'Raw data directory',
        'data/processed': 'Processed data output',
        'artifacts/reports': 'ETL reports',
        'logs': 'Log files',
        'src/DataPipeline': 'Pipeline source code',
        'utils': 'Utilities'
    }
    
    all_exist = True
    for dir_path, description in required_dirs.items():
        full_path = project_root / dir_path
        if full_path.exists():
            logger.info(f"{dir_path} - {description}")
        else:
            logger.error(f"{dir_path} not found - {description}")
            all_exist = False
    
    if all_exist:
        logger.info("\nDirectory Structure: ALL TESTS PASSED\n")
    else:
        logger.warning("\nSome directories missing (they will be created on first run)\n")
    
    return all_exist


def main():
    """Run all validation tests."""
    logger.info("\n" + "="*70)
    logger.info("ETL PIPELINE VALIDATION TESTS")
    logger.info("="*70 + "\n")
    
    results = {
        'Directory Structure': test_directory_structure(),
        'Configuration': test_config_loader(),
        'Imports': test_imports(),
        'Initialization': test_component_initialization()
    }
    
    logger.info("\n" + "="*70)
    logger.info("VALIDATION SUMMARY")
    logger.info("="*70)
    
    for test_name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        logger.info(f"{status} - {test_name}")
    
    all_passed = all(results.values())
    
    if all_passed:
        logger.info("\n🎉 ALL VALIDATION TESTS PASSED!")
        logger.info("You can now run the ETL pipeline with:")
        logger.info("  python3 src/DataPipeline/run_etl.py\n")
    else:
        logger.info("\n⚠️  Some tests failed. Please fix the issues above.\n")
    
    logger.info("="*70 + "\n")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
