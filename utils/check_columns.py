import os
import pandas as pd
import logging
from pathlib import Path
from config_loader import ConfigLoader

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)


def check_columns_consistency():
    """
    Check if all CSV files in data/raw directory have the same columns.
    Uses config.yaml to get the raw data directory path.
    
    Returns:
        bool: True if all files have same columns, False otherwise
    """
    # Load configuration
    config_loader = ConfigLoader()
    config_loader.load_config()
    config = config_loader.get_config()
    
    # Get raw data path from config
    raw_data_path = config['data_paths']['raw']
    
    # Get project root (parent of utils directory)
    project_root = Path(__file__).parent.parent
    raw_data_dir = project_root / raw_data_path
    
    logger.info(f"Checking columns in directory: {raw_data_dir}")
    
    # Get all CSV files
    csv_files = list(raw_data_dir.glob("*.csv"))
    
    if not csv_files:
        logger.warning("No CSV files found in the raw data directory")
        return True
    
    logger.info(f"Found {len(csv_files)} CSV files")
    
    # Dictionary to store columns for each file
    file_columns = {}
    
    # Read columns from each file
    for csv_file in csv_files:
        try:
            # Read only the header (first row) to get column names
            df = pd.read_csv(csv_file, nrows=0)
            columns = df.columns.tolist()
            file_columns[csv_file.name] = columns
            logger.info(f"{csv_file.name}: {len(columns)} columns")
        except Exception as e:
            logger.error(f"Error reading {csv_file.name}: {e}")
            return False
    
    # Compare columns across all files
    reference_file = csv_files[0].name
    reference_columns = set(file_columns[reference_file])
    
    all_same = True
    mismatches = []
    
    for filename, columns in file_columns.items():
        if filename == reference_file:
            continue
        
        current_columns = set(columns)
        
        if current_columns != reference_columns:
            all_same = False
            missing_cols = reference_columns - current_columns
            extra_cols = current_columns - reference_columns
            
            mismatch_info = {
                'file': filename,
                'missing_columns': list(missing_cols),
                'extra_columns': list(extra_cols)
            }
            mismatches.append(mismatch_info)
            
            logger.warning(f"\n{filename} has different columns than {reference_file}:")
            if missing_cols:
                logger.warning(f"  Missing columns: {missing_cols}")
            if extra_cols:
                logger.warning(f"  Extra columns: {extra_cols}")
    
    # Print summary
    print("\n" + "="*70)
    print("COLUMN CONSISTENCY CHECK SUMMARY")
    print("="*70)
    print(f"Total files checked: {len(csv_files)}")
    print(f"Reference file: {reference_file}")
    print(f"Number of columns in reference: {len(reference_columns)}")
    
    if all_same:
        print("\n✓ SUCCESS: All files have the same columns!")
    else:
        print(f"\n✗ FAILURE: {len(mismatches)} file(s) have different columns")
        print("\nFiles with mismatched columns:")
        for mismatch in mismatches:
            print(f"  - {mismatch['file']}")
            if mismatch['missing_columns']:
                print(f"    Missing: {len(mismatch['missing_columns'])} columns")
            if mismatch['extra_columns']:
                print(f"    Extra: {len(mismatch['extra_columns'])} columns")
    
    print("="*70 + "\n")
    
    return all_same


if __name__ == "__main__":
    result = check_columns_consistency()
    exit(0 if result else 1)
