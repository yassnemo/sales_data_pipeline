import pandas as pd
import json
import sys
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, Optional, Callable
from utils.data_cleaner import clean_sales_data
from utils.db_loader import load_to_postgres
from utils.config import get_config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def read_csv(path: str) -> pd.DataFrame:
    """
    Read data from a CSV file.
    
    Args:
        path: Path to the CSV file
        
    Returns:
        DataFrame containing the CSV data
    """
    logger.info(f"Reading CSV: {path}")
    try:
        return pd.read_csv(path)
    except Exception as e:
        logger.error(f"Failed to read CSV {path}: {str(e)}")
        raise

def read_json(path: str) -> pd.DataFrame:
    """
    Read data from a JSON file.
    
    Args:
        path: Path to the JSON file
        
    Returns:
        DataFrame containing the JSON data
    """
    logger.info(f"Reading JSON: {path}")
    try:
        with open(path, "r") as f:
            data = json.load(f)
        return pd.DataFrame(data)
    except Exception as e:
        logger.error(f"Failed to read JSON {path}: {str(e)}")
        raise

def read_excel(path: str) -> pd.DataFrame:
    """
    Read data from an Excel file.
    
    Args:
        path: Path to the Excel file
        
    Returns:
        DataFrame containing the Excel data
    """
    logger.info(f"Reading Excel: {path}")
    try:
        return pd.read_excel(path)
    except Exception as e:
        logger.error(f"Failed to read Excel {path}: {str(e)}")
        raise

# Map file extensions to reader functions
FILE_READERS: Dict[str, Callable[[str], pd.DataFrame]] = {
    ".csv": read_csv,
    ".json": read_json,
    ".xlsx": read_excel,
    ".xls": read_excel,
}

def read_file(file_path: str) -> Optional[pd.DataFrame]:
    """
    Read data from a file based on its extension.
    
    Args:
        file_path: Path to the data file
        
    Returns:
        DataFrame containing the file data or None if format is unsupported
    """
    file_ext = Path(file_path).suffix.lower()
    if file_ext in FILE_READERS:
        return FILE_READERS[file_ext](file_path)
    else:
        supported_formats = ", ".join(FILE_READERS.keys())
        logger.error(f"Unsupported file format: {file_ext}. Supported formats: {supported_formats}")
        return None

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Process sales data and load to database")
    parser.add_argument("data_file", help="Path to the data file")
    parser.add_argument("-t", "--table", default="sales", help="Target table name (default: sales)")
    parser.add_argument("-c", "--chunk-size", type=int, default=0, 
                        help="Process data in chunks of this size (0 for no chunking)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    return parser.parse_args()

def main():
    # Parse command line arguments
    args = parse_args()
    
    # Set log level based on verbosity
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Load configuration
    config = get_config()
    
    # Read the file
    df = read_file(args.data_file)
    if df is None:
        sys.exit(1)
    
    try:
        # Clean and load data
        logger.info("Cleaning data...")
        df_clean = clean_sales_data(df)
        
        logger.info(f"Loading data to {args.table} table...")
        load_to_postgres(df_clean, args.table, config=config, chunk_size=args.chunk_size)
        
        logger.info("✅ Pipeline completed successfully")
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()