import os
import json
from typing import Dict, Any

def get_config() -> Dict[str, Any]:
    """
    Load configuration from environment variables or config file.
    
    Returns:
        Dictionary containing configuration values
    """
    # Default config
    config = {
        "db": {
            "host": "localhost",
            "port": 5432,
            "database": "sales_db",
            "user": "postgres",
            "password": ""
        }
    }
    
    # Override from config file if exists
    config_file = os.environ.get("CONFIG_FILE", "config.json")
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r') as f:
                file_config = json.load(f)
                config.update(file_config)
        except Exception as e:
            print(f"Warning: Failed to load config file: {e}")
    
    # Override from environment variables
    if "DB_HOST" in os.environ:
        config["db"]["host"] = os.environ["DB_HOST"]
    if "DB_PORT" in os.environ:
        config["db"]["port"] = int(os.environ["DB_PORT"])
    if "DB_NAME" in os.environ:
        config["db"]["database"] = os.environ["DB_NAME"]
    if "DB_USER" in os.environ:
        config["db"]["user"] = os.environ["DB_USER"]
    if "DB_PASSWORD" in os.environ:
        config["db"]["password"] = os.environ["DB_PASSWORD"]
    
    return config