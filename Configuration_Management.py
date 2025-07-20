import json
import os
from typing import Dict, Any

class ConfigManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance
    
    def _load_config(self):
        env = os.getenv("ENVIRONMENT", "dev")
        config_file = f"configs/{env}.json"
        
        with open(config_file) as f:
            self.config = json.load(f)
        
        # Load secrets from environment variables
        self.config["secrets"] = {
            "kafka_password": os.getenv("KAFKA_PASSWORD"),
            "db_password": os.getenv("DB_PASSWORD"),
            # ... other secrets
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        return self.config.get(key, default)