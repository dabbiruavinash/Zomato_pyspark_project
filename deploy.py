import subprocess
import logging
from typing import List

class DeploymentManager:
    def __init__(self):
        self.logger = logging.getLogger("DeploymentManager")
    
    def run_tests(self) -> bool:
        self.logger.info("Running unit tests...")
        result = subprocess.run(["pytest", "tests/unit"], capture_output=True)
        if result.returncode != 0:
            self.logger.error("Unit tests failed")
            return False
        
        self.logger.info("Running integration tests...")
        result = subprocess.run(["pytest", "tests/integration"], capture_output=True)
        if result.returncode != 0:
            self.logger.error("Integration tests failed")
            return False
        
        return True
    
    def deploy_to_environment(self, env: str):
        if not self.run_tests():
            raise Exception("Tests failed, aborting deployment")
        
        self.logger.info(f"Deploying to {env} environment")
        # Implementation of actual deployment
        pass