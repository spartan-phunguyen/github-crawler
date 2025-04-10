import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import requests
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GitHubAPI:
    """Class handling basic interactions with GitHub API."""
    
    GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"
    
    def __init__(self, token=None):
        """
        Initialize with GitHub token.
        
        Args:
            token (str): GitHub authentication token
        """
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json"
        } if token else {}
    
    def set_token(self, token):
        """Update the token."""
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json"
        }
    
    def graphql_query(self, query, variables):
        """
        Execute a GraphQL query to GitHub API.
        
        Args:
            query (str): GraphQL query
            variables (dict): Query variables
            
        Returns:
            dict: Response data or empty dict if error
        """
        if not self.token:
            logger.error("No GitHub token provided")
            return {}
            
        try:
            response = requests.post(
                self.GITHUB_GRAPHQL_URL,
                json={"query": query, "variables": variables},
                headers=self.headers
            )
            
            if response.status_code != 200:
                logger.error(f"API Error: {response.status_code}, {response.text}")
                return {}
                
            return response.json()
        except Exception as e:
            logger.error(f"Error executing GraphQL query: {e}")
            return {} 