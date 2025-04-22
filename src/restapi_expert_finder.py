import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import requests
import logging
import time
import json
from pathlib import Path
from tqdm import tqdm

logger = logging.getLogger(__name__)

class RestAPIExpertFinder:
    """GitHub expert finder using REST API as fallback when GraphQL is rate limited."""
    
    def __init__(self, github_token):
        """Initialize the REST API expert finder.
        
        Args:
            github_token (str): GitHub API token
        """
        self.github_token = github_token
        self.headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json",
        }
        
    def _handle_rate_limit(self, response):
        """Handle rate limiting by waiting until reset time."""
        if response.status_code == 403 and 'rate limit exceeded' in response.text.lower():
            reset_time = int(response.headers.get("X-RateLimit-Reset", 0))
            wait_time = max(0, reset_time - int(time.time()))
            logger.warning(f"Rate limit exceeded. Waiting for {wait_time} seconds.")
            time.sleep(wait_time + 1)
            return True
        return False
    
    def search_users(self, language, page=1, per_page=30):
        """Search for GitHub users experienced in a language."""
        url = f"https://api.github.com/search/users?q=language:{language}+followers:>1000+repos:>50&page={page}&per_page={per_page}&sort=followers&order=desc"
        
        while True:
            response = requests.get(url, headers=self.headers)
            
            if self._handle_rate_limit(response):
                continue
                
            if response.status_code != 200:
                logger.error(f"Failed to search users: {response.status_code} - {response.text}")
                return []
                
            return response.json().get("items", [])
    
    def get_user_details(self, username):
        """Get detailed information about a user."""
        # Get basic user info
        user_url = f"https://api.github.com/users/{username}"
        user_response = requests.get(user_url, headers=self.headers)
        
        if self._handle_rate_limit(user_response):
            return self.get_user_details(username)
            
        if user_response.status_code != 200:
            logger.error(f"Failed to get user details: {user_response.status_code} - {user_response.text}")
            return None
            
        user_data = user_response.json()
        followers = user_data.get("followers", 0)
        
        # Get repositories
        repos_url = f"https://api.github.com/users/{username}/repos?per_page=100&type=owner&sort=updated"
        repos_response = requests.get(repos_url, headers=self.headers)
        
        if self._handle_rate_limit(repos_response):
            return self.get_user_details(username)
            
        if repos_response.status_code != 200:
            logger.error(f"Failed to get repositories: {repos_response.status_code} - {repos_response.text}")
            return None
            
        repos = repos_response.json()
        stars = sum(repo.get("stargazers_count", 0) for repo in repos)
        
        # Get PRs created by user 
        # We use search API to get an approximate count
        prs_url = f"https://api.github.com/search/issues?q=author:{username}+is:pr+is:public&per_page=1"
        prs_response = requests.get(prs_url, headers=self.headers)
        
        if self._handle_rate_limit(prs_response):
            return self.get_user_details(username)
            
        prs_count = 0
        if prs_response.status_code == 200:
            prs_count = prs_response.json().get("total_count", 0)
        
        # Get PR reviews - this is more complex with REST API
        # We use search API with 'commenter' to estimate review activity
        reviews_url = f"https://api.github.com/search/issues?q=commenter:{username}+is:pr+is:public&per_page=1"
        reviews_response = requests.get(reviews_url, headers=self.headers)
        
        if self._handle_rate_limit(reviews_response):
            return self.get_user_details(username)
            
        pr_reviews = 0
        if reviews_response.status_code == 200:
            pr_reviews = reviews_response.json().get("total_count", 0)
        
        return {
            "login": username,
            "followers": followers,
            "stars": stars,
            "prs": prs_count,
            "pr_reviews": pr_reviews
        }
    
    def find_experts(self, language, max_users=30):
        """
        Find and rank experts by programming language using REST API.
        
        Args:
            language (str): Programming language (Python, JavaScript,...)
            max_users (int): Maximum number of users to find
            
        Returns:
            list: List of ranked users
        """
        logger.info(f"Finding {language} experts using REST API...")
        results = []
        page = 1
        per_page = 30
        
        
                    
                    # Skip users we've already processed
                    if any(result["login"] == username for result in results):
                        continue
                    
                    logger.info(f"Processing user: {username}")
                    
                    # Get detailed information
                    user_details = self.get_user_details(username)
                    if not user_details:
                        logger.warning(f"Skipping user {username} due to API errors")
                        continue
                    
                    # Calculate score using the same formula as in GraphQL version
                    pr_reviews = user_details.get("pr_reviews", 0)
                    if pr_reviews < 10:
                        logger.info(f"Skipping {username} with only {pr_reviews} PR reviews")
                        continue
                    
                    followers = user_details.get("followers", 0)
                    stars = user_details.get("stars", 0)
                    prs = user_details.get("prs", 0)
                    
                    # Scoring formula - same as GraphQL version
                    weights = {'followers': 1, 'stars': 2, 'prs': 3, 'pr_reviews': 4}
                    score = (
                        (weights['followers'] * followers +
                        weights['stars'] * stars + 
                        weights['prs'] * prs) * weights['pr_reviews'] * pr_reviews
                    )
                    
                    # Add score to user details
                    user_details["score"] = score
                    
                    # Add to results
                    results.append(user_details)
                    pbar.update(1)
                    
                    # Check if we have enough users
                    if len(results) >= max_users:
                        break
                
                # Move to next page
                page += 1
                
                # Avoid hitting rate limits
                time.sleep(1)
        
        # Sort results by score
        return sorted(results, key=lambda x: x.get("score", 0), reverse=True) 