import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import logging
from github_api import GitHubAPI
import argparse
import json
import os
from restapi_expert_finder import RestAPIExpertFinder

logger = logging.getLogger(__name__)

class GitHubExpertFinder:
    """Class for finding and ranking GitHub experts by language."""
    
    def __init__(self, github_tokens):
        """
        Initialize with GitHub token(s).
        
        Args:
            github_tokens (str or list): A single GitHub token or a list of tokens
        """
        # Handle both single token and list of tokens
        if isinstance(github_tokens, str):
            self.github_tokens = [github_tokens]
        else:
            self.github_tokens = github_tokens
            
        self.current_token_index = 0
        self.api = GitHubAPI(self.github_tokens[0])
        
        # Initialize REST finder with first token
        self.rest_finder = RestAPIExpertFinder(self.github_tokens[0])
    
    def rotate_token(self):
        """
        Rotate to the next available GitHub token.
        
        Returns:
            bool: True if successfully rotated to a new token, False if all tokens are exhausted
        """
        if len(self.github_tokens) <= 1:
            # Only one token available, can't rotate
            return False
            
        # Move to next token
        self.current_token_index = (self.current_token_index + 1) % len(self.github_tokens)
        new_token = self.github_tokens[self.current_token_index]
        
        # Update API instances with new token
        self.api = GitHubAPI(new_token)
        self.rest_finder = RestAPIExpertFinder(new_token)
        
        logger.info(f"Rotated to GitHub token {self.current_token_index + 1}/{len(self.github_tokens)}")
        return True
        
    def find_experts(self, language, max_users=30, use_rest_api=False):
        """
        Find and rank experts by programming language.
        
        Args:
            language (str): Programming language (Python, JavaScript,...)
            max_users (int): Maximum number of users to find
            use_rest_api (bool): Force using REST API instead of GraphQL
            
        Returns:
            list: List of ranked users
        """
        # First check if we're forcing REST API
        if use_rest_api:
            logger.info(f"Using REST API for finding {language} experts as requested")
            return self.rest_finder.find_experts(language, max_users)
         
        # Try GraphQL API with token rotation on rate limit or network errors
        token_rotation_attempts = 0
        max_token_rotations = len(self.github_tokens)   
            
        while token_rotation_attempts <= max_token_rotations:
            try:
                logger.info(f"Finding {language} experts using GraphQL...")
                results = []
                after_cursor = None
                fetched = 0
                
                # GraphQL query to find users - using contributionsCollection for PR reviews
                query = """
                query($queryString: String!, $after: String) {
                  search(query: $queryString, type: USER, first: 10, after: $after) {
                    pageInfo {
                      endCursor
                      hasNextPage
                    }
                    edges {
                      node {
                        ... on User {
                          login
                          followers {
                            totalCount
                          }
                          repositories(first: 50, isFork: false, ownerAffiliations: OWNER) {
                            nodes {
                              stargazerCount
                              primaryLanguage {
                                name
                              }
                            }
                          }
                          pullRequests(first: 50) {
                            totalCount
                          }
                          contributionsCollection {
                            pullRequestReviewContributions {
                              totalCount
                            }
                          }
                        }
                      }
                    }
                  }
                }
                """
                round = 0
                while fetched < max_users:
                    print(f"Round {round}")
                    #query_string = f"language:{language} followers:>1000 repos:>50"
                    query_string = f"language:{language}"
                    variables = {"queryString": query_string, "after": after_cursor}
                    
                    data = self.api.graphql_query(query, variables)
                    
                    # Check for network errors and rotate token if needed
                    if isinstance(data, dict) and "error" in data:
                        error_type = data["error"]
                        if error_type in ["connection_error", "timeout_error", "request_error", "general_error"]:
                            logger.warning(f"Network error encountered: {error_type}. Attempting token rotation.")
                            if self.rotate_token():
                                logger.info(f"Rotated to token {self.current_token_index + 1}/{len(self.github_tokens)}")
                                token_rotation_attempts += 1
                                # Short wait before retrying
                                import time
                                time.sleep(2)
                                continue
                            else:
                                logger.error("No more tokens available to rotate to")
                                # Fall back to REST API as last resort
                                logger.info("Falling back to REST API as last resort")
                                return self.rest_finder.find_experts(language, max_users)
                    
                    if not data or 'data' not in data:
                        logger.warning("No valid data received from API")
                        # Try to rotate token and retry
                        if self.rotate_token():
                            logger.info(f"Rotated to token {self.current_token_index + 1}/{len(self.github_tokens)}")
                            token_rotation_attempts += 1
                            import time
                            time.sleep(2)
                            continue
                        else:
                            # Fall back to REST API if rotation fails
                            logger.warning("Token rotation failed, falling back to REST API")
                            return self.rest_finder.find_experts(language, max_users)
                        
                    users = data['data']['search']['edges']
                    for user in users:
                        if user['node'] == {}:
                            continue
                            
                        user_info = self._extract_user_data(user['node'], language)
                        if user_info['score'] == 0:
                            continue
                        results.append(user_info)

                        fetched += 1
                        
                        if fetched >= max_users:
                            break
                            
                    # Check for next page
                    if not data['data']['search']['pageInfo']['hasNextPage']:
                        break
                        
                    after_cursor = data['data']['search']['pageInfo']['endCursor']
                    round += 1
                
                # If we got here without exceptions, we're done
                return sorted(results, key=lambda x: x['score'], reverse=True)
            
            except Exception as e:
                # Check if the error is related to rate limiting
                error_message = str(e).lower()
                if "rate limit" in error_message or "ratelimit" in error_message or "too many requests" in error_message:
                    logger.warning(f"GitHub API rate limited for token {self.current_token_index + 1}/{len(self.github_tokens)}")
                    
                    # Try to rotate token
                    if self.rotate_token():
                        logger.info(f"Rotated to token {self.current_token_index + 1}/{len(self.github_tokens)}")
                        token_rotation_attempts += 1
                        continue
                    else:
                        logger.warning("No more tokens available to rotate to")
                        # Fall back to REST API
                        logger.info("Falling back to REST API due to rate limits")
                        return self.rest_finder.find_experts(language, max_users)
                else:
                    # For network errors and other issues, also try to rotate token
                    logger.error(f"Error finding experts via GraphQL: {e}")
                    logger.info("Attempting to rotate token due to error")
                    
                    if self.rotate_token():
                        logger.info(f"Rotated to token {self.current_token_index + 1}/{len(self.github_tokens)}")
                        token_rotation_attempts += 1
                        continue
                    else:
                        logger.warning("No more tokens available to rotate to")
                        # Fall back to REST API
                        logger.info("Falling back to REST API after all tokens failed")
                        return self.rest_finder.find_experts(language, max_users)
        
        # If we exhausted all token rotations, fall back to REST API
        logger.warning("All token rotation attempts exhausted, falling back to REST API")
        return self.rest_finder.find_experts(language, max_users)
    
    def _extract_user_data(self, node, target_language):
        """
        Extract and calculate score for a user.
        
        Args:
            node (dict): User data from API
            target_language (str): Target language
            
        Returns:
            dict: User information and score
        """
        login = node['login']
        followers = node['followers']['totalCount']
        repos = node['repositories']['nodes']
        pr_count = node['pullRequests']['totalCount']
        
        # Get PR review count (number of PRs commented on)
        pr_review_count = node.get('contributionsCollection', {}).get('pullRequestReviewContributions', {}).get('totalCount', 0)
        
        stars = sum(repo['stargazerCount'] for repo in repos)

        if pr_review_count < 10:
            return {
            "login": login,
            "score": 0,
            "followers": followers,
            "stars": stars,
            "prs": pr_count,
            "pr_reviews": pr_review_count
            }
        
        # Scoring formula - now includes pr_review_count
        weights = {'followers': 1, 'stars': 2, 'prs': 3, 'pr_reviews': 4}
        score = (
            (weights['followers'] * followers +
            weights['stars'] * stars + 
            weights['prs'] * pr_count) *  weights['pr_reviews'] * pr_review_count
        )
        
        return {
            "login": login,
            "score": score,
            "followers": followers,
            "stars": stars,
            "prs": pr_count,
            "pr_reviews": pr_review_count
        } 

# Add command-line functionality when run directly
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    parser = argparse.ArgumentParser(description="Find GitHub experts by programming language")
    parser.add_argument("--tokens", type=str, nargs="+", help="GitHub API tokens (provide one or multiple)", 
                        default=[os.environ.get("GITHUB_TOKEN")])
    parser.add_argument("--language", type=str, default="Python", 
                        help="Programming language to find experts for")
    parser.add_argument("--experts", type=int, default=10, 
                        help="Number of experts to find")
    parser.add_argument("--output-dir", type=str, default="data", 
                        help="Directory to save data")
    parser.add_argument("--use-rest-api", action="store_true", 
                        help="Force using REST API instead of GraphQL")
    
    args = parser.parse_args()
    
    # Filter out None values if GITHUB_TOKEN environment variable not set
    tokens = [token for token in args.tokens if token]
    
    if not tokens:
        logger.error("Missing GitHub token. Please provide via --tokens or GITHUB_TOKEN environment variable")
        exit(1)
    
    # Create output directory if it doesn't exist
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    
    # Find experts
    finder = GitHubExpertFinder(tokens)
    experts = finder.find_experts(
        language=args.language, 
        max_users=args.experts,
        use_rest_api=args.use_rest_api
    )
    
    # Save expert list
    experts_file = os.path.join(args.output_dir, f"{args.language}_experts.json")
    with open(experts_file, "w", encoding="utf-8") as f:
        json.dump(experts, f, indent=2, ensure_ascii=False)
    
    # Print expert list
    print(f"\nTop {len(experts)} {args.language} experts:")
    for i, expert in enumerate(experts, 1):
        print(f"{i}. {expert['login']}: Score={expert['score']} (Followers={expert['followers']}, " 
              f"Stars={expert['stars']}, PRs={expert['prs']}, PR Reviews={expert['pr_reviews']})") 