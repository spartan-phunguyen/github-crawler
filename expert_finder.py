import logging
from github_api import GitHubAPI
import argparse
import json
import os

logger = logging.getLogger(__name__)

class GitHubExpertFinder:
    """Class for finding and ranking GitHub experts by language."""
    
    def __init__(self, token):
        """
        Initialize with GitHub token.
        
        Args:
            token (str): GitHub API token
        """
        self.api = GitHubAPI(token)
        
    def find_experts(self, language, max_users=30):
        """
        Find and rank experts by programming language.
        
        Args:
            language (str): Programming language (Python, JavaScript,...)
            max_users (int): Maximum number of users to find
            
        Returns:
            list: List of ranked users
        """
        logger.info(f"Finding {language} experts...")
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
            query_string = f"language:{language} followers:>1000 repos:>50"
            variables = {"queryString": query_string, "after": after_cursor}
            
            data = self.api.graphql_query(query, variables)
            if not data or 'data' not in data:
                logger.warning("No valid data received from API")
                break
                
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
        # Sort results by score
        return sorted(results, key=lambda x: x['score'], reverse=True)
        
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
    parser.add_argument("--token", type=str, help="GitHub API token", 
                        default=os.environ.get("GITHUB_TOKEN"))
    parser.add_argument("--language", type=str, default="Python", 
                        help="Programming language to find experts for")
    parser.add_argument("--experts", type=int, default=10, 
                        help="Number of experts to find")
    parser.add_argument("--output-dir", type=str, default="data", 
                        help="Directory to save data")
    
    args = parser.parse_args()
    
    if not args.token:
        logger.error("Missing GitHub token. Please provide via --token or GITHUB_TOKEN environment variable")
        exit(1)
    
    # Create output directory if it doesn't exist
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    
    # Find experts
    finder = GitHubExpertFinder(args.token)
    experts = finder.find_experts(args.language, max_users=args.experts)
    
    # Save expert list
    experts_file = os.path.join(args.output_dir, f"{args.language}_experts.json")
    with open(experts_file, "w", encoding="utf-8") as f:
        json.dump(experts, f, indent=2, ensure_ascii=False)
    
    # Print expert list
    print(f"\nTop {len(experts)} {args.language} experts:")
    for i, expert in enumerate(experts, 1):
        print(f"{i}. {expert['login']}: Score={expert['score']} (Followers={expert['followers']}, " 
              f"Stars={expert['stars']}, PRs={expert['prs']}, PR Reviews={expert['pr_reviews']})") 