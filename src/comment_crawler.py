import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import json
import logging
import os
import argparse
from tqdm import tqdm
from github_api import GitHubAPI
from restapi_crawler import RestAPICommentCrawler

logger = logging.getLogger(__name__)

class GitHubCommentCrawler:
    """Crawler for GitHub comments using GraphQL API with REST API fallback."""
    
    def __init__(self, github_token):
        self.github_token = github_token
        # Initialize REST API crawler as fallback
        self.rest_crawler = RestAPICommentCrawler(github_token)
        self.api = GitHubAPI(github_token)
        
    def collect_comments(self, username, limit=200, output_file=None, continue_crawl=True, get_all_historical=False, use_rest_api=False):
        """
        Collect comments for a GitHub user.
        
        Args:
            username (str): GitHub username
            limit (int): Maximum number of comments to collect
            output_file (str): Path to save the output JSON
            continue_crawl (bool): Whether to continue from previous crawl
            get_all_historical (bool): Whether to get all historical comments
            use_rest_api (bool): Force using REST API instead of GraphQL
            
        Returns:
            list: Collected comments
        """
        if output_file is None:
            output_file = f"{username}_comments.json"
        
        # First check if we're forcing REST API
        if use_rest_api:
            logger.info(f"Using REST API for {username} as requested")
            return self.rest_crawler.collect_comments(
                username=username,
                limit=limit,
                output_file=output_file,
                continue_crawl=continue_crawl,
                get_all_historical=get_all_historical
            )
            
        try:
            # Try using GraphQL API first
            logger.info(f"Collecting comments for {username}, limit: {limit}")
                
            # GraphQL query to get PR comments
            query = """
            query ($login: String!, $after: String) {
              user(login: $login) {
                pullRequests(first: 50, after: $after) {
                  pageInfo {
                    endCursor
                    hasNextPage
                  }
                  nodes {
                    number
                    title
                    url
                    repository {
                      name
                      owner {
                        login
                      }
                      nameWithOwner
                    }
                    reviewThreads(first: 50) {
                      nodes {
                        comments(first: 50) {
                          nodes {
                            author {
                              login
                            }
                            body
                            path
                            position
                            diffHunk
                            createdAt
                            updatedAt
                            url
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """
            
            # Initialize state
            all_comments = []
            state = {"after": None, "processed_comments": set()}
            
            # Handle continue crawl or get all historical data
            if continue_crawl and os.path.exists(output_file) and not get_all_historical:
                try:
                    with open(output_file, "r", encoding="utf-8") as f:
                        all_comments = json.load(f)
                    for comment in all_comments:
                        state["processed_comments"].add(comment["comment_url"])
                    
                    state_file = f"{output_file}.state"
                    if os.path.exists(state_file):
                        with open(state_file, "r") as f:
                            loaded_state = json.load(f)
                            state["after"] = loaded_state.get("after")
                    
                    logger.info(f"Continuing crawl with {len(all_comments)} existing comments")
                except Exception as e:
                    logger.error(f"Error loading existing data: {e}")
                    all_comments = []
                    state = {"after": None, "processed_comments": set()}
            elif get_all_historical:
                logger.info("Getting all historical comments (including previously collected ones)")
            
            # Collect comments with progress bar
            with tqdm(total=limit, desc=f"Collecting comments for {username}") as pbar:
                while len(all_comments) < limit:
                    data = self.api.graphql_query(query, {"login": username, "after": state["after"]})
                    
                    if not data or 'data' not in data or not data['data'].get('user'):
                        logger.warning(f"No valid data received for {username}")
                        break
                        
                    pr_data = data['data']['user']['pullRequests']
                    nodes = pr_data.get("nodes", [])
                    state["after"] = pr_data.get("pageInfo", {}).get("endCursor")
                    
                    if not nodes:
                        break
                    
                    # Process each PR
                    for pr in nodes:
                        owner = pr["repository"]["owner"]["login"]
                        repo = pr["repository"]["name"] 
                        pr_number = pr["number"]
                        pr_title = pr["title"]
                        review_threads = pr.get("reviewThreads", {}).get("nodes", [])
                        
                        # Process each thread and comment
                        for thread in review_threads:
                            for comment in thread.get("comments", {}).get("nodes", []):
                                try:
                                    if comment["author"]["login"].lower() != username.lower():
                                        continue
                                    
                                    comment_url = comment.get("url")
                                    
                                    # Skip already processed comments unless we want all historical data
                                    if comment_url in state["processed_comments"] and not get_all_historical:
                                        continue
                                    
                                    new_comment = {
                                        "repo": f"{owner}/{repo}",
                                        "pr_number": pr_number,
                                        "pr_title": pr_title,
                                        "file_path": comment.get("path"),
                                        "position": comment.get("position"),
                                        "comment": comment.get("body"),
                                        "diff_context": comment.get("diffHunk"),
                                        "created_at": comment.get("createdAt"),
                                        "updated_at": comment.get("updatedAt"),
                                        "comment_url": comment_url,
                                    }
                                    
                                    all_comments.append(new_comment)
                                    state["processed_comments"].add(comment_url)
                                    pbar.update(1)
                                    
                                    if len(all_comments) >= limit:
                                        break
                                except Exception as e:
                                    logger.error(f"Error processing comment: {e}")
                                    continue
                    
                    # Check for next page
                    if not pr_data.get("pageInfo", {}).get("hasNextPage"):
                        break
                    
                    # Save crawl progress
                    with open(f"{output_file}.state", "w") as f:
                        json.dump({"after": state["after"]}, f)
            
            # Don't save data if no comments were found
            if not all_comments:
                logger.warning(f"No comments found for {username}. Skipping save.")
                return None
                
            # Save results
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(all_comments, f, indent=2, ensure_ascii=False)
            
            # Save crawl state
            with open(f"{output_file}.state", "w") as f:
                json.dump({"after": state["after"]}, f)
            
            logger.info(f"Saved {len(all_comments)} comments to {output_file}")
            return all_comments
            
        except Exception as e:
            # Check if the error is related to rate limiting
            error_message = str(e).lower()
            if "rate limit" in error_message or "ratelimit" in error_message or "too many requests" in error_message:
                logger.warning(f"GraphQL API rate limited, falling back to REST API for {username}")
                return self.rest_crawler.collect_comments(
                    username=username,
                    limit=limit,
                    output_file=output_file,
                    continue_crawl=continue_crawl,
                    get_all_historical=get_all_historical
                )
            else:
                # For other errors, re-raise
                logger.error(f"Error collecting comments for {username}: {e}")
                raise

# Add command-line functionality when run directly
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    parser = argparse.ArgumentParser(description="Collect PR comments from GitHub experts")
    parser.add_argument("--token", type=str, help="GitHub API token", 
                        default=os.environ.get("GITHUB_TOKEN"))
    parser.add_argument("--expert-name", type=str, required=True,
                        help="GitHub username to collect comments from")
    parser.add_argument("--comments", type=int, default=200, 
                        help="Number of comments to collect per expert")
    parser.add_argument("--output-dir", type=str, default="data", 
                        help="Directory to save data")
    parser.add_argument("--continue-crawl", action="store_true", 
                        help="Continue from previous crawl")
    parser.add_argument("--all-historical", action="store_true", 
                        help="Collect all historical comments")
    parser.add_argument("--use-rest-api", action="store_true", 
                        help="Force using REST API instead of GraphQL")
    
    args = parser.parse_args()
    
    if not args.token:
        logger.error("Missing GitHub token. Please provide via --token or GITHUB_TOKEN environment variable")
        exit(1)
    
    # Create output directory if it doesn't exist
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    
    # Initialize crawler
    crawler = GitHubCommentCrawler(args.token)
    
    # Collect comments
    output_file = os.path.join(args.output_dir, f"{args.expert_name}_comments.json")
    
    comments = crawler.collect_comments(
        username=args.expert_name,
        limit=args.comments,
        output_file=output_file,
        continue_crawl=args.continue_crawl,
        get_all_historical=args.all_historical,
        use_rest_api=args.use_rest_api
    )
    
    if comments is None or comments == []:
        print(f"No comments found for {args.expert_name}.")
        exit(1)
    
    print(f"\nSuccessfully collected {len(comments)} comments for {args.expert_name}.")
    print(f"Comments saved to {output_file}") 