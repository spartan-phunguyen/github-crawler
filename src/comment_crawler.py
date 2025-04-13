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
    """Crawler for GitHub comments using GraphQL API with token rotation and REST API fallback."""
    
    def __init__(self, github_tokens):
        """
        Initialize the crawler with one or multiple GitHub tokens.
        
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
        
        # Initialize REST API crawler as ultimate fallback
        self.rest_crawler = RestAPICommentCrawler(self.github_tokens[0])
    
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
        self.rest_crawler = RestAPICommentCrawler(new_token)
        
        logger.info(f"Rotated to GitHub token {self.current_token_index + 1}/{len(self.github_tokens)}")
        return True
        
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
        
        # Try GraphQL API with token rotation on rate limit
        token_rotation_attempts = 0
        max_token_rotations = len(self.github_tokens)
        
        # Initialize state
        all_comments = []
        state = {"after": None, "processed_comments": set()}
        
        # Load existing comments if continuing
        if continue_crawl and os.path.exists(output_file) and not get_all_historical:
            try:
                with open(output_file, "r", encoding="utf-8") as f:
                    all_comments = json.load(f)
                for comment in all_comments:
                    # Get URL if it exists in the comment
                    if "comment_url" in comment:
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
        
        while token_rotation_attempts <= max_token_rotations:
            try:
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
                
                # Collect comments with progress bar
                with tqdm(total=limit, initial=len(all_comments), desc=f"Collecting comments for {username}") as pbar:
                    while len(all_comments) < limit:
                        data = self.api.graphql_query(query, {"login": username, "after": state["after"]})
                        
                        # Check for network errors and rotate token if needed
                        if isinstance(data, dict) and "error" in data:
                            error_type = data["error"]
                            if error_type in ["connection_error", "timeout_error", "request_error"]:
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
                                    # If no more tokens and already using REST API, we're out of options
                                    if use_rest_api:
                                        logger.error("Both GraphQL and REST API failed. Giving up.")
                                        break
                                    else:
                                        # Fallback to REST API as last resort
                                        logger.info("Falling back to REST API as last resort")
                                        return self.rest_crawler.collect_comments(
                                            username=username,
                                            limit=limit,
                                            output_file=output_file,
                                            continue_crawl=continue_crawl,
                                            get_all_historical=get_all_historical
                                        )
                        
                        # Check for GitHub API errors in the response
                        if isinstance(data, dict) and "errors" in data:
                            error_message = str(data["errors"])
                            logger.warning(f"GitHub API error: {error_message}")
                            
                            # Check if rate limiting error
                            if "rate limit" in error_message.lower() or "ratelimit" in error_message.lower():
                                logger.info("Rate limit error detected in response. Attempting token rotation.")
                                if self.rotate_token():
                                    logger.info(f"Rotated to token {self.current_token_index + 1}/{len(self.github_tokens)}")
                                    token_rotation_attempts += 1
                                    import time
                                    time.sleep(2)
                                    continue
                            # For other errors, try rotating anyway
                            elif self.rotate_token():
                                logger.info(f"Rotated to token {self.current_token_index + 1}/{len(self.github_tokens)} due to API error")
                                token_rotation_attempts += 1
                                import time
                                time.sleep(2)
                                continue
                            else:
                                # If no more tokens, try REST API
                                if not use_rest_api:
                                    logger.info("Falling back to REST API due to GitHub API errors")
                                    return self.rest_crawler.collect_comments(
                                        username=username,
                                        limit=limit,
                                        output_file=output_file,
                                        continue_crawl=continue_crawl,
                                        get_all_historical=get_all_historical
                                    )
                                else:
                                    logger.error("Both GraphQL and REST API failed with errors. Giving up.")
                                    break
                        
                        if not data or 'data' not in data or not data['data'].get('user'):
                            logger.warning(f"No valid data received for {username}")
                            # Try to rotate token and retry instead of breaking
                            if self.rotate_token():
                                logger.info(f"Rotating to token {self.current_token_index + 1}/{len(self.github_tokens)} due to invalid data")
                                token_rotation_attempts += 1
                                # Short wait before retrying
                                import time
                                time.sleep(2)
                                continue
                            else:
                                # If we can't rotate tokens, fall back to REST API if not already using it
                                if not use_rest_api:
                                    logger.info("No more tokens available. Falling back to REST API due to invalid data")
                                    return self.rest_crawler.collect_comments(
                                        username=username,
                                        limit=limit,
                                        output_file=output_file,
                                        continue_crawl=continue_crawl,
                                        get_all_historical=get_all_historical
                                    )
                                else:
                                    logger.error("Both GraphQL and REST API failed to return valid data. Giving up.")
                                    break
                        
                        pr_data = data['data']['user']['pullRequests']
                        nodes = pr_data.get("nodes", [])
                        state["after"] = pr_data.get("pageInfo", {}).get("endCursor")
                        
                        if not nodes:
                            # If there are no nodes but we expected some (first page with no results)
                            if not state["after"]:
                                logger.warning(f"No PR nodes found for {username}")
                                # Try to rotate token and retry
                                if self.rotate_token():
                                    logger.info(f"Rotating to token {self.current_token_index + 1}/{len(self.github_tokens)} due to empty PR list")
                                    token_rotation_attempts += 1
                                    # Short wait before retrying
                                    import time
                                    time.sleep(2)
                                    continue
                                else:
                                    # If we can't rotate tokens, fall back to REST API
                                    if not use_rest_api:
                                        logger.info("No more tokens available. Falling back to REST API due to empty PR list")
                                        return self.rest_crawler.collect_comments(
                                            username=username,
                                            limit=limit,
                                            output_file=output_file,
                                            continue_crawl=continue_crawl,
                                            get_all_historical=get_all_historical
                                        )
                            # Normal case of reaching the end of pages with data
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
                                        
                                        # Get the comment body
                                        comment_body = comment.get("body", "")
                                        
                                        # Check if comment is valid (not too short, in English, etc.)
                                        if not self.is_valid_comment(comment_body):
                                            logger.debug(f"Skipping invalid comment from {username}")
                                            continue
                                        
                                        new_comment = {
                                            "repo": f"{owner}/{repo}",
                                            "pr_number": pr_number,
                                            "pr_title": pr_title,
                                            "file_path": comment.get("path"),
                                            # "position": comment.get("position"),
                                            "comment": comment_body,
                                            "diff_context": comment.get("diffHunk"),
                                            # "created_at": comment.get("createdAt"),
                                            # "updated_at": comment.get("updatedAt"),
                                            "comment_url": comment_url,  # Keep URL for deduplication
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
                
                # If we got here without exceptions, we're done
                break
                
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
                        
                        # As last resort, try REST API
                        if not use_rest_api:
                            logger.info("Falling back to REST API")
                            return self.rest_crawler.collect_comments(
                                username=username,
                                limit=limit,
                                output_file=output_file,
                                continue_crawl=continue_crawl,
                                get_all_historical=get_all_historical
                            )
                else:
                    # For network errors and other issues, also try to rotate token
                    logger.error(f"Error collecting comments via GraphQL: {e}")
                    logger.info("Attempting to rotate token due to error")
                    
                    if self.rotate_token():
                        logger.info(f"Rotated to token {self.current_token_index + 1}/{len(self.github_tokens)}")
                        token_rotation_attempts += 1
                        continue
                    else:
                        # If we ran out of tokens, fall back to REST API
                        if not use_rest_api:
                            logger.info("Falling back to REST API after all tokens failed")
                            return self.rest_crawler.collect_comments(
                                username=username,
                                limit=limit,
                                output_file=output_file,
                                continue_crawl=continue_crawl,
                                get_all_historical=get_all_historical
                            )
                
                # If we've tried all tokens or fallbacks, break the loop
                break
        
        # Save all comments
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_comments, f, ensure_ascii=False, indent=2)
        
        print(f"Comments saved to {output_file}")
        return all_comments

    def is_valid_comment(self, comment_text):
        """
        Check if a comment is valid for collection.
        
        Criteria:
        - Not blank or only whitespace
        - Not too short (at least 10 characters)
        - Appears to be in English (heuristic check)
        
        Args:
            comment_text (str): The comment text to validate
            
        Returns:
            bool: True if the comment is valid, False otherwise
        """
        if not comment_text or not comment_text.strip():
            logger.debug("Skipping blank comment")
            return False
            
        # Skip very short comments
        if len(comment_text.strip()) < 10:
            logger.debug(f"Skipping short comment: {comment_text.strip()}")
            return False
            
        # Simple heuristic to check if comment is likely in English
        # Count English alphabet characters vs. total non-whitespace characters
        text = comment_text.strip()
        alpha_count = sum(c.isalpha() and c.isascii() for c in text)
        non_space_count = sum(not c.isspace() for c in text)
        
        if non_space_count == 0:
            return False
            
        # If less than 40% of characters are English alphabet letters, likely not English
        english_ratio = alpha_count / non_space_count
        if english_ratio < 0.4:
            logger.debug(f"Skipping likely non-English comment (ratio: {english_ratio:.2f})")
            return False
            
        # Additional check for common English words
        # common_words = ['the', 'a', 'an', 'and', 'or', 'but', 'if', 'this', 'that', 'is', 'are', 
        #                  'it', 'to', 'in', 'for', 'with', 'on', 'as', 'by', 'at', 'from']
        
        # words = set(text.lower().split())
        # if not any(word in words for word in common_words) and len(words) > 5:
        #     logger.debug("Comment lacks common English words, might not be English")
        #     return False
            
        return True

# Add command-line functionality when run directly
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    parser = argparse.ArgumentParser(description="Collect PR comments from GitHub experts")
    parser.add_argument("--tokens", type=str, nargs="+", help="GitHub API tokens (provide one or multiple)",
                        default=[os.environ.get("GITHUB_TOKEN")])
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
    
    # Filter out None values if GITHUB_TOKEN environment variable not set
    tokens = [token for token in args.tokens if token]
    
    if not tokens:
        logger.error("Missing GitHub token. Please provide via --tokens or GITHUB_TOKEN environment variable")
        exit(1)
    
    # Create output directory if it doesn't exist
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    
    # Initialize crawler with tokens
    crawler = GitHubCommentCrawler(tokens)
    
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