import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import requests
import json
import time
import logging
import os
from pathlib import Path
from tqdm import tqdm
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

class RestAPICommentCrawler:
    """GitHub comment crawler using REST API as fallback when GraphQL is rate limited."""
    
    def __init__(self, github_token):
        """Initialize the REST API crawler.
        
        Args:
            github_token (str): GitHub API token
        """
        self.github_token = github_token
        self.headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json",
        }
        
    def search_pull_requests(self, username, page=1, per_page=100):
        """Search for PRs where the user has commented."""
        url = f"https://api.github.com/search/issues?q=commenter:{username}+type:pr&page={page}&per_page={per_page}"
        try:
            response = requests.get(url, headers=self.headers)

            if response.status_code == 403:
                reset_time = int(response.headers.get("X-RateLimit-Reset", 0))
                wait_time = max(0, reset_time - int(time.time()))
                logging.warning(f"Rate limit exceeded. Waiting for {wait_time} seconds.")
                time.sleep(wait_time + 1)
                return self.search_pull_requests(username, page, per_page)

            if response.status_code != 200:
                logging.error(f"Failed to search PRs: {response.status_code} - {response.text}")
                return {"items": []}

            return response.json()
        except requests.exceptions.ConnectionError as e:
            logging.error(f"Network connection error in search_pull_requests: {e}")
            # Sleep briefly before returning to allow for retry
            time.sleep(5)
            return {"error": "connection_error", "items": []}
        except requests.exceptions.Timeout as e:
            logging.error(f"Request timeout error in search_pull_requests: {e}")
            time.sleep(5)
            return {"error": "timeout_error", "items": []}
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error in search_pull_requests: {e}")
            time.sleep(2)
            return {"error": "request_error", "items": []}

    def get_pr_comments(self, pr_url):
        """Get comments for a specific PR."""
        try:
            # Get PR details
            response = requests.get(pr_url, headers=self.headers)

            if response.status_code == 403:
                reset_time = int(response.headers.get("X-RateLimit-Reset", 0))
                wait_time = max(0, reset_time - int(time.time()))
                logging.warning(f"Rate limit exceeded. Waiting for {wait_time} seconds.")
                time.sleep(wait_time + 1)
                return self.get_pr_comments(pr_url)

            if response.status_code != 200:
                logging.error(
                    f"Failed to get PR details: {response.status_code} - {response.text}"
                )
                return None

            pr_data = response.json()

            # Get PR review comments
            comments_url = pr_data.get("review_comments_url")
            if not comments_url:
                comments_url = pr_data.get("_links", {}).get("review_comments", {}).get("href")
                if not comments_url:
                    logging.error(f"Could not find review comments URL for PR {pr_url}")
                    return None

            try:
                comments_response = requests.get(comments_url, headers=self.headers)

                if comments_response.status_code == 403:
                    reset_time = int(comments_response.headers.get("X-RateLimit-Reset", 0))
                    wait_time = max(0, reset_time - int(time.time()))
                    logging.warning(f"Rate limit exceeded. Waiting for {wait_time} seconds.")
                    time.sleep(wait_time + 1)
                    return self.get_pr_comments(pr_url)

                if comments_response.status_code != 200:
                    logging.error(
                        f"Failed to get PR comments: {comments_response.status_code} - {comments_response.text}"
                    )
                    return None
            except requests.exceptions.ConnectionError as e:
                logging.error(f"Network connection error in get_pr_comments (comments): {e}")
                return None
            except requests.exceptions.Timeout as e:
                logging.error(f"Request timeout error in get_pr_comments (comments): {e}")
                return None
            except requests.exceptions.RequestException as e:
                logging.error(f"Request error in get_pr_comments (comments): {e}")
                return None

            # Get diff
            diff_url = pr_data.get("diff_url")
            try:
                diff_response = requests.get(
                    diff_url, headers={**self.headers, "Accept": "application/vnd.github.v3.diff"}
                )

                if diff_response.status_code == 403:
                    reset_time = int(diff_response.headers.get("X-RateLimit-Reset", 0))
                    wait_time = max(0, reset_time - int(time.time()))
                    logging.warning(f"Rate limit exceeded. Waiting for {wait_time} seconds.")
                    time.sleep(wait_time + 1)
                    return self.get_pr_comments(pr_url)

                if diff_response.status_code != 200:
                    logging.error(
                        f"Failed to get PR diff: {diff_response.status_code} - {diff_response.text}"
                    )
                    diff_content = "Could not retrieve diff"
                else:
                    diff_content = diff_response.text
            except requests.exceptions.ConnectionError as e:
                logging.error(f"Network connection error in get_pr_comments (diff): {e}")
                diff_content = "Could not retrieve diff due to network error"
            except requests.exceptions.Timeout as e:
                logging.error(f"Request timeout error in get_pr_comments (diff): {e}")
                diff_content = "Could not retrieve diff due to timeout"
            except requests.exceptions.RequestException as e:
                logging.error(f"Request error in get_pr_comments (diff): {e}")
                diff_content = "Could not retrieve diff due to request error"

            return {
                "pr_number": pr_data.get("number"),
                "pr_title": pr_data.get("title"),
                "repo": pr_url.split("/repos/")[1].split("/pulls/")[0],
                "comments": comments_response.json(),
                "diff": diff_content,
            }
        except requests.exceptions.ConnectionError as e:
            logging.error(f"Network connection error in get_pr_comments (main): {e}")
            return None
        except requests.exceptions.Timeout as e:
            logging.error(f"Request timeout error in get_pr_comments (main): {e}")
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error in get_pr_comments (main): {e}")
            return None
        except Exception as e:
            logging.error(f"Error in get_pr_comments: {e}")
            return None

    def get_comment_with_context(self, pr_data, username):
        """Extract comments with their context from PR data."""
        result = []

        if not pr_data or "comments" not in pr_data:
            logging.error(f"Invalid PR data structure: {pr_data}")
            return result

        for comment in pr_data["comments"]:
            author = comment.get("user", {}).get("login")

            # Check if None
            if author is None:
                logging.error(f"Comment has no user information: {comment}")
                continue

            # Only keep comments by the specified user
            if author.lower() != username.lower():
                continue

            comment_text = comment.get("body", "")
            
            # Validate the comment before adding
            if not self.is_valid_comment(comment_text):
                continue

            path = comment.get("path")
            if not path:
                logging.error(f"Comment has no file path: {comment}")
                continue

            position = comment.get("position")
            diff_hunk = comment.get("diff_hunk")

            # Extract the relevant part of the diff
            context = diff_hunk if diff_hunk else "No diff context available"

            result.append(
                {
                    "repo": pr_data["repo"],
                    "pr_number": pr_data["pr_number"],
                    "pr_title": pr_data["pr_title"],
                    "file_path": path,
                    # "position": position,
                    "comment": comment_text,
                    "diff_context": context,
                    # "created_at": comment.get("created_at"),
                    # "updated_at": comment.get("updated_at"),
                    "comment_url": comment.get("html_url"),
                }
            )

        return result

    def collect_comments(self, username, limit=200, output_file=None, continue_crawl=True, get_all_historical=False):
        """
        Collect comments for a GitHub user using REST API.
        
        Args:
            username (str): GitHub username
            limit (int): Maximum number of comments to collect
            output_file (str): Path to save the output JSON
            continue_crawl (bool): Whether to continue from previous crawl
            get_all_historical (bool): Whether to get all historical comments
            
        Returns:
            list: Collected comments
        """
        logging.info(f"Starting to scrape PR review comments for user: {username} using REST API")
        logging.info(f"Comment limit: {limit}")
        
        # Handle existing comments if continue_crawl is True
        existing_comments = []
        if continue_crawl and output_file and os.path.exists(output_file):
            try:
                with open(output_file, 'r', encoding='utf-8') as f:
                    existing_comments = json.load(f)
                    logging.info(f"Loaded {len(existing_comments)} existing comments from {output_file}")
                    
                    # If we already have enough comments and we're not getting all historical,
                    # just return the existing comments
                    if len(existing_comments) >= limit and not get_all_historical:
                        logging.info(f"Already have {len(existing_comments)} comments, which meets the limit of {limit}")
                        return existing_comments
            except Exception as e:
                logging.error(f"Error loading existing comments: {e}")
                existing_comments = []

        all_comments = []
        page = 1
        per_page = 100
        consecutive_errors = 0
        max_consecutive_errors = 3  # Max number of consecutive errors before giving up

        try:
            with tqdm(total=limit, desc=f"REST API: Collecting PR comments for {username}") as pbar:
                while len(all_comments) < limit or get_all_historical:
                    # Search for PRs where the user has commented
                    search_results = self.search_pull_requests(username, page, per_page)
                    
                    # Check for network errors
                    if "error" in search_results:
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            logging.error(f"Too many consecutive errors ({consecutive_errors}). Aborting.")
                            break
                        logging.warning(f"Network error: {search_results['error']}. Retrying in 10 seconds... (Attempt {consecutive_errors}/{max_consecutive_errors})")
                        time.sleep(10)  # Wait longer before retry
                        continue
                    
                    # Reset error counter if successful
                    consecutive_errors = 0
                    
                    items = search_results.get("items", [])

                    if not items:
                        logging.info("No more PRs found for this user")
                        break

                    logging.info(f"Found {len(items)} PRs on page {page}")

                    for item in items:
                        if len(all_comments) >= limit and not get_all_historical:
                            break

                        pr_url = item.get("pull_request", {}).get("url")
                        if not pr_url:
                            logging.error(f"No PR URL found for item: {item}")
                            continue

                        # Check if we already have comments from this PR (for continue_crawl)
                        if continue_crawl and existing_comments:
                            pr_number = pr_url.split('/')[-1]
                            repo = pr_url.split("/repos/")[1].split("/pulls/")[0]
                            if any(c.get('repo') == repo and str(c.get('pr_number')) == pr_number for c in existing_comments):
                                logging.info(f"Skipping PR {pr_url} as we already have comments from it")
                                continue

                        logging.info(f"Processing PR: {pr_url}")

                        # Try to get PR comments with retry for network errors
                        retry_count = 0
                        max_retries = 3
                        pr_data = None
                        
                        while retry_count < max_retries:
                            pr_data = self.get_pr_comments(pr_url)
                            if pr_data:  # If we got data, break the retry loop
                                break
                            
                            retry_count += 1
                            if retry_count < max_retries:
                                logging.warning(f"Failed to get PR data, retrying in {retry_count * 5} seconds... ({retry_count}/{max_retries})")
                                time.sleep(retry_count * 5)  # Exponential backoff
                        
                        if not pr_data:
                            logging.error(f"Failed to get data for PR {pr_url} after {max_retries} retries, skipping")
                            continue

                        # Extract comments for this user
                        comments = self.get_comment_with_context(pr_data, username)
                        if comments:
                            all_comments.extend(comments)
                            pbar.update(len(comments))
                            logging.info(f"Found {len(comments)} comments in PR {pr_url}")

                    # Move to next page if we haven't collected enough comments yet
                    if (len(all_comments) < limit or get_all_historical) and len(items) == per_page:
                        page += 1
                    else:
                        break

                    # Save progress after each page
                    if output_file and all_comments:
                        with open(output_file, 'w', encoding='utf-8') as f:
                            json.dump(all_comments, f, ensure_ascii=False, indent=2)
                        logging.info(f"Saved {len(all_comments)} comments to {output_file} (progress)")

            logging.info(f"Finished collecting comments. Total: {len(all_comments)}")

        except Exception as e:
            logging.error(f"Error in collect_comments: {e}")
            import traceback
            traceback.print_exc()
            # Save what we have so far
            if output_file and all_comments:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(all_comments, f, ensure_ascii=False, indent=2)
                logging.info(f"Saved {len(all_comments)} comments to {output_file} (after error)")

        # Merge with existing comments if continue_crawl is True
        if continue_crawl and existing_comments:
            # Use comment URLs for deduplication
            existing_urls = {c.get('comment_url') for c in existing_comments if 'comment_url' in c}
            new_comments = [c for c in all_comments if c.get('comment_url') not in existing_urls]
            all_comments = existing_comments + new_comments
            logging.info(f"Added {len(new_comments)} new comments to {len(existing_comments)} existing ones")

        # Truncate to limit unless we want all historical comments
        if not get_all_historical and len(all_comments) > limit:
            all_comments = all_comments[:limit]
            logging.info(f"Truncated to {limit} comments")

        # Save final results
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
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
            logging.debug("Skipping blank comment")
            return False
            
        # Skip very short comments
        if len(comment_text.strip()) < 10:
            logging.debug(f"Skipping short comment: {comment_text.strip()}")
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
            logging.debug(f"Skipping likely non-English comment (ratio: {english_ratio:.2f})")
            return False
            
        # Additional check for common English words
        # common_words = ['the', 'a', 'an', 'and', 'or', 'but', 'if', 'this', 'that', 'is', 'are', 
        #                  'it', 'to', 'in', 'for', 'with', 'on', 'as', 'by', 'at', 'from']
        
        # words = set(text.lower().split())
        # if not any(word in words for word in common_words) and len(words) > 5:
        #     logging.debug("Comment lacks common English words, might not be English")
        #     return False
            
        return True


# For backward compatibility
def main(username, token, limit, output):
    """Original main function for backward compatibility."""
    crawler = RestAPICommentCrawler(token)
    return crawler.collect_comments(username, limit, output)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Scrape GitHub PR review comments for a user")
    parser.add_argument("--username", required=True, help="GitHub username")
    parser.add_argument("--token", required=True, help="GitHub API token")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of comments to collect")
    parser.add_argument("--output", default="comments.json", help="Output JSON file")
    parser.add_argument("--continue-crawl", action="store_true", help="Continue from previous crawl")
    parser.add_argument("--all-historical", action="store_true", help="Get all historical comments")
    
    args = parser.parse_args()
    
    crawler = RestAPICommentCrawler(args.token)
    crawler.collect_comments(
        username=args.username,
        limit=args.limit,
        output_file=args.output,
        continue_crawl=args.continue_crawl,
        get_all_historical=args.all_historical
    )
