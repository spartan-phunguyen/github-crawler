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

    def get_pr_comments(self, pr_url):
        """Get comments for a specific PR."""
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

        # Get diff
        diff_url = pr_data.get("diff_url")
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

        return {
            "pr_number": pr_data.get("number"),
            "pr_title": pr_data.get("title"),
            "repo": pr_url.split("/repos/")[1].split("/pulls/")[0],
            "comments": comments_response.json(),
            "diff": diff_content,
        }

    def get_comment_with_context(self, pr_data, username):
        """Extract comments with their context from PR data."""
        result = []

        if not pr_data or "comments" not in pr_data:
            logging.error(f"Invalid PR data structure: {pr_data}")
            return result

        for comment in pr_data["comments"]:
            user = comment.get("user", {}).get("login")

            # Check if None
            if user is None:
                logging.error(f"Comment has no user information: {comment}")
                continue

            # Only keep comments by the specified user
            if user.lower() != username.lower():
                continue

            body = comment.get("body")
            if not body:
                logging.error(f"Comment has empty body: {comment}")
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
                    "position": position,
                    "comment": body,
                    "diff_context": context,
                    "created_at": comment.get("created_at"),
                    "updated_at": comment.get("updated_at"),
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

        try:
            with tqdm(total=limit, desc=f"REST API: Collecting PR comments for {username}") as pbar:
                while len(all_comments) < limit or get_all_historical:
                    # Search for PRs where the user has commented
                    search_results = self.search_pull_requests(username, page, per_page)
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

                        pr_data = self.get_pr_comments(pr_url)
                        if not pr_data:
                            logging.error(f"Could not retrieve data for PR: {pr_url}")
                            continue

                        comments = self.get_comment_with_context(pr_data, username)

                        for comment in comments:
                            all_comments.append(comment)
                            if len(all_comments) <= limit:  # Only update progress for comments within limit
                                pbar.update(1)

                        # Sleep to avoid hitting rate limits
                        time.sleep(0.5)

                    page += 1

                    # If we've processed all available PRs, stop
                    if len(items) < per_page:
                        break
                    
                    # If we're not getting all historical comments and have reached the limit, break
                    if len(all_comments) >= limit and not get_all_historical:
                        break

        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")

        finally:
            # Combine with existing comments if continue_crawl is True
            if continue_crawl and existing_comments:
                # Merge but avoid duplicates
                seen_urls = {comment.get('comment_url') for comment in existing_comments}
                new_comments = [c for c in all_comments if c.get('comment_url') not in seen_urls]
                
                all_comments = existing_comments + new_comments
                logging.info(f"Combined {len(existing_comments)} existing and {len(new_comments)} new comments")
            
            # Save the collected data
            if all_comments and output_file:
                # Create directory if it doesn't exist
                output_path = Path(output_file)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(all_comments, f, indent=2, ensure_ascii=False)

                logging.info(f"Saved {len(all_comments)} comments to {output_file}")
            elif not all_comments:
                logging.warning("No comments were collected")

            return all_comments


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
