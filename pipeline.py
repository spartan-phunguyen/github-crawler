#!/usr/bin/env python3
"""
Automated pipeline for GitHub Expert Comment Collection and Processing.

This script automates the entire process:
1. Find experts for a given programming language
2. Collect comments from those experts
3. Enrich comments with classifications
4. Create embeddings and import to Qdrant

The data is organized in the following structure:
data/
  ├── javascript/
  │   ├── experts.json
  │   ├── pipeline_results.json
  │   └── experts/
  │       └── {expert_username}/
  │           ├── comments.json
  │           ├── comments.enriched.json
  │           └── comments.json.state
  └── python/
      └── ...
"""

import os
import logging
import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Set

# Import dotenv for .env file support
from dotenv import load_dotenv

from src.expert_finder import GitHubExpertFinder
from src.comment_crawler import GitHubCommentCrawler
from src.comment_enricher import CommentEnricher
from src.embedding_importer import CommentEmbedder

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GitHubDataPipeline:
    """Pipeline for collecting and processing GitHub expert comments."""
    
    def __init__(self, github_tokens: List[str] = None, openai_key: str = None, 
                 output_dir: str = None,
                 qdrant_url: str = None, qdrant_key: str = None,
                 openai_model: str = None, embedding_model: str = None):
        """
        Initialize the pipeline with keys from .env or parameters.
        
        Args:
            github_tokens (list, optional): List of GitHub API tokens
            openai_key (str, optional): OpenAI API key
            output_dir (str): Directory to save data
            qdrant_url (str, optional): URL to Qdrant server
            qdrant_key (str, optional): API key for Qdrant authentication
            openai_model (str, optional): OpenAI model for comment enrichment
            embedding_model (str, optional): OpenAI model for embeddings
        """
        # Load GitHub tokens from .env file if not provided
        if github_tokens is None:
            # Check for multiple tokens in env
            github_tokens = []
            
            # Try GITHUB_TOKEN
            main_token = os.getenv("GITHUB_TOKEN")
            if main_token:
                github_tokens.append(main_token)
                
            # Try GITHUB_TOKEN_1, GITHUB_TOKEN_2, GITHUB_TOKEN_3, etc.
            for i in range(1, 10):  # Check up to 9 additional tokens
                token_key = f"GITHUB_TOKEN_{i}"
                token = os.getenv(token_key)
                if token:
                    github_tokens.append(token)
        
        # Store tokens
        self.github_tokens = github_tokens if github_tokens else []
        
        # Load other settings from .env or parameters with defaults
        self.openai_key = openai_key or os.getenv("OPENAI_API_KEY")
        self.output_dir = output_dir or os.getenv("OUTPUT_DIR", "data")
        self.qdrant_url = qdrant_url or os.getenv("QDRANT_URL", "http://localhost:6333")
        self.qdrant_key = qdrant_key or os.getenv("QDRANT_API_KEY")
        self.openai_model = openai_model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.embedding_model = embedding_model or os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
        self.use_rest_api = os.getenv("USE_REST_API", "false").lower() == "true"

        # Validate required keys
        if not self.github_tokens:
            raise ValueError("At least one GitHub token is required. Set in .env file as GITHUB_TOKEN or GITHUB_TOKEN_1, GITHUB_TOKEN_2, etc.")
        
        if not self.openai_key:
            raise ValueError("OpenAI API key is required. Set in .env file as OPENAI_API_KEY.")
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize components with all tokens
        self.expert_finder = GitHubExpertFinder(self.github_tokens)  # Pass all tokens to expert finder for rotation
        self.comment_crawler = GitHubCommentCrawler(self.github_tokens)  # Comment crawler can use all tokens
        self.comment_enricher = CommentEnricher(
            api_key=self.openai_key,
            model=self.openai_model
        )
        self.embedder = CommentEmbedder(
            openai_api_key=self.openai_key,
            embedding_model=self.embedding_model,
            qdrant_url=self.qdrant_url,
            qdrant_api_key=self.qdrant_key
        )
        
        # Task management
        self.active_tasks = set()
        self.max_concurrent_tasks = int(os.getenv("MAX_CONCURRENT_TASKS", "5"))
        self.collection_tasks = {}  # username -> task
        self.enrichment_tasks = {}  # username -> task
        self.embedding_tasks = {}   # username -> task
        self.results = {
            "experts_processed": 0,
            "experts_failed": 0,
            "total_comments": 0,
            "successful_experts": [],
            "failed_experts": []
        }
        
        # Current language being processed
        self.current_language = None
        
    def get_language_dir(self, language: str) -> str:
        """Get the directory path for a specific language."""
        return os.path.join(self.output_dir, language.lower())
        
    def get_expert_dir(self, language: str, username: str) -> str:
        """Get the directory path for a specific expert within a language."""
        return os.path.join(self.get_language_dir(language), "experts", username)
    
    def get_experts_file_path(self, language: str) -> str:
        """Get the path to the experts.json file for a specific language."""
        return os.path.join(self.get_language_dir(language), "experts.json")
        
    def setup_language_dirs(self, language: str) -> None:
        """Create the directory structure for a language."""
        language_dir = self.get_language_dir(language)
        experts_dir = os.path.join(language_dir, "experts")
        os.makedirs(experts_dir, exist_ok=True)
    
    def get_existing_experts(self, language: str) -> Dict[str, Dict[str, Any]]:
        """
        Get existing experts from the experts.json file.
        
        Args:
            language (str): Programming language
            
        Returns:
            dict: Dictionary of username -> expert data
        """
        experts_file = self.get_experts_file_path(language)
        if not os.path.exists(experts_file):
            return {}
        
        try:
            with open(experts_file, "r", encoding="utf-8") as f:
                experts_list = json.load(f)
                # Convert to dictionary with username as key
                return {expert["login"]: expert for expert in experts_list}
        except Exception as e:
            logger.error(f"Error reading existing experts file: {e}")
            return {}
    
    def get_expert_comment_count(self, language: str, username: str) -> int:
        """
        Get the number of comments for an expert.
        
        Args:
            language (str): Programming language
            username (str): GitHub username
            
        Returns:
            int: Number of comments, 0 if no comments or errors
        """
        comments_file = os.path.join(self.get_expert_dir(language, username), "comments.json")
        if not os.path.exists(comments_file):
            return 0
        
        try:
            with open(comments_file, "r", encoding="utf-8") as f:
                comments = json.load(f)
                # Handle empty arrays as having no comments
                if not comments:  # This checks if the array is empty
                    logger.info(f"Comments file for {username} exists but is empty ([])")
                    return 0
                return len(comments)
        except Exception as e:
            logger.error(f"Error reading comments file for {username}: {e}")
            return 0
        
    async def find_experts(self, language: str, max_experts: int = 10) -> List[Dict[str, Any]]:
        """
        Find experts for a given programming language.
        
        Args:
            language (str): Programming language
            max_experts (int): Maximum number of experts to find
            
        Returns:
            list: List of expert information
        """
        logger.info(f"Finding top {max_experts} {language} experts...")
        
        # Setup directory structure for this language
        self.setup_language_dirs(language)
        
        # Run in a thread to avoid blocking the event loop
        experts = await asyncio.to_thread(
            self.expert_finder.find_experts,
            language=language,
            max_users=max_experts,
            use_rest_api=self.use_rest_api
        )
        
        # Save expert list in language directory
        experts_file = self.get_experts_file_path(language)
        with open(experts_file, "w", encoding="utf-8") as f:
            json.dump(experts, f, indent=2, ensure_ascii=False)
        
        # Log experts found
        logger.info(f"Found {len(experts)} {language} experts")
        for i, expert in enumerate(experts, 1):
            logger.info(f"{i}. {expert['login']}: Score={expert['score']} "
                        f"(Followers={expert['followers']}, Stars={expert['stars']}, "
                        f"PRs={expert['prs']}, PR Reviews={expert['pr_reviews']}")
        
        return experts
    
    async def collect_comments(self, username: str, language: str, comment_limit: int = 200, 
                               continue_crawl: bool = True,
                               get_all_historical: bool = False) -> Optional[List[Dict[str, Any]]]:
        """
        Collect comments for a GitHub user.
        
        Args:
            username (str): GitHub username
            language (str): Programming language
            comment_limit (int): Maximum number of comments to collect
            continue_crawl (bool): Continue from previous crawl
            get_all_historical (bool): Get all historical comments
            
        Returns:
            list: List of collected comments or None if no comments found
        """
        logger.info(f"Collecting comments for {username}...")
        expert_dir = self.get_expert_dir(language, username)
        
        # Create directory only if we proceed with crawling
        os.makedirs(expert_dir, exist_ok=True)
        
        output_file = os.path.join(expert_dir, "comments.json")
        
        # Run in a thread to avoid blocking the event loop
        comments = await asyncio.to_thread(
            self.comment_crawler.collect_comments,
            username=username,
            limit=comment_limit,
            output_file=output_file,
            continue_crawl=continue_crawl,
            get_all_historical=get_all_historical,
            use_rest_api=self.use_rest_api
        )
        
        if not comments:
            logger.warning(f"No comments found for {username}")
            # Remove empty directory if no comments were found
            try:
                if os.path.exists(expert_dir):
                    # Check if directory is empty (except for comments.json which might be empty)
                    files = os.listdir(expert_dir)
                    if len(files) <= 1 and (len(files) == 0 or "comments.json" in files):
                        # Remove directory if empty or only contains empty comments.json
                        os.remove(output_file) if os.path.exists(output_file) else None
                        os.rmdir(expert_dir)
                        logger.info(f"Removed empty expert directory for {username}")
            except Exception as e:
                logger.error(f"Error removing empty directory for {username}: {e}")
            return None
        
        # Check if comments list is empty but not None
        if isinstance(comments, list) and len(comments) == 0:
            logger.warning(f"Empty comments list for {username}")
            # Remove empty directory if empty comments list
            try:
                if os.path.exists(expert_dir):
                    # Check if directory is empty (except for comments.json which contains empty array)
                    files = os.listdir(expert_dir)
                    if len(files) <= 1 and (len(files) == 0 or "comments.json" in files):
                        # Remove directory if empty or only contains empty comments.json
                        os.remove(output_file) if os.path.exists(output_file) else None
                        os.rmdir(expert_dir)
                        logger.info(f"Removed empty expert directory for {username} (empty array)")
            except Exception as e:
                logger.error(f"Error removing empty directory for {username}: {e}")
            return None
        
        logger.info(f"Collected {len(comments)} comments for {username}")
        return comments
    
    async def enrich_comments(self, username: str, language: str,
                              continue_enrichment: bool = True) -> Optional[List[Dict[str, Any]]]:
        """
        Enrich comments with OpenAI classifications.
        
        Args:
            username (str): GitHub username
            language (str): Programming language
            continue_enrichment (bool): Continue from previous enrichment
            
        Returns:
            list: List of enriched comments or None if input file not found
        """
        logger.info(f"Enriching comments for {username}...")
        expert_dir = self.get_expert_dir(language, username)
        input_file = os.path.join(expert_dir, "comments.json")
        output_file = os.path.join(expert_dir, "comments.enriched.json")
        
        if not os.path.exists(input_file):
            logger.warning(f"Comment file for {username} not found")
            return None
        
        # Run in a thread to avoid blocking the event loop
        enriched_comments = await asyncio.to_thread(
            self.comment_enricher.enrich_comments,
            input_file=input_file,
            output_file=output_file,
            continue_enrichment=continue_enrichment
        )
        
        logger.info(f"Enriched {len(enriched_comments)} comments for {username}")
        return enriched_comments
    
    async def create_embeddings(self, username: str, language: str, collection_name: str) -> bool:
        """
        Create embeddings and import to Qdrant.
        
        Args:
            username (str): GitHub username
            language (str): Programming language
            collection_name (str): Qdrant collection name
            
        Returns:
            bool: True if successful (at least some comments were embedded), False otherwise
        """
        logger.info(f"Creating embeddings for {username} and importing to Qdrant...")
        expert_dir = self.get_expert_dir(language, username)
        input_file = os.path.join(expert_dir, "comments.enriched.json")
        
        if not os.path.exists(input_file):
            logger.warning(f"Enriched file for {username} not found")
            return False
        
        # Run in a thread to avoid blocking the event loop
        try:
            await asyncio.to_thread(
                self.embedder.process_and_upload,
                input_file=input_file,
                collection_name=collection_name
            )
            logger.info(f"Successfully created embeddings for {username} and imported to Qdrant")
            return True
        except Exception as e:
            logger.error(f"Error creating embeddings for {username}: {e}")
            return False
    
    async def create_collection_task(self, username: str, language: str, comment_limit: int, 
                                    collection_name: str, continue_crawl: bool, 
                                    continue_enrichment: bool, get_all_historical: bool) -> None:
        """
        Create a task for collecting comments and then start enrichment when done.
        
        Args:
            username (str): GitHub username
            language (str): Programming language
            comment_limit (int): Maximum number of comments to collect
            collection_name (str): Qdrant collection name
            continue_crawl (bool): Continue from previous crawl
            continue_enrichment (bool): Continue from previous enrichment
            get_all_historical (bool): Get all historical comments
        """
        comments = None
        try:
            # Collect comments
            comments = await self.collect_comments(
                username=username,
                language=language,
                comment_limit=comment_limit,
                continue_crawl=continue_crawl,
                get_all_historical=get_all_historical
            )
            
            if not comments:
                logger.warning(f"No comments collected for {username}")
                self.results["experts_failed"] += 1
                self.results["failed_experts"].append(username)
                return
            
            # Start enrichment task
            self.active_tasks.add(username)
            self.enrichment_tasks[username] = asyncio.create_task(
                self.create_enrichment_task(
                    username=username,
                    language=language,
                    collection_name=collection_name,
                    continue_enrichment=continue_enrichment
                )
            )
        except Exception as e:
            logger.error(f"Error in collection task for {username}: {e}")
            self.results["experts_failed"] += 1
            self.results["failed_experts"].append(username)
        finally:
            # Remove from active collection tasks
            if username in self.collection_tasks:
                del self.collection_tasks[username]
            # Ensure username is removed from active_tasks if no comments were found or an error occurred
            if not comments:
                self.active_tasks.discard(username)
    
    async def create_enrichment_task(self, username: str, language: str,
                                   collection_name: str, continue_enrichment: bool) -> None:
        """
        Create a task for enriching comments and then start embedding when done.
        
        Args:
            username (str): GitHub username
            language (str): Programming language
            collection_name (str): Qdrant collection name
            continue_enrichment (bool): Continue from previous enrichment
        """
        enriched = None
        try:
            # Enrich comments
            enriched = await self.enrich_comments(
                username=username,
                language=language,
                continue_enrichment=continue_enrichment
            )
            
            if not enriched:
                logger.warning(f"No enriched comments for {username}")
                self.results["experts_failed"] += 1
                self.results["failed_experts"].append(username)
                return
            
            # Start embedding task
            self.embedding_tasks[username] = asyncio.create_task(
                self.create_embedding_task(
                    username=username,
                    language=language,
                    collection_name=collection_name
                )
            )
        except Exception as e:
            logger.error(f"Error in enrichment task for {username}: {e}")
            self.results["experts_failed"] += 1
            self.results["failed_experts"].append(username)
        finally:
            # Remove from active enrichment tasks
            if username in self.enrichment_tasks:
                del self.enrichment_tasks[username]
            # Ensure username is removed from active_tasks if no enriched comments or error
            if not enriched:
                self.active_tasks.discard(username)
    
    async def create_embedding_task(self, username: str, language: str, collection_name: str) -> None:
        """
        Create a task for embedding comments and importing to Qdrant.
        
        Args:
            username (str): GitHub username
            language (str): Programming language
            collection_name (str): Qdrant collection name
        """
        try:
            # Create embeddings and import to Qdrant
            expert_dir = self.get_expert_dir(language, username)
            input_file = os.path.join(expert_dir, "comments.enriched.json")
            
            if not os.path.exists(input_file):
                logger.warning(f"Enriched file for {username} not found")
                self.results["experts_failed"] += 1
                self.results["failed_experts"].append(username)
                return

            # Process and upload to Qdrant
            logger.info(f"Creating embeddings for {username} and importing to Qdrant collection '{collection_name}'...")
            
            # First count the input comments for statistics
            try:
                with open(input_file, 'r', encoding='utf-8') as f:
                    enriched_comments = json.load(f)
                    comment_count = len(enriched_comments)
                    logger.info(f"Found {comment_count} enriched comments to embed for {username}")
            except Exception as e:
                logger.error(f"Error reading enriched comments for {username}: {e}")
                comment_count = 0
            
            # Process embeddings and upload to Qdrant
            try:
                result = await asyncio.to_thread(
                    self.embedder.process_and_upload,
                    input_file=input_file,
                    collection_name=collection_name
                )
                
                # Handle the case where process_and_upload doesn't return a value
                # We'll use the log message to infer success and the comment count for records
                records_uploaded = comment_count  # Assume all records uploaded if no errors
                
                logger.info(f"Successfully processed embeddings for {username} (estimated {records_uploaded} records)")
                
                # Mark as successful even if the return value is None
                # as long as no exception was raised
                self.results["experts_processed"] += 1
                self.results["successful_experts"].append(username)
                
                # Count comments from the original comments file
                comments_file = os.path.join(expert_dir, "comments.json")
                if os.path.exists(comments_file):
                    try:
                        with open(comments_file, 'r', encoding='utf-8') as f:
                            comments = json.load(f)
                            num_comments = len(comments)
                            self.results["total_comments"] += num_comments
                            logger.info(f"Added {num_comments} comments to total from {username}")
                    except Exception as e:
                        logger.error(f"Error counting comments for {username}: {e}")
            except Exception as e:
                logger.error(f"Error in embedder.process_and_upload for {username}: {e}")
                raise
        except Exception as e:
            logger.error(f"Error in embedding task for {username}: {e}")
            self.results["experts_failed"] += 1
            self.results["failed_experts"].append(username)
        finally:
            # Remove from active tasks
            self.active_tasks.discard(username)
            # Remove from embedding tasks
            if username in self.embedding_tasks:
                del self.embedding_tasks[username]
    
    async def run_pipeline(self) -> Dict[str, Any]:
        """
        Run the complete pipeline with settings from .env file.
        
        Returns:
            dict: Pipeline results summary
        """
        # Get pipeline settings from .env
        language = os.getenv("LANGUAGE")
        if not language:
            raise ValueError("LANGUAGE must be defined in .env file")
            
        # Store current language
        self.current_language = language.lower()
        
        # Setup directory structure for this language
        self.setup_language_dirs(self.current_language)
            
        max_experts = int(os.getenv("MAX_EXPERTS", "10"))
        comment_limit = int(os.getenv("COMMENT_LIMIT", "200"))
        collection_name = os.getenv("COLLECTION_NAME", f"github_{language.lower()}_experts")
        continue_crawl = os.getenv("CONTINUE_CRAWL", "true").lower() == "true"
        continue_enrichment = os.getenv("CONTINUE_ENRICHMENT", "true").lower() == "true"
        get_all_historical = os.getenv("ALL_HISTORICAL", "false").lower() == "true"
        
        # Initialize results
        self.results = {
            "language": language,
            "start_time": datetime.now().isoformat(),
            "experts_processed": 0,
            "experts_failed": 0,
            "total_comments": 0,
            "successful_experts": [],
            "failed_experts": []
        }
        
        # Get existing experts
        existing_experts = self.get_existing_experts(language)
        logger.info(f"Found {len(existing_experts)} existing experts for {language}")
        
        # Handle specific expert list
        expert_file = os.getenv("EXPERT_LIST_FILE")
        expert_usernames = []
        
        # First check for comma-separated list in .env
        expert_list_str = os.getenv("EXPERT_USERNAMES")
        if expert_list_str:
            expert_usernames = [name.strip() for name in expert_list_str.split(",") if name.strip()]
        
        # Then check for external file with one username per line
        if expert_file and os.path.exists(expert_file):
            try:
                # Check file extension to determine format
                if expert_file.endswith('.json'):
                    # Read JSON file with expert data
                    with open(expert_file, 'r', encoding='utf-8') as f:
                        experts_data = json.load(f)
                        # Extract usernames from the expert objects
                        file_usernames = [expert.get('login') for expert in experts_data if expert.get('login')]
                        expert_usernames.extend(file_usernames)
                else:
                    # Handle regular text file with one username per line
                    with open(expert_file, 'r', encoding='utf-8') as f:
                        file_usernames = [line.strip() for line in f if line.strip()]
                        expert_usernames.extend(file_usernames)
            except Exception as e:
                logger.error(f"Error reading expert list file: {e}")
        
        # Find experts if no specific list provided
        new_experts_data = []
        if not expert_usernames:
            new_experts_data = await self.find_experts(language, max_experts)
            new_expert_usernames = [expert["login"] for expert in new_experts_data]
            expert_usernames = new_expert_usernames
        
            # Save merged expert list (new experts plus existing experts not in new list)
            if new_experts_data:
                merged_experts = new_experts_data.copy()
                
                # Add existing experts that aren't in the new list
                new_expert_set = {expert["login"] for expert in new_experts_data}
                for username, expert_data in existing_experts.items():
                    if username not in new_expert_set:
                        merged_experts.append(expert_data)
                
                # Save the merged list
                experts_file = self.get_experts_file_path(language)
                with open(experts_file, "w", encoding="utf-8") as f:
                    json.dump(merged_experts, f, indent=2, ensure_ascii=False)
        
        # Combine new experts with existing ones that need recrawling
        experts_to_process = set()
        
        # Process each expert in the desired order
        for username in expert_usernames:
            # Add newly discovered experts
            experts_to_process.add(username)
        
        # Add existing experts with fewer comments than the limit
        for username, expert_data in existing_experts.items():
            # If the expert is not in our current list
            if username not in expert_usernames:
                # Check if they have fewer comments than the limit
                comment_count = self.get_expert_comment_count(language, username)
                if comment_count < comment_limit / 2:
                    logger.info(f"Adding existing expert {username} for recrawl (has {comment_count}/{comment_limit} comments)")
                    experts_to_process.add(username)
        
        logger.info(f"Processing {len(experts_to_process)} experts for {language}")
        
        # Process experts in a controlled parallel manner
        for username in experts_to_process:
            # Wait if we have reached the maximum number of concurrent tasks
            while len(self.active_tasks) >= self.max_concurrent_tasks:
                # Wait for any task to complete
                await asyncio.sleep(1)
            
            # Add to active tasks
            self.active_tasks.add(username)
            
            # Create collection task
            collection_task = asyncio.create_task(
                self.create_collection_task(
                    username=username,
                    language=self.current_language,
                    comment_limit=comment_limit,
                    collection_name=collection_name,
                    continue_crawl=continue_crawl,
                    continue_enrichment=continue_enrichment,
                    get_all_historical=get_all_historical
                )
            )
            self.collection_tasks[username] = collection_task
            
            # Small delay to prevent hitting API rate limits
            await asyncio.sleep(0.5)
        
        # Wait for all tasks to complete (rest of the code remains the same)
        logger.info("Waiting for all tasks to complete...")
        
        # Wait for collection tasks to complete first
        while self.collection_tasks:
            collection_task_list = list(self.collection_tasks.values())
            logger.info(f"Waiting for {len(collection_task_list)} collection tasks to complete...")
            await asyncio.gather(*collection_task_list, return_exceptions=True)
        
        # Wait for enrichment tasks
        while self.enrichment_tasks:
            enrichment_task_list = list(self.enrichment_tasks.values())
            logger.info(f"Waiting for {len(enrichment_task_list)} enrichment tasks to complete...")
            await asyncio.gather(*enrichment_task_list, return_exceptions=True)
            # Short pause to allow tasks to update
            await asyncio.sleep(0.5)
        
        # Wait for embedding tasks
        while self.embedding_tasks:
            embedding_task_list = list(self.embedding_tasks.values())
            logger.info(f"Waiting for {len(embedding_task_list)} embedding tasks to complete...")
            await asyncio.gather(*embedding_task_list, return_exceptions=True)
            # Short pause to allow tasks to update
            await asyncio.sleep(0.5)
        
        # Calculate duration
        end_time = datetime.now()
        start_time = datetime.fromisoformat(self.results["start_time"])
        duration = end_time - start_time
        self.results["end_time"] = end_time.isoformat()
        self.results["duration_seconds"] = duration.total_seconds()
        
        # Save results in language directory
        results_file = os.path.join(self.get_language_dir(language), "pipeline_results.json")
        with open(results_file, "w", encoding="utf-8") as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Pipeline completed in {duration}")
        logger.info(f"Processed {self.results['experts_processed']} experts successfully")
        logger.info(f"Failed to process {self.results['experts_failed']} experts")
        logger.info(f"Total comments collected: {self.results['total_comments']}")
        
        return self.results


async def main():
    """
    Main function to run the pipeline using settings from .env file.
    No command-line arguments are required.
    """
    try:
        # Initialize pipeline with settings from .env file
        pipeline = GitHubDataPipeline()
        
        # Log the number of GitHub tokens available
        token_count = len(pipeline.github_tokens)
        logger.info(f"Running pipeline with {token_count} GitHub token{'s' if token_count > 1 else ''}")
        
        # Run pipeline
        results = await pipeline.run_pipeline()
        
        print("\nPipeline Summary:")
        print(f"Language: {results['language']}")
        print(f"Experts processed successfully: {results['experts_processed']}")
        print(f"Experts failed: {results['experts_failed']}")
        print(f"Total comments collected: {results['total_comments']}")
        print(f"Duration: {results['duration_seconds']/60:.2f} minutes")
        
        # Results file is now in language directory
        language_dir = os.path.join(os.getenv('OUTPUT_DIR', 'data'), results['language'].lower())
        results_file = "pipeline_results.json"
        print(f"Results saved to: {os.path.join(language_dir, results_file)}")
        
        return 0
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    # Use asyncio.run() to run the async main function
    exit(asyncio.run(main())) 