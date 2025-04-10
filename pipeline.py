#!/usr/bin/env python3
"""
Automated pipeline for GitHub Expert Comment Collection and Processing.

This script automates the entire process:
1. Find experts for a given programming language
2. Collect comments from those experts
3. Enrich comments with classifications
4. Create embeddings and import to Qdrant
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
    
    def __init__(self, github_token: str = None, openai_key: str = None, 
                 output_dir: str = None,
                 qdrant_url: str = None, qdrant_key: str = None,
                 openai_model: str = None, embedding_model: str = None):
        """
        Initialize the pipeline with keys from .env or parameters.
        
        Args:
            github_token (str, optional): GitHub API token
            openai_key (str, optional): OpenAI API key
            output_dir (str): Directory to save data
            qdrant_url (str, optional): URL to Qdrant server
            qdrant_key (str, optional): API key for Qdrant authentication
            openai_model (str, optional): OpenAI model for comment enrichment
            embedding_model (str, optional): OpenAI model for embeddings
        """
        # Load keys from .env or parameters with defaults
        self.github_token = github_token or os.getenv("GITHUB_TOKEN")
        self.openai_key = openai_key or os.getenv("OPENAI_API_KEY")
        self.output_dir = output_dir or os.getenv("OUTPUT_DIR", "data")
        self.qdrant_url = qdrant_url or os.getenv("QDRANT_URL", "http://localhost:6333")
        self.qdrant_key = qdrant_key or os.getenv("QDRANT_API_KEY")
        self.openai_model = openai_model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.embedding_model = embedding_model or os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
        self.use_rest_api = os.getenv("USE_REST_API", "false").lower() == "true"

        # Validate required keys
        if not self.github_token:
            raise ValueError("GitHub token is required. Set in .env file as GITHUB_TOKEN.")
        
        if not self.openai_key:
            raise ValueError("OpenAI API key is required. Set in .env file as OPENAI_API_KEY.")
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize components
        self.expert_finder = GitHubExpertFinder(self.github_token)
        self.comment_crawler = GitHubCommentCrawler(self.github_token)
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
        
        # Run in a thread to avoid blocking the event loop
        experts = await asyncio.to_thread(
            self.expert_finder.find_experts,
            language=language,
            max_users=max_experts,
            use_rest_api=self.use_rest_api
        )
        
        # Save expert list
        experts_file = os.path.join(self.output_dir, f"{language}_experts.json")
        with open(experts_file, "w", encoding="utf-8") as f:
            json.dump(experts, f, indent=2, ensure_ascii=False)
        
        # Log experts found
        logger.info(f"Found {len(experts)} {language} experts")
        for i, expert in enumerate(experts, 1):
            logger.info(f"{i}. {expert['login']}: Score={expert['score']} "
                        f"(Followers={expert['followers']}, Stars={expert['stars']}, "
                        f"PRs={expert['prs']}, PR Reviews={expert['pr_reviews']})")
        
        return experts
    
    async def collect_comments(self, username: str, comment_limit: int = 200, 
                               continue_crawl: bool = True,
                               get_all_historical: bool = False) -> Optional[List[Dict[str, Any]]]:
        """
        Collect comments for a GitHub user.
        
        Args:
            username (str): GitHub username
            comment_limit (int): Maximum number of comments to collect
            continue_crawl (bool): Continue from previous crawl
            get_all_historical (bool): Get all historical comments
            
        Returns:
            list: List of collected comments or None if no comments found
        """
        logger.info(f"Collecting comments for {username}...")
        output_file = os.path.join(self.output_dir, f"{username}_comments.json")
        
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
            return None
        
        logger.info(f"Collected {len(comments)} comments for {username}")
        return comments
    
    async def enrich_comments(self, username: str, 
                              continue_enrichment: bool = True) -> Optional[List[Dict[str, Any]]]:
        """
        Enrich comments with OpenAI classifications.
        
        Args:
            username (str): GitHub username
            continue_enrichment (bool): Continue from previous enrichment
            
        Returns:
            list: List of enriched comments or None if input file not found
        """
        logger.info(f"Enriching comments for {username}...")
        input_file = os.path.join(self.output_dir, f"{username}_comments.json")
        output_file = os.path.join(self.output_dir, f"{username}_comments.enriched.json")
        
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
    
    async def create_embeddings(self, username: str, collection_name: str) -> bool:
        """
        Create embeddings and import to Qdrant.
        
        Args:
            username (str): GitHub username
            collection_name (str): Qdrant collection name
            
        Returns:
            bool: True if successful (at least some comments were embedded), False otherwise
        """
        logger.info(f"Creating embeddings for {username} and importing to Qdrant...")
        input_file = os.path.join(self.output_dir, f"{username}_comments.enriched.json")
        
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
            # Even if there's an error, we consider it a partial success if some comments were processed
            # Only return False if there was a catastrophic error
            return False
    
    async def process_expert(self, username: str, comment_limit: int, collection_name: str,
                            continue_crawl: bool = True, continue_enrichment: bool = True,
                            get_all_historical: bool = False) -> bool:
        """
        Process a single expert (collect, enrich, embed).
        
        Args:
            username (str): GitHub username
            comment_limit (int): Maximum number of comments to collect
            collection_name (str): Qdrant collection name
            continue_crawl (bool): Continue from previous crawl
            continue_enrichment (bool): Continue from previous enrichment
            get_all_historical (bool): Get all historical comments
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Collect comments
        comments = await self.collect_comments(
            username=username,
            comment_limit=comment_limit,
            continue_crawl=continue_crawl,
            get_all_historical=get_all_historical
        )
        
        if not comments:
            return False
        
        # Enrich comments
        enriched = await self.enrich_comments(
            username=username,
            continue_enrichment=continue_enrichment
        )
        
        if not enriched:
            return False
        
        # Create embeddings and import to Qdrant
        return await self.create_embeddings(
            username=username,
            collection_name=collection_name
        )
    
    async def create_collection_task(self, username: str, comment_limit: int, collection_name: str,
                                    continue_crawl: bool, continue_enrichment: bool, 
                                    get_all_historical: bool) -> None:
        """
        Create a task for collecting comments and then start enrichment when done.
        
        Args:
            username (str): GitHub username
            comment_limit (int): Maximum number of comments to collect
            collection_name (str): Qdrant collection name
            continue_crawl (bool): Continue from previous crawl
            continue_enrichment (bool): Continue from previous enrichment
            get_all_historical (bool): Get all historical comments
        """
        try:
            # Collect comments
            comments = await self.collect_comments(
                username=username,
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
    
    async def create_enrichment_task(self, username: str, collection_name: str, 
                                   continue_enrichment: bool) -> None:
        """
        Create a task for enriching comments and then start embedding when done.
        
        Args:
            username (str): GitHub username
            collection_name (str): Qdrant collection name
            continue_enrichment (bool): Continue from previous enrichment
        """
        try:
            # Enrich comments
            enriched = await self.enrich_comments(
                username=username,
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
    
    async def create_embedding_task(self, username: str, collection_name: str) -> None:
        """
        Create a task for embedding comments and importing to Qdrant.
        
        Args:
            username (str): GitHub username
            collection_name (str): Qdrant collection name
        """
        try:
            # Create embeddings and import to Qdrant
            input_file = os.path.join(self.output_dir, f"{username}_comments.enriched.json")
            
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
                comments_file = os.path.join(self.output_dir, f"{username}_comments.json")
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
        if not expert_usernames:
            experts = await self.find_experts(language, max_experts)
            expert_usernames = [expert["login"] for expert in experts]
        
        logger.info(f"Processing {len(expert_usernames)} experts for {language}")
        
        # Process experts in a controlled parallel manner
        for username in expert_usernames:
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
        
        # Proper task tracking and completion waiting
        logger.info("Waiting for all tasks to complete...")
        
        # Wait for collection tasks to complete first
        if self.collection_tasks:
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
        
        # Make sure all active tasks are done
        while self.active_tasks:
            logger.info(f"Waiting for {len(self.active_tasks)} remaining active tasks to complete...")
            await asyncio.sleep(2)  # Check every 2 seconds
        
        # Calculate duration
        end_time = datetime.now()
        start_time = datetime.fromisoformat(self.results["start_time"])
        duration = end_time - start_time
        self.results["end_time"] = end_time.isoformat()
        self.results["duration_seconds"] = duration.total_seconds()
        
        # Save results
        results_file = os.path.join(self.output_dir, f"{language}_pipeline_results.json")
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
        
        # Run pipeline
        results = await pipeline.run_pipeline()
        
        print("\nPipeline Summary:")
        print(f"Language: {results['language']}")
        print(f"Experts processed successfully: {results['experts_processed']}")
        print(f"Experts failed: {results['experts_failed']}")
        print(f"Total comments collected: {results['total_comments']}")
        print(f"Duration: {results['duration_seconds']/60:.2f} minutes")
        
        # Fix the syntax error in this line
        output_dir = os.getenv('OUTPUT_DIR', 'data')
        results_file = f"{results['language']}_pipeline_results.json"
        print(f"Results saved to: {os.path.join(output_dir, results_file)}")
        
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