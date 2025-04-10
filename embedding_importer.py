#!/usr/bin/env python3
"""
Tool to create embeddings from GitHub comments and import them into Qdrant.
"""

import os
import json
import logging
import argparse
from pathlib import Path
from typing import List, Dict, Any
import time
from openai import OpenAI
from qdrant_client import QdrantClient
from qdrant_client.http import models
import uuid
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CommentEmbedder:
    """Class for creating embeddings from GitHub comments and importing to Qdrant."""
    
    def __init__(self, openai_api_key=None, embedding_model="text-embedding-3-small", 
                 qdrant_url="http://localhost:6333", qdrant_api_key=None, 
                 batch_size=100, rate_limit_delay=0.1):
        """
        Initialize embedder with API keys and connection settings.
        
        Args:
            openai_api_key (str): OpenAI API key
            embedding_model (str): OpenAI embedding model to use
            qdrant_url (str): URL to Qdrant server
            qdrant_api_key (str): API key for Qdrant authentication
            batch_size (int): Number of vectors to upload in each batch
            rate_limit_delay (float): Delay between API calls in seconds
        """
        # Initialize OpenAI client
        self.openai_api_key = openai_api_key
        if not self.openai_api_key:
            raise ValueError("OpenAI API key not found. Please provide via parameter or OPENAI_API_KEY environment variable")
        
        self.openai_client = OpenAI(api_key=self.openai_api_key)
        self.embedding_model = embedding_model
        self.batch_size = batch_size
        self.rate_limit_delay = rate_limit_delay
        
        # Initialize Qdrant client with API key authentication
        if qdrant_api_key:
            self.qdrant_client = QdrantClient(url=qdrant_url, api_key=qdrant_api_key)
        else:
            # Try to connect without authentication
            self.qdrant_client = QdrantClient(url=qdrant_url)
        logger.info(f"Connected to Qdrant at {qdrant_url}")
    
    def prepare_text_for_embedding(self, comment: Dict[str, Any], expert_name: str = None) -> str:
        """
        Prepare a single comment for embedding by extracting relevant text.
        
        Args:
            comment (dict): A GitHub comment entry
            expert_name (str, optional): Name of the expert who wrote the comment
            
        Returns:
            str: Text prepared for embedding
        """
        # Add expert_name to the comment dictionary if provided
        if expert_name and 'expert_name' not in comment:
            comment_copy = comment.copy()
            comment_copy['expert_name'] = expert_name
        else:
            comment_copy = comment
        
        # Simply convert the entire dictionary to a formatted JSON string
        return json.dumps(comment_copy, indent=2, ensure_ascii=False)
    
    def create_embedding(self, text: str) -> List[float]:
        """
        Create an embedding vector for a text using OpenAI's API.
        
        Args:
            text (str): Text to embed
            
        Returns:
            list: Embedding vector or None if error occurs
        """
        try:
            response = self.openai_client.embeddings.create(
                model=self.embedding_model,
                input=text
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Error creating embedding: {e}")
            return None  # Return None instead of raising exception
    
    def create_collection(self, collection_name: str, vector_size: int = 1536):
        """
        Create a Qdrant collection for storing embeddings.
        
        Args:
            collection_name (str): Name of the collection
            vector_size (int): Size of embedding vectors
        """
        try:
            # Check if collection already exists
            collections = self.qdrant_client.get_collections().collections
            collection_names = [collection.name for collection in collections]
            
            if collection_name in collection_names:
                logger.info(f"Collection '{collection_name}' already exists")
                return
            
            # Create the collection
            self.qdrant_client.create_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(
                    size=vector_size,
                    distance=models.Distance.COSINE
                )
            )
            logger.info(f"Created collection '{collection_name}' with vector size {vector_size}")
        except Exception as e:
            logger.error(f"Error creating collection: {e}")
            raise
    
    def process_and_upload(self, input_file: str, collection_name: str):
        """
        Process comments from JSON file, create embeddings, and upload to Qdrant.
        
        Args:
            input_file (str): Path to JSON file with comments
            collection_name (str): Name of the Qdrant collection
        """
        # Extract expert name from filename
        input_path = Path(input_file)
        filename = input_path.name
        expert_name = None
        
        # Try to extract expert name from filename patterns like "ExpertName_comments.json"
        if "_comments" in filename:
            expert_name = filename.split("_comments")[0]
        
        logger.info(f"Processing comments for expert: {expert_name or 'Unknown'}")
        
        # Load comments from file
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                comments = json.load(f)
            logger.info(f"Loaded {len(comments)} comments from {input_file}")
        except Exception as e:
            logger.error(f"Error loading comments: {e}")
            return
        
        # Add expert_name to each comment if not already present
        for comment in comments:
            if expert_name and 'expert_name' not in comment:
                comment['expert_name'] = expert_name
        
        # Create collection if it doesn't exist
        # First, create a sample embedding to get the vector size
        sample_text = self.prepare_text_for_embedding(comments[0], expert_name)
        sample_embedding = self.create_embedding(sample_text)
        vector_size = len(sample_embedding)
        
        self.create_collection(collection_name, vector_size)
        
        # Process comments in batches
        total_comments = len(comments)
        batch_points = []
        skipped_comments = 0
        
        for i, comment in enumerate(comments):
            # Generate a deterministic UUID based on comment content
            # This ensures the same comment always gets the same ID
            
            # Create a unique string based on available fields
            unique_string = ""
            if 'comment_url' in comment and comment['comment_url']:
                unique_string = comment['comment_url']
            else:
                # Create a unique string from multiple fields if URL isn't available
                unique_string = f"{comment.get('repo', '')}-{comment.get('pr_number', '')}-{comment.get('created_at', '')}-{i}"
            
            # Create a deterministic UUID from the unique string
            hash_bytes = hashlib.md5(unique_string.encode('utf-8')).digest()
            comment_id = str(uuid.UUID(bytes=hash_bytes[:16]))
            
            logger.debug(f"Generated UUID for comment {i}: {comment_id} from {unique_string}")
            
            # Prepare text and create embedding
            text = self.prepare_text_for_embedding(comment, expert_name)
            
            embedding = self.create_embedding(text)
            
            # Skip this comment if embedding failed (too long, etc.)
            if embedding is None:
                skipped_comments += 1
                logger.warning(f"Skipping comment {i+1}/{total_comments} (too long or error occurred)")
                continue
            
            # Prepare point for Qdrant
            point = models.PointStruct(
                id=comment_id,  # UUID-based ID
                vector=embedding,
                payload=comment
            )
            
            batch_points.append(point)
            
            # Upload batch if it reaches batch size or is the last item
            if len(batch_points) >= self.batch_size or i == total_comments - 1:
                if batch_points:  # Only upload if we have points
                    self.qdrant_client.upsert(
                        collection_name=collection_name,
                        points=batch_points
                    )
                    logger.info(f"Uploaded batch of {len(batch_points)} vectors to Qdrant ({i+1}/{total_comments})")
                batch_points = []
            
            # Add delay to avoid rate limiting
            time.sleep(self.rate_limit_delay)
        
        if skipped_comments > 0:
            logger.warning(f"Skipped {skipped_comments} comments due to length or errors")
        
        logger.info(f"Completed upload of {total_comments - skipped_comments} comments to Qdrant collection '{collection_name}'")


def main():
    """Main function to run the embedding and import tool from command line."""
    parser = argparse.ArgumentParser(description="Embed GitHub comments and import to Qdrant")
    
    parser.add_argument("--input", type=str, required=True,
                        help="Path to enriched comments JSON file")
    parser.add_argument("--collection", type=str, default="github_comments",
                        help="Qdrant collection name")
    parser.add_argument("--openai-key", type=str,
                        help="OpenAI API key (or use OPENAI_API_KEY env variable)")
    parser.add_argument("--model", type=str, default="text-embedding-3-small",
                        help="OpenAI embedding model (default: text-embedding-3-small)")
    parser.add_argument("--qdrant-url", type=str, default="http://localhost:6333",
                        help="Qdrant server URL")
    parser.add_argument("--qdrant-key", type=str,
                        help="Qdrant API key for authentication")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Batch size for Qdrant uploads")
    parser.add_argument("--delay", type=float, default=0.1,
                        help="Delay between API calls in seconds")
    
    args = parser.parse_args()
    
    try:
        # Initialize and run embedder
        embedder = CommentEmbedder(
            openai_api_key=args.openai_key,
            embedding_model=args.model,
            qdrant_url=args.qdrant_url,
            qdrant_api_key=args.qdrant_key,
            batch_size=args.batch_size,
            rate_limit_delay=args.delay
        )
        
        embedder.process_and_upload(
            input_file=args.input,
            collection_name=args.collection
        )
        
        return 0
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    exit(main()) 