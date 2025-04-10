import os
import json
import time
import logging
import argparse
from pathlib import Path
from openai import OpenAI

# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CommentEnricher:
    """GitHub comment classifier using OpenAI API."""
    
    def __init__(self, api_key=None, model="gpt-4o-mini", rate_limit_delay=0.5):
        """
        Initialize with OpenAI API key.
        
        Args:
            api_key (str): OpenAI API key
            model (str): OpenAI model to use
            rate_limit_delay (float): Delay between API calls (seconds)
        """
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            logger.error("OpenAI API key not found. Please provide via parameter or OPENAI_API_KEY environment variable")
            raise ValueError("Missing OpenAI API key")
            
        # Initialize client with the new API format
        self.client = OpenAI(api_key=self.api_key)
        self.model = model
        self.rate_limit_delay = rate_limit_delay
    
    def enrich_comments(self, input_file, output_file=None, continue_enrichment=False):
        """
        Enrich comment dataset with classifications from OpenAI.
        
        Args:
            input_file (str): Path to JSON file containing comments
            output_file (str, optional): Path to output file. Default is input_file + ".enriched"
            continue_enrichment (bool): Continue enrichment from previous output file
            
        Returns:
            list: List of enriched comments
        """
        # Determine output file if not provided
        if not output_file:
            input_path = Path(input_file)
            output_file = str(input_path.with_suffix('')) + ".enriched.json"
            
        # Read input data
        try:
            with open(input_file, "r", encoding="utf-8") as f:
                reviews = json.load(f)
                
            logger.info(f"Loaded {len(reviews)} comments from {input_file}")
        except Exception as e:
            logger.error(f"Error reading input file: {e}")
            return []
            
        # Check existing data
        enriched_reviews = []
        if continue_enrichment and os.path.exists(output_file):
            try:
                with open(output_file, "r", encoding="utf-8") as f:
                    enriched_reviews = json.load(f)
                logger.info(f"Continuing from {len(enriched_reviews)} previously enriched comments")
            except Exception as e:
                logger.error(f"Cannot load previously enriched data: {e}")
                enriched_reviews = []
                
        # Identify comments that haven't been enriched yet
        processed_urls = set(review.get("comment_url") for review in enriched_reviews if "comment_url" in review)
        remaining_reviews = [r for r in reviews if r.get("comment_url") not in processed_urls]
        
        logger.info(f"Need to enrich {len(remaining_reviews)} comments")
        
        # Process each comment
        for idx, review in enumerate(remaining_reviews, start=1):
            logger.info(f"Enriching comment {idx}/{len(remaining_reviews)}")
            
            # Build prompt for a single comment
            prompt = f"""
You are a code‐review classifier. Given a single GitHub review comment object in JSON, produce a JSON object with exactly these three keys:

  • review_type: one of [
      "naming_convention",
      "architecture",
      "performance",
      "security",
      "style",
      "documentation",
      "test",
      "dependency",
      "best_practice",
      "build",
      "refactor",
      "logic",
      "other"
    ]
  • language: the primary programming language of the file (infer from file_path or diff_context)
  • framework: the primary framework or library used in that file (or "none" if not applicable)

All values must be in lowercase. Output must be a JSON object (dictionary) with exactly these keys and no extra wrapping.

Here is the review comment:
{json.dumps(review, indent=2)}
"""

            try:
                # Call OpenAI API with new format
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You classify a single code review comment. Always return lowercase values."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0
                )
                content = response.choices[0].message.content.strip()
                
                # Parse JSON result
                try:
                    classification = json.loads(content)
                    
                    # Convert all string values to lowercase
                    for key, value in classification.items():
                        if isinstance(value, str):
                            classification[key] = value.lower()
                    
                    # Combine original data with classification data
                    enriched = {**review, **classification}
                    enriched_reviews.append(enriched)
                    
                    # Save after each comment to avoid data loss
                    with open(output_file, "w", encoding="utf-8") as f:
                        json.dump(enriched_reviews, f, indent=2, ensure_ascii=False)
                        
                except json.JSONDecodeError:
                    logger.error(f"Error parsing JSON from OpenAI for comment #{idx}:")
                    logger.error(content)
                    # Add original comment without enrichment
                    enriched_reviews.append(review)
                    
            except Exception as e:
                logger.error(f"Error calling OpenAI API: {e}")
                # Add original comment without enrichment
                enriched_reviews.append(review)
                
            # Pause to avoid rate limits
            time.sleep(self.rate_limit_delay)
            
        logger.info(f"Complete! Saved {len(enriched_reviews)} enriched comments to {output_file}")
        return enriched_reviews


def main():
    """Main function to run the enrichment tool from command line."""
    parser = argparse.ArgumentParser(description="Enrich GitHub comments with OpenAI classifications")
    
    parser.add_argument("--input", type=str, required=True,
                        help="Path to JSON file containing comments")
    parser.add_argument("--output", type=str,
                        help="Path to output file (default: <input>.enriched.json)")
    parser.add_argument("--api-key", type=str,
                        help="OpenAI API key (or use OPENAI_API_KEY env variable)")
    parser.add_argument("--model", type=str, default="gpt-4o-mini",
                        help="OpenAI model to use (default: gpt-4o-mini)")
    parser.add_argument("--delay", type=float, default=0.5,
                        help="Delay between API calls in seconds (default: 0.5)")
    parser.add_argument("--continue", dest="continue_enrichment", action="store_true",
                        help="Continue enrichment from previous output file")
    
    args = parser.parse_args()
    
    try:
        api_key = args.api_key
        enricher = CommentEnricher(
            api_key=api_key,
            model=args.model,
            rate_limit_delay=args.delay
        )
        
        enricher.enrich_comments(
            input_file=args.input,
            output_file=args.output,
            continue_enrichment=args.continue_enrichment
        )
        return 0
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    exit(main()) 