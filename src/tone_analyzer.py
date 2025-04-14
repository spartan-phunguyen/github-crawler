import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import os
import json
import time
import logging
from pathlib import Path
from openai import OpenAI

# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MapReduceToneAnalyzer:
    """
    Analyzes user tone, style, and language usage using map-reduce approach.
    Processes large comment datasets by breaking them into manageable chunks.
    """
    
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
            
        self.client = OpenAI(api_key=self.api_key)
        self.model = model
        self.rate_limit_delay = rate_limit_delay
        
        # Constants for token management
        self.COMPLETION_TOKEN_BUFFER_RATIO = 0.2  # 20% of context window for completion buffer
        self.MIN_COMPLETION_BUFFER = 1024
        self.TOKENS_PER_MESSAGE = 4  # Approximation for message overhead
        self.ESTIMATED_TOKENS_PER_WORD = 1.3  # Rough estimate
        
    def analyze_tone(self, input_file, output_file=None):
        """
        Analyze tone, style, and language usage from comment dataset.
        
        Args:
            input_file (str): Path to JSON file containing comments
            output_file (str, optional): Path to output file. Default is input_file + ".tone_analysis.json"
            
        Returns:
            dict: Analysis results with raw text
        """
        # Determine output file if not provided
        if not output_file:
            input_path = Path(input_file)
            output_file = str(input_path.with_suffix('')) + ".tone_analysis.json"
            
        # Read input data
        try:
            with open(input_file, "r", encoding="utf-8") as f:
                comments = json.load(f)
                
        except Exception as e:
            logger.error(f"Error reading input file: {e}")
            return {}
            
        logger.info(f"Analyzing {len(comments)} comments")
        
        # Use map-reduce approach if there are many comments
        max_context_size = self._estimate_max_context_size()

        if len(comments) <= 10 or self._estimate_token_count(comments) <= max_context_size:
            # Small dataset - analyze directly
            analysis = self._analyze_comments(comments)
        else:
            # Large dataset - use map-reduce
            analysis = self._map_reduce_analysis(comments, max_context_size)
            
        # Save analysis results
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(analysis, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Analysis complete! Results saved to {output_file}")
        return analysis
        
    def _map_reduce_analysis(self, comments, max_context_size):
        """
        Process comments using map-reduce approach.
        
        Args:
            comments (list): List of comment objects
            max_context_size (int): Maximum context size in tokens
            
        Returns:
            dict: Combined analysis with raw text
        """
        # Split comments into chunks
        chunks = self._chunk_comments(comments, max_context_size)
        logger.info(f"Split {len(comments)} comments into {len(chunks)} chunks")
        
        # Map phase: analyze each chunk
        chunk_analyses = []
        for i, chunk in enumerate(chunks):
            logger.info(f"Analyzing chunk {i+1}/{len(chunks)} with {len(chunk)} comments")
            chunk_analysis = self._analyze_comments(chunk)
            chunk_analyses.append(chunk_analysis)
            time.sleep(self.rate_limit_delay)
            
        # Reduce phase: combine analyses
        combined_analysis = self._reduce_analyses(chunk_analyses)
        
        return combined_analysis
        
    def _analyze_comments(self, comments):
        """
        Analyze a set of comments for tone, style, and language usage.
        
        Args:
            comments (list): List of comment objects
            
        Returns:
            dict: Analysis results with raw text
        """
        # Extract comment text
        comment_texts = []
        for comment in comments:
            text = comment.get("comment", "")  # Use comment key instead of body
            if text:
                comment_texts.append(text)
                
        if not comment_texts:
            return {"error": "No comment texts found"}
            
        # Build prompt for analysis
        system_prompt = "You are an expert in linguistic profiling and text analysis."
        
        user_prompt = """
Prompt to Analyze User Tone, Style, and Text Usage
You are an expert in linguistic profiling and text analysis.
Given the following list of user comments, analyze and summarize the user's writing tone, style, and language usage patterns.
Your output should include:
Tone: Describe the emotional tone (e.g., positive, sarcastic, formal, casual, enthusiastic, neutral, critical, etc.).
Style: Comment on sentence structure, punctuation, use of emojis or slang, use of emphasis (like capitalization or repetition), and any stylistic quirks.
Language Usage: Highlight vocabulary complexity, frequent word choices or phrases, formality level, and common linguistic patterns.
Be detailed but concise. Provide examples from the comments if necessary.
Here is the list of comments:

{}
""".format("\n\n".join([f"Comment: {text}" for text in comment_texts]))

        try:
            # Call OpenAI API
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0
            )
            content = response.choices[0].message.content.strip()
            
            # Create output with raw text only
            analysis = {
                "raw_analysis": content,
                "num_comments_analyzed": len(comment_texts)
            }
            
            return analysis
                
        except Exception as e:
            logger.error(f"Error calling OpenAI API: {e}")
            return {"error": str(e)}
            
    def _reduce_analyses(self, analyses):
        """
        Combine multiple analyses into one.
        
        Args:
            analyses (list): List of analysis results
            
        Returns:
            dict: Combined analysis with raw text
        """
        if not analyses:
            return {}
            
        if len(analyses) == 1:
            return analyses[0]
            
        # Combine all analyses into a meta-analysis
        combined_texts = []
        total_comments = 0
        
        for analysis in analyses:
            raw = analysis.get("raw_analysis", "")
            if raw:
                combined_texts.append(raw)
            num_comments = analysis.get("num_comments_analyzed", 0)
            total_comments += num_comments
            
        if not combined_texts:
            return {"error": "No valid analyses to combine"}
            
        # Meta-analysis prompt
        system_prompt = "You are an expert in linguistic profiling and text analysis."
        
        user_prompt = """
You are presented with multiple analyses of user comments. Each analysis covers tone, style, and language usage.
Synthesize these analyses into a single comprehensive analysis that captures the overall patterns.
Your output should include:
Tone: Describe the emotional tone (e.g., positive, sarcastic, formal, casual, enthusiastic, neutral, critical, etc.).
Style: Comment on sentence structure, punctuation, use of emojis or slang, use of emphasis (like capitalization or repetition), and any stylistic quirks.
Language Usage: Highlight vocabulary complexity, frequent word choices or phrases, formality level, and common linguistic patterns.
Provide examples if they appear in multiple analyses.
Here are the analyses to combine:

{}
""".format("\n\n--- ANALYSIS ---\n".join(combined_texts))

        try:
            # Call OpenAI API for meta-analysis
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0
            )
            content = response.choices[0].message.content.strip()
            
            # Create output with raw text only
            combined_analysis = {
                "raw_analysis": content,
                "num_comments_analyzed": total_comments,
                "meta_analysis": True,
                "num_analyses_combined": len(analyses)
            }
            
            return combined_analysis
                
        except Exception as e:
            logger.error(f"Error in meta-analysis: {e}")
            # Fallback: return the first analysis
            return analyses[0]
            
    def _chunk_comments(self, comments, max_tokens):
        """
        Split comments into chunks that fit within token limits.
        
        Args:
            comments (list): List of comment objects
            max_tokens (int): Maximum tokens per chunk
            
        Returns:
            list: List of comment chunks
        """
        chunks = []
        current_chunk = []
        current_token_count = 0
        
        for comment in comments:
            text = comment.get("comment", "")  # Use comment key instead of body
            if not text:
                continue
                
            # Estimate tokens for this comment
            comment_tokens = len(text.split()) * self.ESTIMATED_TOKENS_PER_WORD
            
            if (current_token_count + comment_tokens > max_tokens) and current_chunk:
                chunks.append(current_chunk)
                current_chunk = []
                current_token_count = 0
                
            current_chunk.append(comment)
            current_token_count += comment_tokens
            
        if current_chunk:
            chunks.append(current_chunk)
            
        return chunks
        
    def _estimate_max_context_size(self):
        """
        Estimate the maximum context size based on model.
        
        Returns:
            int: Estimated maximum context size in tokens
        """
        # Default context windows by model
        context_windows = {
            "gpt-3.5-turbo": 16385,
            "gpt-4": 8192,
            "gpt-4-turbo": 128000,
            "gpt-4o": 128000,
            "gpt-4o-mini": 128000,
        }
        
        base_model = self.model.split("-")[0] + "-" + self.model.split("-")[1]
        context_window = context_windows.get(self.model, context_windows.get(base_model, 4096))
        
        # Apply buffer
        completion_buffer = max(
            int(context_window * self.COMPLETION_TOKEN_BUFFER_RATIO),
            self.MIN_COMPLETION_BUFFER
        )
        
        return context_window - completion_buffer
        
    def _estimate_token_count(self, comments):
        """
        Estimate token count for a list of comments.
        
        Args:
            comments (list): List of comment objects
            
        Returns:
            int: Estimated token count
        """
        total_words = 0
        for comment in comments:
            text = comment.get("comment", "")  # Use comment key instead of body
            if text:
                total_words += len(text.split())
                
        # Add overhead for prompts
        prompt_overhead = 500  # Rough estimate
        
        return int(total_words * self.ESTIMATED_TOKENS_PER_WORD) + prompt_overhead


def main():
    """Main function to run the tone analyzer from command line."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Analyze tone, style, and language usage in GitHub comments")
    
    parser.add_argument("--input", type=str,
                      help="Path to JSON file containing comments")
    parser.add_argument("--output", type=str,
                      help="Path to output file (default: <input>.tone_analysis.json)")
    parser.add_argument("--api-key", type=str,
                      help="OpenAI API key (or use OPENAI_API_KEY env variable)")
    parser.add_argument("--model", type=str, default="gpt-4o-mini",
                      help="OpenAI model to use (default: gpt-4o-mini)")
    parser.add_argument("--delay", type=float, default=0.5,
                      help="Delay between API calls in seconds (default: 0.5)")
    
    args = parser.parse_args()
    
    try:
        analyzer = MapReduceToneAnalyzer(
            api_key=args.api_key,
            model=args.model,
            rate_limit_delay=args.delay
        )
        
        analyzer.analyze_tone(
            input_file=args.input,
            output_file=args.output
        )
        return 0
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    exit(main()) 