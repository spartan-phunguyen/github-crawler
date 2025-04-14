import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import os
import json
import logging
import argparse
from pathlib import Path
from src.tone_analyzer import MapReduceToneAnalyzer

# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ToneAnalysisPipeline:
    """
    Pipeline for analyzing tone, style, and language usage in GitHub comments.
    This pipeline is separate from other analysis pipelines.
    """
    
    def __init__(self, api_key=None, model="gpt-4o-mini", data_dir="data"):
        """
        Initialize the pipeline.
        
        Args:
            api_key (str): OpenAI API key
            model (str): OpenAI model to use
            data_dir (str): Directory for input/output data
        """
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model
        self.data_dir = Path(data_dir)
        
        # Initialize the tone analyzer
        self.analyzer = MapReduceToneAnalyzer(
            api_key=self.api_key,
            model=self.model
        )
        
    def run(self, input_file, output_dir=None, file_pattern="*.json"):
        """
        Run the tone analysis pipeline on a single file or directory.
        
        Args:
            input_file (str): Path to input file or directory
            output_dir (str, optional): Directory for output files
            file_pattern (str): File pattern to match if input_file is a directory
            
        Returns:
            list: Paths to output files
        """
        input_path = Path(input_file)
        
        # Determine output directory
        if output_dir:
            output_path = Path(output_dir)
        else:
            output_path = self.data_dir / "tone_analysis"
            
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Collect input files
        input_files = []
        if input_path.is_dir():
            input_files = list(input_path.glob(file_pattern))
            logger.info(f"Found {len(input_files)} files matching pattern '{file_pattern}' in {input_path}")
        else:
            input_files = [input_path]
            
        # Process each file
        output_files = []
        for input_file in input_files:
            # Ensure input_file is a Path object
            input_file = Path(input_file)
            
            # Determine output file path
            if input_path.is_dir():
                try:
                    rel_path = input_file.relative_to(input_path)
                except ValueError:
                    # Fallback if relative path can't be determined
                    rel_path = Path(input_file.name)
            else:
                rel_path = Path(input_file.name)
                
            output_file = output_path / f"{rel_path.stem}_tone_analysis.json"
            
            logger.info(f"Processing {input_file} -> {output_file}")
            
            # Run tone analysis
            analysis = self.analyzer.analyze_tone(
                input_file=str(input_file),
                output_file=str(output_file)
            )
            
            # Save the raw analysis directly to a text file for easier access
            raw_output_file = output_path / f"{rel_path.stem}_tone_analysis.txt"
            with open(raw_output_file, "w", encoding="utf-8") as f:
                f.write(analysis.get("raw_analysis", "No analysis available"))
            
            output_files.append(str(output_file))
            
        return output_files
        
    def process_repo_data(self, repo_path=None):
        """
        Process all repository data from standard GitHub crawler data structure.
        Assumes the data is in a directory structure like:
        <data_dir>/<repo_owner>/<repo_name>/<data_files>.json
        
        Args:
            repo_path (str, optional): Path to specific repo dir to process
            
        Returns:
            dict: Summary of processing results
        """
        results = {
            "processed_repos": 0,
            "processed_files": 0,
            "output_files": []
        }
        
        # Determine repo directories to process
        if repo_path:
            repo_dirs = [Path(repo_path)]
        else:
            # Find all directories in data_dir
            repo_dirs = []
            for owner_dir in self.data_dir.iterdir():
                if owner_dir.is_dir():
                    for repo_dir in owner_dir.iterdir():
                        if repo_dir.is_dir():
                            repo_dirs.append(repo_dir)
        
        logger.info(f"Processing {len(repo_dirs)} repositories")
        
        # Process each repository
        for repo_dir in repo_dirs:
            # Ensure repo_dir is a Path object
            repo_dir = Path(repo_dir)
            logger.info(f"Processing repository: {repo_dir}")
            
            # Create output directory for this repo
            try:
                # Handle the case where repo_dir is absolute
                rel_repo_dir = repo_dir.relative_to(self.data_dir)
                output_dir = self.data_dir / "tone_analysis" / rel_repo_dir
            except ValueError:
                # If repo_dir isn't relative to data_dir, use the basename
                output_dir = self.data_dir / "tone_analysis" / repo_dir.name
                if repo_dir.parent.name:
                    output_dir = self.data_dir / "tone_analysis" / repo_dir.parent.name / repo_dir.name
                
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Find comment files
            comment_files = [
                f for f in repo_dir.glob("*.json") 
                if "experts" not in f.name and "pipeline_results" not in f.name
            ]
            
            if not comment_files:
                logger.warning(f"No comment files found in {repo_dir}")
                continue
                
            # Process each file
            for comment_file in comment_files:
                # Ensure comment_file is a Path object
                comment_file = Path(comment_file)
                output_file = output_dir / f"{comment_file.stem}_tone_analysis.json"
                raw_output_file = output_dir / f"{comment_file.stem}_tone_analysis.txt"
                
                logger.info(f"Processing {comment_file.relative_to(self.data_dir) if comment_file.is_relative_to(self.data_dir) else comment_file} -> {output_file.relative_to(self.data_dir) if output_file.is_relative_to(self.data_dir) else output_file}")
                
                try:
                    analysis = self.analyzer.analyze_tone(
                        input_file=str(comment_file),
                        output_file=str(output_file)
                    )
                    
                    # Save the raw analysis directly to a text file for easier access
                    with open(raw_output_file, "w", encoding="utf-8") as f:
                        f.write(analysis.get("raw_analysis", "No analysis available"))
                    
                    results["processed_files"] += 1
                    results["output_files"].append(str(output_file))
                    
                except Exception as e:
                    logger.error(f"Error processing {comment_file}: {e}")
                    
            results["processed_repos"] += 1
            
        logger.info(f"Pipeline complete! Processed {results['processed_repos']} repositories and {results['processed_files']} files")
        return results


def main():
    """Main function to run the tone analysis pipeline from command line."""
    parser = argparse.ArgumentParser(description="GitHub comment tone analysis pipeline")
    
    parser.add_argument("--input", type=str,
                        help="Path to input file or directory")
    parser.add_argument("--output", type=str,
                        help="Path to output directory")
    parser.add_argument("--data-dir", type=str, default="data",
                        help="Base directory for data (default: 'data')")
    parser.add_argument("--api-key", type=str,
                        help="OpenAI API key (or use OPENAI_API_KEY env variable)")
    parser.add_argument("--model", type=str, default="gpt-4o-mini",
                        help="OpenAI model to use (default: gpt-4o-mini)")
    parser.add_argument("--process-all", action="store_true",
                        help="Process all repositories in data directory")
    parser.add_argument("--repo", type=str,
                        help="Process specific repository (format: owner/name or path)")
    
    args = parser.parse_args()
    
    try:
        pipeline = ToneAnalysisPipeline(
            api_key=args.api_key,
            model=args.model,
            data_dir=args.data_dir
        )
        
        if args.process_all:
            # Process all repositories
            results = pipeline.process_repo_data()
            
        elif args.repo:
            # Process specific repository
            if "/" in args.repo and not os.path.exists(args.repo):
                # Format is owner/name
                owner, name = args.repo.split("/")
                repo_path = os.path.join(args.data_dir, owner, name)
            else:
                # Format is path
                repo_path = args.repo
                
            results = pipeline.process_repo_data(repo_path)
            
        elif args.input:
            # Process specific input file or directory
            output_files = pipeline.run(
                input_file=args.input,
                output_dir=args.output
            )
            results = {
                "processed_files": len(output_files),
                "output_files": output_files
            }
            
        else:
            logger.error("No action specified. Use --process-all, --repo, or --input")
            return 1
            
        # Print summary
        print("\n=== Pipeline Results ===")
        if "processed_repos" in results:
            print(f"Processed repositories: {results['processed_repos']}")
        print(f"Processed files: {results['processed_files']}")
        print(f"Output files: {len(results['output_files'])}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        return 1


if __name__ == "__main__":
    exit(main()) 