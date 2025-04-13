#!/usr/bin/env python3
import os
import argparse
import logging
from pathlib import Path
import json
import shutil
from dotenv import load_dotenv
from datetime import datetime
from tone_pipeline import ToneAnalysisPipeline

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_api_key():
    """Load OpenAI API key from .env file"""
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.error("No OPENAI_API_KEY found in .env file")
        raise ValueError("Missing OpenAI API key in .env file")
    return api_key

def find_expert_files(data_dir):
    """
    Find all expert comment files in the data directory, prioritizing .enriched files
    
    Returns:
        list of tuples: (file_path, language, expert_name)
    """
    data_path = Path(data_dir)
    expert_files = []
    
    # Keep track of processed expert directories to avoid redundant files
    processed_experts = set()
    
    # Walk through all directories in data_dir
    for language_dir in data_path.iterdir():
        if not language_dir.is_dir():
            continue
            
        language = language_dir.name
        experts_dir = language_dir / "experts"
        if not experts_dir.exists() or not experts_dir.is_dir():
            continue
            
        # Handle top-level experts.json file
        experts_file = experts_dir / "experts.json"
        if experts_file.exists() and experts_file.is_file():
            expert_files.append((experts_file, language, None))
        
        # Process individual expert directories
        for expert_dir in experts_dir.iterdir():
            if not expert_dir.is_dir():
                continue
                
            expert_name = expert_dir.name
            expert_key = f"{language}/{expert_name}"
            
            # Skip if we've already processed this expert
            if expert_key in processed_experts:
                continue
                
            # Look for comments.enriched.json files first
            found_enriched_comments = False
            for file in expert_dir.glob("*comments*.enriched.json"):
                if file.is_file():
                    expert_files.append((file, language, expert_name))
                    found_enriched_comments = True
            
            # If no enriched comment files found, use regular comment files
            if not found_enriched_comments:
                for file in expert_dir.glob("*comments*.json"):
                    if file.is_file() and ".enriched." not in file.name:
                        expert_files.append((file, language, expert_name))
            
            # Mark this expert as processed
            processed_experts.add(expert_key)
    
    return expert_files

def should_analyze(file_info, force=False, days_threshold=7):
    """
    Check if file should be analyzed:
    - If output file exists, skip analysis
    - If output file doesn't exist, perform analysis
    
    Args:
        file_info: Tuple of (file_path, language, expert_name)
        force: Force reanalysis
        days_threshold: Not used, kept for backward compatibility
        
    Returns:
        bool: Whether file should be analyzed
    """
    if force:
        return True
        
    file_path, language, expert_name = file_info
    
    # Check if tone analysis already exists
    input_path = Path(file_path)
    output_dir = Path("data") / "tone_analysis"
    
    # Construct output path that mirrors input structure
    if expert_name:
        # For files in expert subdirectories
        output_path = output_dir / language / "experts" / expert_name
    else:
        # For experts.json files
        output_path = output_dir / language / "experts"
        
    output_file = output_path / f"{input_path.stem}_tone_analysis.json"
    
    # Simple logic: If analysis exists, don't analyze again
    # If analysis doesn't exist, perform analysis
    return not output_file.exists()

def analyze_file(pipeline, file_info):
    """
    Analyze a single file and save results in the appropriate directory
    
    Args:
        pipeline: ToneAnalysisPipeline instance
        file_info: Tuple of (file_path, language, expert_name)
    """
    file_path, language, expert_name = file_info
    input_path = Path(file_path)
    
    # Create output directory that mirrors input structure
    output_dir = Path("data") / "tone_analysis"
    
    if expert_name:
        # For files in expert subdirectories
        output_path = output_dir / language / "experts" / expert_name
    else:
        # For experts.json files
        output_path = output_dir / language / "experts"
        
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Set output file path
    output_file = output_path / f"{input_path.stem}_tone_analysis.json"
    raw_output_file = output_path / f"{input_path.stem}_tone_analysis.txt"
    
    logger.info(f"Analyzing {input_path} -> {output_file}")
    
    # Run analysis
    analysis = pipeline.analyzer.analyze_tone(
        input_file=str(input_path),
        output_file=str(output_file)
    )
    
    # Save raw analysis as text
    with open(raw_output_file, "w", encoding="utf-8") as f:
        f.write(analysis.get("raw_analysis", "No analysis available"))
    
    return output_file

def main():
    parser = argparse.ArgumentParser(description="Run tone analysis on all experts in data directory")
    parser.add_argument("--data-dir", type=str, default="data", 
                        help="Base directory for data (default: 'data')")
    parser.add_argument("--force", action="store_true", 
                        help="Force reanalysis even if already done")
    parser.add_argument("--days", type=int, default=7, 
                        help="Reanalyze files older than this many days (default: 7)")
    parser.add_argument("--model", type=str, default="gpt-4o-mini",
                        help="OpenAI model to use (default: gpt-4o-mini)")
    parser.add_argument("--language", type=str, 
                        help="Process only this language (e.g. 'php')")
    parser.add_argument("--expert", type=str,
                        help="Process only this expert (must use with --language)")
    
    args = parser.parse_args()
    
    try:
        # Load API key from .env
        api_key = load_api_key()
        
        # Find all expert files
        expert_files = find_expert_files(args.data_dir)
        
        # Filter by language and expert if specified
        if args.language:
            expert_files = [file_info for file_info in expert_files if file_info[1] == args.language]
            
            if args.expert:
                expert_files = [file_info for file_info in expert_files if file_info[2] == args.expert]
        
        logger.info(f"Found {len(expert_files)} expert files to potentially analyze")
        
        # Filter files that need analysis
        files_to_analyze = []
        for file_info in expert_files:
            if should_analyze(file_info, args.force, args.days):
                files_to_analyze.append(file_info)
        
        logger.info(f"Will analyze {len(files_to_analyze)} expert files")
        
        # Create pipeline
        pipeline = ToneAnalysisPipeline(
            api_key=api_key,
            model=args.model,
            data_dir=args.data_dir
        )
        
        # Process each file
        results = []
        for file_info in files_to_analyze:
            try:
                output_file = analyze_file(pipeline, file_info)
                results.append(output_file)
            except Exception as e:
                logger.error(f"Error analyzing {file_info[0]}: {e}")
        
        logger.info(f"All analysis complete! Processed {len(results)} files")
        return 0
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1

if __name__ == "__main__":
    exit(main()) 