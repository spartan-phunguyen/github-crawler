#!/bin/bash

# Script to run the GitHub Expert Finder pipeline for multiple languages

set -e  # Exit on error

# Default values
MAX_EXPERTS=10
COMMENT_LIMIT=100
ENV_FILE=".env"
LANGUAGES=("Python" "JavaScript" "Rust" "Go" "TypeScript" "C#" "Ruby" "PHP" "Swift" "Kotlin" "Scala" "R" "SQL" "HTML" "CSS" "Java" "C++" "C" "Objective-C" "Ruby" "PHP" "Swift" "Kotlin" "Scala" "R" "SQL" "HTML" "CSS" "Java" "C++" "C" "Objective-C")

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --max-experts)
      MAX_EXPERTS="$2"
      shift 2
      ;;
    --comment-limit)
      COMMENT_LIMIT="$2"
      shift 2
      ;;
    --env-file)
      ENV_FILE="$2"
      shift 2
      ;;
    --languages)
      IFS=',' read -r -a LANGUAGES <<< "$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--max-experts N] [--comment-limit N] [--env-file path] [--languages lang1,lang2,...]"
      exit 1
      ;;
  esac
done

# Function to update .env file
update_env() {
  local key=$1
  local value=$2
  
  # Check if key exists in the file
  if grep -q "^$key=" "$ENV_FILE"; then
    # Update existing key
    sed -i "s/^$key=.*/$key=$value/" "$ENV_FILE"
  else
    # Add new key
    echo "$key=$value" >> "$ENV_FILE"
  fi
}

# Display run configuration
echo "Running pipeline with the following configuration:"
echo "  Languages: ${LANGUAGES[*]}"
echo "  Max Experts: $MAX_EXPERTS"
echo "  Comment Limit: $COMMENT_LIMIT"
echo "  Using .env file: $ENV_FILE"
echo ""

# Process each language
for language in "${LANGUAGES[@]}"; do
  echo "===================================================="
  echo "Processing language: $language"
  echo "===================================================="
  
  # Update .env file
  update_env "LANGUAGE" "$language"
  update_env "MAX_EXPERTS" "$MAX_EXPERTS"
  update_env "COMMENT_LIMIT" "$COMMENT_LIMIT"
  
  echo "Updated .env file with LANGUAGE=$language, MAX_EXPERTS=$MAX_EXPERTS, COMMENT_LIMIT=$COMMENT_LIMIT"
  
  # Run the pipeline
  echo "Starting pipeline for $language..."
  python3 pipeline.py
  
  echo "Completed processing for $language"
  echo ""
  
  # Optional: Add a delay to avoid hitting API rate limits
  sleep 5
done

echo "===================================================="
echo "Pipeline completed for all languages!"
echo "====================================================" 