# GitHub Expert Finder

A sophisticated pipeline for identifying domain experts on GitHub based on their contributions, comments, and code quality.

## Overview

GitHub Expert Finder is an automated system that builds a knowledge base of domain experts on GitHub by analyzing their activity, comments, and contributions. The pipeline collects comments from top GitHub users in specific programming languages, enriches them with AI-powered classifications, and makes them searchable through vector embeddings.

## Pipeline Architecture

The system follows a comprehensive data processing workflow:

1. **Expert Identification**: Finds top GitHub users in a specific programming language based on followers, stars, PRs, and other metrics
2. **Comment Collection**: Gathers issue and PR comments from identified experts
3. **Comment Enrichment**: Uses OpenAI to classify and analyze comment content
4. **Vector Embedding**: Creates searchable embeddings and stores them in Qdrant vector database
5. **Parallel Processing**: Handles multiple experts concurrently with controlled task management

## Key Features

- **Multi-step Pipeline Architecture**: Complete workflow from expert discovery to searchable embeddings
- **Parallel Processing**: Efficiently processes multiple experts simultaneously
- **Configurable Settings**: Customize processing parameters via environment variables
- **Persistent Storage**: Saves data at each step for resume capability and analysis
- **Comprehensive Logging**: Detailed progress tracking and error handling
- **Task Management**: Controls concurrency to avoid API rate limits
- **Dual API Support**: Uses either GitHub's GraphQL API or REST API for data collection

## Key Advantages

- **Time-Efficient Expert Discovery**: Automates the process of finding domain experts
- **Evidence-Based Expertise Ranking**: Uses objective metrics rather than self-reported skills
- **Scalable Architecture**: Process thousands of potential experts with controlled concurrency
- **Rich Data Collection**: Gathers valuable knowledge and insights from top GitHub contributors
- **AI-Enhanced Analysis**: Uses OpenAI to classify and enhance comment data
- **Vector Search Capabilities**: Makes expert knowledge searchable via semantic embeddings

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/github-expert-finder.git
   cd github-expert-finder
   ```

2. Create and activate a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Configure environment variables in a `.env` file:
   ```
   GITHUB_TOKEN=your_github_token_here
   OPENAI_API_KEY=your_openai_api_key_here
   LANGUAGE=python  # Language to find experts for
   MAX_EXPERTS=10   # Maximum number of experts to process
   COMMENT_LIMIT=200  # Maximum comments per expert
   OUTPUT_DIR=data  # Directory to save results
   QDRANT_URL=http://localhost:6333  # Qdrant server URL
   QDRANT_API_KEY=your_qdrant_api_key  # Optional
   OPENAI_MODEL=gpt-4o-mini  # Model for comment enrichment
   EMBEDDING_MODEL=text-embedding-3-small  # Model for embeddings
   MAX_CONCURRENT_TASKS=5  # Maximum parallel tasks
   CONTINUE_CRAWL=true  # Continue from previous crawl
   CONTINUE_ENRICHMENT=true  # Continue from previous enrichment
   ALL_HISTORICAL=false  # Get all historical comments
   ```

## Usage

### Running the Pipeline for a Single Language

Run the complete pipeline with settings from your `.env` file:

```
python pipeline.py
```

### Running the Pipeline for Multiple Languages

Use the bash script to run the pipeline for multiple languages in sequence:

```
chmod -R +x run_pipeline.sh
./run_pipeline.sh
```

Available parameters:

- `--languages`: Comma-separated list of languages (default: several popular languages)
- `--max-experts`: Maximum number of experts per language (default: 10)
- `--comment-limit`: Maximum number of comments per expert (default: 100)
- `--env-file`: Path to .env file (default: ".env")

### Using Specific Expert Lists

You can specify experts to process in two ways:

1. List usernames in `.env`:
   ```
   EXPERT_USERNAMES=torvalds,antirez,gaearon
   ```

2. Provide a file with usernames (one per line or JSON format):
   ```
   EXPERT_LIST_FILE=experts.json
   ```

## Components

The pipeline uses several specialized components:

1. **GitHubExpertFinder**: Identifies top users in a programming language (supports both REST API and GraphQL)
2. **GitHubCommentCrawler**: Collects comments from GitHub users (supports both REST API and GraphQL)
3. **CommentEnricher**: Uses OpenAI to analyze and classify comments
4. **CommentEmbedder**: Creates vector embeddings and uploads to Qdrant

## Output

The pipeline generates several outputs in the data directory:

- `{language}_experts.json`: List of identified experts
- `{username}_comments.json`: Raw comments for each expert
- `{username}_comments.enriched.json`: Enriched comments with classifications
- `{language}_pipeline_results.json`: Pipeline execution summary

## Troubleshooting

If you encounter API rate limit errors:

- Reduce `MAX_CONCURRENT_TASKS` in the `.env` file
- Increase the sleep time between languages in the `run_pipeline.sh` script
- Make sure you're using a GitHub token with appropriate scopes
- Try using `USE_REST_API=true` to use GitHub's REST API which may have different rate limits

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

