# MCP Population Script

The `populate_mcp.py` script scrapes the top 100 reuses from data.gouv.fr and populates your local Hydra database with the actual data, ensuring the MCP server only exposes resources that are actually available.

## Features

- **Scrapes top 100 reuses** from data.gouv.fr
- **Downloads and processes** CSV/Excel files from each resource
- **Creates database tables** for each resource with proper column types
- **Updates tables_index** mapping between resource IDs and table names
- **Supports batch processing** for large datasets
- **Optional database population** via command-line flag

## Usage

### Install Dependencies

```bash
# Install the required dependencies
uv sync --group dev
```

### Basic Usage (JSON only)

```bash
# Generate JSON configuration only (no database population)
uv run python api_tabular/mcp/data/populate_mcp.py
```

### Full Usage (with database population)

```bash
# Populate the Hydra database with scraped data
uv run python api_tabular/mcp/data/populate_mcp.py --populate-db
```

### Custom Database Configuration

```bash
# Use custom database settings
uv run python api_tabular/mcp/data/populate_mcp.py \
  --populate-db \
  --db-host localhost \
  --db-port 5434 \
  --db-name postgres \
  --db-user postgres \
  --db-password postgres
```

## What it does

1. **Reads** the `reuses_top100.csv` file
2. **Fetches** dataset information from data.gouv.fr API
3. **Downloads** CSV/Excel resources from each dataset
4. **Creates** PostgreSQL tables with appropriate column types
5. **Inserts** data in batches for efficiency
6. **Updates** the `tables_index` mapping
7. **Generates** the `mcp_available_resources.json` configuration file

## Database Schema

The script creates tables with:
- `__id` - Primary key (auto-increment)
- Columns based on the original CSV/Excel structure
- Proper PostgreSQL data types (TEXT, INTEGER, REAL, BOOLEAN)
- Clean column names (spaces and special characters replaced)

## Requirements

- Python 3.11+
- PostgreSQL database running on port 5434
- Internet connection for downloading data
- Sufficient disk space for the datasets

## Notes

- The script respects API rate limits (0.2s delay between requests)
- Large datasets are processed in batches of 1000 rows
- Only CSV and Excel files are processed (other formats are skipped)
- Database operations are transactional (all-or-nothing)
