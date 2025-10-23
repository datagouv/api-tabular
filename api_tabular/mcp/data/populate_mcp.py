import argparse
import csv
import hashlib
import json
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

import httpx
import pandas as pd
import psycopg
from datagouv import Dataset

script_dir = Path(__file__).parent
input_csv = script_dir / "reuses_top100.csv"
output_json = script_dir / "mcp_available_resources.json"

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5434,  # Hydra CSV database port
    "dbname": "postgres",  # psycopg3 uses 'dbname' instead of 'database'
    "user": "postgres",
    "password": "postgres",
}

ids: set[str] = set()


def get_db_connection():
    """Get database connection."""
    return psycopg.connect(**DB_CONFIG)


def generate_parsing_table_name(resource_id: str) -> str:
    """Generate a parsing table name from resource ID."""
    # Create a deterministic hash from the resource ID
    hash_obj = hashlib.md5(resource_id.encode())
    return hash_obj.hexdigest()


def create_table_for_resource(
    cursor, resource_id: str, resource_name: str, data_url: str
) -> str | None:
    """Create a table for a resource and return the parsing table name."""
    parsing_table = generate_parsing_table_name(resource_id)

    try:
        print(f"   🗄️  Creating table: {parsing_table}")

        # Download and analyze the data
        print(f"   📥 Downloading data from: {data_url}")

        # Try to read the data with pandas
        try:
            if data_url.endswith(".csv"):
                df = pd.read_csv(data_url)
            elif data_url.endswith((".xlsx", ".xls")):
                df = pd.read_excel(data_url)
            else:
                print(f"   ❌ Unsupported format for: {data_url}")
                return None

            print(f"   📊 Data shape: {df.shape}")

            # Create table with appropriate columns
            columns = []
            for col in df.columns:
                # Clean column name for PostgreSQL
                clean_col = (
                    col.replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "")
                )
                clean_col = "".join(c for c in clean_col if c.isalnum() or c == "_")
                if not clean_col or clean_col[0].isdigit():
                    clean_col = f"col_{clean_col}"

                # Determine PostgreSQL type
                if df[col].dtype == "object":
                    pg_type = "TEXT"
                elif "int" in str(df[col].dtype):
                    pg_type = "INTEGER"
                elif "float" in str(df[col].dtype):
                    pg_type = "REAL"
                elif "bool" in str(df[col].dtype):
                    pg_type = "BOOLEAN"
                else:
                    pg_type = "TEXT"

                columns.append(f'"{clean_col}" {pg_type}')

            # Create table
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS "{parsing_table}" (
                __id SERIAL PRIMARY KEY,
                {", ".join(columns)}
            )
            """

            cursor.execute(create_sql)
            print(f"   ✅ Table created: {parsing_table}")

            # Insert data in batches using psycopg3's execute_values
            batch_size = 1000
            total_rows = len(df)
            print(f"   📝 Inserting {total_rows} rows...")

            # Prepare column names for insertion
            clean_columns = [
                col.replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "")
                for col in df.columns
            ]
            clean_columns = [
                f"col_{col}" if not col or col[0].isdigit() else col for col in clean_columns
            ]
            clean_columns = [
                "".join(c for c in col if c.isalnum() or c == "_") for col in clean_columns
            ]

            for i in range(0, total_rows, batch_size):
                batch_df = df.iloc[i : i + batch_size]

                # Prepare data for insertion using psycopg3's execute_values
                data_tuples = []
                for _, row in batch_df.iterrows():
                    values = []
                    for col in df.columns:
                        value = row[col]
                        if pd.isna(value):
                            values.append(None)
                        else:
                            values.append(value)
                    data_tuples.append(tuple(values))

                if data_tuples:
                    # Use psycopg3's executemany for efficient batch insertion
                    from psycopg import sql

                    insert_sql = sql.SQL("""
                        INSERT INTO {} ({}) VALUES ({})
                    """).format(
                        sql.Identifier(parsing_table),
                        sql.SQL(", ").join(sql.Identifier(col) for col in clean_columns),
                        sql.SQL(", ").join(sql.Placeholder() for _ in clean_columns),
                    )
                    cursor.executemany(insert_sql, data_tuples)

                print(
                    f"   📊 Inserted batch {i // batch_size + 1}/{(total_rows - 1) // batch_size + 1}"
                )

            print(f"   ✅ Successfully inserted {total_rows} rows into {parsing_table}")
            return parsing_table

        except Exception as e:
            print(f"   ❌ Error processing data: {str(e)}")
            # Rollback the transaction to clear the aborted state
            try:
                cursor.connection.rollback()
            except:
                pass
            return None

    except Exception as e:
        print(f"   ❌ Error creating table: {str(e)}")
        # Rollback the transaction to clear the aborted state
        try:
            cursor.connection.rollback()
        except:
            pass
        return None


def update_tables_index(cursor, resource_id: str, parsing_table: str, data_url: str):
    """Update the tables_index with the new resource mapping."""
    try:
        # Check if resource already exists
        cursor.execute("SELECT id FROM tables_index WHERE resource_id = %s", (resource_id,))
        existing = cursor.fetchone()

        if existing:
            # Update existing entry
            cursor.execute(
                """
                UPDATE tables_index
                SET parsing_table = %s, url = %s, created_at = NOW()
                WHERE resource_id = %s
            """,
                (parsing_table, data_url, resource_id),
            )
            print(f"   🔄 Updated existing mapping: {resource_id} -> {parsing_table}")
        else:
            # Insert new entry
            cursor.execute(
                """
                INSERT INTO tables_index (resource_id, parsing_table, url, created_at)
                VALUES (%s, %s, %s, NOW())
            """,
                (resource_id, parsing_table, data_url),
            )
            print(f"   ➕ Added new mapping: {resource_id} -> {parsing_table}")

    except Exception as e:
        print(f"   ❌ Error updating tables_index: {str(e)}")


def get_id(client: httpx.Client, url: str) -> str | None:
    if not url or "data.gouv.fr" not in url:
        print(f"❌ Invalid URL: {url}")
        return None
    slug = urlparse(url).path.split("/datasets/")[-1].split("/")[0]
    print(f"🔍 Processing URL: {url} -> slug: {slug}")

    r = client.get(f"https://www.data.gouv.fr/api/1/datasets/{slug}/")
    if r.status_code == 200:
        dataset_id = r.json().get("id")
        print(f"✅ Direct match found: {dataset_id}")
        return dataset_id

    print(f"❌ No match found for slug: {slug}")
    return None


# Parse command line arguments
parser = argparse.ArgumentParser(
    description="Scrape top 100 reuses and optionally populate Hydra database"
)
parser.add_argument(
    "--populate-db", action="store_true", help="Populate the Hydra database with scraped data"
)
parser.add_argument("--db-host", default="localhost", help="Database host (default: localhost)")
parser.add_argument("--db-port", type=int, default=5434, help="Database port (default: 5434)")
parser.add_argument("--db-name", default="postgres", help="Database name (default: postgres)")
parser.add_argument("--db-user", default="postgres", help="Database user (default: postgres)")
parser.add_argument(
    "--db-password", default="postgres", help="Database password (default: postgres)"
)

args = parser.parse_args()

# Update database config with command line arguments
DB_CONFIG.update(
    {
        "host": args.db_host,
        "port": args.db_port,
        "dbname": args.db_name,  # psycopg3 uses 'dbname' instead of 'database'
        "user": args.db_user,
        "password": args.db_password,
    }
)

print("\n🚀 Starting processing with httpx client...")
if args.populate_db:
    print("🗄️  Database population ENABLED")
    print(f"   Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"   Database: {DB_CONFIG['dbname']}")
    print(f"   User: {DB_CONFIG['user']}")
else:
    print("📄 JSON generation only (use --populate-db to enable database population)")

with httpx.Client(timeout=10) as client, input_csv.open(newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    total_rows = 0
    processed_rows = 0
    successful_ids = 0

    for row in reader:
        total_rows += 1
        url = row.get("Lien du jeu de données")
        print(f"\n📊 Row {total_rows}: {url}")

        dataset_id = get_id(client, url)
        if dataset_id:
            ids.add(dataset_id)
            successful_ids += 1
            print(f"✅ Added to collection (total unique: {len(ids)})")
        else:
            print("❌ Failed to get dataset ID")

        processed_rows += 1
        print("⏱️  Waiting 0.2s before next request...")
        time.sleep(0.2)  # respect API rate limits

print("\n📈 Processing complete:")
print(f"   - Total rows processed: {processed_rows}")
print(f"   - Successful dataset IDs: {successful_ids}")
print(f"   - Unique dataset IDs: {len(ids)}")

# Build JSON data with dataset details and optionally populate database
print("\n💾 Building JSON output with dataset details...")
if args.populate_db:
    print("🗄️  Database population ENABLED")
    print("   Only successfully processed resources will be included in MCP configuration")
else:
    print("📄 Database population DISABLED")
    print("   All resources will be included in MCP configuration")

data: list[dict[str, any]] = []
total_resources_processed = 0
total_resources_successful = 0
total_resources_excluded = 0

# Connect to database if population is enabled
if args.populate_db:
    print("\n🗄️  Connecting to Hydra database...")
    try:
        conn = get_db_connection()
        print("✅ Database connected successfully")
    except Exception as e:
        print(f"❌ Failed to connect to database: {str(e)}")
        print("   Make sure your Hydra database is running on port 5434")
        exit(1)
else:
    conn = None

for i, dataset_id in enumerate(sorted(ids), 1):
    print(f"\n📊 Processing dataset {i}/{len(ids)}: {dataset_id}")

    try:
        # Fetch dataset information using datagouv client
        dataset = Dataset(dataset_id)

        # Get dataset basic info
        dataset_info = {
            "dataset_id": dataset_id,
            "name": dataset.title,
            # "description": dataset.description,
            # "created_at": dataset.created_at,
            "resources": [],
        }

        print(f"   📋 Dataset: {dataset.title}")
        # print(
        #     f"   📝 Description: {dataset.description[:100]}..."
        #     if len(dataset.description) > 100
        #     else f"   📝 Description: {dataset.description}"
        # )

        # Get resources information and process them
        successful_resources = []
        dataset_resources_processed = 0
        dataset_resources_successful = 0
        dataset_resources_excluded = 0

        for resource in dataset.resources:
            dataset_resources_processed += 1
            resource_info = {
                "resource_id": resource.id,
                "name": resource.title,
                # "url": resource.url,
                # "format": resource.format,
                # "type": resource.type,
            }

            # Process resource for database if it's a supported format and DB population is enabled
            if (
                args.populate_db
                and resource.format
                and resource.format.lower() in ["csv", "xlsx", "xls"]
            ):
                # Check if resource already exists in tables_index
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT parsing_table FROM tables_index WHERE resource_id = %s",
                        (resource.id,),
                    )
                    existing = cursor.fetchone()

                    if existing:
                        print(
                            f"   ⏭️  Skipping {resource.title} (already processed: {existing[0]}) - INCLUDED in MCP"
                        )
                        successful_resources.append(resource_info)
                        dataset_resources_successful += 1
                        continue

                print(f"   📄 Processing resource: {resource.title} ({resource.format})")

                # Create table and insert data using psycopg3 context manager
                # Use autocommit=False to handle transactions properly
                with conn.cursor() as cursor:
                    try:
                        # Start a new transaction for this resource
                        cursor.execute("BEGIN")

                        parsing_table = create_table_for_resource(
                            cursor, resource.id, resource.title, resource.url
                        )

                        if parsing_table:
                            # Update tables_index only if table creation was successful
                            update_tables_index(cursor, resource.id, parsing_table, resource.url)
                            # Commit the transaction
                            cursor.execute("COMMIT")
                            print(f"   ✅ Database processing complete for {resource.title}")
                            # Only add to successful resources if database processing succeeded
                            successful_resources.append(resource_info)
                            dataset_resources_successful += 1
                        else:
                            # Rollback if parsing_table creation failed
                            cursor.execute("ROLLBACK")
                            print(
                                f"   ❌ Failed to process {resource.title} in database - EXCLUDED from MCP"
                            )
                            dataset_resources_excluded += 1
                    except Exception as e:
                        # Rollback on any error
                        try:
                            cursor.execute("ROLLBACK")
                        except:
                            pass
                        print(
                            f"   ❌ Error processing {resource.title}: {str(e)} - EXCLUDED from MCP"
                        )
                        dataset_resources_excluded += 1
            elif args.populate_db:
                print(
                    f"   ⏭️  Skipping {resource.title} (unsupported format: {resource.format}) - EXCLUDED from MCP"
                )
                dataset_resources_excluded += 1
            else:
                print(
                    f"   📄 Resource: {resource.title} ({resource.format}) - Database population disabled"
                )
                # If database population is disabled, include all resources
                successful_resources.append(resource_info)
                dataset_resources_successful += 1

        # Only add resources that were successfully processed (or all if DB population disabled)
        dataset_info["resources"] = successful_resources

        data.append(dataset_info)
        total_resources_processed += dataset_resources_processed
        total_resources_successful += dataset_resources_successful
        total_resources_excluded += dataset_resources_excluded

        print(f"   ✅ Added {len(dataset_info['resources'])} resources to MCP config")
        if args.populate_db:
            print(
                f"   📊 Dataset stats: {dataset_resources_successful} successful, {dataset_resources_excluded} excluded"
            )

    except Exception as e:
        print(f"   ❌ Error processing dataset {dataset_id}: {str(e)}")
        # Add basic info even if detailed fetch fails
        data.append(
            {
                "dataset_id": dataset_id,
                "name": f"Error: {str(e)}",
                # "description": "Failed to fetch dataset details",
                # "created_at": None,
                "resources": [],
            }
        )

# Commit database changes
if args.populate_db:
    print("\n💾 Committing database changes...")
    try:
        conn.commit()
        print("✅ Database changes committed successfully")
    except Exception as e:
        print(f"❌ Error committing database changes: {str(e)}")
        conn.rollback()
    finally:
        conn.close()
        print("🔌 Database connection closed")

print(f"\n📝 Writing {len(data)} datasets with details to {output_json}")
with output_json.open("w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

total_resources = sum(len(dataset["resources"]) for dataset in data)
print(f"✅ {len(data)} datasets with {total_resources} total resources saved to {output_json}")

# Print final statistics
print("\n📊 Final Statistics:")
print(f"   - Total resources processed: {total_resources_processed}")
print(f"   - Resources successfully added to MCP: {total_resources_successful}")
if args.populate_db:
    print(f"   - Resources excluded from MCP: {total_resources_excluded}")
    print(f"   - Success rate: {(total_resources_successful/total_resources_processed*100):.1f}%")
else:
    print("   - All resources included (database population disabled)")

print("🎉 Script completed successfully!")
