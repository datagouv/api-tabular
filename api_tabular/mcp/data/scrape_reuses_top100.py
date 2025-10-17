import csv
import json
import time
from pathlib import Path
from urllib.parse import urlparse

import httpx
from datagouv import Dataset

script_dir = Path(__file__).parent
input_csv = script_dir / "reuses_top100.csv"
output_json = script_dir / "reuses_top100.json"

ids: set[str] = set()


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


print("\n🚀 Starting processing with httpx client...")

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

# Build JSON data with dataset details
print("\n💾 Building JSON output with dataset details...")
data: list[dict[str, any]] = []

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

        # Get resources information
        for resource in dataset.resources:
            resource_info = {
                "resource_id": resource.id,
                "name": resource.title,
                # "url": resource.url,
                # "format": resource.format,
                # "type": resource.type,
            }
            dataset_info["resources"].append(resource_info)
            # print(f"   📄 Resource: {resource.title} ({resource.format})")

        data.append(dataset_info)
        print(f"   ✅ Added {len(dataset_info['resources'])} resources")

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

print(f"\n📝 Writing {len(data)} datasets with details to {output_json}")
with output_json.open("w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

total_resources = sum(len(dataset["resources"]) for dataset in data)
print(f"✅ {len(data)} datasets with {total_resources} total resources saved to {output_json}")
print("🎉 Script completed successfully!")
