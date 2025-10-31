#!/usr/bin/env python3
"""
Populate MCP_AVAILABLE_RESOURCE_IDS in api_tabular/config_default.toml.

Two modes:
1. From CSV: reads reuses_top100.csv and fetches resources from listed datasets
2. From Dataset ID/Slug: fetches resources for specific dataset IDs or slugs

Usage:
  # From CSV
  uv run python api_tabular/mcp/data/populate_mcp_config.py

  # From Dataset ID(s) or slug(s)
  uv run python api_tabular/mcp/data/populate_mcp_config.py --dataset 5a0b0be188ee3871db07ce4e
  uv run python api_tabular/mcp/data/populate_mcp_config.py --dataset acco-accords-dentreprise
  uv run python api_tabular/mcp/data/populate_mcp_config.py --dataset id1 --dataset slug1
"""

import argparse
import asyncio
import csv
import re
import sys
import tomllib
from pathlib import Path

import httpx

# Import our async API client
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from api_tabular.mcp import datagouv_api_client

SCRIPT_DIR = Path(__file__).parent
ROOT = Path(__file__).resolve().parents[3]
CSV_FILE = "reuses_top100.csv"
OUTPUT_TOML = ROOT / "api_tabular" / "config_default.toml"


def extract_dataset_slug(url: str) -> str | None:
    """Extract dataset slug from data.gouv.fr URL."""
    parts = url.strip().rstrip("/").split("/")
    if "datasets" in parts:
        idx = parts.index("datasets")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return None


def load_all_resources_by_slug() -> tuple[dict[str, list[tuple[str, str]]], dict[str, str]]:
    """
    Fetch resources for all datasets in CSV.

    Returns:
        tuple of (slug_to_resources dict, url_to_dataset_title dict)
    """
    # Gather all unique dataset slugs
    slugs: set[str] = set()
    url_to_dataset_title: dict[str, str] = {}
    with (SCRIPT_DIR / CSV_FILE).open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("Lien du jeu de données", "").strip()
            jdd_name = row.get("Jeu de données du top 100", "").strip()
            if url and jdd_name:
                slug = extract_dataset_slug(url)
                if slug:
                    slugs.add(slug)
                    url_to_dataset_title[slug] = jdd_name

    print(f"Found {len(slugs)} unique dataset slugs")

    slug_to_resources: dict[str, list[tuple[str, str]]] = {}
    with httpx.Client(timeout=30) as cli:
        for i, slug in enumerate(sorted(slugs), 1):
            print(f"{i}/{len(slugs)} Fetching resources for {slug}")
            try:
                r = cli.get(f"https://www.data.gouv.fr/api/1/datasets/{slug}/")
                if r.status_code == 200:
                    ds = r.json()
                    resources = ds.get("resources", [])
                    res_list = [
                        (res.get("id"), res.get("title", "") or res.get("name", ""))
                        for res in resources
                        if res.get("id")
                    ]
                    if res_list:
                        slug_to_resources[slug] = res_list
                        print(f"   Found {len(res_list)} resources")
            except Exception as e:
                print(f"   ❌ Error: {e}")

    return slug_to_resources, url_to_dataset_title


async def load_resources_from_datasets(dataset_ids_or_slugs: list[str]) -> list[tuple[str, str]]:
    """
    Fetch resources for specific dataset IDs or slugs.

    Returns:
        list of (resource_id, dataset_title) tuples
    """
    results: list[tuple[str, str]] = []
    seen_ids: set[str] = set()

    import aiohttp

    async with aiohttp.ClientSession() as session:
        for i, dataset_id_or_slug in enumerate(dataset_ids_or_slugs, 1):
            print(
                f"{i}/{len(dataset_ids_or_slugs)} Fetching resources for dataset {dataset_id_or_slug}"
            )
            try:
                data = await datagouv_api_client.get_resources_for_dataset(
                    dataset_id_or_slug, session=session
                )
                ds = data.get("dataset", {})
                ds_title = ds.get("title") or dataset_id_or_slug
                resources = data.get("resources", [])
                for rid, rname in resources:
                    if rid not in seen_ids:
                        seen_ids.add(rid)
                        results.append((rid, ds_title))
                print(f"   Found {len(resources)} resources")
            except Exception as e:
                print(f"   ❌ Error: {e}")

    return results


def build_flat_list(
    slug_to_resources: dict[str, list[tuple[str, str]]], url_to_dataset_title: dict[str, str]
) -> list[tuple[str, str]]:
    """Flatten resources with dataset titles as comments."""
    flat: list[tuple[str, str]] = []
    seen_ids: set[str] = set()

    for slug, res_list in slug_to_resources.items():
        ds_title = url_to_dataset_title.get(slug, slug)
        for rid, rname in res_list:
            if rid not in seen_ids:
                seen_ids.add(rid)
                flat.append((rid, ds_title))

    return flat


def load_existing_resource_ids(file_path: Path) -> list[tuple[str, str]]:
    """Load existing resource IDs from TOML file."""
    existing: list[tuple[str, str]] = []

    try:
        # Parse raw TOML to extract comments (tomllib doesn't preserve comments)
        raw_text = file_path.read_text(encoding="utf-8")
        # Validate with tomllib
        with open(file_path, "rb") as f:
            tomllib.load(f)
        # Find the MCP_AVAILABLE_RESOURCE_IDS section and extract comments
        pattern = re.compile(
            r"MCP_AVAILABLE_RESOURCE_IDS\s*=\s*\[(.*?)\]", re.MULTILINE | re.DOTALL
        )
        match = pattern.search(raw_text)
        if match:
            content = match.group(1)
            for line in content.split("\n"):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                id_match = re.search(r'"([^"]+)"', line)
                comment_match = re.search(r"#\s*(.+)", line)
                if id_match:
                    rid = id_match.group(1)
                    comment = comment_match.group(1).strip() if comment_match else ""
                    existing.append((rid, comment))
    except Exception as e:
        print(f"⚠️  Warning: Could not load existing IDs: {e}")

    return existing


def replace_toml_block(text: str, items: list[tuple[str, str]]) -> str:
    """Replace MCP_AVAILABLE_RESOURCE_IDS block in TOML."""
    pattern = re.compile(
        r"(MCP_AVAILABLE_RESOURCE_IDS\s*=\s*\[)(.*?)(\])", re.MULTILINE | re.DOTALL
    )

    def mk_inner() -> str:
        lines: list[str] = []
        for rid, ds_title in items:
            lines.append(f'    "{rid}",  # {ds_title}')
        return "\n".join(lines)

    if pattern.search(text):
        # Keep the original header (group 1) and closing bracket (group 3), replace inner content only
        return pattern.sub(lambda m: m.group(1) + "\n" + mk_inner() + "\n" + m.group(3), text)
    # If the block is not present, append a fresh block at the end
    return text.rstrip() + "\n\n" + "MCP_AVAILABLE_RESOURCE_IDS = [\n" + mk_inner() + "\n]" + "\n"


def main():
    parser = argparse.ArgumentParser(
        description="Populate MCP_AVAILABLE_RESOURCE_IDS in config_default.toml"
    )
    parser.add_argument(
        "--dataset",
        action="append",
        dest="datasets",
        help="Dataset ID or slug to fetch resources from (can be used multiple times)",
    )
    args = parser.parse_args()

    items: list[tuple[str, str]] = []

    if args.datasets:
        # Mode 2: From dataset ID(s) or slug(s)
        print(f"🚀 Fetching resources for {len(args.datasets)} dataset(s)...")
        new_items = asyncio.run(load_resources_from_datasets(args.datasets))
        # Merge with existing IDs, avoiding duplicates
        existing = load_existing_resource_ids(OUTPUT_TOML)
        seen_ids = {rid for rid, _ in existing}
        for rid, ds_title in new_items:
            if rid not in seen_ids:
                existing.append((rid, ds_title))
                seen_ids.add(rid)
        items = existing
        print(f"   {len(new_items)} new resources added")
    else:
        # Mode 1: From CSV
        print(f"🚀 Reading {CSV_FILE} and fetching resources...")
        slug_to_resources, url_to_dataset_title = load_all_resources_by_slug()
        items = build_flat_list(slug_to_resources, url_to_dataset_title)

    print(f"\n✅ Found {len(items)} resource IDs total")
    print(f"📝 Updating {OUTPUT_TOML}")
    current = OUTPUT_TOML.read_text(encoding="utf-8")
    updated = replace_toml_block(current, items)
    OUTPUT_TOML.write_text(updated, encoding="utf-8")
    print("✅ Done!")


if __name__ == "__main__":
    main()
