#!/usr/bin/env python3
"""
Populate MCP_AVAILABLE_RESOURCE_IDS in api_tabular/config_default.toml from reuses_top100.csv.

Usage:
  uv run python api_tabular/mcp/data/populate_mcp_config.py
"""

import csv
import re
from pathlib import Path

import httpx

SCRIPT_DIR = Path(__file__).parent
ROOT = Path(__file__).resolve().parents[3]
INPUT_CSV = SCRIPT_DIR / "reuses_top100.csv"
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
    with INPUT_CSV.open("r", encoding="utf-8") as f:
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
                        and res.get("format", "").lower() in ["csv", "xlsx", "xls", "json"]
                    ]
                    if res_list:
                        slug_to_resources[slug] = res_list
                        print(f"   Found {len(res_list)} processable resources")
            except Exception as e:
                print(f"   ❌ Error: {e}")

    return slug_to_resources, url_to_dataset_title


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


def replace_toml_block(text: str, items: list[tuple[str, str]]) -> str:
    """Replace MCP_AVAILABLE_RESOURCE_IDS block in TOML."""
    pattern = re.compile(
        r"(MCP_AVAILABLE_RESOURCE_IDS\s*=\s*\[)(.*?)(\])", re.MULTILINE | re.DOTALL
    )

    def mk_list() -> str:
        lines = ["MCP_AVAILABLE_RESOURCE_IDS = ["]
        for rid, ds_title in items:
            lines.append(f'    "{rid}",  # {ds_title}')
        lines.append("]")
        return "\n".join(lines)

    if pattern.search(text):
        return pattern.sub(lambda m: m.group(1) + mk_list() + m.group(3), text)
    return text.rstrip() + "\n\n" + mk_list() + "\n"


def main():
    print("🚀 Reading reuses_top100.csv and fetching resources...")
    slug_to_resources, url_to_dataset_title = load_all_resources_by_slug()
    flat = build_flat_list(slug_to_resources, url_to_dataset_title)
    print(f"\n✅ Found {len(flat)} resource IDs")
    print(f"📝 Updating {OUTPUT_TOML}")
    current = OUTPUT_TOML.read_text(encoding="utf-8")
    updated = replace_toml_block(current, flat)
    OUTPUT_TOML.write_text(updated, encoding="utf-8")
    print("✅ Done!")


if __name__ == "__main__":
    main()
