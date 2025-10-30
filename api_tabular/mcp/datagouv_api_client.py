from typing import Any

import aiohttp

from api_tabular import config


def _base_url() -> str:
    base = config.DATAGOUV_API_BASE_URL or "https://www.data.gouv.fr/api/"
    return str(base).rstrip("/") + "/"


async def _fetch_json(session: aiohttp.ClientSession, url: str) -> dict[str, Any]:
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        resp.raise_for_status()
        return await resp.json()


async def get_resource_metadata(
    resource_id: str, session: aiohttp.ClientSession | None = None
) -> dict[str, Any]:
    own = session is None
    if own:
        session = aiohttp.ClientSession()
    assert session is not None
    try:
        # Use API v2 for resources
        url = f"{_base_url()}2/datasets/resources/{resource_id}/"
        data = await _fetch_json(session, url)
        # API v2 returns nested structure
        resource = data.get("resource", {})
        return {
            "id": resource.get("id") or resource_id,
            "title": resource.get("title") or resource.get("name"),
            "description": resource.get("description"),
            "dataset_id": data.get("dataset_id"),
        }
    finally:
        if own:
            await session.close()


async def get_dataset_metadata(
    dataset_id: str, session: aiohttp.ClientSession | None = None
) -> dict[str, Any]:
    own = session is None
    if own:
        session = aiohttp.ClientSession()
    assert session is not None
    try:
        # Use API v1 for datasets
        url = f"{_base_url()}1/datasets/{dataset_id}/"
        data = await _fetch_json(session, url)
        return {
            "id": data.get("id"),
            "title": data.get("title") or data.get("name"),
            "description_short": data.get("description_short"),
            "description": data.get("description"),
        }
    finally:
        if own:
            await session.close()


async def get_resource_and_dataset_metadata(
    resource_id: str, session: aiohttp.ClientSession | None = None
) -> dict[str, Any]:
    own = session is None
    if own:
        session = aiohttp.ClientSession()
    try:
        res: dict[str, Any] = await get_resource_metadata(resource_id, session=session)
        ds: dict[str, Any] = {}
        ds_id = res.get("dataset_id")
        if ds_id:
            ds = await get_dataset_metadata(str(ds_id), session=session)
        return {"resource": res, "dataset": ds}
    finally:
        if own and session:
            await session.close()
