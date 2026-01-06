from importlib.metadata import version


async def get_app_version() -> str:
    """Get the version from the installed package metadata."""
    try:
        return version("udata-hydra-csvapi")
    except Exception:
        return "unknown"
