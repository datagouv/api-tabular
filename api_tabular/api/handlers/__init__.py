"""
Request handlers for the API module.
"""

from .resource_handlers import (
    handle_resource_data,
    handle_resource_data_csv,
    handle_resource_data_json,
    handle_resource_meta,
    handle_resource_profile,
    handle_resource_swagger,
)

__all__ = [
    "handle_resource_meta",
    "handle_resource_profile",
    "handle_resource_data",
    "handle_resource_data_csv",
    "handle_resource_data_json",
    "handle_resource_swagger",
]
