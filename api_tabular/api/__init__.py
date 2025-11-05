"""
API module for api_tabular.

This module contains the API layer that handles HTTP requests and responses.
It uses the core module for business logic and provides REST endpoints.
"""

from .app import app_factory

__all__ = ["app_factory"]
