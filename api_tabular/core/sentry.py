from sentry_sdk.integrations.aiohttp import AioHttpIntegration

from api_tabular import config


def get_sentry_kwargs():
    """
    Returns Sentry configuration kwargs.

    The integration is created fresh each time to ensure proper initialization
    when sentry_sdk.init() is called, which is required for performance tracking.
    Creating the integration at module import time (before init) can prevent
    proper hooking into aiohttp's internals.
    """
    return {
        "dsn": config.SENTRY_DSN,
        "integrations": [AioHttpIntegration()],
        "environment": config.SERVER_NAME or "unknown",
        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        # Sentry recommends adjusting this value in production.
        "traces_sample_rate": config.SENTRY_SAMPLE_RATE or 1.0,
        "profiles_sample_rate": config.SENTRY_SAMPLE_RATE or 1.0,
    }
