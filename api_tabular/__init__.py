from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("api-tabular")
except PackageNotFoundError:
    __version__ = "unknown"
