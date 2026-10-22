from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("udata-hydra-csvapi")
except PackageNotFoundError:
    __version__ = "unknown"
