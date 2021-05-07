import importlib.metadata

try:
    __version__ = importlib.metadata.version("workbot")
except importlib.metadata.PackageNotFoundError:
    pass


def version() -> str:
    """Return the current version"""
    return __version__
