__version__ = "0.80.0"
import logging

from .api import (
    head,
    ahead,
    get,
    aget,
    put,
    aput,
    sync_paths,
    async_paths,
    delete,
    adelete,
    Head,
    Get,
    Put,
    Sync,
    Delete,
)
from .cli import main as cli_main


__all__ = [
    "head",
    "ahead",
    "get",
    "aget",
    "put",
    "aput",
    "sync_paths",
    "async_paths",
    "delete",
    "adelete",
    "Head",
    "Get",
    "Put",
    "Sync",
    "Delete",
    "cli_main",
]

logging.getLogger(__name__).addHandler(logging.NullHandler())
