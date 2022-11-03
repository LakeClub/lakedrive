import re

from typing import List, Dict, Optional

from ..core.objects import FileObject
from ..localfs.handler import LocalFileHandler
from ..s3.handler import S3Handler, S3HandlerError
from .handlers import ObjectStoreHandler, SchemeError


def get_scheme_handler(
    location: str,
    credentials: Dict[str, str] = {},
    max_read_threads: Optional[int] = None,
    max_write_threads: Optional[int] = None,
) -> ObjectStoreHandler:
    if re.match("^s3://", location):
        try:
            return S3Handler(
                location,
                credentials=credentials,
                max_read_threads=max_read_threads,
                max_write_threads=max_write_threads,
            )
        except S3HandlerError as error:
            raise SchemeError(str(error))

    elif location and not re.match("^[-a-zA-Z0-9]*://", location):
        return LocalFileHandler(
            location,
            max_read_threads=max_read_threads,
            max_write_threads=max_write_threads,
        )
    else:
        raise SchemeError("unsupported scheme")


async def search_contents(
    location: str = ".",
    checksum: bool = False,
    skip_hidden: bool = False,
    filters: List[Dict[str, str]] = [],
) -> List[FileObject]:
    handler = get_scheme_handler(location)
    return await handler.list_contents(
        checksum=checksum, skip_hidden=skip_hidden, filters=filters
    )
