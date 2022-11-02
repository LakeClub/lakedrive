import re

from typing import List, Dict, Optional

from ..core.objects import FileObject
from ..core.handlers import SchemeError
from ..localfs.handler import LocalFileHandler
from ..s3.handler import S3Handler, S3HandlerError
from ..core.handlers import ObjectStoreHandler


async def get_scheme_handler(
    location: str,
    credentials: Dict[str, str] = {},
    max_read_threads: Optional[int] = None,
    max_write_threads: Optional[int] = None,
) -> ObjectStoreHandler:
    if re.match("^s3://", location):
        try:
            return await S3Handler(
                location,
                credentials=credentials,
                max_read_threads=max_read_threads,
                max_write_threads=max_write_threads,
            ).__ainit__()
        except S3HandlerError as error:
            raise SchemeError(error)

    elif location and not re.match("^[-a-zA-Z0-9]*://", location):
        return await LocalFileHandler(
            location,
            max_read_threads=max_read_threads,
            max_write_threads=max_write_threads,
        ).__ainit__()
    else:
        raise SchemeError("unsupported scheme")


async def search_contents(
    location: str = ".",
    checksum: bool = False,
    skip_hidden: bool = False,
    filters: List[Dict[str, str]] = [],
) -> List[FileObject]:
    handler = await get_scheme_handler(location)
    return await handler.list_contents(
        checksum=checksum, skip_hidden=skip_hidden, filters=filters
    )
