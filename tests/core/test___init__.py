from lakedrive.core import get_scheme_handler, search_contents
from lakedrive.core.handlers import SchemeError
from lakedrive.localfs.handler import LocalFileHandler
from lakedrive.s3.handler import S3Handler

from tests.helpers.async_parsers import pytest_asyncio


def test_get_scheme_handler() -> None:
    localfs_handler = get_scheme_handler("/testdir")
    assert isinstance(localfs_handler, LocalFileHandler)

    s3_handler = get_scheme_handler("s3://dummybucket/abc")
    assert isinstance(s3_handler, S3Handler)

    try:
        get_scheme_handler("http://dummy_handler")
    except SchemeError as error:
        assert isinstance(error.__str__(), str)


@pytest_asyncio
async def test_search_contents() -> None:
    files = await search_contents("s3://dummybucket/abc")
    assert isinstance(files, list)
    assert len(files) == 0
