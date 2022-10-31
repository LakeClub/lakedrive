import os
import time
import logging
import pytest

from typing import List, Tuple

from lakedrive.core.handlers import ObjectStoreHandler
from lakedrive.core.objects import ByteStream, FileObject, HashTuple

from .helpers.misc import compute_md5sum
from .helpers.generate_files import MockFileHandler
from .helpers.generate_objects import MockFileObjects
from .helpers.async_parsers import list_to_async_iterator


# no need to get debug logging from these imported libraries
logging.getLogger("asyncio").setLevel(logging.WARNING)

# default switch logging to CRITICAL to silence error- and warning- logs
# (purposely) triggered by testing
LOG_LEVEL = os.environ.get("PYTEST_LOG_LEVEL", "CRITICAL")
try:
    logging.getLogger("lakedrive").setLevel(getattr(logging, LOG_LEVEL))
except AttributeError:
    logging.getLogger("lakedrive").setLevel(logging.CRITICAL)


@pytest.fixture(scope="class", autouse=True)  # type: ignore[misc]
def file_objects_n1() -> List[Tuple[FileObject, bytes]]:
    contents = b"Dummy Bytes\nThis a test.\n"
    contents_chunked = [contents[i : i + 8] for i in range(0, len(contents), 8)]
    fo = FileObject(
        name="dummy.txt",
        hash=HashTuple(algorithm="md5", value=compute_md5sum(contents)),
        size=len(contents),
        mtime=int(time.time()) - 86400,
        tags=b"",
        source=ByteStream(
            stream=list_to_async_iterator(contents_chunked),
            chunk_size=8,
        ),
    )
    return [(fo, contents)]


@pytest.fixture(scope="session", autouse=True)  # type: ignore[misc]
def mock_source_handler_files_random() -> ObjectStoreHandler:
    handler: ObjectStoreHandler = MockFileHandler("files_random")
    return handler


@pytest.fixture(scope="session", autouse=True)  # type: ignore[misc]
def mock_source_handler_files_corrupt() -> ObjectStoreHandler:
    handler: ObjectStoreHandler = MockFileHandler("files_corrupt")
    return handler


@pytest.fixture(scope="session", autouse=True)  # type: ignore[misc]
def mock_source_handler_directories_empty() -> ObjectStoreHandler:
    handler: ObjectStoreHandler = MockFileHandler("directories_empty")
    return handler


@pytest.fixture(scope="session", autouse=True)  # type: ignore[misc]
def mock_file_objects() -> MockFileObjects:
    return MockFileObjects().initialize(file_count=2000)
