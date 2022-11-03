import pytest

from lakedrive.core.handlers import ObjectStoreHandler
from lakedrive.core import get_scheme_handler
from lakedrive.s3.handler import S3Handler

from ..helpers.object_store import create_object_store_s3


@pytest.fixture(scope="class", autouse=True)  # type: ignore[misc]
def s3_source_handler(directory: str = "s3://source") -> S3Handler:
    handler = get_scheme_handler(directory, max_read_threads=512, max_write_threads=128)
    assert isinstance(handler, S3Handler)
    return handler


@pytest.fixture(scope="class", autouse=True)  # type: ignore[misc]
def s3_target_handler(directory: str = "s3://target") -> S3Handler:
    handler = get_scheme_handler(directory, max_read_threads=512, max_write_threads=128)
    assert isinstance(handler, S3Handler)
    return handler


@pytest.fixture(scope="class", autouse=True)  # type: ignore[misc]
def s3_source_handler_subpath(directory: str = "s3://source/subpath") -> S3Handler:
    handler = get_scheme_handler(directory)
    assert isinstance(handler, S3Handler)
    return handler


@pytest.fixture(scope="class", autouse=True)  # type: ignore[misc]
def s3_target_handler_subpath(directory: str = "s3://target/subpath") -> S3Handler:
    handler = get_scheme_handler(directory)
    assert isinstance(handler, S3Handler)
    return handler


@pytest.fixture(scope="class", autouse=True)  # type: ignore[misc]
def object_store_s3_files(location: str = "s3://test-files") -> ObjectStoreHandler:
    return create_object_store_s3(location)
