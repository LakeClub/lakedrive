import pytest

from typing import AsyncIterator, Callable, Coroutine

from lakedrive.core.objects import ByteStream, FileObject
from lakedrive.httplibs.objects import (
    ConnectConfiguration,
    HttpConnectionMeta,
)

from ..helpers.generate_objects import mock_file_objects
from ..helpers.http_responses import (
    aiter_yield_httpresponse_403,
    aiter_yield_httpresponse_404,
    aiter_yield_httpresponse_500,
)
from ..helpers.async_parsers import pytest_asyncio
from ..httplibs.test_request import FakeHttpRequest


def _file_object_with_read_error(
    aiter_function: Callable[[bytes], AsyncIterator[bytes]]
) -> FileObject:
    return FileObject(
        name="mock",
        source=ByteStream(
            aiter_function(b""),
            http_client=FakeHttpRequest(),
        ),
    )


class TestFileObject:
    def test_read_no_data(self) -> None:
        empty_file_object = FileObject(name="dummy")
        data = empty_file_object.read()
        assert isinstance(data, bytes)
        assert data == b""

    @pytest_asyncio
    async def test_read_no_data_async(self) -> None:
        empty_file_object = FileObject(name="dummy")
        async_func = empty_file_object.read()
        assert isinstance(async_func, Coroutine)
        data = await async_func
        assert isinstance(data, bytes)
        assert data == b""

    def test___len__(self) -> None:
        file_object = mock_file_objects(count=1, minsize_kb=1, maxsize_kb=1)[0]
        size = len(file_object)
        assert isinstance(size, int)
        assert size == 1024

    @pytest_asyncio
    async def test_byte_stream(self) -> None:
        file_object = mock_file_objects(
            count=1, minsize_kb=1, maxsize_kb=1, chunk_size_kb=1
        )[0]
        tup = await file_object.byte_stream()
        assert isinstance(tup, tuple)
        assert len(tup) == 2
        stream, chunk_size = tup
        assert isinstance(stream, AsyncIterator)
        assert isinstance(chunk_size, int)
        assert chunk_size == 1024

    @pytest_asyncio
    async def test_byte_stream_invalid_data(self) -> None:
        """test with invalid data type"""
        file_object = FileObject(
            name="dummy", source="invalid"  # type: ignore[arg-type]
        )
        assert await file_object.byte_stream() is None

    @pytest_asyncio
    async def test_byte_stream_no_stream_or_content(self) -> None:
        file_object = mock_file_objects(
            count=1, minsize_kb=1, maxsize_kb=1, chunk_size_kb=1
        )[0]
        assert not isinstance(file_object.source, ByteStream)
        assert file_object.source is not None
        assert callable(file_object.source) is True
        file_object.source = await file_object.source()
        assert isinstance(file_object.source, ByteStream)
        file_object.source.stream = None  # "disable" stream
        assert await file_object.byte_stream() is None

    @pytest_asyncio
    async def test_read_http_error_permission(self) -> None:
        erroneous_fo = _file_object_with_read_error(aiter_yield_httpresponse_403)
        with pytest.raises(PermissionError) as error:
            response = erroneous_fo.read()
            assert isinstance(response, Coroutine)
            await response
        assert str(error.value) == "mock"

    @pytest_asyncio
    async def test_read_http_error_file_not_found(self) -> None:
        erroneous_fo = _file_object_with_read_error(aiter_yield_httpresponse_404)
        with pytest.raises(FileNotFoundError) as error:
            response = erroneous_fo.read()
            assert isinstance(response, Coroutine)
            await response
        assert str(error.value) == "mock"

    @pytest_asyncio
    async def test_read_http_error_misc(self) -> None:
        erroneous_fo = _file_object_with_read_error(aiter_yield_httpresponse_500)
        with pytest.raises(ConnectionError) as error:
            response = erroneous_fo.read()
            assert isinstance(response, Coroutine)
            await response
        assert str(error.value) == "mock"


class TestHttpConnectionMeta:
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(self) -> None:
        self.http_conn_meta = HttpConnectionMeta(ConnectConfiguration())

    def test_generate_headers(self) -> None:
        headers = self.http_conn_meta.generate_headers("/resource", method="GET")
        assert isinstance(headers, dict)
        assert headers == {}

    def test_generate_headers_streaming(self) -> None:
        headers = self.http_conn_meta.generate_headers_streaming("/resource", 0, 0)
        assert isinstance(headers, dict)
        assert headers == {}
