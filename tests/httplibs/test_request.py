import re
import asyncio

from gzip import compress as gzip_compress
from zlib import compress as zlib_compress
from typing import List, Any, AsyncIterator

from lakedrive.core.objects import (
    ByteStream,
    FileObject,
    HashTuple,
)
from lakedrive.httplibs.objects import (
    ConnectConfiguration,
    HttpConnectionMeta,
    HttpResponse,
)
from lakedrive.httplibs.request import (
    HttpRequest,
    content_length_from_headers,
    update_http_response_body,
)
from lakedrive.utils.helpers import compute_md5sum

from ..helpers.async_parsers import pytest_asyncio
from ..helpers.generate_objects import mock_file_objects
from ..helpers.http_responses import RESPONSES
from ..helpers.http_connection import FakeHttpConnection, MockHttpRequest


async def async_iterator_tolist(iterator: AsyncIterator[Any]) -> List[Any]:
    return [chunk async for chunk in iterator]


class FakeLock:
    async def __aenter__(self) -> None:
        pass

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        pass


class FakeHttpRequest(HttpRequest):
    def __init__(self, **extra_args: Any):
        self.http_connections = [FakeHttpConnection(**extra_args)]

        self.config = ConnectConfiguration()
        self.http_class = HttpConnectionMeta
        self.connections = 1
        # ignore incompatible type as its a dummy placeholder -- we are not testing lock
        self.locks = [FakeLock() for _ in range(self.connections)]  # type: ignore[misc]


def test_content_length_from_headers() -> None:
    empty = content_length_from_headers({})
    assert empty is None

    valid_int = content_length_from_headers({"content-length": "1024"})
    assert isinstance(valid_int, int)
    assert valid_int == 1024

    invalid_int = content_length_from_headers({"content-length": "1024x"})
    assert invalid_int is None

    invalid_value = content_length_from_headers({"content-length": "-1"})
    assert invalid_value is None


def test_update_http_response_body() -> None:
    body_input = "random content".encode()
    http_response = update_http_response_body(
        HttpResponse(status_code="200"), body_input
    )
    assert http_response.body == body_input

    http_response = update_http_response_body(
        HttpResponse(status_code="200", headers={"content-encoding": "gzip"}),
        gzip_compress(body_input),
    )
    assert http_response.body == body_input

    http_response = update_http_response_body(
        HttpResponse(status_code="200", headers={"content-encoding": "deflated"}),
        zlib_compress(body_input),
    )
    assert http_response.body == body_input


class TestHttpRequestFailures:
    """Tests aimed at triggering HttpRequest failures, to reach code that is
    not hit in a (succesful) use-case/ integration test"""

    def test_http_request_get_close(self) -> None:
        request = FakeHttpRequest(response_template="200_close")
        http_response = asyncio.run(request.get())
        assert isinstance(http_response, HttpResponse)
        assert http_response.status_code == "200"
        assert http_response.headers == {"connection": "Close"}

    def test_http_request_get_content(self) -> None:
        request = FakeHttpRequest(
            response_template="200_content-length", content_char="."
        )
        http_response = asyncio.run(request.get())
        assert isinstance(http_response, HttpResponse)
        assert http_response.status_code == "200"
        assert http_response.headers == {"content-length": "1024"}
        assert http_response.body == ("." * 1024).encode()

    def test_http_request_get_content_incomplete(self) -> None:
        request = FakeHttpRequest(
            response_template="200_content-length",
            read_content_exception=asyncio.IncompleteReadError,
        )
        http_response = asyncio.run(request.get())
        assert isinstance(http_response, HttpResponse)
        assert http_response.status_code == "408"
        assert http_response.headers == {}
        assert http_response.error_msg == "IncompleteReadError"

    def test_http_request_downstream_empty(self) -> None:
        request = FakeHttpRequest(response_template="200_chunked", content_char="0")
        chunks = asyncio.run(async_iterator_tolist(request.downstream()))
        assert len(chunks) == 1
        assert chunks[0] == b""

        # same, but with error during reading
        request = FakeHttpRequest(
            response_template="200_chunked",
            content_char="0",
            read_content_exception=asyncio.IncompleteReadError,
        )
        chunks = asyncio.run(async_iterator_tolist(request.downstream()))
        assert len(chunks) == 0


class TestHttpRequestWriteFailures:
    def setup_method(self) -> None:
        """Note: these defaults are (/must be) reset for each test
        in this class -- i.e. do not re-use"""
        self.bytes_written = b""
        self.ready_to_receive = False
        self.write_count = 0
        self.sabotage_count = 0
        self.sabotage_max = 3

    async def _mock_reader_readuntil(self) -> bytes:
        return bytes(RESPONSES["200_close"].encode())

    def _faulty_writer(self, data: bytes) -> None:
        self.write_count += 1

        # upstream code should be able to recover from <=N network-failures
        # to test this, throw an exception every 4th (4, 8, 12,...) round
        if self.sabotage_count < self.sabotage_max:
            if self.write_count % (4 + self.sabotage_count * 4) == 0:
                self.write_count = 0
                self.sabotage_count += 1
                self.bytes_written = b""
                raise ConnectionError("random network error")

        # reset bytes written at abort
        if len(data) == 4 and data == b"\r\n\r\n":
            self.bytes_written = b""
            self.ready_to_receive = False
            return

        # only start writing data after a PUT is sent
        if self.ready_to_receive is False:
            if re.match(b"^PUT ", data[0:4]):
                self.ready_to_receive = True
            return

        self.bytes_written += data

    @pytest_asyncio
    async def test_upstream_incorrect_bytes_smallfile(self) -> None:
        # get object where chunk_size_kb > object_size
        file_object = mock_file_objects(
            count=1, chunk_size_kb=128, minsize_kb=64, maxsize_kb=64
        )[0]
        assert isinstance(file_object, FileObject)
        if callable(file_object.source):
            file_object.source = await file_object.source()
        assert isinstance(file_object.source, ByteStream)
        assert isinstance(file_object.source.stream, AsyncIterator)

        bad_content_length = file_object.size - 1024  # sabotage bytes length

        async with MockHttpRequest(
            mock_reader_readuntil=self._mock_reader_readuntil,
        ) as client:
            http_response = await client.upstream(
                file_object.name,
                bad_content_length,
                (file_object.source.stream, file_object.source.chunk_size),
            )
            assert isinstance(http_response, HttpResponse)
            assert http_response.status_code == "424"
            bytes_read_str = f"{file_object.size}/{file_object.size - 1024}"
            assert (
                http_response.error_msg
                == f"Error reading from source: {bytes_read_str} bytes read"
            )

    @pytest_asyncio
    async def test_upstream_retry(self) -> None:
        """objects <= (chunk_size * 32) are retried up to 3 times,
        by using the default self.sabotage_max of 3, this test
        should succeed despite using the _faulty_writer()"""
        file_object = mock_file_objects(
            count=1, chunk_size_kb=4, minsize_kb=64, maxsize_kb=64
        )[0]
        assert isinstance(file_object, FileObject)
        if callable(file_object.source):
            file_object.source = await file_object.source()
        assert isinstance(file_object.source, ByteStream)
        assert isinstance(file_object.source.stream, AsyncIterator)

        async with MockHttpRequest(
            mock_writer_write=self._faulty_writer,
            mock_reader_readuntil=self._mock_reader_readuntil,
        ) as client:
            http_response = await client.upstream(
                file_object.name,
                file_object.size,
                (file_object.source.stream, file_object.source.chunk_size),
            )
            assert isinstance(http_response, HttpResponse)
            assert http_response.status_code == "200"

        # verify if file_object attributes match file received by _mock_writer_write()
        # fo = stream_object.file
        assert file_object.size == len(self.bytes_written)
        assert isinstance(file_object.hash, HashTuple)
        assert file_object.hash.value == compute_md5sum(self.bytes_written)

    @pytest_asyncio
    async def test_upstream_retry_max(self) -> None:
        """objects <= (chunk_size * 32) are retried up to 3 times,
        this test function retries > 3 times and should thus lead to a
        failed write using the _faulty_writer()"""
        self.sabotage_max = 4

        file_object = mock_file_objects(
            count=1, chunk_size_kb=4, minsize_kb=64, maxsize_kb=64
        )[0]
        assert isinstance(file_object, FileObject)
        if callable(file_object.source):
            file_object.source = await file_object.source()
        assert isinstance(file_object.source, ByteStream)
        assert isinstance(file_object.source.stream, AsyncIterator)

        async with MockHttpRequest(
            mock_writer_write=self._faulty_writer,
            mock_reader_readuntil=self._mock_reader_readuntil,
        ) as client:
            http_response = await client.upstream(
                file_object.name,
                file_object.size,
                (file_object.source.stream, file_object.source.chunk_size),
            )
            assert isinstance(http_response, HttpResponse)
            assert http_response.status_code != "200"
            assert http_response.error_msg == "Upload retries exceeded"

    @pytest_asyncio
    async def test_upstream_no_retry(self) -> None:
        """objects > (chunk_size * 32) are not retried,
        and should fail using the _faulty_writer()"""
        file_object = mock_file_objects(
            count=1, chunk_size_kb=1, minsize_kb=64, maxsize_kb=64
        )[0]
        assert isinstance(file_object, FileObject)
        if callable(file_object.source):
            file_object.source = await file_object.source()
        assert isinstance(file_object.source, ByteStream)
        assert isinstance(file_object.source.stream, AsyncIterator)

        async with MockHttpRequest(
            mock_writer_write=self._faulty_writer,
            mock_reader_readuntil=self._mock_reader_readuntil,
        ) as client:
            http_response = await client.upstream(
                file_object.name,
                file_object.size,
                (file_object.source.stream, file_object.source.chunk_size),
            )
            assert isinstance(http_response, HttpResponse)
            assert http_response.status_code != "200"
            assert http_response.error_msg == "Upload retries exceeded"
