from __future__ import annotations

from asyncio import StreamReader, StreamWriter
from typing import Dict, Iterator, Any, Tuple

from lakedrive.httplibs.objects import ConnectConfiguration, HttpConnectionMeta
from lakedrive.httplibs.request import HttpRequest
from lakedrive.httplibs.connection import HttpConnection

from ..helpers.http_responses import RESPONSES


def fake_response(template: str) -> Iterator[bytes]:
    for line in template.split("\n"):
        yield line.encode()


class FakeLock:
    async def __aenter__(self) -> None:
        pass

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        pass


class FakeReader:
    def __init__(self, **extra_args: Any):

        try:
            self.response_exception = extra_args["read_response_exception"]
        except KeyError:
            pass
        try:
            self.content_exception = extra_args["read_content_exception"]
        except KeyError:
            pass

        self.reader_at_eof = extra_args.get("reader_at_eof", False)

        try:
            response_template = extra_args["response_template"]
            self.fake_response = fake_response(RESPONSES[response_template])
        except KeyError:
            pass

        self.content_char = extra_args.get("content_char", ".")

    async def readuntil(self) -> bytes:
        if hasattr(self, "response_exception"):
            raise self.response_exception(b"", None)
        if hasattr(self, "fake_response"):
            try:
                bytes_read = next(self.fake_response)
                assert isinstance(bytes_read, bytes)
                return bytes_read
            except StopIteration:
                return b""
            except AssertionError:
                return b""
        else:
            return b""

    async def readline(self) -> bytes:
        if hasattr(self, "content_exception"):
            raise self.content_exception(b"", None)
        bytes_read = self.content_char.encode()
        try:
            assert isinstance(bytes_read, bytes)
        except AssertionError:
            return b""
        return bytes_read

    async def readexactly(self, size: int) -> bytes:
        if hasattr(self, "content_exception"):
            raise self.content_exception(b"", None)
        bytes_read = (self.content_char * size).encode()
        try:
            assert isinstance(bytes_read, bytes)
        except AssertionError:
            return b""
        return bytes_read

    def at_eof(self) -> bool:
        try:
            assert isinstance(self.reader_at_eof, bool)
        except AssertionError:
            return False
        return self.reader_at_eof


class FakeWriter:
    def __init__(self, **extra_args: Any):
        self.extra_info = extra_args.get("writer_extra_info", {"peername": "dummy"})

    def get_extra_info(self, key: str) -> Any:
        return self.extra_info.get(key, None)

    async def wait_closed(self) -> None:
        return

    def write(self, data: bytes) -> None:
        return

    async def drain(self) -> None:
        return

    def close(self) -> None:
        return


class FakeHttpConnection(HttpConnection):
    def __init__(self, **extra_args: Any):
        self.connection: Tuple[StreamReader, StreamWriter] = (
            FakeReader(**extra_args),  # type: ignore[assignment]
            FakeWriter(**extra_args),
        )
        self.thread_no = 0
        self.is_open = False

    async def _new_connection(self) -> Tuple[StreamReader, StreamWriter]:
        return self.connection


class FakeHttpConnect:
    def __init__(self, config: ConnectConfiguration):
        self.config = config
        self.request_url = ""

    def generate_headers(
        self,
        resource: str,
        query_string: str = "",
        method: str = "GET",
        headers: Dict[str, str] = {},
        payload_hash: str = "UNSIGNED-PAYLOAD",
    ) -> Dict[str, str]:
        return {}


class MockReader:
    def __init__(self, **extra_args: Any):
        self._readuntil = extra_args.get("mock_reader_readuntil", None)

    async def readuntil(self) -> bytes:
        if self._readuntil:
            r = await self._readuntil()
            assert isinstance(r, bytes)
            return r
        return b""

    async def readline(self) -> bytes:
        return b""

    async def readexactly(self, size: int) -> bytes:
        return b""

    def at_eof(self) -> bool:
        return False


class MockWriter:
    def __init__(self, **extra_args: Any):
        self.extra_info = {"peername": "dummy"}
        self._write = extra_args.get("mock_writer_write", None)

    def get_extra_info(self, key: str) -> Any:
        return self.extra_info.get(key, None)

    async def wait_closed(self) -> None:
        return

    def write(self, data: bytes) -> None:
        if self._write:
            self._write(data)

    async def drain(self) -> None:
        pass

    def close(self) -> None:
        pass


class MockHttpConnection(HttpConnection):
    def __init__(self, **extra_args: Any):
        self.connection: Tuple[StreamReader, StreamWriter] = (
            MockReader(**extra_args),  # type: ignore[assignment]
            MockWriter(**extra_args),
        )
        self.thread_no = 0
        self.is_open = False

    async def _new_connection(self) -> Tuple[StreamReader, StreamWriter]:
        return self.connection


class MockHttpRequest(HttpRequest):
    def __init__(self, **extra_args: Any):
        self.http_connections = [MockHttpConnection(**extra_args)]

        self.config = ConnectConfiguration()
        self.http_class = HttpConnectionMeta

        self.connections = 1
        # ignore incompatible type as its a dummy placeholder -- we are not testing lock
        self.locks = [FakeLock() for _ in range(self.connections)]  # type: ignore[misc]
