from __future__ import annotations
import re
import ssl
import socket
import asyncio
import logging

from asyncio.streams import StreamReader, StreamWriter
from typing import AsyncIterator, Tuple, Dict, List, Any

from ..httplibs.objects import HttpResponse


TIMEOUT_READ = 10


logger = logging.getLogger(__name__)


def parse_header_line(line: str) -> Tuple[str, str]:
    if not re.match("^[-a-zA-Z0-9]*: ?.*", line):
        raise ValueError("Invalid header line")

    key, value = line.split(":", 1)
    return key.lower().strip(" "), value.strip(" ")


class HttpConnection:
    """Manage an HTTP connection"""

    def __init__(self, connection_args: List[Dict[str, Any]], thread_no: int) -> None:
        self.connection_args = connection_args
        self.thread_no = thread_no
        self.is_open = False

    def _verify_connection(self) -> bool:
        """Verify if http_connection is open by checking read/write state"""
        if not hasattr(self, "connection"):
            return False

        reader, writer = self.connection
        peer_info = writer.get_extra_info("peername")
        if peer_info is None:
            return False
        if reader.at_eof():
            return False
        return True

    async def _new_connection(self) -> Tuple[StreamReader, StreamWriter]:
        """Open a new http connection"""
        logger.debug("Initiating network connection")
        self.ssl_context = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH,
        )
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE

        # only validated ipv4, should implement and use ipv6 if possible
        connection_args_ipv4 = [
            ca for ca in self.connection_args if ca["family"] == socket.AF_INET
        ]
        if connection_args_ipv4:
            self.connection = await asyncio.open_connection(
                **connection_args_ipv4[0], ssl=self.ssl_context
            )
            self.is_open = True
            return self.connection
        raise ConnectionError("Cant setup ip(v4/v6) connection")

    async def get_connection(self) -> Tuple[StreamReader, StreamWriter]:
        if self._verify_connection() is False:
            return await self._new_connection()
        return self.connection

    async def write(self, data: bytes) -> Tuple[StreamReader, StreamWriter]:
        _, writer = await self.get_connection()

        writer.write(data)
        await writer.drain()
        return self.connection

    async def write_abort(self) -> None:
        await self.write(b"\r\n\r\n")
        await self.get_response_headers_safe()

    async def yield_response(self, reader: StreamReader) -> str:
        try:
            response = await asyncio.wait_for(reader.readuntil(), TIMEOUT_READ)
            return response.decode("latin1").rstrip()
        except asyncio.IncompleteReadError:
            logger.error(
                f"Connection IncompleteReadError,threadno:{str(self.thread_no)}"
            )
            raise
        except asyncio.TimeoutError:
            logger.error(f"Connection TimeoutError,threadno:{str(self.thread_no)}")
            raise

    async def get_response_headers(self) -> HttpResponse:
        reader, _ = await self.get_connection()

        status_line = await self.yield_response(reader)
        if not re.match("^HTTP/[1-9].[0-9]* [1-5][0-9]{2} ", status_line):
            # invalid http response
            raise asyncio.IncompleteReadError(b"", None)

        response_dict: Dict[str, str] = {}
        status_code = status_line.split(" ")[1]

        while True:
            try:
                key, value = parse_header_line(await self.yield_response(reader))
            except ValueError:
                break
            response_dict[key] = value
        return HttpResponse(status_code=status_code, headers=response_dict)

    async def get_response_headers_safe(self) -> HttpResponse:
        error_msg = ""
        try:
            http_response = await self.get_response_headers()
        except asyncio.IncompleteReadError:
            error_msg = "IncompleteReadError"
        except asyncio.TimeoutError:
            error_msg = "TimeoutError"

        if error_msg:
            http_response = HttpResponse(status_code="408", error_msg=error_msg)

        elif http_response and http_response.headers.get("connection", "") != "close":
            # connection can be re-used - skip close
            return http_response

        await self.close_connection()
        return http_response

    async def read_content(
        self,
        reader: StreamReader,
        content_lenght: int,
        chunk_size: int = 1024**2 * 16,
    ) -> AsyncIterator[bytes]:
        remaining = content_lenght
        while True:
            try:
                if remaining <= 0:
                    break
                if remaining <= chunk_size:
                    chunk_size = remaining
                    remaining = 0
                else:
                    remaining -= chunk_size
                chunk = await reader.readexactly(chunk_size)
                yield chunk
            except Exception as error:
                logger.error(str(error))
                raise asyncio.IncompleteReadError(b"", None)

    async def read_chunks(self, reader: StreamReader) -> AsyncIterator[bytes]:
        """Read chunks from chunked response."""
        while True:
            try:
                line = (await reader.readline()).rstrip()
                chunk_size = int(line, 16)
                if chunk_size == 0:
                    # read last CRLF
                    await reader.readline()
                    yield b""
                    break
                chunk = await reader.readexactly(chunk_size + 2)
                yield chunk[:-2]
            except Exception as error:
                logger.debug(error)
                raise asyncio.IncompleteReadError(b"", None)

    async def execute(self, query: bytes, retries: int = 3) -> HttpResponse:
        error_msg = ""
        while retries > 0:
            retries -= 1
            await self.write(query)
            http_response = await self.get_response_headers_safe()
            if not http_response.error_msg:
                # assume success
                return http_response
            # store last error message
            error_msg = http_response.error_msg

        # failed (retries depleted)
        await self.close_connection()
        return HttpResponse(status_code="408", error_msg=error_msg)

    async def close_stream(self, stream: StreamWriter) -> None:
        """Close underlying network connection"""
        # important: ignore subsequent calls to close connection as this function
        # may be called >1 in certain (race) conditions and we need to guarantuee
        # this wont propagate
        if self.is_open is False:
            return
        self.is_open = False
        # close() with sleep will ensure connection gets closed in time
        # DO NOT USE stream.wait_closed() because we cant guarantuee task is on
        # same event_loop as we also want read() be used outside (or in a new) loop
        stream.close()
        await asyncio.sleep(0)

    async def close_connection(self) -> None:
        """Close connection if active/ open"""
        if self._verify_connection() is True:
            logger.debug("Closing networking connection")
            _, writer = self.connection
            await self.close_stream(writer)
