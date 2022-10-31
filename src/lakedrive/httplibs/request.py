from __future__ import annotations
import ssl
import logging
import asyncio
import hashlib

from shutil import ReadError
from asyncio.streams import StreamWriter
from typing import List, Optional, Tuple, Dict, AsyncIterator, Any, Type
from gzip import decompress as gzip_decompress
from zlib import decompress as zlib_decompress

from ..httplibs.connection import HttpConnection
from ..httplibs.helpers import headers_to_string, bytestream_to_bytes
from ..httplibs.objects import (
    ConnectConfiguration,
    HttpConnectionMeta,
    HttpResponse,
    HttpResponseError,
)

logger = logging.getLogger(__name__)

MAX_BUFFERED_CHUNKS = 32


HTTP_BASE_HEADERS = {
    "User-Agent": "python-lakedrive/0.80.0",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "*/*",
    "Connection": "keep-alive",
}


def failed_dependency_read(error_msg: str = "Cant read from source") -> HttpResponse:
    return HttpResponse(
        status_code="424",
        error_msg=f"Error reading from source: {error_msg}",
    )


def content_length_from_headers(headers: Dict[str, str]) -> Optional[int]:
    """Return content-length as integer only if defined and >=0,
    if undefined or not correctly defined, return None"""
    try:
        val = int(headers["content-length"])
        if val < 0:
            raise ValueError
        return val
    except KeyError:
        return None
    except ValueError:
        return None


def update_http_response_body(
    http_response: HttpResponse,
    body: bytes,
) -> HttpResponse:
    content_encoding = http_response.headers.get("content-encoding", "")
    if content_encoding == "gzip":
        http_response.body = gzip_decompress(body)
    elif content_encoding == "deflated":
        http_response.body = zlib_decompress(body)
    else:
        # pass as-is
        http_response.body = body
    return http_response


async def write_chunks(
    writer: StreamWriter, buffered_chunks: List[Tuple[int, bytes]] = []
) -> int:
    # chunk_buffer = buffered_chunks
    _content_written = 0

    for content_length, chunk in buffered_chunks:
        try:
            writer.write(chunk)
            # ensure not writing faster than data gets sent
            await writer.drain()
        except Exception as error:
            logger.error(error)
            # log original error
            # re-raise every write-error as a ConnectionError
            raise ConnectionError
        _content_written += content_length
    return _content_written


class HttpRequest:
    """"""

    def __init__(
        self,
        config: ConnectConfiguration,
        http_class: Type[HttpConnectionMeta],
        connections: int = 2,
    ) -> None:
        self.ssl_context = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH,
        )
        self.connections = connections
        self.locks = [asyncio.Lock() for _ in range(connections)]
        self.http_connections = [
            HttpConnection(config.connection_args, thread_no)
            for thread_no in range(connections)
        ]

        self.config = config
        self.http_class = http_class
        # self._event_loop = None

    def new_request(self) -> HttpConnectionMeta:
        """Default, this is typically overridden in inherited class
        e.g. HttpRequestS3"""
        return HttpConnectionMeta(ConnectConfiguration())

    async def __aenter__(self) -> HttpRequest:
        # self._event_loop = asyncio.get_running_loop()
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Exit context"""
        await self.close_connections()

    async def close_connections(self) -> None:
        """Closing all connections"""
        logger.debug("Closing all connections")
        await asyncio.gather(
            *[conn.close_connection() for conn in self.http_connections]
        )

    async def head(
        self,
        resource: str = "",
        parameter_str: str = "",
        tid: int = 0,
    ) -> HttpResponse:

        http_connection = self.new_request()

        headers = http_connection.generate_headers(
            resource, query_string=parameter_str, method="HEAD"
        )
        query = "\r\n".join(
            [
                f"HEAD /{http_connection.request_url} HTTP/1.1",
                headers_to_string(headers),
                "",
            ]
        )

        logger.debug(query)
        thread_no = tid % self.connections

        async with self.locks[thread_no]:
            http_conn = self.http_connections[thread_no]
            http_response = await http_conn.execute(query.encode())
        return http_response

    async def get(
        self,
        resource: str = "",
        parameter_str: str = "",
        tid: int = 0,
    ) -> HttpResponse:
        http_connection = self.new_request()
        headers = http_connection.generate_headers(
            resource, query_string=parameter_str, method="GET"
        )

        thread_no = tid % self.connections

        query = "\r\n".join(
            [
                f"GET /{http_connection.request_url} HTTP/1.1",
                headers_to_string(headers),
                "",
            ]
        )

        async with self.locks[thread_no]:
            http_conn = self.http_connections[thread_no]

            body = b""
            reader, _ = await http_conn.write(query.encode())
            http_response = await http_conn.get_response_headers()
            if http_response.headers.get("connection", "").lower() == "close":
                await http_conn.close_connection()
            else:
                content_length = int(http_response.headers.get("content-length", -1))
                try:
                    if content_length > 0:
                        async for chunk in http_conn.read_content(
                            reader,
                            content_length,
                        ):
                            body += chunk
                    else:
                        async for chunk in http_conn.read_chunks(reader):
                            body += chunk

                except asyncio.IncompleteReadError:
                    return HttpResponse(
                        status_code="408", error_msg="IncompleteReadError"
                    )

        if body:
            return update_http_response_body(http_response, body)
        else:
            return http_response

    async def put(
        self,
        resource: str,
        parameter_str: str = "",
        body: bytes = b"",
        tid: int = 0,
    ) -> HttpResponse:
        thread_no = tid % self.connections

        content_length = len(body)
        http_connection = self.new_request()
        headers = http_connection.generate_headers(
            resource,
            query_string=parameter_str,
            method="PUT",
            payload_hash=hashlib.sha256(body).hexdigest(),
        )
        headers["content-length"] = (
            str(len(body)) if content_length <= 0 else str(content_length)
        )

        query = "\r\n".join(
            [
                f"PUT /{http_connection.request_url} HTTP/1.1",
                headers_to_string(headers),
                "",
            ]
        )

        async with self.locks[thread_no]:
            http_conn = self.http_connections[thread_no]
            http_response = await http_conn.execute(query.encode() + body, retries=3)
        return http_response

    async def upstream(
        self,
        resource: str,
        content_length: int,
        filestream: Optional[Tuple[AsyncIterator[bytes], int]] = None,
        thread_id: int = 0,
    ) -> HttpResponse:
        """"""
        if filestream:
            stream, chunk_size = filestream
        else:
            stream = None
            chunk_size = 0

        # if chunk_size is unknown, or filesize is small, upload directly
        if (
            stream is None
            or chunk_size == 0
            or content_length <= chunk_size
            or content_length <= (1024 * 32)
        ):
            # direct upload
            if isinstance(stream, AsyncIterator):
                try:
                    body, bytes_read = await bytestream_to_bytes(stream)
                    if content_length != bytes_read:
                        raise ReadError(
                            f"{bytes_read}/{str(content_length)} bytes read"
                        )
                except ReadError as error:
                    return failed_dependency_read(error_msg=str(error))

            else:
                body = b""

            return await self.put(
                resource,
                body=body,
                tid=thread_id,
            )

        # else: chunked upload
        thread_no = thread_id % self.connections
        http_connection = self.new_request()
        headers = http_connection.generate_headers_streaming(
            resource, content_length, chunk_size
        )
        query = "\r\n".join(
            [
                f"PUT /{http_connection.request_url} HTTP/1.1",
                headers_to_string(headers),
                "",
            ]
        )
        """To enable retry capability reads are kept in memory via buffered_chunks[].
        Because memory is finite, this is only done for parts up to N bytes.

        If MAX_BUFFERED_CHUNKS == 32 AND DEFAULT_CHUNK_SIZE == 1MB, N = 32MB.
        I.e. files up to 32MB are kept in memory until fully written using these
        defaults which seems reasonable.

        Setting it to high risks memory issues when sending many files in parallel
        (e.g. 30 parallel uploads requires 960MB max. additional memory).
        Setting it to low may raise number of re-reads at the source.

        Files larger than 32MB (/MAX_BUFFERED_CHUNKS * DEFAULT_CHUNK_SIZE) should
        really be sent using partial uploads."""
        buffered_chunks: List[Tuple[int, bytes]] = []
        retry_count = 0

        if content_length <= (MAX_BUFFERED_CHUNKS * chunk_size):
            retry_max = 3
        else:
            retry_max = 0

        http_response = HttpResponse()

        async with self.locks[thread_no]:

            http_conn = self.http_connections[thread_no]
            content_length_written = 0

            while True:
                if retry_count > retry_max:
                    error_msg = "Upload retries exceeded"
                    http_response.error_msg = error_msg
                    logger.error(error_msg)
                    break
                retry_count += 1

                if retry_count > 1:
                    # abort previous failed upload
                    await http_conn.write_abort()

                try:
                    _, writer = await http_conn.write(query.encode())
                    content_length_written = await write_chunks(writer, buffered_chunks)

                    while True:
                        # write individual chunks
                        try:
                            chunk = bytes(await stream.__anext__())

                            if len(chunk) != chunk_size:
                                # read chunk size should never exceed specified chunk
                                assert len(chunk) < chunk_size

                                # last chunk is often smaller. Verify if (length written
                                # + length of last chunk) equals expected content_length
                                assert (
                                    content_length_written + len(chunk)
                                ) == content_length
                            # else: valid chunk

                        except StopAsyncIteration:
                            # no more chunks to read, last upload is an empty chunk
                            assert content_length_written == content_length
                            chunk = b""
                        except Exception as error:
                            raise ReadError(str(error))

                        chunk_p = (len(chunk), http_connection.encapsulate_chunk(chunk))
                        if retry_max > 0:
                            # keep chunk for possibly retry
                            buffered_chunks.append(chunk_p)

                        # write chunk
                        content_length_written += await write_chunks(writer, [chunk_p])
                        if not chunk:
                            break  # last chunk sent, stop inner (chunks-)loop

                    logger.debug("Written all chunks, waiting for response")
                    http_response = await http_conn.get_response_headers_safe()

                    break  # upload finished, stop outer loop

                except ReadError as error:
                    # abort upload due to error reading from source
                    logger.error(error)
                    await http_conn.write_abort()
                    return failed_dependency_read(error_msg=str(error))

                except ConnectionError as error:
                    # failed to write chunk due to a networking issue
                    logger.error(error)
                    continue  # continue outer loop (== retry upload)

        return http_response

    async def downstream(
        self,
        resource: str = "",
        thread_id: int = 0,
        chunk_size: int = 1024**2 * 16,
    ) -> AsyncIterator[bytes]:
        http_connection = self.new_request()
        headers = http_connection.generate_headers(resource, method="GET")

        thread_no = thread_id % self.connections
        query = "\r\n".join(
            [
                f"GET /{http_connection.request_url} HTTP/1.1",
                headers_to_string(headers),
                "",
            ]
        )

        async with self.locks[thread_no]:
            http_conn = self.http_connections[thread_no]
            reader, _ = await http_conn.write(query.encode())

            http_response = await http_conn.get_response_headers_safe()
            if http_response.status_code != "200":
                http_response.error_msg = (
                    http_response.error_msg or http_connection.request_url
                )
                raise HttpResponseError(http_response)

            content_length = content_length_from_headers(http_response.headers)

            try:
                if content_length is None:
                    # undefined size -- use chunked
                    async for chunk in http_conn.read_chunks(reader):
                        yield chunk
                else:
                    if content_length > 0:
                        # fixed size file
                        async for chunk in http_conn.read_content(
                            reader,
                            content_length,
                            chunk_size=chunk_size,
                        ):
                            yield chunk
                    else:
                        # empty file
                        yield b""
            except asyncio.IncompleteReadError:
                return

    async def delete(
        self,
        resource: str = "",
        thread_id: int = 0,
    ) -> HttpResponse:
        http_connection = self.new_request()
        headers = http_connection.generate_headers(resource, method="DELETE")
        thread_no = thread_id % self.connections

        query = "\r\n".join(
            [
                f"DELETE /{http_connection.request_url} HTTP/1.1",
                headers_to_string(headers),
                "",
            ]
        )

        async with self.locks[thread_no]:
            http_conn = self.http_connections[thread_no]
            http_response = await http_conn.execute(query.encode())

        return http_response
