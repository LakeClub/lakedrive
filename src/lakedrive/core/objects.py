import logging
import asyncio

from io import BytesIO
from collections import namedtuple
from dataclasses import dataclass
from typing import (
    ByteString,
    Coroutine,
    List,
    NamedTuple,
    Optional,
    AsyncIterator,
    Tuple,
    Union,
    Callable,
    Any,
)

from ..httplibs.objects import HttpResponseError
from ..httplibs.request import HttpRequest
from ..utils.helpers import aiterator_bytes_io
from ..utils.event_loop import run_on_event_loop

HashTuple = namedtuple("HashTuple", ["algorithm", "value"])

logger = logging.getLogger(__name__)


@dataclass
class ByteStream:
    stream: Optional[AsyncIterator[bytes]] = None
    chunk_size: int = 0
    http_client: Optional[HttpRequest] = None

    content: Optional[BytesIO] = None
    content_position: int = 0
    end_of_stream: bool = False

    def new_content(self) -> BytesIO:
        self.content = BytesIO()
        return self.content

    def content_length(self) -> int:
        if not self.content:
            return 0
        else:
            return self.content.getbuffer().nbytes

    async def aseek(self, position: int) -> BytesIO:
        mem = self.content or self.new_content()
        content_length = mem.getbuffer().nbytes
        if content_length < position:
            # not sufficient data in buffer
            if self.end_of_stream is False:
                # get data first
                await self._read_stream(position - content_length)
                content_length = mem.getbuffer().nbytes  # update content_length
            else:
                # cant fetch more data
                pass

        if content_length > 0:
            self.content_position = min(content_length, position)
        else:
            self.content_position = 0
        mem.seek(self.content_position)
        return mem

    async def close(self) -> None:
        if self.end_of_stream is False:
            if isinstance(self.http_client, HttpRequest):
                await self.http_client.__aexit__()
                self.http_client = None
            self.end_of_stream = True
            self.stream = None  # stream is consumed
        else:
            pass

    async def _read_stream(self, bytes_requested: int) -> int:
        """
        if bytes_requested > 0, read until bytes_read is at or exceeding this number,
        else read until the end"""
        bytes_read = 0
        mem = None

        assert isinstance(self.stream, AsyncIterator)
        if isinstance(self.http_client, HttpRequest):
            # http-source
            try:
                while True:
                    if bytes_requested:
                        if bytes_read >= bytes_requested:
                            break
                    chunk = await self.stream.__anext__()
                    mem = mem or await self.aseek(self.content_length())  # go to end
                    assert mem.write(chunk) == len(chunk)
                    bytes_read += len(chunk)

            except HttpResponseError as error:
                await self.http_client.__aexit__()
                # catch common Http errors, and re-raise to
                # build-in equivalent
                if error.status_code == "404":
                    raise FileNotFoundError
                if error.status_code == "403":
                    raise PermissionError
                # un-common Http error
                # re-raise to default ConnectionError
                logger.error(error)
                raise ConnectionError
            except StopAsyncIteration:
                await self.close()

        else:
            # (local disk/in-memory) file-source
            try:
                while True:
                    if bytes_requested:
                        if bytes_read >= bytes_requested:
                            break
                    chunk = await self.stream.__anext__()
                    mem = mem or await self.aseek(self.content_length())  # go to end
                    assert mem.write(chunk) == len(chunk)
                    bytes_read += len(chunk)
            except StopAsyncIteration:
                await self.close()

        return bytes_read

    async def content_read(self, bytes_count: int) -> bytes:
        if bytes_count > 0:
            # read until 1. current position + (requested) byte_count,
            # 2. length of data -- whichever is less
            read_until_position = min(
                self.content_position + bytes_count, self.content_length()
            )
        else:
            read_until_position = self.content_length()

        mem = await self.aseek(self.content_position)
        data = mem.read(read_until_position - self.content_position)
        # update position by actual length of data in our array and return
        self.content_position += len(data)
        return data

    async def aread(self, size: Optional[int] = None) -> bytes:
        """Read n bytes from stream
        if bytes_requested (n) is 0 | None then"""

        bytes_requested = size or 0

        if (
            self.end_of_stream is True
            or not isinstance(self.stream, AsyncIterator)
            or (
                size
                and (self.content_position + bytes_requested) <= self.content_length()
            )
        ):
            # End-Of-Stream -> fetch nothing
            # Or, sufficient data available in cache
            pass
        else:
            # fetch new data
            await self._read_stream(bytes_requested)

        return await self.content_read(bytes_requested)


@dataclass
class FileObject:
    name: str
    size: int = -1
    hash: Optional[HashTuple] = None
    mtime: int = -1
    tags: ByteString = b""
    source: Optional[
        Union[ByteStream, Callable[..., Coroutine[Any, Any, ByteStream]]]
    ] = None
    content: Optional[BytesIO] = None

    def __len__(self) -> int:
        return self.size

    async def byte_stream(self) -> Optional[Tuple[AsyncIterator[bytes], int]]:

        if callable(self.source):
            self.source = await self.source()

        if not isinstance(self.source, ByteStream):
            return None

        if isinstance(self.source.stream, AsyncIterator):
            return (self.source.stream, self.source.chunk_size)
        elif isinstance(self.source.content, BytesIO):
            return (
                aiterator_bytes_io(self.source.content, self.source.chunk_size),
                self.source.chunk_size,
            )
        else:
            return None

    async def aread(self, size: Optional[int] = None) -> bytes:
        if self.source is None:
            # can safely assume file is empty
            return b""
        try:
            if not isinstance(self.source, ByteStream):
                self.source = await self.source()
                assert isinstance(self.source, ByteStream)

            data = await self.source.aread(size=size)

            if self.source.end_of_stream is True and not self.content:
                # stream consumed succesfully
                # ensure self.content points to the data
                self.size = self.source.content_length()
                # update self.size to actual size
                self.content = self.source.content

            else:
                pass
            return data

        except FileNotFoundError:
            raise FileNotFoundError(self.name)
        except PermissionError:
            raise PermissionError(self.name)
        except ConnectionError:
            raise ConnectionError(self.name)

    async def close(self) -> None:
        isinstance(self.source, ByteStream) and await self.source.close()

    def read(
        self, size: Optional[int] = None
    ) -> Union[bytes, Coroutine[Any, Any, bytes]]:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # no current event loop
            loop = None

        if loop and loop.is_running():
            return self.aread(size=size)
        else:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            task = asyncio.ensure_future(self.aread(size=size))
            return loop.run_until_complete(task)

    async def aseek(self, position: int) -> None:
        isinstance(self.source, ByteStream) and await self.source.aseek(position)

    def seek(self, position: int) -> None:
        isinstance(self.source, ByteStream) and run_on_event_loop(self.aseek, position)


# limited version used when immutability is required
# e.g. for set class operations
class FrozenFileObject(NamedTuple):
    name: str
    size: int = -1
    hash: Optional[HashTuple] = None
    mtime: int = -1
    tags: ByteString = b""


class FileUpdate(NamedTuple):
    action: str
    files: List[FileObject]
