import pytest
import asyncio

from datetime import datetime
from typing import AsyncIterator, List, Any

from lakedrive.core.objects import ByteStream, FileObject
from lakedrive.core.handlers import (
    iter_bytes,
    ObjectStoreHandler,
)


async def get_iter_bytes(bytes_array: bytes) -> bytes:
    return b"".join([b async for b in iter_bytes(bytes_array)])


async def read_async_iterator_bytes(iter: AsyncIterator[bytes]) -> bytes:
    return b"".join([b async for b in iter])


async def read_async_iterator_anylist(iter: AsyncIterator[List[Any]]) -> List[Any]:
    return list([b async for b in iter])


def test_iter_bytes() -> None:
    array_in = b"helloworld"
    array_out = asyncio.run(get_iter_bytes(array_in))
    assert array_in == array_out


class TestObjectStoreHandler:
    """Minimal (/empty) tests to ensure structural integrity of the class only,
    intensive testing on data is done via implementations like localfs,s3"""

    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(self) -> None:
        self.file_object = FileObject(
            name="dummy",
            size=len("nobytes"),
            mtime=int(datetime.now().timestamp()),
            hash=None,
            tags=b"",
            source=ByteStream(
                stream=iter_bytes(b"nobytes"),
                chunk_size=1,
            ),
        )

        self.handler = ObjectStoreHandler("dummy", 1, 2, 3)
        assert isinstance(self.handler, ObjectStoreHandler)
        assert self.handler.storage_target == "dummy"
        assert self.handler.stream_chunk_size == 1
        assert self.handler.max_read_threads == 2
        assert self.handler.max_write_threads == 3

        self.handler.object_list = [self.file_object]

    def test_filter_list(self) -> None:
        filters = [
            {"FilterBy": "name", "Value": "dummy"},
            {"FilterBy": "mtime", "Value": "1H"},
            {"FilterBy": "size", "Value": str(len("nobytes"))},
        ]
        for filter in filters:
            return_value = self.handler.filter_list([filter])
            assert isinstance(return_value, list)
            assert return_value == [self.file_object]

    def test_create_storage_target(self) -> None:
        assert (
            asyncio.run(self.handler.create_storage_target())
            == self.handler.storage_target
        )

    def test_storage_target_exists(self) -> None:
        return_value = asyncio.run(self.handler.storage_target_exists())
        assert isinstance(return_value, bool)
        assert return_value is False

    def test_list_contents(self) -> None:
        return_value = asyncio.run(self.handler.list_contents())
        assert isinstance(return_value, list)
        assert return_value == [self.file_object]

    def test_write_file(self) -> None:
        assert asyncio.run(self.handler.write_file(self.file_object)) is None

    def test_head_file(self) -> None:
        assert asyncio.run(self.handler.head_file(self.file_object.name)) is None

    def test_read_file(self) -> None:
        bytes_cmp = b"nobytes"
        new_file_object = asyncio.run(self.handler.read_file(self.file_object))
        assert isinstance(new_file_object, FileObject)

        # validate repeat calls of read() return same data
        for _ in range(2):
            new_file_object.seek(0)  # ensure stream starts at beginning
            bytes_read = new_file_object.read()

            assert isinstance(bytes_read, bytes)
            assert bytes_read == bytes_cmp

    def test_delete_file(self) -> None:
        assert asyncio.run(self.handler.delete_file(self.file_object)) is None

    def test_read_batch(self) -> None:
        return_value = asyncio.run(
            read_async_iterator_anylist(
                self.handler.read_batch([self.file_object], stream_chunk_size=1)
            )
        )
        assert isinstance(return_value, list)
        assert len(return_value) == 1
        assert isinstance(return_value[0], list)
        assert len(return_value[0]) == 1

        new_file_object = return_value[0][0]
        assert isinstance(new_file_object, FileObject)
        assert isinstance(new_file_object.source, ByteStream)
        assert isinstance(new_file_object.source.stream, AsyncIterator)
        assert isinstance(self.file_object.source, ByteStream)
        assert isinstance(self.file_object.source.stream, AsyncIterator)
        assert new_file_object.source.chunk_size == self.file_object.source.chunk_size

        assert asyncio.run(
            read_async_iterator_bytes(new_file_object.source.stream)
        ) == asyncio.run(read_async_iterator_bytes(self.file_object.source.stream))

    def test_write_batch(self) -> None:
        file_objects_batched = self.handler.read_batch(
            [self.file_object], stream_chunk_size=1
        )
        remaining_objects = asyncio.run(self.handler.write_batch(file_objects_batched))
        assert isinstance(remaining_objects, list)
        assert len(remaining_objects) == 0

    def test_delete_empty_batch(self) -> None:
        remaining = asyncio.run(self.handler.delete_batch())
        assert isinstance(remaining, list)
        assert len(remaining) == 0

    def test_delete_batch(self) -> None:
        remaining = asyncio.run(
            self.handler.delete_batch(file_objects=[self.file_object])
        )
        assert isinstance(remaining, list)
        assert len(remaining) == 0

    def test_delete_storage_target(self) -> None:
        deleted = asyncio.run(self.handler.delete_storage_target())
        assert isinstance(deleted, bool)
        assert deleted is True
