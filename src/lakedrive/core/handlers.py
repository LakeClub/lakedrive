from __future__ import annotations
import asyncio

from dataclasses import dataclass
from typing import List, Dict, AsyncIterator, Union, Optional

from ..core.objects import ByteStream, FileObject
from ..core.list_filter import filter_file_objects


async def iter_bytes(byte_str: bytes) -> AsyncIterator[bytes]:
    """Yield bytes from 0 to `to` every `delay` seconds."""
    for b in byte_str:
        yield b.to_bytes(1, "little")
        await asyncio.sleep(0)


@dataclass
class StorageSupportFlags:
    # should be False by default. Only True for local fs. For object stores
    # it may be useful to show virtual dirs in certain cases (e.g. syncing
    # localfs to S3 including empty dirs, but not as default behavior.
    list_directories: bool = False


class SchemeError(Exception):
    """Custom Exception, raise on unknown scheme"""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def __str__(self) -> str:
        return str(self.message)


class ObjectStoreHandler:
    def __init__(
        self,
        storage_target: str,
        stream_chunk_size: int,
        max_read_threads: int,
        max_write_threads: int,
    ) -> None:
        self.storage_target = storage_target
        self.stream_chunk_size = stream_chunk_size
        self.max_read_threads = max_read_threads
        self.max_write_threads = max_write_threads

        self.support = StorageSupportFlags()
        self.object_list: List[FileObject] = []

    def filter_list(self, filters: List[Dict[str, str]]) -> List[FileObject]:
        return filter_file_objects(self.object_list, filters)

    async def create_storage_target(self, mode: int = 0o755) -> str:
        return self.storage_target

    async def storage_target_exists(
        self, raise_on_exist_nodir: bool = True, raise_on_not_found: bool = False
    ) -> bool:
        return False

    async def list_contents(
        self,
        checksum: bool = False,
        skip_hidden: bool = False,
        prefixes: List[str] = [],
        recursive: bool = True,
        filters: List[Dict[str, str]] = [],
        include_directories: bool = False,
        raise_on_permission_error: bool = False,
    ) -> List[FileObject]:
        return self.object_list

    async def write_file(
        self,
        file_object: FileObject,
    ) -> None:
        pass

    async def head_file(
        self,
        filename: str,
        checksum: bool = True,
    ) -> None:
        pass

    async def read_file(
        self,
        file_object: FileObject,
        validate: bool = False,
        chunk_size: int = 1024**2 * 16,
    ) -> FileObject:
        return file_object

    async def delete_file(
        self,
        file_object: FileObject,
    ) -> None:
        pass

    async def read_batch(
        self,
        file_objects: List[FileObject],
        stream_chunk_size: int = 1,
        threads_per_worker: int = 1,
    ) -> AsyncIterator[List[FileObject]]:
        """Provide a list of StreamObjects at every iteration"""
        file_objects_nested = [
            [
                FileObject(
                    name=fo.name,
                    size=fo.size,
                    mtime=fo.mtime,
                    hash=fo.hash,
                    tags=fo.tags,
                    source=ByteStream(
                        stream=iter_bytes(b"nobytes"),
                        chunk_size=stream_chunk_size,
                    ),
                )
                for fo in self.object_list
            ]
        ]
        for file_object_list in file_objects_nested:
            yield file_object_list

    async def write_batch(
        self,
        file_objects: Union[List[FileObject], AsyncIterator[List[FileObject]]],
        max_read_threads: Optional[int] = None,
    ) -> List[FileObject]:
        """Write given list of file_objects to storage,
        return list of file_objects that have not been written"""
        return []

    async def delete_batch(
        self, file_objects: Optional[List[FileObject]] = None
    ) -> List[FileObject]:
        if file_objects is None:
            file_objects = self.object_list
        return []

    async def delete_storage_target(self) -> bool:
        return True


async def batch_stream_objects(
    source_handler: ObjectStoreHandler,
    target_handler: ObjectStoreHandler,
    file_objects: List[FileObject],
) -> List[FileObject]:

    stream_objects_batched = source_handler.read_batch(
        file_objects,
    )

    remaining_file_objects = await target_handler.write_batch(stream_objects_batched)
    return remaining_file_objects
