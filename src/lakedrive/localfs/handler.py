from __future__ import annotations
import os
import stat
import time
import logging

from typing import List, Dict, AsyncIterator, Union, Optional

from ..utils.executors import run_async_threadpool
from ..localfs.files import (
    remove_file_if_exists,
    directory_list,
    put_file,
    put_file_batched,
    md5_hash_tuple,
)
from ..utils.helpers import (
    split_in_chunks,
    chunklist_aiter,
    utc_offset,
)
from ..core.objects import ByteStream, FileObject
from ..core.handlers import ObjectStoreHandler


logger = logging.getLogger(__name__)


DEFAULT_MAX_READ_THREADS = 128
DEFAULT_MAX_WRITE_THREADS = 32


class LocalFileHandler(ObjectStoreHandler):
    def __init__(
        self,
        storage_target: str,
        stream_chunk_size: int = 1024 * 128,
        max_read_threads: Optional[int] = None,
        max_write_threads: Optional[int] = None,
    ) -> None:
        super().__init__(
            storage_target,
            stream_chunk_size,
            max_read_threads or DEFAULT_MAX_READ_THREADS,
            max_write_threads or DEFAULT_MAX_WRITE_THREADS,
        )
        self.support.list_directories = True

        # ensure file_path ends with "/" -- rstrip first to prevent double "/"
        self.storage_path = f'{self.storage_target.rstrip("/")}/'

    async def storage_target_exists(
        self, raise_on_exist_nodir: bool = True, raise_on_not_found: bool = False
    ) -> Optional[bool]:
        """Check if storage target exists, and can be used as such"""
        try:
            fp = os.lstat(self.storage_target)
            if not stat.S_ISDIR(fp.st_mode):
                if raise_on_exist_nodir is True:
                    raise FileExistsError(
                        f"'{self.storage_target}' exists, but not a directory"
                    )
                raise FileNotFoundError

            if os.access(self.storage_target, os.R_OK) is True:
                return True
            else:
                raise PermissionError(self.storage_target)
        except FileNotFoundError:
            if raise_on_not_found is True:
                raise FileNotFoundError(self.storage_target)
            else:
                return False
        except PermissionError as error:
            raise PermissionError(error)

    async def create_storage_target(self, mode: int = 0o755) -> str:
        """Ensure storage target exists (create if needed), and is a directory with
        sufficient (read,write and execute) permissions for current user."""
        try:
            target_exists = await self.storage_target_exists(raise_on_exist_nodir=True)
            if target_exists is False:
                os.makedirs(self.storage_target, mode)
            else:
                pass

        except PermissionError:
            raise PermissionError(f"No permission to create: {self.storage_target}")
        except FileExistsError:
            raise

        return self.storage_target

    async def write_file(
        self,
        file_object: FileObject,
    ) -> None:
        await put_file(self.storage_path, file_object)

    async def _read_file(self, name: str) -> AsyncIterator[bytes]:
        """Read file in chunks of fixed size, until all parts are read"""
        filepath = f"{self.storage_path}{name}"
        if not os.path.exists(filepath):
            raise FileNotFoundError(name)
        with open(filepath, "rb") as stream:
            while True:
                chunk = stream.read(self.stream_chunk_size)
                if chunk:
                    yield chunk
                else:
                    break

    async def head_file(
        self,
        filename: str,
        checksum: bool = True,
    ) -> None:
        filepath = f"{self.storage_path}{filename}"

        try:
            fs = os.stat(filepath)
            file_object = FileObject(
                name=filename,
                size=int(fs.st_size),
                hash=(lambda: md5_hash_tuple(filepath) if checksum else None)(),
                mtime=int(fs.st_mtime) - utc_offset(),
            )
        except FileNotFoundError:
            raise FileNotFoundError(filename)
        except PermissionError:
            raise PermissionError(filename)

        self.object_list = [file_object]

    async def read_file(
        self,
        file_object: FileObject,
        validate: bool = False,
        chunk_size: int = 1024**2 * 16,
    ) -> FileObject:

        if validate is True:
            filepath = f"{self.storage_path}{file_object.name}"
            try:
                fp = open(filepath, "r")
            except FileNotFoundError:
                raise FileNotFoundError(file_object.name)
            except PermissionError:
                raise PermissionError(file_object.name)
            else:
                fp.close()
        else:
            pass

        file_object.content = None  # ensure reset
        file_object.source = ByteStream(stream=self._read_file(file_object.name))

        self.object_list = [file_object]
        return file_object

    async def delete_file(
        self,
        file_object: FileObject,
    ) -> None:
        filepath = f"{self.storage_path}{file_object.name}"
        if not os.path.exists(filepath):
            raise FileNotFoundError(file_object.name)
        try:
            remove_file_if_exists(f"{self.storage_path}{file_object.name}")
        except PermissionError:
            raise PermissionError(f"{self.storage_path}{file_object.name}")

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
        """Get list of files that are contained in location.

        Optional:
        : checksum: add checksum per file
        : skip_hidden: skips files or directories starting with "."

        return as list of FileObjects"""
        self.object_list = directory_list(
            self.storage_path,
            checksum=checksum,
            skip_hidden=skip_hidden,
            prefixes=prefixes,
            recursive=recursive,
            include_directories=include_directories,
            raise_on_permission_error=raise_on_permission_error,
        )

        if filters:
            self.object_list = self.filter_list(filters)
        return self.object_list

    async def read_batch(
        self,
        file_objects: List[FileObject],
        stream_chunk_size: int = (1024 * 256),
        threads_per_worker: int = 0,
    ) -> AsyncIterator[List[FileObject]]:
        """Divided list of file objects in smaller fixed-size batches,
        every time this function is called a new batch of files are returned"""
        self.stream_chunk_size = stream_chunk_size

        # split in batches
        # skip files ending with "/" as there are assumed to be directories
        batches = split_in_chunks(
            [fo for fo in file_objects if fo.name[-1] != "/"],
            threads_per_worker or self.max_read_threads,
        )

        self.object_list = []
        for batch in batches:
            for file_object in batch:
                file_object.content = None  # ensure reset
                file_object.source = ByteStream(
                    stream=self._read_file(file_object.name),
                    chunk_size=stream_chunk_size,
                )
            self.object_list += batch
            yield batch

    async def write_batch(
        self,
        file_objects: Union[List[FileObject], AsyncIterator[List[FileObject]]],
        max_read_threads: Optional[int] = None,
    ) -> List[FileObject]:
        """Write the given list of files in batches.

        All directories are created first, underlying _write_file() function calls
        can then safely assume the target directory to be present.

        This functions works in tandem with a read_batch() function. Files are
        split in read_batch(), then read and written through a loop in this function."""
        max_read_threads = max_read_threads or self.max_read_threads
        await self.create_storage_target()  # ensure target exists

        if isinstance(file_objects, AsyncIterator):
            file_objects_list_aiter = file_objects
        else:
            file_objects_list_aiter = chunklist_aiter(file_objects, max_read_threads)

        file_objects, remaining_file_objects = await put_file_batched(
            self.storage_path,
            file_objects_list_aiter,
        )

        # copy directories from file_objects, this may include empty directories
        # ensure directory have correct mtime -- must be in order,
        # and after copying files to keep original mtime
        directories_with_mtime = sorted(
            [
                (f"{self.storage_path}{fo.name}/", fo.mtime)
                for fo in file_objects
                if fo.name[-1] == "/" and fo.mtime > 0
            ],
            reverse=True,
        )
        if directories_with_mtime:
            for directory, mtime in directories_with_mtime:
                os.makedirs(directory, exist_ok=True)
                os.utime(directory, (int(time.time()), mtime))

        return remaining_file_objects

    async def delete_batch(
        self, file_objects: Optional[List[FileObject]] = None
    ) -> List[FileObject]:
        """Delete a batch of files in one call. First all files are deleted,
        and than directories using the os system calls"""

        if file_objects is None:
            file_objects = self.object_list

        # delete all (reg)files first
        responses = await run_async_threadpool(
            remove_file_if_exists,
            [
                (f"{self.storage_path}{fo.name}",)
                for fo in file_objects
                if fo.name[-1] != "/"
            ],
            return_exceptions=True,
        )

        failed_items = list(
            [file_objects[idx] for idx, exception in enumerate(responses) if exception]
        )
        if len(failed_items) > 0:
            non_empty_directories = set(
                [f"{os.path.dirname(fo.name)}/" for fo in failed_items]
            )
        else:
            non_empty_directories = set([])

        # (empty) directories must be unlinked in order due to possible hiearchy
        directories_sorted = sorted(
            [
                "/".join([self.storage_target, fo.name])
                for fo in file_objects
                if fo.name[-1] == "/" and fo.name not in non_empty_directories
            ],
            reverse=True,
        )
        for directory in directories_sorted:
            os.rmdir(directory)
        return failed_items

    async def delete_storage_target(self) -> bool:
        """Delete storage target.
        Note: function will fail if directory is not emptied before"""
        if await self.storage_target_exists(raise_on_exist_nodir=False) is False:
            return True  # target does not exist
        # else: delete
        os.rmdir(self.storage_target)
        return True  # if no error, assume target is deleted
