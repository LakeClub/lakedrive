from __future__ import annotations
import os
import re
import logging

from typing import List, Dict, AsyncIterator, Union, Optional

from ..httplibs.objects import HttpResponseError
from ..s3.objects import S3Bucket
from ..s3.bucket import bucket_create, bucket_delete, bucket_list, aws_misc_to_epoch
from ..s3.files import (
    file_exists,
    s3_head_file,
    get_file,
    get_file_batched,
    put_file,
    put_file_stream,
    put_file_batched,
    delete_file_batched,
)
from ..utils.helpers import chunklist_aiter
from ..core.objects import FileObject, HashTuple
from ..core.handlers import ObjectStoreHandler

logger = logging.getLogger(__name__)


DEFAULT_MAX_READ_THREADS = 512
DEFAULT_MAX_WRITE_THREADS = 128


class S3HandlerError(Exception):
    """Exceptions called by S3Handler"""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def __str__(self) -> str:
        return str(self.message)


class S3Handler(ObjectStoreHandler):
    def __init__(
        self,
        storage_target: str,
        credentials: Dict[str, str] = {},
        stream_chunk_size: int = 1024 * 256,
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
        self.aws_credentials = credentials

        tmp = self.storage_target.split("s3://")[-1].strip("/")
        if "/" in tmp:
            # target is a sub-path (/prefix) within a bucket
            bucket_name, self.bucket_path = tmp.split("/", 1)
            self.bucket_path += "/"
        else:
            # target is the root of bucket
            bucket_name = tmp
            self.bucket_path = ""

        if not bucket_name:
            raise S3HandlerError("Bucketname empty")

        self.bucket = S3Bucket(bucket_name)

    async def __ainit__(self) -> S3Handler:
        await self._set_bucket()
        return self

    def _get_aws_credentials(self) -> Dict[str, str]:
        if not self.aws_credentials:
            try:
                self.aws_credentials = {
                    "access_id": os.environ["AWS_ACCESS_KEY_ID"],
                    "secret_key": os.environ["AWS_SECRET_ACCESS_KEY"],
                }
            except KeyError as error:
                key = str(error).strip("'")
                raise PermissionError(f"{key} not found in environment")
        return self.aws_credentials

    async def _get_bucket(
        self, raise_not_found: bool = True, create_if_not_exists: bool = False
    ) -> S3Bucket:
        try:
            if self.bucket.exists is False:
                if create_if_not_exists is True:
                    await self.create_storage_target()
                    return self.bucket
                if raise_not_found is True:
                    raise S3HandlerError(f"Bucket not exists: {self.storage_target}")
            return self.bucket
        except AttributeError:
            # bucket not yet initialized
            return await self._set_bucket(
                raise_not_found=raise_not_found, raise_no_permission=True
            )

    async def _set_bucket(
        self, raise_not_found: bool = False, raise_no_permission: bool = False
    ) -> S3Bucket:
        try:
            if self.bucket.exists is True:
                return self.bucket
        except AttributeError:
            pass

        await self.bucket.validate(
            self._get_aws_credentials(),
            raise_not_found=raise_not_found,
            raise_no_permission=raise_no_permission,
        )
        assert isinstance(self.bucket, S3Bucket)
        return self.bucket

    async def storage_target_exists(
        self, raise_on_exist_nodir: bool = True, raise_on_not_found: bool = False
    ) -> bool:
        """Check if storage target exists, and can be used as such"""
        try:
            await self._set_bucket(raise_no_permission=True, raise_not_found=True)
            # bucket exists
            if not self.bucket_path:
                return True
            # else -- verify if path exists
            path_exists = await file_exists(self.bucket, self.bucket_path)
            if path_exists is False:
                raise FileNotFoundError
            else:
                return True

        except FileNotFoundError:
            if raise_on_not_found is True:
                raise FileNotFoundError(self.storage_target)
            else:
                return False
        except PermissionError:
            raise PermissionError(self.storage_target)
        except HttpResponseError:
            raise

    async def create_storage_target(
        self, mode: int = 0o755, raise_exists: bool = False
    ) -> str:
        """Ensure storage target exists (create if needed), and is a directory with
        sufficient (read,write and execute) permissions for current user."""
        try:
            if self.bucket_path:
                # over-ride raise_exist for (root)bucket itself
                await bucket_create(
                    self.bucket, self._get_aws_credentials(), raise_exist=False
                )
                if await file_exists(self.bucket, self.bucket_path) is True:
                    if raise_exists is True:
                        raise FileExistsError(f'Bucket "{self.storage_target}" exists')
                    return self.storage_target
                assert self.bucket_path[-1] == "/"  # redundant safety-check
                # create empty file
                await put_file(self.bucket, self.bucket_path, body=b"")
            else:
                # regular bucket
                await bucket_create(
                    self.bucket,
                    self._get_aws_credentials(),
                    raise_exist=raise_exists,
                )

        except PermissionError:
            raise
        except HttpResponseError:
            # if failed to create bucket(_path), but exists is True,
            # AND if bucket is empty, delete bucket
            try:
                # catch the case where a bucket is succesfully created,
                # but storage_target failed (e.g. bucket_path failed to create)
                # in this case we need to delete bucket (but only if empty)
                assert self.bucket.exists is False
            except AssertionError:
                self.bucket_path = ""  # re-set path
                len(
                    await self.list_contents(recursive=False, include_directories=True)
                ) != 0 or await self.delete_storage_target()

            raise
        except FileExistsError:
            raise

        return self.storage_target

    async def write_file(
        self,
        file_object: FileObject,
    ) -> None:
        await put_file_stream(
            await self._get_bucket(create_if_not_exists=True),
            file_object,
            prefix=self.bucket_path,
        )

    async def head_file(
        self,
        filename: str,
        checksum: bool = True,
    ) -> None:
        filepath = f"{self.bucket_path}{filename}"

        try:
            headers = await s3_head_file(
                await self._get_bucket(),
                filepath,
            )
            file_object = FileObject(
                name=filename,
                size=int(headers.get("content-length", -1)),
                hash=(
                    lambda: HashTuple(algorithm="md5", value=headers["etag"].strip('"'))
                    if checksum and "etag" in headers
                    else None
                )(),
                mtime=aws_misc_to_epoch(headers.get("last-modified", "")),
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
        if (
            validate is True
            and await file_exists(
                await self._get_bucket(),
                f"{self.bucket_path}{file_object.name}",
            )
            is False
        ):
            raise FileNotFoundError(file_object.name)
        else:
            pass
        file_object = await get_file(
            await self._get_bucket(),
            file_object,
            prefix=self.bucket_path,
            chunk_size=chunk_size,
        )
        self.object_list = [file_object]
        return file_object

    async def delete_file(
        self,
        file_object: FileObject,
    ) -> None:
        # test if file exists and is accessible before delete
        await self.head_file(file_object.name, checksum=False)
        remaining = await self.delete_batch(file_objects=[file_object])
        assert len(remaining) == 0

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
        bucket = await self._get_bucket(raise_not_found=False)
        if bucket.exists is False:
            return []

        self.object_list = await bucket_list(
            bucket,
            self.bucket_path,
            prefixes=prefixes,
            recursive=recursive,
            checksum=checksum,
            skip_hidden=skip_hidden,
        )

        # if bucket_path is set (i.e. sub-path), this must be stripped of from
        # each file_object.name as we must always give back relative paths
        if self.bucket_path:
            self.object_list = [
                FileObject(
                    name=re.sub(f"^{self.bucket_path}", "", fo.name),
                    size=fo.size,
                    mtime=fo.mtime,
                    hash=fo.hash,
                    tags=fo.tags,
                )
                for fo in self.object_list
                if fo.name != self.bucket_path  # dont add bucket_path itself
            ]

        if filters:
            self.object_list = self.filter_list(filters)
        return self.object_list

    async def read_batch(
        self,
        file_objects: List[FileObject],
        stream_chunk_size: int = (1024 * 256),
        threads_per_worker: int = 0,
    ) -> AsyncIterator[List[FileObject]]:
        bucket = await self._get_bucket()

        self.object_list = []
        async for batch in get_file_batched(
            bucket,
            file_objects,
            prefix=self.bucket_path,
        ):
            self.object_list += batch
            yield batch

    async def write_batch(
        self,
        file_objects: Union[List[FileObject], AsyncIterator[List[FileObject]]],
        max_read_threads: Optional[int] = None,
    ) -> List[FileObject]:
        """Write batch of FileObjects, return list of remaining FileObjects"""
        # throughput is based on weakest link
        max_read_threads = max_read_threads or self.max_read_threads
        threads = min(max_read_threads, self.max_write_threads)
        if isinstance(file_objects, AsyncIterator):
            file_objects_list_aiter = file_objects
        else:
            no_chunks = (len(file_objects) + threads - 1) // threads  # round-up
            file_objects_list_aiter = chunklist_aiter(file_objects, no_chunks)

        remaining_file_objects = await put_file_batched(
            await self._get_bucket(create_if_not_exists=True),
            file_objects_list_aiter,
            prefix=self.bucket_path,
            tcp_connections=threads,
        )
        return remaining_file_objects

    async def delete_batch(
        self, file_objects: Optional[List[FileObject]] = None
    ) -> List[FileObject]:
        """Delete a batch of files in one call"""

        if file_objects is None:
            file_objects = self.object_list

        bucket = await self._get_bucket(raise_not_found=False)
        if bucket.exists is False:
            # if bucket does not exist there are no remaining files
            return []

        remaining_file_objects = await delete_file_batched(
            bucket,
            file_objects,
            prefix=self.bucket_path,
        )
        return remaining_file_objects

    async def delete_storage_target(self) -> bool:
        """Delete storage target.
        Note: function will fail if directory is not emptied before"""
        if self.bucket_path:
            # only delete path in bucket

            # no-named file will lead to prefix itself being deleted
            file_object = FileObject(
                name="",
                size=0,
                mtime=0,
                hash=None,
                tags=b"",
            )
            remaining = await self.delete_batch(file_objects=[file_object])
            return remaining == []  # true if deleted

        else:
            # delete bucket
            bucket = await self._get_bucket(raise_not_found=False)
            if bucket.exists is True:
                success = await bucket_delete(
                    bucket,
                )
                return success is True
            return True  # assume bucket does not exist
