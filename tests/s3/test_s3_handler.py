import os
import pytest
import asyncio

from typing import List, Tuple

from lakedrive.s3.objects import S3Bucket
from lakedrive.s3.handler import S3Handler, S3HandlerError
from lakedrive.core.handlers import ObjectStoreHandler
from lakedrive.core.objects import FileObject
from lakedrive.httplibs.objects import HttpResponseError

from ..helpers.async_parsers import pytest_asyncio
from ..helpers.generate_objects import MockFileObjects
from ..helpers.object_store import clear_object_store
from ..templates.object_store_handler import (
    _TestObjectStoreHandler,
    _TestObjectStoreHandlerSingleFile,
    _TestObjectStoreHandlerMultiFile,
    _TestObjectStoreHandlerSync,
)


class TestS3HandlerError:
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(self) -> None:
        self.error_message = "dummy s3 error"
        self._exception = S3HandlerError(self.error_message)
        assert self._exception.message == self.error_message

    def test___str__(self) -> None:
        assert self._exception.__str__() == self.error_message


class TestS3HandlerDummy(_TestObjectStoreHandler):
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(self) -> None:
        self.bucket_name = "dummystore"
        self.fake_credentials = {
            "access_id": "FAKE_ACCESS_ID",
            "secret_key": "FAKE_SECRET_KEY",
        }

        self.bad_handler = S3Handler(
            self.bucket_name,
            credentials=self.fake_credentials,
        )

    def test__set_bucket_dummy(self) -> None:
        asyncio.run(self.bad_handler._set_bucket())
        assert isinstance(self.bad_handler.bucket, S3Bucket)
        assert self.bad_handler.bucket.exists is False

        with pytest.raises(S3HandlerError) as error:
            asyncio.run(self.bad_handler._get_bucket())
        assert str(error.value) == f"Bucket not exists: {self.bucket_name}"

    def test_storage_target_exists_permission_error(self) -> None:
        with pytest.raises(PermissionError) as error:
            asyncio.run(self.bad_handler.storage_target_exists())
        assert str(error.value) == self.bucket_name

    def test_storage_target_exists_permission_error_subpath(self) -> None:
        object_store = S3Handler(f"{self.bucket_name}/subpath")
        asyncio.run(object_store._set_bucket())
        asyncio.run(object_store.create_storage_target())

        # configure false bucket credentials
        original_credentials = object_store.bucket.s3_credentials
        object_store.bucket.s3_credentials = self.fake_credentials
        with pytest.raises(HttpResponseError) as error:
            asyncio.run(object_store.storage_target_exists())
        assert (
            str(error.value) == f"cant access 's3://{self.bucket_name}/subpath/' (403)"
        )
        # reset credentials and cleanup
        object_store.bucket.s3_credentials = original_credentials
        self.object_store_delete(object_store)

    def test_storage_target_exists_permission_error_file(self) -> None:
        object_store = S3Handler(f"{self.bucket_name}/filepath")
        asyncio.run(object_store._set_bucket())
        asyncio.run(object_store.create_storage_target())

        # configure false bucket credentials
        original_credentials = object_store.bucket.s3_credentials
        object_store.bucket.s3_credentials = self.fake_credentials

        # note -- file does not need to exist to test/trigger error
        with pytest.raises(PermissionError) as error:
            asyncio.run(object_store.head_file("dummy"))
        assert str(error.value) == "dummy"

        # reset credentials and cleanup
        object_store.bucket.s3_credentials = original_credentials
        self.object_store_delete(object_store)

    def test_delete_empty_storage_target(self) -> None:
        self.object_store_delete(S3Handler(self.bucket_name))


class TestS3HandlerMock(_TestObjectStoreHandler):
    def _create_storage_target_repeat(self, bucket_name: str) -> None:
        # create bucket -- idempotent by default
        object_store = S3Handler(bucket_name)
        asyncio.run(object_store.create_storage_target())
        asyncio.run(object_store.create_storage_target())

        # create bucket that exist with raise_exists flag
        with pytest.raises(FileExistsError) as error:
            asyncio.run(object_store.create_storage_target(raise_exists=True))
        assert str(error.value) == f'Bucket "{bucket_name}" exists'
        self.object_store_delete(object_store, delete_files=True)

    def test_create_storage_target_repeat(self) -> None:
        self._create_storage_target_repeat("mockstore")
        self._create_storage_target_repeat("mockstore/with_subpath")

    def test_create_storage_target_invalid_path(self) -> None:
        invalid_bucket_path = "mockstore/../"
        object_store = S3Handler(invalid_bucket_path)
        with pytest.raises(HttpResponseError) as error:
            asyncio.run(object_store.create_storage_target())
        assert str(error.value) == f"cant access 's3://{invalid_bucket_path}' (400)"
        # check (empty) parent-bucket is not accidentally retained
        exists = asyncio.run(S3Handler("mockstore").storage_target_exists())
        assert isinstance(exists, bool)
        assert exists is False


class TestS3FileHandler(_TestObjectStoreHandlerSingleFile):
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
        s3_source_handler: ObjectStoreHandler,
        file_objects_n1: List[Tuple[FileObject, bytes]],
    ) -> None:
        self.target = s3_source_handler
        self.file_objects = file_objects_n1


class TestS3FileHandlerSubpath(_TestObjectStoreHandlerSingleFile):
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
        s3_source_handler_subpath: ObjectStoreHandler,
        file_objects_n1: List[Tuple[FileObject, bytes]],
    ) -> None:
        self.target = s3_source_handler_subpath
        self.file_objects = file_objects_n1


class TestBatchS3FileHandler(_TestObjectStoreHandlerMultiFile):
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
        mock_source_handler_files_random: ObjectStoreHandler,
        mock_source_handler_files_corrupt: ObjectStoreHandler,
        mock_source_handler_directories_empty: ObjectStoreHandler,
        s3_source_handler: ObjectStoreHandler,
    ) -> None:
        self.target = s3_source_handler
        self.mock_source = mock_source_handler_files_random
        self.mock_source_files_corrupt = mock_source_handler_files_corrupt
        self.mock_source_directories_empty = mock_source_handler_directories_empty

    def test_delete_batch_error(self) -> None:
        # attempt to delete files that dont exist on the object store
        sample_file_objects = asyncio.run(self.mock_source.list_contents())[0:2]

        # ensure storage target exists
        asyncio.run(self.target.create_storage_target())
        assert asyncio.run(self.target.storage_target_exists()) is True

        # temporarily inject fake credentials to trigger a non 204 http-response
        fake_credentials = {
            "access_id": "FAKE_ACCESS_ID",
            "secret_key": "FAKE_SECRET_KEY",
        }
        tmp_copy = self.target.bucket.s3_credentials  # type: ignore[attr-defined]
        self.target.bucket.s3_credentials = (  # type: ignore[attr-defined]
            fake_credentials
        )

        remaining_items = asyncio.run(
            self.target.delete_batch(file_objects=sample_file_objects)
        )
        assert isinstance(remaining_items, list)
        assert remaining_items == sample_file_objects

        # restore to original credentials
        self.target.bucket.s3_credentials = tmp_copy  # type: ignore[attr-defined]


class TestBatchS3FileHandlerSubpath(_TestObjectStoreHandlerMultiFile):
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
        mock_source_handler_files_random: ObjectStoreHandler,
        mock_source_handler_files_corrupt: ObjectStoreHandler,
        mock_source_handler_directories_empty: ObjectStoreHandler,
        s3_source_handler_subpath: ObjectStoreHandler,
    ) -> None:
        self.target = s3_source_handler_subpath
        self.mock_source = mock_source_handler_files_random
        self.mock_source_files_corrupt = mock_source_handler_files_corrupt
        self.mock_source_directories_empty = mock_source_handler_directories_empty


class TestSyncS3FileHandler(_TestObjectStoreHandlerSync):
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
        mock_source_handler_files_random: ObjectStoreHandler,
        s3_source_handler: ObjectStoreHandler,
        s3_target_handler: ObjectStoreHandler,
    ) -> None:
        self.source = s3_source_handler
        self.target = s3_target_handler
        self.mock_source = mock_source_handler_files_random


class TestSyncS3FileHandlerSubpath(_TestObjectStoreHandlerSync):
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
        mock_source_handler_files_random: ObjectStoreHandler,
        s3_source_handler_subpath: ObjectStoreHandler,
        s3_target_handler_subpath: ObjectStoreHandler,
    ) -> None:
        self.source = s3_source_handler_subpath
        self.target = s3_target_handler_subpath
        self.mock_source = mock_source_handler_files_random


class TestS3Custom:
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
        s3_target_handler: ObjectStoreHandler,
        mock_file_objects: MockFileObjects,
    ) -> None:
        self.target = s3_target_handler
        self.mock = mock_file_objects

    @pytest_asyncio
    async def test_list_manyfiles(self) -> None:
        await clear_object_store(self.target, delete_target=False)

        file_count = 2000
        file_objects = self.mock.file_objects(file_count=file_count)
        await self.target.write_batch(file_objects, max_read_threads=500)
        file_objects = await self.target.list_contents(
            checksum=False, include_directories=False
        )
        assert isinstance(file_objects, list)
        assert len(file_objects) == file_count


def test_no_s3_credentials() -> None:
    env_keys = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]

    # backup current env -- need to restore for subsequent tests
    aws_credentials_backup = {
        key: os.environ[key] for key in env_keys if key in os.environ
    }

    # ensure all keys are set with some value
    for key in env_keys:
        os.environ[key] = "dummy"

    # test for missing key one at a time
    for key in env_keys:
        del os.environ[key]
        with pytest.raises(PermissionError) as error:
            S3Handler("s3://bucket")._get_aws_credentials()
        assert str(error.value) == f"{key} not found in environment"
        os.environ[key] = "dummy"

    # restore backup env keys
    for key, value in aws_credentials_backup.items():
        os.environ[key] = value
