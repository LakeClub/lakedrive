import os
from lakedrive.api import validate_storage_target
import pytest
import asyncio

from typing import Iterator, List, Tuple

from lakedrive.api import Response
from lakedrive.core import get_scheme_handler
from lakedrive.core.objects import FileObject
from lakedrive.core.handlers import ObjectStoreHandler
from lakedrive.localfs.handler import LocalFileHandler
from tests.localfs.helpers import cleanup_tempdir_localfs, create_files_localfs

from ..helpers.async_parsers import pytest_asyncio
from ..helpers.generate_objects import MockFileObjects
from ..helpers.object_store import clear_object_store
from ..templates.object_store_handler import (
    _TestObjectStoreHandlerSingleFile,
    _TestObjectStoreHandlerMultiFile,
    _TestObjectStoreHandlerSync,
    _TestWriteMisc,
)


TEMPDIR_LOCALFS = "/tmp/lakedrive-tests/localfs"


@pytest.fixture(scope="class", autouse=True)  # type: ignore[misc]
def tempdir_localfs() -> str:
    os.makedirs(TEMPDIR_LOCALFS, exist_ok=True)
    assert os.path.exists(TEMPDIR_LOCALFS) is True
    return TEMPDIR_LOCALFS


@pytest.fixture(scope="class", autouse=True)  # type: ignore[misc]
def localfs_source_handler(
    tempdir_localfs: str, directory: str = "source"
) -> LocalFileHandler:
    return LocalFileHandler("/".join([tempdir_localfs, directory]))


@pytest.fixture(scope="class", autouse=True)  # type: ignore[misc]
def localfs_target_handler(
    tempdir_localfs: str, directory: str = "target"
) -> LocalFileHandler:
    return LocalFileHandler("/".join([tempdir_localfs, directory]))


class TestLocalFileHandler(_TestObjectStoreHandlerSingleFile):
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
        file_objects_n1: List[Tuple[FileObject, bytes]],
        localfs_target_handler: LocalFileHandler,
    ) -> None:
        self.target = localfs_target_handler
        self.file_objects = file_objects_n1


class TestBatchLocalFileHandler(_TestObjectStoreHandlerMultiFile):
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
        mock_source_handler_files_random: ObjectStoreHandler,
        mock_source_handler_files_corrupt: ObjectStoreHandler,
        mock_source_handler_directories_empty: ObjectStoreHandler,
        localfs_target_handler: LocalFileHandler,
    ) -> None:
        self.target = localfs_target_handler
        self.mock_source = mock_source_handler_files_random
        self.mock_source_files_corrupt = mock_source_handler_files_corrupt
        self.mock_source_directories_empty = mock_source_handler_directories_empty


class TestSyncLocalFileHandler(_TestObjectStoreHandlerSync):
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
        mock_source_handler_files_random: ObjectStoreHandler,
        localfs_source_handler: LocalFileHandler,
        localfs_target_handler: LocalFileHandler,
    ) -> None:
        self.source = localfs_source_handler
        self.target = localfs_target_handler
        self.mock_source = mock_source_handler_files_random


class TestWriteMiscLocalFs(_TestWriteMisc):
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
        localfs_target_handler: LocalFileHandler,
        mock_file_objects: MockFileObjects,
    ) -> None:
        self.target = localfs_target_handler
        self.mock = mock_file_objects

        # clear target before each test
        asyncio.run(clear_object_store(self.target, delete_target=False))


class TestInvalidStorageTarget:
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(self, tempdir_localfs: str) -> Iterator[None]:
        self.object_store = LocalFileHandler(
            f"{tempdir_localfs}/test_invalid_file_or_dir"
        )
        self.local_path = self.object_store.storage_target

        # ensure invalid file/ dir is removed after each test
        yield
        try:
            os.rmdir(self.local_path)
        except NotADirectoryError:
            os.unlink(self.local_path)  # pragma: no cover
        except FileNotFoundError:
            pass

    def test_storage_target_not_dir(self) -> None:
        # write file at filepath
        with open(self.local_path, "w") as stream:
            stream.write("")

        with pytest.raises(FileExistsError) as error:
            exists = asyncio.run(self.object_store.storage_target_exists())
        assert str(error.value) == f"'{self.local_path}' exists, but not a directory"
        exists = asyncio.run(
            self.object_store.storage_target_exists(raise_on_exist_nodir=False)
        )
        assert isinstance(exists, bool)
        assert exists is False

        with pytest.raises(FileExistsError) as error:
            asyncio.run(self.object_store.create_storage_target())
        assert str(error.value) == f"'{self.local_path}' exists, but not a directory"
        os.unlink(self.local_path)

    def test_storage_target_bad_permission(self) -> None:
        # create directory at filepath with bad permissions
        os.makedirs(self.object_store.storage_target)
        os.chmod(self.object_store.storage_target, 0o555)

        # create storage target in a directory without sufficient permissions
        self.object_store.storage_target += "/test_sub"
        with pytest.raises(PermissionError) as error:
            asyncio.run(self.object_store.create_storage_target())
        assert (
            str(error.value)
            == f"No permission to create: {self.object_store.storage_target}"
        )

        with pytest.raises(PermissionError) as error:
            asyncio.run(self.object_store.create_storage_target())
        assert (
            str(error.value)
            == f"No permission to create: {self.object_store.storage_target}"
        )
        os.rmdir(self.local_path)


class TestLocalFsErrorsMisc:
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(self, tempdir_localfs: str) -> Iterator[None]:
        self.object_store = LocalFileHandler(f"{tempdir_localfs}/test_erroneous")
        self.local_tempdir = self.object_store.storage_target

        # create tempdir before test
        os.makedirs(self.local_tempdir)

        # cleanup after tests
        yield
        cleanup_tempdir_localfs(self.local_tempdir)

    def test_read_file_not_exist(self) -> None:
        # create file with no read permissions
        filepath = f"{self.local_tempdir}/test_file_not_exists"
        file_object = FileObject(name=os.path.basename(filepath))
        with pytest.raises(FileNotFoundError) as error:
            asyncio.run(self.object_store.read_file(file_object, validate=True))
        assert str(error.value) == file_object.name

    def test_head_file_no_permission(self) -> None:
        # create file with no read permissions
        filename = "test_file_no_head_permission"
        filepath = f"{self.local_tempdir}/{filename}"
        with open(filepath, "w") as stream:
            stream.write(".")
        os.chmod(filepath, 0o000)

        with pytest.raises(PermissionError) as error:
            asyncio.run(self.object_store.head_file(filename))
        assert str(error.value) == filename

    def test_read_file_no_permission(self) -> None:
        # create file with no read permissions
        filepath = f"{self.local_tempdir}/test_file_no_read_permission"
        with open(filepath, "w") as stream:
            stream.write(".")
        os.chmod(filepath, 0o000)
        file_object = FileObject(name=os.path.basename(filepath))

        # trigger PermissionError by attempting to get file data
        read_file_object = asyncio.run(self.object_store.read_file(file_object))
        with pytest.raises(PermissionError) as error:
            _ = read_file_object.read()
        assert str(error.value) == file_object.name

        # trigger PermissionError by using validate=True option
        with pytest.raises(PermissionError) as error:
            asyncio.run(self.object_store.read_file(file_object, validate=True))
        assert str(error.value) == file_object.name

    @pytest_asyncio
    async def test_validate_storage_target_file_exists(self) -> None:
        filepath = f"{self.local_tempdir}/test_validate_storage_target_file_exists"
        with open(filepath, "w") as stream:
            stream.write(".")
        response = await validate_storage_target("", get_scheme_handler(filepath))
        assert isinstance(response, Response)
        assert response.status_code == 400
        assert response.error_message == f"'{filepath}' exists, but not a directory"

    @pytest_asyncio
    async def test_not_list_nonreg(self) -> None:
        # only list regfile or directories
        no_files = 3
        directory = f"{self.local_tempdir}/test_not_list_nonreg"
        filenames = create_files_localfs(directory, "", no_files)
        # add symlinks
        for file_path, _ in filenames:
            os.symlink(os.path.basename(file_path), f"{file_path}.link")
        assert len(os.listdir(directory)) == (no_files * 2)  # regiles + links
        regfiles = await self.object_store.list_contents(
            prefixes=[os.path.basename(directory)]
        )
        assert len(regfiles) == no_files

    @pytest_asyncio
    async def test_list_contents_no_perm(self, capsys: pytest.CaptureFixture) -> None:
        directory = f"{self.local_tempdir}/test_list_contents_no_perm"
        no_files = 3
        no_subdirs = 3
        for j in range(no_subdirs):
            create_files_localfs(f"{directory}/test_{j}", "", no_files)

        # test baseline
        files = await self.object_store.list_contents(
            prefixes=[os.path.basename(directory)]
        )
        assert len(files) == no_files * no_subdirs
        os.chmod(f"{directory}/test_1", 0o000)

        # test with permission fail True
        with pytest.raises(PermissionError) as error:
            await self.object_store.list_contents(
                prefixes=[os.path.basename(directory)],
                raise_on_permission_error=True,
            )
        error_message = f"[Errno 13] Permission denied: '{directory}/test_1'"
        assert str(error.value) == error_message

        # test with permission fail False
        files = await self.object_store.list_contents(
            prefixes=[os.path.basename(directory)], raise_on_permission_error=False
        )
        assert len(files) == no_files * (no_subdirs - 1)
        captured = capsys.readouterr()
        assert captured.err == f"lakedrive: list failed: {error_message}\n"
