import pytest
import asyncio

from typing import List, Tuple

from lakedrive.core.handlers import ObjectStoreHandler
from lakedrive.core.objects import FileObject, HashTuple

from ..helpers.misc import utc_offset, filter_regfiles, flatten_list
from ..helpers.object_store import clear_object_store
from ..helpers.async_parsers import async_iterator_to_list, pytest_asyncio
from ..helpers.generate_objects import MockFileObjects, compute_md5sum
from ..helpers.validators import (
    validate_read_file,
    validate_file_objects_exclude_mtime,
)


class _TestObjectStoreHandler:
    def object_store_create(self, object_store: ObjectStoreHandler) -> None:
        object_store_name = asyncio.run(object_store.create_storage_target())
        assert isinstance(object_store_name, str)
        assert object_store_name == object_store.storage_target
        exists = asyncio.run(object_store.storage_target_exists())
        assert isinstance(exists, bool)
        assert exists is True

    def object_store_empty(self, object_store: ObjectStoreHandler) -> None:
        file_objects = asyncio.run(
            object_store.list_contents(checksum=False, include_directories=True)
        )
        asyncio.run(object_store.delete_batch(file_objects=file_objects))

        # expect 0 files after delete all
        file_objects = asyncio.run(
            object_store.list_contents(checksum=False, include_directories=True)
        )
        assert isinstance(file_objects, list)
        assert len(file_objects) == 0

    def object_store_delete(
        self, object_store: ObjectStoreHandler, delete_files: bool = False
    ) -> None:
        if delete_files is True:
            self.object_store_empty(object_store)
        success = asyncio.run(object_store.delete_storage_target())
        assert isinstance(success, bool)
        assert success is True
        exists = asyncio.run(object_store.storage_target_exists())
        assert isinstance(exists, bool)
        assert exists is False

    def object_store_exists_raise_not_found(
        self,
        object_store: ObjectStoreHandler,
    ) -> None:
        with pytest.raises(FileNotFoundError) as error:
            asyncio.run(object_store.storage_target_exists(raise_on_not_found=True))
        assert str(error.value) == object_store.storage_target


class _TestObjectStoreHandlerSingleFile(_TestObjectStoreHandler):
    """Test one-way synchronization of single-object between ObjectStores"""

    target: ObjectStoreHandler = None  # type: ignore[assignment]
    file_objects: List[Tuple[FileObject, bytes]] = []

    @pytest_asyncio
    async def test_object_store_exists(self) -> None:
        await clear_object_store(self.target, delete_target=True)
        exists = await self.target.storage_target_exists()
        assert isinstance(exists, bool)
        assert exists is False

    def test_write_file(self) -> None:
        """Write a single file. Note object_store is not pre-created,
        it should be created if not exists by default"""
        file_object, _ = self.file_objects[0]
        asyncio.run(self.target.write_file(file_object))

    def test_list_contents(self) -> None:
        # validate if write has succeeded by checking (new) storage-contents
        fo, _ = self.file_objects[0]
        file_objects = asyncio.run(self.target.list_contents(checksum=False))
        assert isinstance(file_objects, list)

        # expect 1 file
        assert len(file_objects) == 1

        # validate file meta
        list_fo = file_objects[0]
        assert list_fo.name == fo.name
        assert list_fo.size == fo.size
        assert list_fo.hash is None

        # un-commented for now - should be enabled for targets supporting setting mtime
        assert list_fo.mtime >= fo.mtime - utc_offset()

    def test_head_file(self) -> None:
        file_object, _ = self.file_objects[0]
        file_object_minimal = FileObject(
            name=file_object.name,
            size=file_object.size,
            hash=file_object.hash,
            mtime=file_object.mtime,
        )
        asyncio.run(self.target.head_file(file_object.name))
        assert len(self.target.object_list) == 1
        validate_file_objects_exclude_mtime(
            self.target.object_list, [file_object_minimal]
        )

    def test_read_file(self) -> None:
        """Read a single file
        basename is used -- see test_write_file()"""
        file_object, contents = self.file_objects[0]
        file_object.seek(0)  # ensure stream starts at beginning

        new_file_object = asyncio.run(self.target.read_file(file_object))
        assert isinstance(new_file_object, FileObject)

        data_read = new_file_object.read()
        assert isinstance(data_read, bytes)
        assert data_read == contents
        # using validate=True should give same outcome
        file_object.seek(0)  # ensure stream starts at beginning
        new_file_object = asyncio.run(self.target.read_file(file_object, validate=True))
        assert new_file_object.read() == contents

    def test_read_file_chunked(self) -> None:
        file_object, contents = self.file_objects[0]
        file_object.seek(0)  # ensure stream starts at beginning

        chunk_size = int(file_object.size / 4)

        new_file_object = asyncio.run(
            self.target.read_file(file_object, chunk_size=chunk_size)
        )
        assert isinstance(new_file_object, FileObject)

        no_chunks = (file_object.size + chunk_size - 1) // chunk_size

        data = b""
        count = 0

        while True:
            chunk = new_file_object.read(chunk_size)
            assert isinstance(chunk, bytes)
            assert len(chunk) <= chunk_size
            data += chunk
            count += 1
            if count >= no_chunks:
                break
        assert count == no_chunks

        # verify additional read() is empty
        assert new_file_object.read(chunk_size) == b""
        assert len(data) == new_file_object.size
        assert data == contents

    def test_list_contents_checksum(self) -> None:
        """similar as test_list_contents(),
        but now check if checksum validation works"""
        fo, _ = self.file_objects[0]
        file_objects = asyncio.run(self.target.list_contents(checksum=True))

        # expect 1 file
        assert isinstance(file_objects, list)
        assert len(file_objects) == 1
        list_fo = file_objects[0]

        # validate file meta
        assert list_fo.name == fo.name
        assert list_fo.size == fo.size
        assert list_fo.hash is not None
        assert fo.hash is not None
        assert list_fo.hash.algorithm == fo.hash.algorithm
        assert list_fo.hash.value == fo.hash.value
        # un-commented for now - should be enabled for targets supporting setting mtime
        # assert list_fo.mtime == fo.mtime - utc_offset()

    def test_write_batch(self) -> None:
        # only test function with empty input
        file_objects_batched = self.target.read_batch(
            [],
            stream_chunk_size=self.target.stream_chunk_size,
        )
        asyncio.run(self.target.write_batch(file_objects_batched))

    def test_read_batch(self) -> None:
        # only test function with empty inputs
        asyncio.run(async_iterator_to_list(self.target.read_batch([])))

    def test_delete_file(self) -> None:
        file_object, _ = self.file_objects[0]
        asyncio.run(self.target.delete_file(file_object))

        file_objects = asyncio.run(
            self.target.list_contents(checksum=False, include_directories=True)
        )
        assert isinstance(file_objects, list)
        # expect 0 files
        assert len(file_objects) == 0

    def test_delete_file_not_exist(self) -> None:
        """Similar to test_delete_file(), but now file
        should not exist because its deleted in previous test"""
        file_object, _ = self.file_objects[0]
        with pytest.raises(FileNotFoundError) as error:
            asyncio.run(self.target.delete_file(file_object))
        assert str(error.value) == file_object.name

    def test_read_file_not_exist(self) -> None:
        """Similar to test_read_file(), but now file
        should not exist because its deleted in previous test"""
        file_object, _ = self.file_objects[0]
        file_object_readable = asyncio.run(self.target.read_file(file_object))
        with pytest.raises(FileNotFoundError) as error:
            _ = file_object_readable.read()
        assert str(error.value) == file_object.name

    def test_read_file_not_exist_by_validate(self) -> None:
        """using validate=True option should check if file exists before
        read() is called"""
        file_object, _ = self.file_objects[0]
        with pytest.raises(FileNotFoundError) as error:
            asyncio.run(self.target.read_file(file_object, validate=True))
        assert str(error.value) == file_object.name

    def test_delete_batch_nofiles(self) -> None:
        asyncio.run(self.target.delete_batch())

    def test_object_store_delete(self) -> None:
        self.object_store_delete(self.target)
        self.object_store_exists_raise_not_found(self.target)


class _TestObjectStoreHandlerMultiFile(_TestObjectStoreHandler):
    """Test one-way synchronization of multiple-objects between ObjectStores"""

    target: ObjectStoreHandler = None  # type: ignore[assignment]
    mock_source: ObjectStoreHandler = None  # type: ignore[assignment]
    mock_source_files_corrupt: ObjectStoreHandler = None  # type: ignore[assignment]
    mock_source_directories_empty: ObjectStoreHandler = None  # type: ignore[assignment]

    @pytest_asyncio
    async def test_object_store_exists(self) -> None:
        await clear_object_store(self.target, delete_target=True)
        exists = await self.target.storage_target_exists()
        assert isinstance(exists, bool)
        assert exists is False

    def test_object_store_create(self) -> None:
        self.object_store_create(self.target)

    def test_list_contents(self) -> None:
        file_objects = asyncio.run(self.target.list_contents(checksum=False))
        # expect 0 files
        assert isinstance(file_objects, list)
        assert len(file_objects) == 0

    def test_write_batch(self) -> None:
        source_files = asyncio.run(self.mock_source.list_contents(checksum=True))
        file_objects_batched = self.mock_source.read_batch(
            source_files,
            stream_chunk_size=self.mock_source.stream_chunk_size,
        )
        remaining_file_objects = asyncio.run(
            self.target.write_batch(file_objects_batched)
        )
        assert isinstance(remaining_file_objects, list)
        assert len(remaining_file_objects) == 0

        # expect equal number of (reg)files locally as on source
        # filter file_objects on regfiles (source does not list directories)
        target_files = asyncio.run(self.target.list_contents(checksum=True))
        validate_file_objects_exclude_mtime(source_files, target_files)

    @pytest_asyncio
    async def test_read_files(self) -> None:
        """Validate each file written on target"""
        target_files = await self.target.list_contents(checksum=True)
        for fo in target_files:
            assert isinstance(fo.hash, HashTuple)
            file_object = await self.target.read_file(fo)
            await validate_read_file(file_object, fo.hash)

    def test_list_contents_multipath(self) -> None:
        # lists of all file objects
        source_files = asyncio.run(self.mock_source.list_contents(checksum=False))
        target_files = asyncio.run(self.target.list_contents(checksum=False))
        validate_file_objects_exclude_mtime(source_files, target_files)

        # lists of file objects in rootdir only
        # -- length not fixed, but should always be smaller than list of all objects
        source_rootdir_files = asyncio.run(
            self.mock_source.list_contents(checksum=False, recursive=False)
        )
        target_rootdir_files = asyncio.run(
            self.target.list_contents(checksum=False, recursive=False)
        )

        assert isinstance(target_rootdir_files, list)
        assert len(target_rootdir_files) < len(target_files)
        validate_file_objects_exclude_mtime(source_rootdir_files, target_rootdir_files)

        # copy regfiles from rootdir into new "merged" list
        target_rootdir_regfiles = [
            fo for fo in target_rootdir_files if fo.name[-1] != "/"
        ]

        # get by prefix -- one prefix at a time
        _target_files_by_prefix_single = [
            asyncio.run(
                self.target.list_contents(
                    checksum=False, prefixes=[fo.name], recursive=True
                )
            )
            for fo in target_rootdir_files
            if fo.name[-1] == "/"
        ]
        target_files_by_prefix_single = [
            item for sublist in _target_files_by_prefix_single for item in sublist
        ] + target_rootdir_regfiles
        validate_file_objects_exclude_mtime(target_files_by_prefix_single, target_files)

        # get by prefix -- multiple prefixes at once
        target_files_by_prefix_multi = (
            asyncio.run(
                self.target.list_contents(
                    checksum=False,
                    prefixes=[
                        fo.name for fo in target_rootdir_files if fo.name[-1] == "/"
                    ],
                    recursive=True,
                ),
            )
            + target_rootdir_regfiles
        )
        validate_file_objects_exclude_mtime(target_files_by_prefix_multi, target_files)

    def test_list_prefix_overlap(self) -> None:
        source_file_objects = asyncio.run(self.mock_source.list_contents())
        # get list of grant-parent and parent directories
        # -- this will ensure overlap
        directories_gp_p_tmp = [
            (fo.name.rsplit("/", 2)[0], fo.name.rsplit("/", 1)[0])
            for fo in source_file_objects
            if fo.name[-1] != "/"
        ]
        # filter out paths without grand-parent directory
        # ensure grant-parent/parent relationship is valid
        # limit to 5 sets
        limit = 5
        directories_gp_p: List[List[str]] = [
            [gp_dir, p_dir]
            for gp_dir, p_dir in directories_gp_p_tmp
            if gp_dir and p_dir and gp_dir != p_dir and f"{gp_dir}/" in p_dir
        ][0:limit]
        assert len(directories_gp_p) == limit
        directories = flatten_list(directories_gp_p)

        target_files = asyncio.run(
            self.target.list_contents(
                checksum=False,
                prefixes=directories,
                recursive=True,
            )
        )
        # number of files found should at least equal given directory limit
        assert len(target_files) >= limit

        # ensure paths are unique despite given overlapping prefixes
        file_names = [fo.name for fo in target_files]
        assert len(file_names) == len(set(file_names))

    def test_list_contents_filter(self) -> None:
        filters = [
            {
                "FilterBy": "size",
                "Value": "+100k",
            }
        ]
        source_file_objects = asyncio.run(
            self.mock_source.list_contents(filters=filters)
        )
        target_file_objects = asyncio.run(self.target.list_contents(filters=filters))
        assert len(source_file_objects) == len(target_file_objects)

    @pytest_asyncio
    async def test_read_files_chunked(self) -> None:
        filters = [
            {
                "FilterBy": "size",
                "Value": "+100k",
            }
        ]
        # get list of files > 100k
        files = await self.target.list_contents(checksum=True, filters=filters)
        # get data for one file using defined chunk_size
        file_object = await self.target.read_file(files[0], chunk_size=1024 * 64)
        data = await file_object.aread()
        assert len(data) == file_object.size
        assert isinstance(file_object.hash, HashTuple)
        assert compute_md5sum(data) == file_object.hash.value

    def test_write_bad_batch(self) -> None:
        source_file_objects = asyncio.run(
            self.mock_source_files_corrupt.list_contents()
        )

        file_objects_batched = self.mock_source_files_corrupt.read_batch(
            source_file_objects,
            stream_chunk_size=self.mock_source_files_corrupt.stream_chunk_size,
        )

        remaining_file_objects = asyncio.run(
            self.target.write_batch(file_objects_batched)
        )
        assert isinstance(remaining_file_objects, list)
        assert len(remaining_file_objects) == len(source_file_objects)

    def test_write_empty_directories(self) -> None:
        source_directories = asyncio.run(
            self.mock_source_directories_empty.list_contents(include_directories=True)
        )

        file_objects_batched = self.mock_source_directories_empty.read_batch(
            source_directories,
            stream_chunk_size=0,
        )
        remaining_file_objects = asyncio.run(
            self.target.write_batch(file_objects_batched)
        )
        assert isinstance(remaining_file_objects, list)
        assert len(remaining_file_objects) == 0

    def test_read_batch(self) -> None:
        # only test function with empty inputs
        asyncio.run(async_iterator_to_list(self.target.read_batch([])))

    def test_delete_batch(self) -> None:
        self.object_store_empty(self.target)
        file_objects = asyncio.run(
            self.target.list_contents(checksum=False, include_directories=True)
        )

        assert isinstance(file_objects, list)
        # expect 0 files
        assert len(file_objects) == 0

    def test_object_store_delete(self) -> None:
        self.object_store_delete(self.target)


class _TestObjectStoreHandlerSync(_TestObjectStoreHandler):
    """Test two-way synchronization of objects between ObjectStores"""

    source: ObjectStoreHandler = None  # type: ignore[assignment]
    target: ObjectStoreHandler = None  # type: ignore[assignment]
    mock_source: ObjectStoreHandler = None  # type: ignore[assignment]

    @pytest_asyncio
    async def test_object_store_exists(self) -> None:
        for object_store in [self.source, self.target]:
            await clear_object_store(object_store, delete_target=True)
            exists = await object_store.storage_target_exists()
            assert isinstance(exists, bool)
            assert exists is False

    def test_object_stores_create(self) -> None:
        self.object_store_create(self.source)
        self.object_store_create(self.target)

    def test_copy_mock_to_source(self) -> None:
        """copy objects from mock to source"""
        source_file_objects = asyncio.run(self.mock_source.list_contents())
        assert isinstance(source_file_objects, list)

        file_objects_batched = self.mock_source.read_batch(
            source_file_objects,
            stream_chunk_size=self.mock_source.stream_chunk_size,
        )
        asyncio.run(self.source.write_batch(file_objects_batched))

        # expect equal number of (reg)files locally as on source
        # filter file_objects on regfiles (source does not list directories)
        file_objects = asyncio.run(self.source.list_contents(checksum=False))
        assert isinstance(file_objects, list)
        assert len(filter_regfiles(file_objects)) == len(source_file_objects)

    def test_copy_source_to_target(self) -> None:
        """Copy from source to target"""
        source_files = asyncio.run(self.source.list_contents())
        assert isinstance(source_files, list)

        file_objects_batched = self.source.read_batch(
            source_files,
            stream_chunk_size=self.source.stream_chunk_size,
        )
        asyncio.run(self.target.write_batch(file_objects_batched))

        # source and target should have equal number of files
        target_files = asyncio.run(self.target.list_contents(checksum=False))
        validate_file_objects_exclude_mtime(source_files, target_files)

    def test_object_stores_delete(self) -> None:
        """Delete all objects in source and target"""
        # delete source (including all objects)
        self.object_store_empty(self.source)
        self.object_store_delete(self.source)

        # delete target (including all objects)
        self.object_store_empty(self.target)
        self.object_store_delete(self.target)


class _TestWriteMisc(_TestObjectStoreHandler):
    target: ObjectStoreHandler = None  # type: ignore[assignment]
    mock: MockFileObjects = None  # type: ignore[assignment]

    def test_write_no_mtime(self) -> None:
        file_objects = self.mock.file_objects(file_count=10, file_mtime=-1)
        remaining = asyncio.run(self.target.write_batch(file_objects))
        assert isinstance(remaining, list)
        assert len(remaining) == 0
        target_files = asyncio.run(self.target.list_contents())
        assert isinstance(target_files, list)
        assert len(target_files) == len(file_objects)
