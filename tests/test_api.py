import os
import re
import json
import pytest

from typing import Dict, List, Tuple, Iterator, AsyncGenerator, Any

from lakedrive.api import (
    parse_target,
    Request,
    Response,
    Head,
    head,
    Get,
    get,
    Sync,
    Put,
    put,
    Delete,
    delete,
)
from lakedrive.api import FileBatch
from lakedrive.core.objects import HashTuple, ByteStream, FileObject
from lakedrive.core.handlers import ObjectStoreHandler, SchemeError
from lakedrive.localfs.handler import LocalFileHandler
from lakedrive.utils.event_loop import run_on_event_loop

from .localfs.helpers import cleanup_tempdir_localfs, create_files_localfs
from .helpers.mock_classes import MockApiRequestPermissionError
from .helpers.generate_objects import mock_file_objects
from .helpers.misc import compute_md5sum
from .helpers.async_parsers import pytest_asyncio
from .helpers.validators import validate_file_objects_exclude_mtime

TEMPDIR_API = "/tmp/lakedrive-tests/api"


async def mock_object_store_get_object_exception(
    *args: Any, **kwargs: Any
) -> FileObject:
    raise Exception("Dummy Exception GetObject")


async def mock_object_store_head_object_exception(*args: Any, **kwargs: Any) -> None:
    raise Exception("Dummy Exception HeadObject")


async def mock_object_store_list_objects_exception(
    *args: Any, **kwargs: Any
) -> List[FileObject]:
    raise Exception("Dummy Exception ListObjects")


async def mock_object_store_get_object_batch_exception(
    *args: Any, **kwargs: Any
) -> List[FileObject]:
    raise Exception("Dummy Exception GetObjectBatch")


async def mock_object_store_delete_file_exception(*args: Any, **kwargs: Any) -> None:
    raise Exception("Dummy Exception Delete")


async def mock_object_store_delete_storage_target_permission_error() -> bool:
    raise PermissionError("Access denied")


async def mock_object_store_delete_storage_target_unknown_error() -> bool:
    raise Exception("Unknown error")


async def mock_parse_request_unknown_error() -> None:
    raise Exception("Unknown error")


async def monkey_patched_ahead(target: str, recursive: bool = False) -> Response:
    return Response("MonkeyMethod", status_code=509, error_message="monkeypatched")


@pytest.fixture(scope="session", autouse=True)  # type: ignore[misc]
def tempdir_api() -> Iterator[str]:
    os.makedirs(TEMPDIR_API, exist_ok=True)
    assert os.path.exists(TEMPDIR_API) is True
    yield TEMPDIR_API
    cleanup_tempdir_localfs(TEMPDIR_API)


@pytest.fixture(scope="session", autouse=True)  # type: ignore[misc]
def tempfile_api(tempdir_api: str) -> Tuple[str, str, int]:
    """Write test file to disk, return (name, hashvalue, size)"""
    file_path = f"{tempdir_api}/test_tempfile_api"
    file_object = mock_file_objects(count=1, minsize_kb=8, maxsize_kb=8)[0]
    assert isinstance(file_object.hash, HashTuple)
    assert isinstance(file_object.size, int)
    put(file_path, data=file_object)  # write to disk
    return file_path, file_object.hash.value, file_object.size


def test_parse_target() -> None:
    # list of good examples that need to pass succesfully
    # (original target, storage_target, file_path )
    targets_map_validate = [
        ("s3://test-bucket", "s3://test-bucket", ""),
        ("s3://test-bucket/", "s3://test-bucket", ""),
        ("test-folder", "test-folder", ""),
        ("/test-folder", "/test-folder", ""),
        ("/test-folder/", "/test-folder", ""),
        ("/test-folder/file", "/test-folder", "file"),
        ("/test-folder/file/file", "/test-folder/file", "file"),
    ]

    for test_case in targets_map_validate:
        target, storage_target_x, file_path_x = test_case

        object_store, file_path_y = parse_target(target)
        assert isinstance(object_store, ObjectStoreHandler)
        assert isinstance(file_path_y, str)

        # validate if storage_target is correct
        assert object_store.storage_target == storage_target_x
        # validate if file_path is correct
        assert file_path_y == file_path_x

    # invalid schemes
    targets_map_invalid = ["", "s1://test-bucket"]
    for target in targets_map_invalid:
        with pytest.raises(SchemeError) as error:
            parse_target(target)
        assert str(error.value) == "unsupported scheme"


class TestFileBatch:
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
    ) -> None:
        self.max_read_threads = 128
        self.handler = ObjectStoreHandler("dummy", 1024**2, self.max_read_threads, 32)
        self.file_batch = FileBatch(self.handler)

    def test___init__(self) -> None:
        assert isinstance(self.file_batch, FileBatch)

    def test_max_read_threads(self) -> None:
        max_read_threads = self.file_batch.max_read_threads()
        assert isinstance(max_read_threads, int)
        assert max_read_threads == self.max_read_threads

    def test_list_objects(self) -> None:
        objects = self.file_batch.list_objects()
        assert isinstance(objects, list)
        assert len(objects) == 0

    @pytest_asyncio
    async def test_get_batch_reader(self) -> None:
        batch_reader = await self.file_batch.get_batch_reader()
        assert isinstance(batch_reader, AsyncGenerator)


class TestResponse:
    def test_response_nofiles(self) -> None:
        method = "GetObject"
        response = Response(method, status_code=200)
        assert isinstance(response.method, str)
        assert response.method == method
        assert isinstance(response.status_code, int)
        assert response.status_code == 200
        assert not response.file_batch

        assert isinstance(response.file_count(), int)
        assert response.file_count() == 0

        with pytest.raises(FileNotFoundError) as error:
            _ = response.read()
        assert isinstance(error.value, FileNotFoundError)  # test "empty" value

        with pytest.raises(FileNotFoundError) as error:
            response.seek(0)
        assert isinstance(error.value, FileNotFoundError)  # test "empty" value

    def test_response_get_object(self) -> None:
        method = "GetObject"
        file_object = mock_file_objects(count=1, maxsize_kb=16)[0]
        dummy_handler = ObjectStoreHandler("dummy", 1024**2, 128, 32)
        dummy_handler.object_list = [file_object]
        response = Response(
            method,
            status_code=200,
            file_batch=FileBatch(dummy_handler),
        )
        assert isinstance(response.method, str)
        assert response.method == method
        assert isinstance(response.status_code, int)
        assert response.status_code == 200
        file_batch = response.file_batch
        assert isinstance(file_batch, FileBatch)
        assert response.file_count() == 1
        assert file_batch.list_objects()[0] == file_object

        file_object_data = response.read()
        assert isinstance(file_object_data, bytes)
        assert len(file_object_data) == file_object.size
        assert isinstance(file_object.hash, HashTuple)
        assert compute_md5sum(file_object_data) == file_object.hash.value

    def test_response_get_object_no_data(self) -> None:

        dummy_handler = ObjectStoreHandler("dummy", 1024**2, 128, 32)
        dummy_handler.object_list = [FileObject("emptyfile")]
        response = Response(
            "GetObject",
            status_code=200,
            file_batch=FileBatch(dummy_handler),
        )
        data = response.read()
        assert isinstance(data, bytes)
        assert len(data) == 0

    def test_response_seek_no_get_object(
        self, tempfile_api: Tuple[str, str, int]
    ) -> None:
        """Test to ensure doing a seek on non GetObject method products no error"""
        dummy_handler = ObjectStoreHandler("dummy", 1024**2, 128, 32)
        dummy_handler.object_list = [FileObject("emptyfile")]
        Response(
            "ListObjects",
            status_code=200,
            file_batch=FileBatch(dummy_handler),
        ).seek(0)

    def test_response_list_objects(self) -> None:
        file_count = 3

        dummy_handler = ObjectStoreHandler("dummy", 1024**2, 128, 32)
        file_objects = mock_file_objects(count=file_count, maxsize_kb=16)
        dummy_handler.object_list = file_objects
        method = "ListObjects"

        response = Response(
            method,
            status_code=200,
            file_batch=FileBatch(dummy_handler),
        )
        assert isinstance(response.method, str)
        assert response.method == method
        assert isinstance(response.status_code, int)
        assert response.status_code == 200
        file_batch = response.file_batch
        assert isinstance(file_batch, FileBatch)
        assert response.file_count() == file_count
        for idx in range(file_count):
            assert file_batch.source.object_list[idx] == file_objects[idx]

        file_object_list = b"\n".join([fo.name.encode() for fo in file_objects])
        assert response.read() == file_object_list

    def test_response_list_no_objects(self) -> None:
        dummy_handler = ObjectStoreHandler("dummy", 1024**2, 128, 32)
        method = "ListObjects"

        response = Response(
            method,
            status_code=200,
            file_batch=FileBatch(dummy_handler),
        )
        assert isinstance(response.method, str)
        assert response.method == method
        assert isinstance(response.status_code, int)
        assert response.status_code == 200
        file_batch = response.file_batch
        assert isinstance(file_batch, FileBatch)
        assert response.file_count() == 0
        assert response.read() == b""

        # test read without file_batch set
        response.file_batch = None
        assert response.file_count() == 0
        assert response.read() == b""

    def test_response_get_object_batch(self) -> None:
        method = "GetObjectBatch"
        response = Response(method, status_code=200)
        assert isinstance(response.method, str)
        assert response.method == method
        assert isinstance(response.status_code, int)
        assert response.status_code == 200
        assert not response.file_batch

        assert isinstance(response.file_count(), int)
        assert response.file_count() == 0
        assert response.read() == b""

    @pytest_asyncio
    async def test_close_function(self) -> None:
        dummy_handler = ObjectStoreHandler("dummy", 1024**2, 128, 32)
        file_object = mock_file_objects(count=1, minsize_kb=1, maxsize_kb=1)[0]
        dummy_handler.object_list = [file_object]

        response = Response(
            "GetObject",
            status_code=200,
            file_batch=FileBatch(dummy_handler),
        )
        # test undefined close_function, file_object should be un-affected
        prev_state = file_object.source
        await response.aclose()
        assert file_object.source == prev_state

        #  do a partial read, this will define close_function
        await response.aread(size=1)

        # test defined close_function
        # file_object.source should changed to consumed state
        await response.aclose()
        assert isinstance(file_object.source, ByteStream)
        assert file_object.source.end_of_stream is True
        assert file_object.source.stream is None

        # test repeat run of defined close_function
        await response.aclose()
        assert file_object.source.end_of_stream is True
        assert file_object.source.stream is None

    def test_method_not_exists(self) -> None:
        dummy_handler = ObjectStoreHandler("dummy", 1024**2, 128, 32)
        dummy_handler.object_list = [FileObject("emptyfile")]
        response = Response(
            "MethodNotExists",
            status_code=200,
            file_batch=FileBatch(dummy_handler),
        )
        with pytest.raises(ValueError) as error:
            response.read()
        assert str(error.value) == "Method not supported: MethodNotExists"


class TestRequest:
    def test___init__(self) -> None:
        request = Request("target")
        assert isinstance(request.target, str)
        assert request.target == "target"
        for attr in ["Response", "object_store", "file_path"]:
            assert hasattr(request, attr)
            assert getattr(request, attr) is None

    def test___enter__(self) -> None:
        request = Request("target/path").__enter__()
        assert isinstance(request, Request)
        assert isinstance(request.object_store, ObjectStoreHandler)
        assert request.object_store.storage_target == "target"
        assert isinstance(request.file_path, str)
        assert request.file_path == "path"

    def test___enter__permission_error(self) -> None:
        request = MockApiRequestPermissionError("target/path").__enter__()
        assert isinstance(request, Request)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 403
        assert request.Response.error_message == "MockApiRequestPermissionError"

    def test___enter__scheme_error(self) -> None:
        request = Request("").__enter__()
        assert isinstance(request, Request)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 400
        assert request.Response.error_message == "unsupported scheme"

    def test___exit__(self) -> None:
        """Exist tests should pass silently"""
        request = Request("target")

        # test with Response un-defined
        request.__exit__()
        run_on_event_loop(request.__aexit__)

        dummy_handler = ObjectStoreHandler("dummy", 1024**2, 128, 32)
        dummy_handler.object_list = [FileObject("emptyfile")]
        request.Response = Response(
            "GetObject",
            status_code=200,
            file_batch=FileBatch(dummy_handler),
        )
        # test with Response defined
        request.__exit__()
        run_on_event_loop(request.__aexit__)


class TestPut:
    def test___init___no_data(self) -> None:
        for target in ["target/", "target/file.txt"]:
            request = Put(target)
            assert isinstance(request, Put)
            assert isinstance(request.target, str)
            assert request.target == target
            assert isinstance(request.data, bytes)
            assert len(request.data) == 0

    def test_put_file_directory_exception(self) -> None:
        """Trying to write data to a (virtual) directory
        should raise an Exception"""
        with pytest.raises(Exception) as error:
            Put("target/virtual_dir/", data=b"abc")
        assert str(error.value) == "Cant write file to a (virtual) directory"

    def test_put_object_localfs(self, tempdir_api: str) -> None:
        file_name = "test_lorem.txt"
        file_data = b"lorem ipsum"
        file_path = f"{tempdir_api}/{file_name}"
        request = Put(file_path, data=file_data).__enter__()
        assert isinstance(request, Put)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 201

        assert isinstance(request.target, str)
        assert isinstance(request.object_store, ObjectStoreHandler)
        assert request.object_store.storage_target == tempdir_api

        # validate if file contains correct contents
        assert os.path.isfile(file_path)
        with open(file_path, "rb") as stream:
            data = stream.read()
        assert data == file_data

        # validate function has same outcome as class
        response = put(file_path, data=(file_data + b"-2"))
        assert isinstance(response, Response)
        assert request.Response.status_code == 201

        assert os.path.isfile(file_path)
        with open(file_path, "rb") as stream:
            data = stream.read()
        assert data == (file_data + b"-2")

    def test_copy_objects_localfs_recursive(self, tempdir_api: str) -> None:
        source_dir = f"{tempdir_api}/test_copy_objects"
        target_dir = f"{source_dir}_copy"
        file_count = 3

        # create test files
        create_files_localfs(source_dir, "", file_count)

        # read test files
        read_request = Get(f"{source_dir}/", recursive=True).__enter__()
        assert isinstance(read_request, Get)
        read_response = read_request.Response
        assert isinstance(read_response, Response)
        assert read_response.status_code == 200
        assert isinstance(read_response.file_batch, FileBatch)
        assert read_response.file_count() == file_count

        # write test files to copy target
        write_request = Put(f"{target_dir}/", data=read_response.file_batch).__enter__()
        write_response = write_request.Response
        assert isinstance(write_response, Response)
        assert write_response.status_code == 201

        # test by reading from target
        tgt_response = get(f"{target_dir}/", recursive=True)
        assert tgt_response.file_count() == file_count
        assert isinstance(tgt_response.file_batch, FileBatch)
        validate_file_objects_exclude_mtime(
            read_response.file_batch.list_objects(),
            tgt_response.file_batch.list_objects(),
        )

    def test_copy_objects_localfs_recursive_with_remaining(
        self, tempdir_api: str
    ) -> None:
        source_dir = f"{tempdir_api}/test_copy_objects_remaining"
        target_dir = f"{source_dir}_copy"
        file_count = 5

        # create test files
        os.makedirs(source_dir)
        testfiles = [
            f"{source_dir}/test_{str(idx)}/test_{str(idx)}" for idx in range(file_count)
        ]
        for idx, file_path in enumerate(testfiles):
            source_dir_sub = os.path.dirname(file_path)
            os.makedirs(source_dir_sub)
            with open(file_path, "wb") as stream:
                stream.write(f"file {str(idx)}".encode())

        # read test files
        read_request = Get(f"{source_dir}/", recursive=True).__enter__()
        assert isinstance(read_request, Get)
        read_response = read_request.Response
        assert isinstance(read_response, Response)
        assert read_response.status_code == 200
        assert isinstance(read_response.file_batch, FileBatch)
        assert read_response.file_count() == file_count

        # sabotage to trigger list of remaining (failed) files
        sabotaged_files: List[str] = []
        for idx, file_path in enumerate(testfiles):
            if (idx + 1) % 3 == 0:
                # remove file
                os.unlink(file_path)
                sabotaged_files.append(re.sub(f"^{source_dir}/", "", file_path))
            if (idx + 1) % 4 == 0:
                # remove permission for filedir
                os.chmod(os.path.dirname(file_path), 0o000)
                sabotaged_files.append(re.sub(f"^{source_dir}/", "", file_path))

        # write test files to copy target
        write_request = Put(f"{target_dir}/", data=read_response.file_batch).__enter__()
        write_response = write_request.Response
        assert isinstance(write_response, Response)
        assert write_response.status_code == 207
        assert isinstance(write_response.body, bytes)
        json_body = json.loads(write_response.body.decode())
        assert isinstance(json_body, dict)
        assert isinstance(json_body["PutObjects"], dict)
        failed_files = json_body["PutObjects"]["failed"]
        assert isinstance(failed_files, list)
        assert len(failed_files) == len(sabotaged_files)
        failed_files_by_name = [ff["name"] for ff in failed_files]
        assert sorted(failed_files_by_name) == sorted(sabotaged_files)

    def test_put_empty_file_batch(self, tempdir_api: str) -> None:
        dummy_handler = ObjectStoreHandler("dummy", 1024**2, 128, 32)
        dummy_handler.object_list = []
        empty_file_batch = FileBatch(dummy_handler)

        request = Put(f"{tempdir_api}/somedir/", data=empty_file_batch).__enter__()
        response = request.Response
        assert isinstance(response, Response)
        assert response.status_code == 400

    def test_put_file_batch_target_rewrite(self, tempdir_api: str) -> None:
        dummy_handler = ObjectStoreHandler("dummy", 1024**2, 128, 32)
        dummy_handler.object_list = [FileObject(str(x)) for x in range(3)]
        dummy_file_batch = FileBatch(dummy_handler)

        # if FileBatch contains more than 1 file, and target is not a (virtual)
        # directory (i.e. ends with "/"), it should be added in Put()
        for target in ["", "target"]:
            request = Put(target, data=dummy_file_batch)
            assert isinstance(request, Put)
            assert isinstance(request.target, str)
            assert request.target == f"{target}/"

    def test_put_file_localfs_error_directory_exists(self, tempdir_api: str) -> None:
        file_path_directory = f"{tempdir_api}/test_directory"
        os.makedirs(file_path_directory, exist_ok=True)

        request = Put(file_path_directory, data=b"abc").__enter__()
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 409
        assert isinstance(request.Response.error_message, str)
        assert (
            request.Response.error_message
            == f"path '{file_path_directory}' taken by a directory"
        )

    def test_put_file_localfs_error_invalid_source(self, tempdir_api: str) -> None:
        bad_sources: List[Any] = [123, None, "abc", [], ()]
        for source in bad_sources:
            with pytest.raises(ValueError) as error:
                Put(f"{tempdir_api}/dummy", data=source).__enter__()
            assert (
                str(error.value) == "Data should be either FileObject or bytes object"
            )

    @pytest_asyncio
    async def test_put_file_error_permission(self, tempdir_api: str) -> None:
        file_path_directory = f"{tempdir_api}/test_directory_no_perm"
        os.makedirs(file_path_directory, exist_ok=True)
        os.chmod(file_path_directory, 0o000)
        file_path = f"{file_path_directory}/test_lorem.txt"

        request = await Put(file_path).__aenter__()
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 403
        assert isinstance(request.Response.error_message, str)
        assert request.Response.error_message == f"Permission denied: '{file_path}'"

        # similar as previous, but this tests PermissionError in _put_single_object()
        # which is normally skipped for localfs
        request = Put(file_path)
        assert isinstance(request, Put)
        # temporarily restore to get passed parse_target
        os.chmod(file_path_directory, 0o777)
        request.object_store, request.file_path = parse_target(file_path)

        os.chmod(file_path_directory, 0o000)
        await request._put_single_object(FileObject("test_lorem.txt"))
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 403
        assert isinstance(request.Response.error_message, str)
        assert request.Response.error_message == f"Permission denied: '{file_path}'"


class TestHead:
    def test_head_file(self, tempfile_api: Tuple[str, str, int]) -> None:
        file_path, file_hash, file_size = tempfile_api
        request = Head(file_path).__enter__()
        assert isinstance(request, Head)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 200
        assert isinstance(request.Response.file_batch, FileBatch)
        file_objects = request.Response.file_batch.list_objects()
        assert len(file_objects) == 1
        assert isinstance(file_objects[0].hash, HashTuple)
        assert file_objects[0].hash.value == file_hash
        assert file_objects[0].size == file_size

        # validate function has same outcome as class
        response = head(file_path)
        assert isinstance(response, Response)
        assert response.status_code == 200
        assert isinstance(response.file_batch, FileBatch)
        file_objects = response.file_batch.list_objects()
        assert len(file_objects) == 1
        assert isinstance(file_objects[0].hash, HashTuple)
        assert file_objects[0].hash.value == file_hash
        assert file_objects[0].size == file_size

    def test_list_objects_recursive(self, tempdir_api: str) -> None:
        virtual_dir = f"{tempdir_api}/test_head_directory_objects"

        # create test files
        os.makedirs(virtual_dir)
        testfiles = [f"{virtual_dir}/test_{str(idx)}" for idx in range(3)]
        for file_path in testfiles:
            with open(file_path, "wb") as stream:
                stream.write(b"")

        request = Head(f"{virtual_dir}/", recursive=True).__enter__()
        assert isinstance(request, Head)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 200
        data = request.Response.read()
        assert isinstance(data, bytes)
        assert sorted(data.split(b"\n")) == sorted(
            [os.path.basename(tf).encode() for tf in testfiles]
        )

    def test_list_objects_non_recursive_error(self, tempdir_api: str) -> None:
        virtual_dir = f"{tempdir_api}/test_head_list_objects_empty"
        os.makedirs(virtual_dir)

        request = Head(f"{virtual_dir}/").__enter__()
        assert isinstance(request, Head)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 400
        assert (
            request.Response.error_message
            == "source is a directory, \
add '-r/--recursive' to run recursively"
        )

    def test_head_target_not_found(self, tempdir_api: str) -> None:
        file_path = f"{tempdir_api}/test_head_target_not_exists/somefile"
        request = Head(file_path).__enter__()
        assert isinstance(request, Head)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 404
        assert (
            request.Response.error_message
            == f"'{os.path.dirname(file_path)}' not exists"
        )

    def test_head_file_not_found(self, tempdir_api: str) -> None:
        file_path = f"{tempdir_api}/test_head_file_not_exists"
        request = Head(file_path).__enter__()
        assert isinstance(request, Head)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 404

        # validate function has same outcome as class
        response = head(file_path)
        assert isinstance(response, Response)
        assert response.status_code == 404
        assert isinstance(request.Response.error_message, str)
        assert request.Response.error_message == "FileNotFoundError"

    def test_head_file_no_permission(self, tempdir_api: str) -> None:
        # create empty file with no permissions
        file_path = f"{tempdir_api}/test_head_file_no_perm"

        with open(file_path, "wb") as stream:
            stream.write(b"")
        os.chmod(file_path, 0o000)

        request = Head(file_path).__enter__()
        assert isinstance(request, Head)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 403
        assert isinstance(request.Response.error_message, str)
        assert request.Response.error_message == "PermissionError"

    def test_list_objects_no_permission(self, tempdir_api: str) -> None:
        file_path_directory = f"{tempdir_api}/test_virtualdir_head_no_perm/"
        os.makedirs(file_path_directory, exist_ok=True)
        os.chmod(file_path_directory, 0o000)

        request = Head(file_path_directory).__enter__()
        assert isinstance(request, Head)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 403
        assert isinstance(request.Response.error_message, str)
        assert (
            request.Response.error_message
            == f"denied access to '{file_path_directory[0:-1]}'"
        )

    @pytest_asyncio
    async def test_head_object_dummy_exception(self, tempdir_api: str) -> None:
        # create empty file with no permissions
        target = f"{tempdir_api}/dummy"
        request = Head(target)
        assert isinstance(request, Head)
        request.object_store, request.file_path = parse_target(target)

        # force assign to mock function to test specific behavior
        request.object_store.head_file = (  # type: ignore[assignment]
            mock_object_store_head_object_exception
        )
        await request._parse_request()
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 500
        assert isinstance(request.Response.error_message, str)
        assert request.Response.error_message == "Dummy Exception HeadObject"

    @pytest_asyncio
    async def test_list_objects_recursive_dummy_exception(
        self, tempdir_api: str
    ) -> None:
        # create empty file with no permissions
        target = f"{tempdir_api}/test_virtualdir_head_exception/"
        os.makedirs(target)

        request = Head(target, recursive=True)
        assert isinstance(request, Head)
        request.object_store, request.file_path = parse_target(target)

        # force assign to mock function to test specific behavior
        request.object_store.list_contents = (  # type: ignore[assignment]
            mock_object_store_list_objects_exception
        )
        await request._parse_request()
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 500
        assert isinstance(request.Response.error_message, str)
        assert request.Response.error_message == "Dummy Exception ListObjects"


class TestGet:
    def test_get_file(self, tempfile_api: Tuple[str, str, int]) -> None:
        file_path, file_hash, file_size = tempfile_api
        request = Get(file_path).__enter__()
        assert isinstance(request, Get)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 200

        data = request.Response.read(seek=0)
        assert isinstance(data, bytes)
        assert len(data) == file_size
        assert compute_md5sum(data) == file_hash

        # validate function has same outcome as class
        response = get(file_path)
        assert isinstance(response, Response)
        assert compute_md5sum(response.read(seek=0)) == file_hash

    def test_get_target_not_found(self, tempdir_api: str) -> None:
        file_path = f"{tempdir_api}/test_get_target_not_exists/somefile"
        request = Get(file_path).__enter__()
        assert isinstance(request, Get)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 404
        assert (
            request.Response.error_message
            == f"'{os.path.dirname(file_path)}' not exists"
        )

    def test_get_target_not_found_localfs_relative(self) -> None:
        relative_file_path_prefixes = [
            "./",
            "../",
            "../../",
            "/",
            "./.",
            "/.",
        ]
        for prefix in relative_file_path_prefixes:
            file_path = f"{prefix}test_target_not_exist"
            request = Get(file_path).__enter__()
            assert isinstance(request, Get)
            assert isinstance(request.Response, Response)
            assert request.Response.status_code == 404
            assert request.Response.error_message == f"'{file_path}' not exists"

    def test_get_batch_objects_recursive(self, tempdir_api: str) -> None:
        virtual_dir = f"{tempdir_api}/test_get_directory_objects"

        # create test files
        os.makedirs(virtual_dir)
        testfiles = [f"{virtual_dir}/test_{str(idx)}" for idx in range(3)]
        for file_path in testfiles:
            with open(file_path, "wb") as stream:
                stream.write(b"")

        request = Get(f"{virtual_dir}/", recursive=True).__enter__()
        assert isinstance(request, Get)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 200
        file_batch = request.Response.file_batch
        assert isinstance(file_batch, FileBatch)
        assert sorted([fn.name for fn in file_batch.list_objects()]) == sorted(
            [os.path.basename(tf) for tf in testfiles]
        )

    def test_get_objects_non_recursive_error(self, tempdir_api: str) -> None:
        virtual_dir = f"{tempdir_api}/test_get_list_objects_empty"
        os.makedirs(virtual_dir)

        request = Get(f"{virtual_dir}/").__enter__()
        assert isinstance(request, Get)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 400
        assert (
            request.Response.error_message
            == "source is a directory, \
add '-r/--recursive' to run recursively"
        )

    def test_seek_tail_bytes(self, tempfile_api: Tuple[str, str, int]) -> None:
        file_path, file_hash, file_size = tempfile_api
        request = Get(file_path).__enter__()
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 200

        # try to get last 1/10th of data by using seek
        tail_bytes = int(file_size / 10)
        data = request.Response.read(seek=(file_size - tail_bytes))
        assert isinstance(data, bytes)
        assert len(data) == tail_bytes

    def test_seek_extra_bytes(self, tempfile_api: Tuple[str, str, int]) -> None:
        file_path, file_hash, file_size = tempfile_api
        request = Get(file_path).__enter__()
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 200

        # try to get more bytes than are available
        extra_bytes = file_size + 1000
        data = request.Response.read(seek=extra_bytes)
        assert isinstance(data, bytes)
        assert len(data) == 0

        # like previous test, but this time data is pre-fetched
        request.Response.seek(0)  # rewind
        data = request.Response.read(seek=extra_bytes)
        assert isinstance(data, bytes)
        assert len(data) == 0

    def test_get_file_chunked(self, tempfile_api: Tuple[str, str, int]) -> None:
        file_path, file_hash, file_size = tempfile_api
        chunk_size = 1024
        request = Get(file_path, chunk_size=chunk_size).__enter__()
        assert isinstance(request, Get)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 200

        no_chunks = (file_size + chunk_size - 1) // chunk_size
        data = b""
        for i in range(no_chunks):
            data += request.Response.read(chunk_size)

        assert isinstance(data, bytes)
        assert len(data) == file_size
        assert compute_md5sum(data) == file_hash

    def test_get_file_not_found(self, tempdir_api: str) -> None:
        file_path = f"{tempdir_api}/test_get_file_not_exists"
        request = Get(file_path).__enter__()
        assert isinstance(request, Get)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 404

        # validate function has same outcome as class
        response = get(file_path)
        assert isinstance(response, Response)
        assert response.status_code == 404

    def test_get_file_no_permission(self, tempdir_api: str) -> None:
        # create empty file with no permissions
        file_path = f"{tempdir_api}/test_get_file_no_perm"

        with open(file_path, "wb") as stream:
            stream.write(b"")
        os.chmod(file_path, 0o000)

        request = Get(file_path).__enter__()
        assert isinstance(request, Get)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 403

    def test_get_object_batch_no_permission(self, tempdir_api: str) -> None:
        file_path_directory = f"{tempdir_api}/test_virtualdir_get_no_perm/"
        os.makedirs(file_path_directory, exist_ok=True)
        os.chmod(file_path_directory, 0o000)

        request = Get(file_path_directory).__enter__()
        assert isinstance(request, Get)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 403

    @pytest_asyncio
    async def test_get_object_dummy_exception(self, tempdir_api: str) -> None:
        # create empty file
        target = f"{tempdir_api}/dummy"
        request = Get(target)
        assert isinstance(request, Get)
        request.object_store, request.file_path = parse_target(target)

        # force assign to mock function to test specific behavior
        request.object_store.read_file = (  # type: ignore[assignment]
            mock_object_store_get_object_exception
        )
        await request._parse_request()
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 500
        assert isinstance(request.Response.error_message, str)
        assert request.Response.error_message == "Dummy Exception GetObject"

    @pytest_asyncio
    async def test_get_object_batch_recursive_dummy_exception(
        self, tempdir_api: str
    ) -> None:
        # create empty file with no permissions
        target = f"{tempdir_api}/test_virtualdir_get_exception/"
        os.makedirs(target)

        request = Get(target, recursive=True)
        assert isinstance(request, Get)
        request.object_store, request.file_path = parse_target(target)

        # force assign to mock function to test specific behavior
        request.object_store.list_contents = (  # type: ignore[assignment]
            mock_object_store_get_object_batch_exception
        )
        await request._parse_request()
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 500
        assert isinstance(request.Response.error_message, str)
        assert request.Response.error_message == "Dummy Exception GetObjectBatch"


class TestSync:
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
    ) -> None:
        self.safe_params: Dict[str, bool] = {
            "checksum": False,
            "skip_newer_on_target": True,
            "delete_extraneous": False,
            "dry_run": True,
        }

    def test_sync_parse_dry(self) -> None:
        request = Sync(params=self.safe_params).__enter__()
        assert isinstance(request, Sync)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 200

    @pytest_asyncio
    async def test_sync__exit__(self) -> None:
        request: Sync = Sync(params=self.safe_params)
        # both __exit__() functions should pass silently
        request.__exit__()
        await request.__aexit__()

    def test_sync_no_permission_source(self, tempdir_api: str) -> None:
        source_directory = f"{tempdir_api}/test_virtualdir_sync_no_perm_source"
        os.makedirs(source_directory, exist_ok=True)
        os.chmod(source_directory, 0o000)

        request = Sync(
            source=source_directory, destination=f"{source_directory}_dest"
        ).__enter__()
        assert isinstance(request, Sync)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 403

    def test_sync_no_permission_destination(self, tempdir_api: str) -> None:
        destination_directory = (
            f"{tempdir_api}/test_virtualdir_sync_no_perm_destination"
        )
        os.makedirs(destination_directory, exist_ok=True)
        os.chmod(destination_directory, 0o000)

        request = Sync(destination=destination_directory).__enter__()
        assert isinstance(request, Sync)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 403

    def test_sync_invalid_scheme(self) -> None:
        source = "xyz://test_invalid_scheme"
        request = Sync(source=source).__enter__()
        assert isinstance(request, Sync)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 400
        assert isinstance(request.Response.error_message, str)
        assert request.Response.error_message == "unsupported scheme"

    def test_sync_unknown_error(self) -> None:
        request = Sync()
        assert isinstance(request, Sync)
        request._parse_request = (  # type: ignore[assignment]
            mock_parse_request_unknown_error
        )
        request.__enter__()
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 500
        assert request.Response.error_message == "Unknown error"


def delete_from_sub_path(original_file_path: str) -> Delete:
    # Instead of calling Delete().__enter__(), we manipulate paths such that
    # storage_target is not file_path_directory, but its parent.

    # this enables testing Exceptions (e.g. PermissionError) on file_objects
    # that are otherwise triggered on storage_path.

    # this can also be done by setting specific file-attributes (e.g. using
    # cflags/ chattr), but this is very OS dependant which is difficult to test
    request = Delete(original_file_path)
    _object_store, _file_path = parse_target(original_file_path)
    _object_store.storage_target, remainder = _object_store.storage_target.rsplit(
        "/", 1
    )
    assert isinstance(_object_store, LocalFileHandler)
    _object_store.storage_path = f"{_object_store.storage_target}/"
    request.object_store = _object_store
    request.file_path = f"{remainder}/{_file_path}"
    run_on_event_loop(request._parse_request)
    return request


class TestDelete:
    def test_delete_file_localfs(self, tempdir_api: str) -> None:
        # create empty file
        file_path = f"{tempdir_api}/test_delete_file"
        with open(file_path, "wb") as stream:
            stream.write(b"")
        request = Delete(file_path).__enter__()
        assert isinstance(request, Delete)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 204

        # validate function has same outcome as class
        with open(file_path, "wb") as stream:
            stream.write(b"")
        response = delete(file_path)
        assert isinstance(response, Response)
        assert request.Response.status_code == 204

    def test_delete_file_error_not_exist(self, tempdir_api: str) -> None:
        """Delete is idempotent,
        so if file does not exist should also give a 204"""
        file_path = f"{tempdir_api}/test_file_does_not_exist"
        request = Delete(file_path).__enter__()
        assert isinstance(request, Delete)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 204

    def test_delete_file_error_no_permission(self, tempdir_api: str) -> None:
        file_path_directory = f"{tempdir_api}/test_delete_directory_noperm"
        file_path = f"{file_path_directory}/test_no_permission"
        os.makedirs(file_path_directory, exist_ok=True)
        with open(file_path, "wb") as stream:
            stream.write(b"lorum")
        os.chmod(file_path_directory, 0o0555)  # set read only on directory

        request = Delete(file_path).__enter__()
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 403
        assert isinstance(request.Response.error_message, str)
        assert request.Response.error_message == f"denied access to '{file_path}'"
        os.chmod(file_path_directory, 0o777)  # restore -- else cleanup fails

    def test_delete_object_batch_localfs_recursive(self, tempdir_api: str) -> None:
        source_dir = f"{tempdir_api}/test_delete_objects"
        file_count = 5

        # create files
        create_files_localfs(source_dir, "", file_count)
        # test if source contains correct number of files
        response = head(f"{source_dir}/", recursive=True)
        assert isinstance(response, Response)
        assert response.status_code == 200
        assert isinstance(response.file_batch, FileBatch)
        assert response.file_count() == file_count

        # test Delete using file_batch
        response = delete(response.file_batch, recursive=True)
        assert isinstance(response, Response)
        assert response.status_code == 204

        # test if empty after file_batch based delete
        assert head(f"{source_dir}/", recursive=True).file_count() == 0

        # (re-)create files
        source_dir = f"{source_dir}_2"
        create_files_localfs(source_dir, "", file_count)
        assert head(f"{source_dir}/", recursive=True).file_count() == file_count

        # test delete using directory
        response = delete(f"{source_dir}/", recursive=True)
        assert isinstance(response, Response)
        assert response.status_code == 204

        # test if empty after directory-based delete
        assert head(f"{source_dir}/", recursive=True).file_count() == 0

    def test_delete_object_batch_localfs_target_not_exist(
        self, tempdir_api: str
    ) -> None:
        source_dir = f"{tempdir_api}/test_delete_object_batch_not_exists"
        response = delete(f"{source_dir}/")
        assert isinstance(response, Response)
        assert response.status_code == 404

    def test_delete_object_batch_localfs_no_file_batch(
        self, tempdir_api: str, monkeypatch: Any
    ) -> None:
        source_dir = f"{tempdir_api}/test_delete_object_batch_no_file_batch"
        os.makedirs(source_dir)

        monkeypatch.setattr("lakedrive.api.ahead", monkey_patched_ahead)
        response = delete(f"{source_dir}/")
        assert isinstance(response, Response)
        assert response.status_code == 509

    def test_delete_object_batch_localfs_recursive_with_remaining(
        self, tempdir_api: str
    ) -> None:

        source_dir = f"{tempdir_api}/test_delete_objects_remaining"
        file_count = 5

        # create test files
        os.makedirs(source_dir)
        testfiles = [
            f"{source_dir}/test_{str(idx)}/test_{str(idx)}" for idx in range(file_count)
        ]
        for idx, file_path in enumerate(testfiles):
            source_dir_sub = os.path.dirname(file_path)
            os.makedirs(source_dir_sub)
            with open(file_path, "wb") as stream:
                stream.write(f"file {str(idx)}".encode())

        # read test files
        read_request = Get(f"{source_dir}/", recursive=True).__enter__()
        assert isinstance(read_request, Get)
        read_response = read_request.Response
        assert isinstance(read_response, Response)
        assert read_response.status_code == 200
        assert isinstance(read_response.file_batch, FileBatch)
        assert read_response.file_count() == file_count

        # sabotage to trigger list of remaining (failed) files
        sabotaged_files: List[str] = []
        for idx, file_path in enumerate(testfiles):
            if (idx + 1) % 4 == 0:
                # remove permission for filedir
                os.chmod(os.path.dirname(file_path), 0o000)
                sabotaged_files.append(re.sub(f"^{source_dir}/", "", file_path))

        # delete test files
        response = delete(read_response.file_batch)
        assert isinstance(response, Response)
        assert response.status_code == 207

        assert isinstance(response.body, bytes)
        json_body = json.loads(response.body.decode())
        assert isinstance(json_body, dict)

        assert isinstance(json_body["DeleteObjectBatch"], dict)
        failed_files = json_body["DeleteObjectBatch"]["failed"]
        assert isinstance(failed_files, list)
        assert len(failed_files) == len(sabotaged_files)
        failed_files_by_name = [ff["name"] for ff in failed_files]
        assert sorted(failed_files_by_name) == sorted(sabotaged_files)

    def test_delete_file_not_exists(self, tempdir_api: str) -> None:
        file_path = f"{tempdir_api}/test_delete_file_not_exists"
        request = Delete(file_path).__enter__()
        assert isinstance(request, Delete)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 204

    def test_delete_file_no_permission(self, tempdir_api: str) -> None:
        file_path_directory = f"{tempdir_api}/test_delete_file_no_perm"
        os.makedirs(file_path_directory, exist_ok=True)
        file_path = f"{file_path_directory}/test_file"
        with open(file_path, "wb") as stream:
            stream.write(b"")
        # add read-only perm on directory (i.e. file-delete not allowed)
        os.chmod(file_path_directory, 0o555)

        request = delete_from_sub_path(file_path)

        assert isinstance(request, Delete)
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 403

    def test_delete_file_error_invalid_target(self) -> None:
        bad_targets: List[Any] = [123, None, {}, [], ()]
        for target in bad_targets:
            with pytest.raises(ValueError) as error:
                Delete(target).__enter__()
            assert str(error.value) == "Target should be either a string or FileBatch"

    @pytest_asyncio
    async def test_delete_dummy_exception(self, tempdir_api: str) -> None:
        # note: target does not need to exist for the test to pass
        target = f"{tempdir_api}/test_delete_dummy"

        request = Delete(target)
        assert isinstance(request, Delete)
        request.object_store, request.file_path = parse_target(target)

        # force assign to mock function to test specific behavior
        request.object_store.delete_file = (  # type: ignore[assignment]
            mock_object_store_delete_file_exception
        )

        await request._parse_request()
        assert isinstance(request.Response, Response)
        assert request.Response.status_code == 500
        assert isinstance(request.Response.error_message, str)
        assert request.Response.error_message == "Dummy Exception Delete"

    @pytest_asyncio
    async def test_delete_storage_target_permission_error(
        self, tempdir_api: str
    ) -> None:
        storage_target = f"{tempdir_api}/test_storage_target"
        request = await Delete(storage_target).__aenter__()
        assert isinstance(request, Delete)
        assert isinstance(request.object_store, ObjectStoreHandler)

        request.object_store.delete_storage_target = (  # type: ignore[assignment]
            mock_object_store_delete_storage_target_permission_error
        )
        await request._delete_storage_target("dummy")
        response = request.Response
        assert isinstance(response, Response)
        assert response.status_code == 207
        assert isinstance(response.error_message, str)
        assert response.error_message == "PermissionError; Access denied"
        assert isinstance(response.body, bytes)
        json_body = json.loads(response.body)
        assert json_body["dummy"]["failed"][0]["name"] == storage_target

    @pytest_asyncio
    async def test_delete_storage_target_unknown_error(self, tempdir_api: str) -> None:
        storage_target = f"{tempdir_api}/test_storage_target"
        request = await Delete(storage_target).__aenter__()
        assert isinstance(request, Delete)
        assert isinstance(request.object_store, ObjectStoreHandler)

        request.object_store.delete_storage_target = (  # type: ignore[assignment]
            mock_object_store_delete_storage_target_unknown_error
        )
        await request._delete_storage_target("dummy")
        response = request.Response
        assert isinstance(response, Response)
        assert response.status_code == 207
        assert isinstance(response.error_message, str)
        assert response.error_message == "UnknownError; Unknown error"
        assert isinstance(response.body, bytes)
        json_body = json.loads(response.body)
        assert json_body["dummy"]["failed"][0]["name"] == storage_target
