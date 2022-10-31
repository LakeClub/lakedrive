import pytest

from typing import AsyncIterator, Coroutine

from lakedrive.core.objects import FileObject, ByteStream
from lakedrive.httplibs.request import HttpRequest
from lakedrive.httplibs.objects import HttpResponse, HttpResponseError
from lakedrive.s3.handler import S3Handler
from lakedrive.s3.files import (
    parse_head_response_headers,
    file_exists,
    put_file,
    get_file,
)

from ..helpers.object_store import clear_object_store
from ..helpers.async_parsers import pytest_asyncio


def test_parse_head_response_headers() -> None:
    http_headers = {"foo": "bar"}
    assert (
        parse_head_response_headers(
            HttpResponse(status_code="200", headers=http_headers)
        )
        == http_headers
    )
    with pytest.raises(FileNotFoundError):
        parse_head_response_headers(HttpResponse(status_code="404"))
    with pytest.raises(PermissionError):
        parse_head_response_headers(HttpResponse(status_code="403"))
    with pytest.raises(HttpResponseError) as error:
        parse_head_response_headers(HttpResponse(status_code="500"))
    assert isinstance(error.value, HttpResponseError)


class TestS3Files:
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(self, object_store_s3_files: S3Handler) -> None:
        self.object_store = object_store_s3_files
        self.original_credentials = self.object_store.bucket.s3_credentials
        self.fake_credentials = {
            "access_id": "FAKE_ACCESS_ID",
            "secret_key": "FAKE_SECRET_KEY",
        }
        self.testfiles = [("lorum.txt", b"ipsum")]

    def set_credentials(self, fake: bool = False) -> None:
        if fake is True:
            self.object_store.bucket.s3_credentials = self.fake_credentials
        else:
            self.object_store.bucket.s3_credentials = self.original_credentials

    @pytest_asyncio
    async def test_put_file(self) -> None:
        filename, filedata = self.testfiles[0]
        await put_file(self.object_store.bucket, filename, filedata)

    @pytest_asyncio
    async def test_get_file(self) -> None:
        filename, filedata_written = self.testfiles[0]

        file_object_in = FileObject(name=filename, size=len(filedata_written))
        file_object_out = await get_file(self.object_store.bucket, file_object_in)
        assert isinstance(file_object_out, FileObject)

        assert file_object_out.source is not None
        assert not isinstance(file_object_out.source, ByteStream)

        file_object_out.source = await file_object_out.source()
        assert isinstance(file_object_out.source, ByteStream)
        assert isinstance(file_object_out.source.stream, AsyncIterator)
        filedata_read = b"".join(
            [chunk async for chunk in file_object_out.source.stream]
        )
        assert isinstance(file_object_out.source.http_client, HttpRequest)
        await file_object_out.source.http_client.__aexit__()
        assert filedata_read == filedata_written

    @pytest_asyncio
    async def test_put_file_nopermission(self) -> None:
        self.set_credentials(fake=True)
        filename, filedata = self.testfiles[0]
        s3_path = f"s3://{self.object_store.bucket.name}/{filename}"

        with pytest.raises(HttpResponseError) as error:
            await put_file(self.object_store.bucket, filename, filedata)
        assert str(error.value) == f"cant write to '{s3_path}' (403)"

        self.set_credentials(fake=False)

    @pytest_asyncio
    async def test_read_file_nopermission(self) -> None:
        self.set_credentials(fake=True)
        filename, _ = self.testfiles[0]
        file_object = await get_file(
            self.object_store.bucket, FileObject(name=filename)
        )
        with pytest.raises(PermissionError) as error:
            response = file_object.read()
            assert isinstance(response, Coroutine)
            await response
        assert str(error.value) == filename
        self.set_credentials(fake=False)

    @pytest_asyncio
    async def test_head_file_nopermission(self) -> None:
        self.set_credentials(fake=True)
        filename, _ = self.testfiles[0]
        with pytest.raises(HttpResponseError) as error:
            await file_exists(self.object_store.bucket, filename)
        assert (
            str(error.value)
            == f"cant access 's3://{self.object_store.bucket.name}/{filename}' (403)"
        )
        self.set_credentials(fake=False)

    # cleanup after tests
    @pytest_asyncio
    async def test_clear_object_store(self) -> None:
        await clear_object_store(self.object_store, delete_target=True)
