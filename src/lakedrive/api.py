from __future__ import annotations
import os
import re
import stat
import json
import logging

from dataclasses import dataclass
from typing import (
    List,
    Dict,
    Optional,
    Tuple,
    AsyncIterator,
    Any,
    Callable,
    Coroutine,
    Union,
)

from .utils.event_loop import run_on_event_loop
from .core import get_scheme_handler
from .core.sync import synchronise_paths
from .core.handlers import ObjectStoreHandler, SchemeError
from .core.objects import ByteStream, FileObject


logger = logging.getLogger(__name__)


async def async_iterator(data: bytes) -> AsyncIterator[bytes]:
    yield data


def parse_target(target: str) -> Tuple[ObjectStoreHandler, str]:

    storage_target = ""

    if "://" in target:
        # exclude scheme when doing rsplit. Add it back in result.
        scheme, _target = target.split("://")
        storage_target = f'{scheme}://{_target.rsplit("/", 1)[0]}'
    elif target:
        # no scheme given == localfs
        # if target exists, AND is a directory, ensure it ends with "/". This will
        # ensure the target is still handled as a directory despite no "/" added.
        try:
            if stat.S_ISDIR(os.lstat(target).st_mode):
                if target[-1] != "/":
                    target += "/"
        except FileNotFoundError:
            # IF target does not exist (e.g. in case of Put, or repeated Delete,
            # no need to add anything
            pass
        except NotADirectoryError:
            pass
        except PermissionError:
            raise PermissionError(f"Permission denied: '{target}'")

        # regular file or directory on localfs
        relative_path_prefix = re.match("^[./]*/", target)
        if relative_path_prefix:
            # strip of relative path prefix (e.g. "/", "./", "../") before doing
            # rsplit or else risk having empty storage_target (when "/" occurs once).
            # add it back in final result.
            storage_target = (
                relative_path_prefix.group()
                + re.sub("^[./]*/", "", target).rsplit("/", 1)[0]
            )
        else:
            storage_target = target.rsplit("/", 1)[0]
    else:
        # pass silenty at this stage, a SchemeError will be triggered
        # in get_scheme_handler()
        pass

    file_path = re.sub(storage_target, "", target).lstrip("/")

    object_store = get_scheme_handler(storage_target)
    return object_store, file_path


def http_207_response(
    method: str, remaining_files: List[FileObject], error_message: Optional[str] = None
) -> Response:
    body_json = {
        method: {
            "failed": [
                {
                    "name": fo.name,
                    "size": str(fo.size),
                    "mtime": str(fo.mtime),
                }
                for fo in remaining_files
            ]
        }
    }
    return Response(
        method,
        body=json.dumps(body_json).encode(),
        status_code=207,
        error_message=error_message,
    )


async def validate_storage_target(
    method: str, object_store: ObjectStoreHandler
) -> Optional[Response]:
    try:
        await object_store.storage_target_exists(raise_on_not_found=True)

    except FileNotFoundError as error:
        return Response(
            method, status_code=404, error_message=f"'{str(error)}' not exists"
        )
    except PermissionError as error:
        return Response(
            method,
            status_code=403,
            error_message=f"denied access to '{str(error)}'",
        )
    except FileExistsError as error:
        return Response(
            method,
            status_code=400,
            error_message=str(error),
        )

    return None


@dataclass
class FileBatch:
    source: ObjectStoreHandler

    def __len__(self) -> int:
        return len(self.source.object_list)

    def max_read_threads(self) -> int:
        return self.source.max_read_threads

    def list_objects(self) -> List[FileObject]:
        return self.source.object_list

    async def get_batch_reader(self) -> AsyncIterator[List[FileObject]]:
        return self.source.read_batch(self.list_objects())


class Response:
    def __init__(
        self,
        method: str,
        status_code: int,
        error_message: Optional[str] = None,
        body: Optional[bytes] = None,
        file_batch: Optional[FileBatch] = None,
    ) -> None:
        self.method = method
        self.status_code = status_code
        self.error_message = error_message
        # note: body does not contain file-data
        # it is used when Response contains further information
        # e.g. in case of HTTP 207 Multi-Status response
        self.body = body

        self.file_batch = file_batch

        # self._context_manager: bool = False
        self._close_function: Optional[Callable[[], Coroutine[Any, Any, None]]] = None

    def file_count(self) -> int:
        if self.file_batch:
            return len(self.file_batch)
        else:
            return 0

    async def aclose(self) -> None:
        if self._close_function:
            await self._close_function()
        else:
            pass

    async def aread(
        self,
        size: Optional[int] = None,
        seek: Optional[int] = None,
    ) -> bytes:
        if self.method == "GetObject":
            # get file contents
            if not isinstance(self.file_batch, FileBatch) or self.file_count() != 1:
                # assume file not found
                raise FileNotFoundError
            else:
                file_object = self.file_batch.list_objects()[0]

            if isinstance(seek, int):
                await file_object.aseek(seek)  # go to position

            data = await file_object.aread(size=size)
            if (
                isinstance(file_object.source, ByteStream)
                and file_object.source.end_of_stream is False
            ):
                # configure close_function to run when done if not yet defined
                # example: to close http_connection(s)
                # in case end_of_stream is reached, can assume it's already closed
                self._close_function = self._close_function or file_object.close
            else:
                pass

            return data
        elif self.method == "ListObjects":
            # produce list of files in (virtual) directory
            if isinstance(self.file_batch, FileBatch):
                return b"\n".join(
                    fo.name.encode() for fo in self.file_batch.list_objects()
                )
            else:  # no files
                return b""
        elif self.method == "GetObjectBatch":
            logger.warning(f"cant do a read on {self.method}")
            return b""
        else:
            raise ValueError(f"Method not supported: {self.method}")

    async def aseek(self, position: int) -> None:
        if self.method == "GetObject":
            # get file contents
            if not isinstance(self.file_batch, FileBatch) or self.file_count() != 1:
                # assume file not found
                raise FileNotFoundError
            else:
                file_object = self.file_batch.list_objects()[0]
            await file_object.aseek(position)
        else:
            # nothing to do
            pass

    def close(self) -> None:
        run_on_event_loop(self.aclose)

    def read(
        self,
        size: Optional[int] = None,
        seek: Optional[int] = None,
    ) -> bytes:
        data: bytes = run_on_event_loop(self.aread, size=size, seek=seek)
        return data

    def seek(self, position: int) -> None:
        run_on_event_loop(self.aseek, position)


class Request:
    """Parent class to Get, Put, Delete requests"""

    def __init__(self, target: str):
        self.target = target
        self.Response: Optional[Response] = None

        self.object_store: Optional[ObjectStoreHandler] = None
        self.file_path: Optional[str] = None

    def __enter__(self) -> Request:
        request: Request = run_on_event_loop(self.__aenter__)
        return request

    def __exit__(self, *args: Any) -> None:
        if self.Response:
            self.Response.close()
        else:
            pass

    async def __aenter__(self) -> Request:
        try:
            self.object_store, self.file_path = parse_target(self.target)
            await self._parse_request()
        except PermissionError as error:
            self.Response = Response(
                "",
                status_code=403,
                error_message=str(error),
            )
        except SchemeError as error:
            self.Response = Response(
                "",
                status_code=400,
                error_message=str(error),
            )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self.Response:
            await self.Response.aclose()
        else:
            pass

    async def _parse_request(self) -> None:
        """To be overwritten in sibling classes"""
        pass


class Head(Request):
    def __init__(self, target: str, recursive: bool = False):
        super().__init__(target)
        self.recursive = recursive

    async def _parse_request(self) -> None:
        """Parse request and store Response"""
        assert isinstance(self.object_store, ObjectStoreHandler)
        assert isinstance(self.file_path, str)
        if self.file_path:
            # single object
            method = "HeadObject"

            self.Response = await validate_storage_target(method, self.object_store)
            if self.Response:
                return

            try:
                await self.object_store.head_file(self.file_path)
                self.Response = Response(
                    method,
                    status_code=200,
                    file_batch=FileBatch(self.object_store),
                )
            except FileNotFoundError:
                self.Response = Response(
                    method, status_code=404, error_message="FileNotFoundError"
                )
            except PermissionError:
                self.Response = Response(
                    method, status_code=403, error_message="PermissionError"
                )
            except Exception as error:
                self.Response = Response(
                    method, status_code=500, error_message=str(error)
                )

        else:
            # list (virtual/ real) directory contents
            method = "ListObjects"

            self.Response = await validate_storage_target(method, self.object_store)
            if self.Response:
                return

            if self.recursive is True:
                try:
                    await self.object_store.list_contents(include_directories=True)
                    self.Response = Response(
                        method,
                        status_code=200,
                        file_batch=FileBatch(self.object_store),
                    )
                except Exception as error:
                    self.Response = Response(
                        method, status_code=500, error_message=str(error)
                    )
            else:
                self.Response = Response(
                    method,
                    status_code=400,
                    error_message="source is a directory, \
add '-r/--recursive' to run recursively",
                )


class Get(Request):
    def __init__(
        self,
        target: str,
        chunk_size: int = 1024**2 * 16,
        recursive: bool = False,
        filters: List[Dict[str, str]] = [],
    ):
        super().__init__(target)
        self.chunk_size = chunk_size
        self.recursive = recursive
        self.filters = filters

    async def _parse_request(self) -> None:
        """Parse request and store Response"""
        assert isinstance(self.object_store, ObjectStoreHandler)
        assert isinstance(self.file_path, str)
        if self.file_path:
            # get contents of a single file
            method = "GetObject"

            self.Response = await validate_storage_target(method, self.object_store)
            if self.Response:
                return

            try:
                await self.object_store.read_file(
                    FileObject(name=self.file_path),
                    validate=True,
                    chunk_size=self.chunk_size,
                )
                self.Response = Response(
                    method, status_code=200, file_batch=FileBatch(self.object_store)
                )
            except FileNotFoundError:
                self.Response = Response(
                    method, status_code=404, error_message="FileNotFoundError"
                )
            except PermissionError:
                self.Response = Response(
                    method, status_code=403, error_message="PermissionError"
                )
            except Exception as error:
                self.Response = Response(
                    method, status_code=500, error_message=str(error)
                )

        else:
            # get batch of objects
            method = "GetObjectBatch"
            self.Response = await validate_storage_target(method, self.object_store)
            if self.Response:
                return

            if self.recursive is True:
                try:
                    await self.object_store.list_contents(filters=self.filters)
                    self.Response = Response(
                        method,
                        status_code=200,
                        file_batch=FileBatch(self.object_store),
                    )
                except Exception as error:
                    self.Response = Response(
                        method, status_code=500, error_message=str(error)
                    )
            else:
                self.Response = Response(
                    method,
                    status_code=400,
                    error_message="source is a directory, \
add '-r/--recursive' to run recursively",
                )


class Put(Request):
    def __init__(self, target: str, data: Union[bytes, FileObject, FileBatch] = b""):

        if isinstance(data, FileBatch) and len(data) > 1:
            # copying more than 1 object, ensure target is a (virtual) directory path
            if not target or target[-1] != "/":
                target += "/"
            else:
                # no need to modify target
                pass

        super().__init__(target)
        self.data = data

        if target[-1] == "/" and not isinstance(data, FileBatch) and len(self.data) > 0:
            raise Exception("Cant write file to a (virtual) directory")
        else:
            pass

    async def _put_single_object(self, file_object: FileObject) -> None:
        method = "PutObject"

        try:
            assert isinstance(self.object_store, ObjectStoreHandler)
            await self.object_store.write_file(file_object)
            self.Response = Response(method, status_code=201)
        except PermissionError:
            self.Response = Response(
                method,
                status_code=403,
                error_message=f"Permission denied: '{self.target}'",
            )
        except IsADirectoryError as error:
            self.Response = Response(
                method,
                status_code=409,
                error_message=str(error),
            )
        except NotADirectoryError as error:
            self.Response = Response(
                method,
                status_code=409,
                error_message=str(error),
            )

    async def _parse_request(self) -> None:
        """Parse request and store Response"""
        assert isinstance(self.object_store, ObjectStoreHandler)

        if isinstance(self.data, FileBatch):
            if len(self.data) > 1:
                method = "PutObjects"

                file_batch = self.data
                batch_reader = await file_batch.get_batch_reader()

                max_read_threads = min(file_batch.max_read_threads(), len(self.data))
                remaining = await self.object_store.write_batch(
                    batch_reader, max_read_threads=max_read_threads
                )

                if len(remaining) == 0:
                    self.Response = Response(method, status_code=201)
                else:
                    self.Response = http_207_response(method, remaining)

                # done
                return
            elif len(self.data) == 1:
                # single file object should be handled by _put_single_object(),
                # this preserves destination (self.file_path) which may be
                # different than source filename in the event destination is
                # not a (virtual) directory
                self.data = self.data.list_objects()[0]

                # file_path is empty if target is directory, destination (file)name
                # should than be the same as source (file)name
                self.file_path = self.file_path or self.data.name
            else:
                # empty Filebatch -- nothing to do
                self.Response = Response("", status_code=400)
                return

        self.file_path = self.file_path or ""

        if isinstance(self.data, FileObject):
            await self._put_single_object(
                FileObject(
                    name=self.file_path,
                    size=self.data.size,
                    source=self.data.source,
                ),
            )
        elif isinstance(self.data, bytes):
            await self._put_single_object(
                FileObject(
                    name=self.file_path,
                    size=len(self.data),
                    source=ByteStream(stream=async_iterator(self.data)),
                ),
            )
        else:
            raise ValueError("Data should be either FileObject or bytes object")


class Delete(Request):
    def __init__(self, target: Union[str, FileBatch], recursive: bool = False):

        if isinstance(target, str):
            self.delete_file_batch: Optional[FileBatch] = None
        elif isinstance(target, FileBatch):
            self.delete_file_batch = target
            target = f'{target.source.storage_target.rstrip("/")}/'  # actual target
        else:
            raise ValueError("Target should be either a string or FileBatch")
        super().__init__(target)
        self.recursive = recursive

    async def _delete_storage_target(self, method: str) -> None:
        assert isinstance(self.object_store, ObjectStoreHandler)
        try:
            success = await self.object_store.delete_storage_target()
            assert success is True
            self.Response = Response(method, status_code=204)
            return  # successful delete
        except PermissionError as error:
            error_message = f"PermissionError; {str(error)}"
        except Exception as error:
            error_message = f"UnknownError; {str(error)}"

        # failed to delete
        remaining = [FileObject(self.target)]
        self.Response = http_207_response(
            method, remaining, error_message=error_message
        )

    async def _delete_object(self) -> None:
        assert isinstance(self.object_store, ObjectStoreHandler)
        assert isinstance(self.file_path, str)
        method = "DeleteObject"
        self.Response = await validate_storage_target(method, self.object_store)
        if self.Response:
            return

        try:
            await self.object_store.delete_file(FileObject(name=self.file_path))
            self.Response = Response(method, status_code=204)
        except FileNotFoundError:
            # return 204 because the http-call can be considered
            # successful and api-call is idempotent (i.e. no client-error)
            self.Response = Response(method, status_code=204)
        except PermissionError as error:
            self.Response = Response(
                method,
                status_code=403,
                error_message=f"denied access to '{str(error)}'",
            )
        except Exception as error:
            self.Response = Response(method, status_code=500, error_message=str(error))

    async def _delete_object_batch(self) -> None:
        assert isinstance(self.object_store, ObjectStoreHandler)
        method = "DeleteObjectBatch"

        delete_storage_target = False
        if not isinstance(self.delete_file_batch, FileBatch):
            # check if target exists
            response = await ahead(self.target, recursive=self.recursive)

            if response.status_code != 200:
                # forward Response by ahead()
                self.Response = response
                return
            delete_storage_target = True
            self.delete_file_batch = response.file_batch or FileBatch(self.object_store)

        remaining = await self.delete_file_batch.source.delete_batch()
        if len(remaining) == 0:
            if delete_storage_target is True:
                await self._delete_storage_target(method)
                return
            else:
                self.Response = Response(method, status_code=204)
        else:
            self.Response = http_207_response(method, remaining)

    async def _parse_request(self) -> None:
        """Parse request and store Response"""
        if self.file_path:
            await self._delete_object()
        else:
            await self._delete_object_batch()


class Sync:
    def __init__(
        self,
        source: str = "./",
        destination: str = "./",
        params: Dict[str, bool] = {},
    ):
        # default to current directory
        self.source = source or "./"
        self.destination = destination or "./"

        self.params = params
        self.Response: Optional[Response] = None

    def __enter__(self) -> Sync:
        request: Sync = run_on_event_loop(self.__aenter__)
        return request

    def __exit__(self, *args: Any) -> None:
        pass

    async def __aenter__(self) -> Sync:
        try:
            # ensure both source and destination are considered (virtual) directories
            # this will be validated in _parse_request()
            if self.source[-1] != "/":
                self.source += "/"
            if self.destination[-1] != "/":
                self.destination += "/"

            self.source_object_store, _ = parse_target(self.source)
            self.destination_object_store, _ = parse_target(self.destination)

            await self._parse_request()
            assert isinstance(self.Response, Response)
        except SchemeError as error:
            self.Response = Response(
                "",
                status_code=400,
                error_message=str(error),
            )
        except Exception as error:
            # un-expected error
            self.Response = Response(
                "",
                status_code=500,
                error_message=str(error),
            )
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass

    async def _parse_request(self) -> None:
        method = "Sync"

        # validate source
        self.Response = await validate_storage_target(method, self.source_object_store)
        if self.Response:
            return
        # validate destination
        self.Response = await validate_storage_target(
            method, self.destination_object_store
        )
        if self.Response:
            if self.Response.status_code == 404:
                # not found -- try to create it
                # note common Exceptions are caught in __aenter__()
                await self.destination_object_store.create_storage_target()
                # assume destination created succesfully, reset Response
                self.Response = None
            else:
                # return at any other Response
                return

        await synchronise_paths(
            self.source_object_store,
            self.destination_object_store,
            params=self.params,
        )
        self.Response = Response(method, status_code=200)


async def ahead(target: str, recursive: bool = False) -> Response:
    session = await Head(target, recursive=recursive).__aenter__()
    response: Response = session.Response or Response("", status_code=400)
    return response


def head(target: str, recursive: bool = False) -> Response:
    response: Response = run_on_event_loop(ahead, target, recursive=recursive)
    return response


async def aget(
    target: str,
    chunk_size: int = 1024**2 * 16,
    recursive: bool = False,
    get_object: bool = True,
    filters: List[Dict[str, str]] = [],
) -> Response:

    session = await Get(
        target, chunk_size=chunk_size, recursive=recursive, filters=filters
    ).__aenter__()
    response: Response = session.Response or Response("", status_code=400)
    if (
        response.status_code == 200
        and get_object is True
        and response.file_count() == 1
    ):
        # single object - download
        await response.aread()  # ensure file is complete
        await response.aseek(0)  # rewind
    return response


def get(
    target: str,
    chunk_size: int = 1024**2 * 16,
    recursive: bool = False,
    filters: List[Dict[str, str]] = [],
    get_object: bool = True,
) -> Response:

    response: Response = run_on_event_loop(
        aget,
        target,
        chunk_size=chunk_size,
        recursive=recursive,
        filters=filters,
    )
    return response


async def aput(
    target: str, data: Union[bytes, FileObject, FileBatch] = b""
) -> Response:
    session = await Put(target, data=data).__aenter__()
    response: Response = session.Response or Response("", status_code=400)
    return response


def put(target: str, data: Union[bytes, FileObject, FileBatch] = b"") -> Response:
    response: Response = run_on_event_loop(aput, target, data=data)
    return response


async def adelete(target: Union[str, FileBatch], recursive: bool = False) -> Response:
    session = await Delete(target, recursive=recursive).__aenter__()
    response: Response = session.Response or Response("", status_code=400)
    return response


def delete(target: Union[str, FileBatch], recursive: bool = False) -> Response:
    response: Response = run_on_event_loop(adelete, target, recursive=recursive)
    return response


async def async_paths(
    source: str, destination: str, params: Dict[str, bool] = {}
) -> Response:
    session = await Sync(
        source=source, destination=destination, params=params
    ).__aenter__()
    response: Response = session.Response or Response("", status_code=400)
    return response


def sync_paths(source: str, destination: str, params: Dict[str, bool] = {}) -> Response:
    response: Response = run_on_event_loop(
        async_paths, source, destination, params=params
    )
    return response
