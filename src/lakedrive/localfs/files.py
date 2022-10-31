from __future__ import annotations

import os
import sys
import time
import stat
import logging
import asyncio

from typing import AsyncIterator, List, Tuple

from ..core.objects import FileObject, HashTuple
from ..utils.helpers import utc_offset, file_md5sum, path_filter_overlap


logger = logging.getLogger(__name__)


def md5_hash_tuple(filepath: str) -> HashTuple:
    return HashTuple(algorithm="md5", value=file_md5sum(filepath))


def remove_file_if_exists(filepath: str) -> None:
    try:
        os.unlink(filepath)
    except FileNotFoundError:
        pass


def list_files(
    directory: str,
    relpath: str = "",
    checksum: bool = False,
    skip_hidden: bool = False,
    recursive: bool = True,
    include_directories: bool = False,
    raise_on_permission_error: bool = False,
) -> List[FileObject]:
    """Return list files_objects from a from a directory, and all sub-directories
    that are found within it (recursive search).

    Optional:
    - calculate checksum (md5)
    - skip hidden files and directories (starting with ".")"""
    if not os.path.exists(directory):
        return []

    if relpath and relpath[-1] != "/":
        # ensure relpath ends with "/" because it should be a directory
        relpath += "/"

    skip_mark = "." if skip_hidden is True else ""

    try:
        files_in_directory = os.listdir(directory)
    except PermissionError as error:
        if raise_on_permission_error is True:
            raise PermissionError(error)
        # else - log error only
        error_message = f"lakedrive: list failed: {error}\n"
        sys.stderr.write(error_message)
        return []

    # get information about files (dont follow symlinks)
    files = {
        name: os.lstat("/".join([directory, name]))
        for name in files_in_directory
        if name[0] != skip_mark
    }

    # st_mtime is based on system time, deduct offset to get UTC time
    _utc_offset = utc_offset()

    file_objects: List[FileObject] = []
    for name, st in files.items():
        relpath_name = "".join(filter(None, [relpath, name]))

        if stat.S_ISREG(st.st_mode):
            file_objects.append(
                FileObject(
                    name=relpath_name,
                    size=int(st.st_size),
                    mtime=int(st.st_mtime) - _utc_offset,
                    hash=(
                        lambda: md5_hash_tuple("/".join([directory, name]))
                        if checksum
                        else None
                    )(),
                    tags=b"",
                )
            )
            # done
            continue

        # only list ISDIR and ISREG
        # skip symlinks or any other filetype
        if not stat.S_ISDIR(st.st_mode):
            continue

        # path is a directory
        if include_directories is True or recursive is False:
            # add directory as file object so subsequent queries
            # can traverse into it
            file_objects.append(
                FileObject(
                    name=f"{relpath_name}/",
                    size=0,
                    mtime=int(st.st_mtime),
                    hash=None,
                    tags=b"",
                )
            )
        else:
            pass

        if recursive is True:
            new_directory = "/".join([directory, name])
            file_objects += list_files(
                new_directory,
                relpath=relpath_name,
                checksum=checksum,
                skip_hidden=skip_hidden,
                recursive=recursive,
                include_directories=include_directories,
                raise_on_permission_error=raise_on_permission_error,
            )
    return file_objects


def directory_list(
    storage_path: str,
    checksum: bool = False,
    skip_hidden: bool = False,
    prefixes: List[str] = [],
    recursive: bool = True,
    include_directories: bool = False,
    raise_on_permission_error: bool = False,
) -> List[FileObject]:

    if prefixes:
        prefix_paths = [
            (f"{storage_path}{prefix}", prefix)
            for prefix in path_filter_overlap(prefixes)
        ]
    else:
        prefix_paths = [(storage_path, "")]

    object_list = []

    for directory, relpath in prefix_paths:
        object_list += list_files(
            directory.rstrip("/"),
            relpath=relpath,
            checksum=checksum,
            skip_hidden=skip_hidden,
            recursive=recursive,
            include_directories=include_directories,
            raise_on_permission_error=raise_on_permission_error,
        )
    return object_list


def create_directories(
    storage_path: str,
    file_objects: List[FileObject],
) -> Tuple[List[str], str]:
    # extract directories from filenames
    directories_from_files = list(
        set(
            [
                f"{storage_path}{os.path.dirname(fo.name)}/"
                if fo.name[-1] != "/"
                else f"{storage_path}{fo.name}"
                for fo in file_objects
            ]
        )
    )

    # create (empty) directories -- must be in hierarchical order
    directories_sorted = sorted(directories_from_files, reverse=True)
    last_leaf = ""
    for directory in directories_sorted:
        # skip if directory is part of last created leaf
        # i.e. dont create "abc/aaa" if "/abc/aaa/aaa" is created
        if directory in last_leaf:
            continue
        os.makedirs(directory, exist_ok=True)
        last_leaf = directory

    return directories_sorted, last_leaf


async def put_file(
    storage_path: str,
    file_object: FileObject,
) -> None:
    """Read incoming chunks (async stream), and write the chunks to a file."""

    file_path = f"{storage_path}{file_object.name}"

    if os.path.isdir(file_path):
        # cant write a file if a directory exists
        error_msg = f"path '{file_path.rstrip('/')}' taken by a directory"
        raise IsADirectoryError(error_msg)
    else:
        try:
            parent_directory = os.path.dirname(file_path)
            if os.path.exists(parent_directory) and not os.path.isdir(parent_directory):
                error_msg = f"path '{parent_directory}' taken by non-directory"
                raise NotADirectoryError(error_msg)
            fo = open(file_path, "wb")
        except FileNotFoundError:
            # ensure directory is created, and retry
            os.makedirs(os.path.dirname(file_path))
            fo = open(file_path, "wb")

    if callable(file_object.source):
        file_object.source = await file_object.source()

    byte_stream = await file_object.byte_stream()
    if byte_stream:
        aiterator, _ = byte_stream
        async for chunk in aiterator:
            fo.write(chunk)
    else:
        # assume empty file
        fo.write(b"")
    fo.close()

    if file_object.mtime >= 0:
        os.utime(file_path, (int(time.time()), file_object.mtime))


async def localfs_batch_put(
    storage_target: str,
    batch: List[FileObject],
) -> List[FileObject]:
    """Write a batch of stream_objects. Return a list of items that
    have failed, or empy list if none failed."""
    responses = await asyncio.gather(
        *[
            put_file(
                storage_target,
                file_object,
            )
            for file_object in batch
            if file_object.name[-1] != "/"
        ],
        return_exceptions=True,
    )
    items_failed = []
    for item_no, error in enumerate(responses):
        if error is None:
            continue
        # else: failed to write item at target
        items_failed.append(batch[item_no])
    return items_failed


async def put_file_batched(
    storage_path: str,
    # stream_objects_batched: AsyncIterator[List[StreamObject]],
    file_objects_batched: AsyncIterator[List[FileObject]],
) -> Tuple[List[FileObject], List[FileObject]]:
    """Put files on localfs batch by batch.
    Return list of FileObject (derived from StreamObject) that have failed"""

    file_objects_total: List[FileObject] = []
    file_objects_remaining: List[FileObject] = []

    async for batch in file_objects_batched:
        create_directories(
            storage_path,
            batch,
        )
        file_objects_total += batch
        items_remaining = await localfs_batch_put(storage_path, batch)
        if items_remaining:
            file_objects_remaining += items_remaining

    return file_objects_total, file_objects_remaining
