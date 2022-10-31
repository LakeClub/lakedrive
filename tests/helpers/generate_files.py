import os
import time
import logging
import zipfile

from io import BytesIO
from shutil import ReadError
from datetime import datetime
from typing import cast, Tuple, List, Dict, AsyncIterator

from lakedrive.core.handlers import ObjectStoreHandler
from lakedrive.utils.helpers import split_in_chunks
from lakedrive.core.handlers import StorageSupportFlags
from lakedrive.core.objects import (
    ByteStream,
    FileObject,
    HashTuple,
)

from tests.helpers.generate_objects import (
    generate_file_paths,
    generate_file_objects,
    compute_md5sum,
    GenerateFileConfig,
)

logger = logging.getLogger(__name__)


def epochtime_to_zipfile_datetime(
    epoch_time: int,
) -> Tuple[int, int, int, int, int, int]:
    year, *tt = tuple(datetime.fromtimestamp(epoch_time).timetuple())[0:6]
    # zipfile does not support dates <1980
    if year < 1980:
        year = 1980
    # cast to fixed size int tuple to comply with typing
    return cast(Tuple[int, int, int, int, int, int], tuple(x for x in [year] + tt))


def generate_mock_file_configs() -> List[GenerateFileConfig]:
    file_configs: List[GenerateFileConfig] = []
    file_configs.append(
        GenerateFileConfig(
            directories=[(3, 3), (3, 3), (2, 2)],
            path="subpaths",
            files_in_dir=3,
            filesize_kbytes=(0, 100),
        )
    )
    file_configs.append(
        GenerateFileConfig(
            directories=[],
            path="1_4Mfiles",
            files_in_dir=3,
            filesize_kbytes=(1000, 4000),
        )
    )
    file_configs.append(
        GenerateFileConfig(
            directories=[], path="1_1Mfiles", files_in_dir=1, filesize_kbytes=1024
        )
    )
    return file_configs


def flatten_fo_list(nested_list: List[List[FileObject]]) -> List[FileObject]:
    """Flatten a list of lists in to one (un-nested) list"""
    return [item for sublist in nested_list for item in sublist]


def generate_file_objects_random(
    file_configs: List[GenerateFileConfig],
) -> List[FileObject]:
    return flatten_fo_list(
        [
            generate_file_objects(
                generate_file_paths(file_config),
                file_config.filesize_kbytes,
                filetime_mtime=file_config.filetime_mtime,
                tags=file_config.tags,
                checksum=False,
            )
            for file_config in file_configs
        ]
    )


def _fo_samelength_diff_prefix() -> List[FileObject]:
    """test same length but different prefix cut to test directory
    based regex searches"""
    file_paths = [
        "abc/aaa/aaa/aaa/aaa.txt",
        "abc/aaa/aaa/aaaaaaa.txt",
        "abc/aaa/aaaaaaaaaaa.txt",
        "abc/aaaaaaa/a/aaa/a.txt",
        "abc/aaaaaaa/a/aaaaa.txt",
        "abc/aaaaaaa/aaaaaaa.txt",
        "abc/aaaaa/aa/a/aaaa.txt",
        "abc/aaaaa/aa/aaaaaa.txt",
        "abc/aaaaa/aaaaaaaaa.txt",
        "abc/aa/aaa/aaaa/aaa.txt",
        "abc/aa/aaa/aaa/a/aa.txt",
        "abc/aa/aaaa/a/aaaaa.txt",
    ]
    return generate_file_objects(file_paths, 1, checksum=False)


def _fo_path_overlap() -> List[FileObject]:
    """test files in different paths with overlapping path"""
    file_paths = [
        "xyz/sub_a/sub_b/file.txt",
        "xyz/sub_a/file.txt",
        "xyz/sub_a/sub_b/sub_c/file.txt",
        "xyz/file.txt",
    ]
    return generate_file_objects(file_paths, 1, checksum=False)


def _fo_no_mtime() -> List[FileObject]:
    """File objects with mtime disabled"""
    return generate_file_objects(
        [f"no_mtimes/{i}.bin" for i in range(3)],
        checksum=False,
        filesize_kbytes=0,
        filetime_mtime=-1,
    )


def generate_file_objects_custom() -> List[FileObject]:
    """Custom file_paths to capture specific edge-cases"""
    file_objects: List[FileObject] = []
    generate_functions = [
        _fo_samelength_diff_prefix,
        _fo_no_mtime,
        _fo_path_overlap,
    ]
    for func in generate_functions:
        file_objects += func()
    return file_objects


def generate_file_objects_invalid() -> List[FileObject]:
    """Corrupt files to test if read Exceptions are triggered correctly"""

    corrupt_small = generate_file_objects(
        [
            "abc/def/corrupt-small.txt",
        ],
        1,
        checksum=False,
    )
    corrupt_large = generate_file_objects(
        [
            "abc/def/corrupt-large.txt",
        ],
        1024 * 8,
        checksum=False,
    )
    return corrupt_small + corrupt_large


def generate_file_objects_directories() -> List[FileObject]:

    mtime = int(time.time()) - 86400
    object_list = [
        FileObject(
            name=name,
            hash=None,
            size=0,
            mtime=mtime,
            tags=b"",
        )
        for name in ["empty_abc/", "empty_def/", "empty_ghi/"]
    ]
    return object_list


def generate_file_archive(file_objects: List[FileObject]) -> Tuple[int, BytesIO]:
    """Create an in-memory zip-archive from a directory on disk
    return size and bytesio obj"""
    buffer = BytesIO()

    # read every file from disk, and write to zipfile (in-memory)
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_STORED) as zf:
        for file_object in file_objects:
            file_size = file_object.size
            data = os.urandom(file_size)
            md5sum = compute_md5sum(data)

            zipinfo = zipfile.ZipInfo()
            zipinfo.filename = file_object.name
            zipinfo.compress_type = zipfile.ZIP_STORED
            zipinfo.comment = md5sum.encode("utf-8")

            if file_object.mtime >= 0:
                zipinfo.date_time = epochtime_to_zipfile_datetime(file_object.mtime)
            zf.writestr(zipinfo, data)

    # last position is equal to size in bytes
    size_in_bytes = buffer.tell()

    # rewind before returning
    buffer.seek(0)
    return (size_in_bytes, buffer)


def filter_by_prefix(
    filepaths: List[str], prefix: str, recursive: bool = True
) -> List[str]:
    # get filepaths excluding prefix
    # if separator == "/", prefix is a (virtual) parent directory and filepaths
    # are absolute paths under it pointing to files (objects)
    if prefix:
        filepaths = [
            name.split(prefix, 1)[1] for name in filepaths if name.startswith(prefix)
        ]
    else:
        # if no prefix -- keep filepaths as-is
        pass

    if recursive is False:
        # only show (absolute) paths of depth 1

        # filter out directories and add "/" back to remember path is a directory
        # parse through set() to get unique dirs. E.g. abc/1.txt, abc/2.txt should
        # give one single "abc/" entry
        directories = list(
            set([f'{fp.split("/")[0]}/' for fp in filepaths if "/" in fp])
        )
        regfiles = [fp for fp in filepaths if "/" not in fp]
        # merge back in to a single list
        filepaths = regfiles + directories
    else:
        # pass filepaths as-is
        # note: (virtual) directory paths are optional, and only useful if
        # recursive is set to False (to know which prefixes to query in
        # subsequent calls)
        pass

    return filepaths


class MockFileHandler(ObjectStoreHandler):
    """Class used as a test source_handler to list and read file objects"""

    def __init__(
        self,
        storage_target: str,
        stream_chunk_size: int = 1024 * 128,
        max_read_threads: int = 160,
        max_write_threads: int = 8,
    ):
        self.storage_target = storage_target
        self.stream_chunk_size = stream_chunk_size
        self.max_read_threads = max_read_threads
        self.max_write_threads = max_write_threads
        self.object_list = []

        self.support = StorageSupportFlags()
        # preset to empty archive to allow checking if filled
        self.zip_archive: BytesIO = BytesIO()
        self.fo_directories = []

        # when set to true, Exceptions are thrown at file reads
        self.corrupt_archive = False

        if storage_target == "files_random":
            _, self.zip_archive = generate_file_archive(
                generate_file_objects_random(generate_mock_file_configs())
                + generate_file_objects_custom()
            )
        elif storage_target == "files_corrupt":
            _, self.zip_archive = generate_file_archive(generate_file_objects_invalid())
            # pretend archive is corrupted
            self.corrupt_archive = True
        elif storage_target == "directories_empty":
            self.fo_directories = generate_file_objects_directories()
        else:
            raise Exception(f"Cant generate target:{storage_target}")

    async def _read_file(self, filepath: str) -> AsyncIterator[bytes]:
        """Read file in chunks of fixed size, until all parts are read"""
        with zipfile.ZipFile(self.zip_archive, mode="r") as zf:
            buf = BytesIO(zf.read(filepath))
            while True:
                chunk = buf.read(self.stream_chunk_size)
                if chunk:
                    if self.corrupt_archive is True:
                        raise ReadError("failure to read corrupt archive")
                    yield chunk
                else:
                    break

    def _list_zip_archive(
        self,
        checksum: bool = False,
        skip_hidden: bool = False,
        prefixes: List[str] = [],
        recursive: bool = True,
    ) -> List[FileObject]:
        object_list = []

        with zipfile.ZipFile(self.zip_archive, mode="r") as zf:
            filenames = [
                name
                for name in zf.namelist()
                if skip_hidden is False or (name[0] != "." and "/." not in name)
            ]
            if not prefixes:
                prefixes = [""]

            for prefix in prefixes:
                filenames = filter_by_prefix(filenames, prefix, recursive=recursive)

            # zip_objects are for regfiles only -- i.e. skip "directory" paths
            zip_objects = [zf.getinfo(name) for name in filenames if name[-1] != "/"]

            # add regular files
            object_list = [
                FileObject(
                    name=zo.filename,
                    hash=(
                        HashTuple(algorithm="md5", value=zo.comment.decode())
                        if checksum
                        else None
                    ),
                    size=zo.file_size,
                    mtime=int(datetime(*zo.date_time).timestamp()),
                    tags=b"",
                )
                for zo in zip_objects
            ]

            # add virtual directories
            object_list += [
                FileObject(
                    name=name,
                    hash=None,
                    size=0,
                    mtime=0,
                    tags=b"",
                )
                for name in filenames
                if name[-1] == "/"
            ]
        return object_list

    async def list_contents(
        self,
        checksum: bool = False,
        skip_hidden: bool = False,
        prefixes: List[str] = [],
        recursive: bool = True,
        filters: List[Dict[str, str]] = [],
        include_directories: bool = False,
        fail_on_permission_error: bool = False,
    ) -> List[FileObject]:
        """Get list of files that are contained in location.

        Optional:
          checksum: add checksum per file
          skip_hidden: skips files or directories starting with "."

        return as list of FileObjects"""
        if self.zip_archive.getbuffer().nbytes > 0:
            self.object_list = self._list_zip_archive(
                checksum=checksum,
                skip_hidden=skip_hidden,
                prefixes=prefixes,
                recursive=recursive,
            )
        else:
            self.object_list = []

        if self.fo_directories:
            self.object_list += self.fo_directories

        if filters:
            return self.filter_list(filters)
        return self.object_list

    async def read_batch(
        self,
        file_objects: List[FileObject],
        stream_chunk_size: int = (1024 * 256),
        threads_per_worker: int = 0,
    ) -> AsyncIterator[List[FileObject]]:
        """Divided list of file objects in smaller fixed-size batches,
        every time this function is called a new batch of files are returned"""
        if threads_per_worker < 1:
            threads_per_worker = self.max_read_threads

        self.stream_chunk_size = stream_chunk_size
        # split in batches
        batches: List[List[FileObject]] = split_in_chunks(
            file_objects, threads_per_worker
        )

        for batch in batches:
            for file_object in batch:
                if file_object.size > 0:
                    file_object.source = ByteStream(
                        stream=self._read_file(file_object.name),
                        chunk_size=stream_chunk_size,
                    )
                else:
                    file_object.source = None
            yield batch
