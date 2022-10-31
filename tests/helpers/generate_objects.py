from __future__ import annotations
import os
import string
import random
import hashlib

from io import BytesIO
from functools import partial
from dataclasses import dataclass, field
from typing import List, Tuple, Union
from faker import Faker

from lakedrive.core.objects import ByteStream, FileObject, HashTuple

from .async_parsers import buffer_to_aiterator
from .misc import get_random_string, flatten_list


@dataclass
class FilenameConfig:
    base_minlength: int = 1
    base_maxlength: int = 32
    prefix: str = ""
    infix: str = ""
    postfix: str = ".bin"


@dataclass
class GenerateFileConfig:
    """
    control depth via number of levels,
    count is (min,max) number of directories per level
    example:
    directories = [(6, 8), (3, 4), (1, 2)]
    """

    path: str = ""
    directories: List[Tuple[int, int]] = field(default_factory=lambda: [])
    # min, max number of files in final directory
    files_in_dir: Union[int, Tuple[int, int]] = (3, 3)
    # min, max size of file in kbytes
    filesize_kbytes: Union[int, Tuple[int, int]] = (1, 4)
    filetime_mtime: Union[int, Tuple[int, int]] = (0, 1636410888)
    filename_config: FilenameConfig = FilenameConfig()
    tags: bytes = b""


def compute_md5sum(data: bytes) -> str:
    md5_hash = hashlib.md5()
    md5_hash.update(data)
    return md5_hash.hexdigest()


def get_random_md5() -> str:
    valid_chars = string.ascii_lowercase + string.digits
    return "".join(random.choice(valid_chars) for i in range(32))


def get_strings(
    output_range: int, minlength: int = 1, maxlength: int = 32
) -> List[str]:
    return [
        get_random_string(random.randint(minlength, maxlength))
        for _ in range(output_range)
    ]


def _generate_file_paths(
    directories: List[Tuple[int, int]],
    files_in_dir: Union[int, Tuple[int, int]],
    path: str,
    fn_config: FilenameConfig,
) -> List[str]:
    # for level in configuration:
    if len(directories) > 0:
        # not final directory
        random_strings = get_strings(
            random.randint(*directories[0]),
            minlength=1,
            maxlength=16,
        )
        return flatten_list(
            [
                _generate_file_paths(
                    directories[1:], files_in_dir, f"{path}/{name}", fn_config
                )
                for name in random_strings
            ]
        )
    else:
        # directory containing files
        if isinstance(files_in_dir, tuple):
            strings_needed = random.randint(*files_in_dir)
        else:
            strings_needed = files_in_dir

        if path:
            path_prefix = f"{path.strip('/')}/"
        else:
            path_prefix = ""

        random_strings = get_strings(
            strings_needed,
            minlength=fn_config.base_minlength,
            maxlength=fn_config.base_maxlength,
        )
        return [
            f"{path_prefix}{fn_config.prefix}{name}{fn_config.infix}{fn_config.postfix}"
            for name in random_strings
        ]


def generate_file_paths_random(count: int = 100) -> List[str]:
    Faker.seed(0)
    return [
        Faker().file_path(depth=(count % 5 + 1), absolute=False) for _ in range(count)
    ]


def generate_file_paths(
    file_config: GenerateFileConfig,
) -> List[str]:
    """Wrapper around _generate_file_paths. Ensures paths are unique by parsing
    results through set(). Cant do this in the recursive version because there is
    no global view in recursive calls. Note, same names are allowed to exist on
    different paths"""
    if file_config.files_in_dir:
        files_in_dir = file_config.files_in_dir
    else:
        files_in_dir = (3, 3)
    fn_config = file_config.filename_config
    return list(
        set(
            _generate_file_paths(
                file_config.directories,
                files_in_dir,
                file_config.path,
                fn_config,
            )
        )
    )


def random_bytes(filesize_kbytes: Union[Tuple[int, int], int]) -> int:
    if isinstance(filesize_kbytes, int):
        return filesize_kbytes * 1024
    bytes_min, bytes_max = filesize_kbytes
    return random.randint(bytes_min * 1024, bytes_max * 1024)


def random_mtime(filetime_mtime: Union[Tuple[int, int], int]) -> int:
    if isinstance(filetime_mtime, int):
        return filetime_mtime
    return random.randint(*filetime_mtime)


def generate_file_objects(
    file_paths: List[str],
    filesize_kbytes: Union[Tuple[int, int], int] = 0,
    filetime_mtime: Union[Tuple[int, int], int] = (0, 999999999),
    tags: bytes = b"",
    checksum: bool = True,
) -> List[FileObject]:
    """Generate a list of FileObjects, with a randomized name,size,mtime and hash,
    to be used as input for tests"""
    return list(
        [
            FileObject(
                name=fp,
                size=random_bytes(filesize_kbytes),
                mtime=random_mtime(filetime_mtime),
                hash=HashTuple(algorithm="md5", value=get_random_md5())
                if checksum
                else None,
                tags=tags,
            )
            for fp in file_paths
        ]
    )


def generate_data_object(length: int) -> Tuple[str, BytesIO]:
    data = os.urandom(length)
    md5sum = compute_md5sum(data)
    return md5sum, BytesIO(data)


async def buffer_bytestream(buffer: BytesIO, chunk_size_kb: int) -> ByteStream:
    return ByteStream(
        stream=buffer_to_aiterator(buffer, chunk_size_kb=chunk_size_kb),
        chunk_size=(chunk_size_kb * 1024),
    )


def mock_file_object(file_object: FileObject, chunk_size_kb: int = 128) -> FileObject:

    buffer = BytesIO(os.urandom(file_object.size))
    md5sum = compute_md5sum(buffer.getbuffer())

    file_object.hash = HashTuple(algorithm="md5", value=md5sum)
    file_object.source = partial(buffer_bytestream, buffer, chunk_size_kb)
    return file_object


def mock_file_objects(
    count: int = 1,
    chunk_size_kb: int = 128,
    minsize_kb: int = 1,
    maxsize_kb: int = 1024,
    mtime: int = 0,
) -> List[FileObject]:

    file_objects = generate_file_objects(
        generate_file_paths_random(count=count),
        filesize_kbytes=(minsize_kb, maxsize_kb),
        filetime_mtime=mtime,
        checksum=False,
    )
    return [mock_file_object(fo, chunk_size_kb=chunk_size_kb) for fo in file_objects]


class MockFileObjects:
    def __init__(
        self,
    ) -> None:
        self.file_count: int = 0
        self.file_size_kb: int = 0
        self.read_chunk_size_kb: int = 0
        self.data_objects: Tuple[bytes, ...] = ()

    def initialize(
        self,
        file_count: int = 1000,
        file_size_kb: int = 1,
        read_chunk_size_kb: int = 1,
    ) -> MockFileObjects:

        self.file_count = file_count
        self.file_size_kb = file_size_kb
        self.read_chunk_size_kb = read_chunk_size_kb
        self.data_objects = tuple(
            os.urandom(self.file_size_kb * 1024) for _ in range(self.file_count)
        )
        return self

    def file_objects(
        self,
        file_count: int = -1,
        file_mtime: int = 0,
        tags: bytes = b"",
    ) -> List[FileObject]:
        if file_count > self.file_count:
            raise ValueError(f"Insufficient files available ({self.file_count})")
        if file_count < 0:
            file_count = self.file_count  # use default
        return [
            FileObject(
                name=f"{str(j)}.bin",
                size=self.file_size_kb * 1024,
                mtime=file_mtime,
                hash=None,
                tags=tags,
                source=ByteStream(
                    stream=buffer_to_aiterator(
                        BytesIO(self.data_objects[j]),
                        chunk_size_kb=self.read_chunk_size_kb,
                    ),
                    chunk_size=(self.read_chunk_size_kb * 1024),
                ),
            )
            for j in range(file_count)
        ]
