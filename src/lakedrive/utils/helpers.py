import hashlib

from io import BytesIO
from datetime import datetime

from typing import List, Iterator, AsyncIterator, Any


def compute_md5sum(file_bytes: bytes) -> str:
    """Compute md5sum from array of bytes"""
    return hashlib.md5(file_bytes).hexdigest()


def file_md5sum(filepath: str) -> str:
    md5_hash = hashlib.md5()
    with open(filepath, "rb") as stream:
        md5_hash.update(stream.read())
    return md5_hash.hexdigest()


def splitlist(input_list: List[Any], no_batches: int) -> Iterator[Any]:
    k, m = divmod(len(input_list), no_batches)
    return (
        input_list[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)]
        for i in range(min(len(input_list), no_batches))
    )


async def aiterator_bytes_io(data: BytesIO, chunk_size: int) -> AsyncIterator[bytes]:
    # if chunk_size == 0, change to -1 to let BytesIO decide proper size
    chunk_size = chunk_size or -1
    while True:
        chunk = data.read(chunk_size)
        if len(chunk) == 0:
            break
        yield chunk


async def chunklist_aiter(input_list: List[Any], no_batches: int) -> AsyncIterator[Any]:
    """Iterate over list async"""
    k, m = divmod(len(input_list), no_batches)
    for batch_no in range(min(len(input_list), no_batches)):
        batch = input_list[
            batch_no * k + min(batch_no, m) : (batch_no + 1) * k + min(batch_no + 1, m)
        ]
        yield batch


def split_in_chunks(input_list: List[Any], length: int) -> List[Any]:
    """Split input_list into new lists of max length"""
    return [input_list[i : i + length] for i in range(0, len(input_list), length)]


def utc_offset() -> int:
    """Calculate offset in seconds to UTC time
    positive means ahead of UTC time, negative means behind UTC time"""
    return round((datetime.now() - datetime.utcnow()).total_seconds())


def path_filter_overlap(paths: List[str]) -> List[str]:
    """Filter out overlapping paths from a list of paths"""
    # sort based on "/" length -- strip off "/" at end
    paths_sorted = sorted(
        [p.rstrip("/") if p[-1] == "/" else p for p in paths],
        key=lambda p: p.count("/"),
    )
    paths_filtered = []

    for p in paths_sorted:
        if p in paths_filtered:
            continue
        gp = p.rsplit("/", 1)[0]
        if gp in paths_filtered:
            continue
        paths_filtered.append(p)
    return paths_filtered
