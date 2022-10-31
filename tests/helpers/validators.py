import hashlib

from typing import List, AsyncIterator

from lakedrive.core.objects import ByteStream, FileObject, HashTuple


async def validate_read_file(file_object: FileObject, hash: HashTuple) -> None:
    if hash.algorithm == "md5":
        _hash = hashlib.md5()
    else:
        raise Exception("Currently only md5sum supported")

    if file_object.source:
        if not isinstance(file_object.source, ByteStream):
            file_object.source = await file_object.source()
            assert isinstance(file_object.source, ByteStream)
        assert isinstance(file_object.source.stream, AsyncIterator)

        iterator = file_object.source.stream
        async for chunk in iterator:
            _hash.update(chunk)
        if file_object.source.http_client:
            await file_object.source.http_client.__aexit__()
    else:
        # assume empty file
        _hash.update(b"")
    assert _hash.hexdigest() == hash.value


def validate_file_objects_exclude_mtime(
    source_files: List[FileObject], target_files: List[FileObject]
) -> None:
    assert isinstance(source_files, list)
    assert isinstance(target_files, list)
    assert len(source_files) == len(target_files)

    source_files_sorted = sorted(source_files, key=lambda t: t.name)
    target_files_sorted = sorted(target_files, key=lambda t: t.name)

    for idx, fo_1 in enumerate(source_files_sorted):
        fo_2 = target_files_sorted[idx]
        assert fo_1.name == fo_2.name
        assert fo_1.size == fo_2.size
        assert fo_1.hash == fo_2.hash
        assert fo_1.tags == fo_2.tags
