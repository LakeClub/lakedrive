import os
import re

from typing import List, Tuple


def cleanup_tempdir_localfs(tempdir: str) -> None:
    for filename in os.listdir(tempdir):
        file_path = f"{tempdir}/{filename}"
        if not re.match("^test[-_\\.]", filename):
            continue
        try:
            os.unlink(file_path)
        except IsADirectoryError:
            os.chmod(file_path, 0o700)
            cleanup_tempdir_localfs(file_path)
    os.rmdir(tempdir)


def create_files_localfs(
    source_dir: str,
    prefix: str,
    file_count: int,
) -> List[Tuple[str, int]]:
    """Create a set of test-files on local filesystem"""
    os.makedirs(source_dir, exist_ok=True)
    filepaths = [f"{source_dir}/test_{prefix}{str(idx)}" for idx in range(file_count)]
    files: List[Tuple[str, int]] = []  # list of (file_path, file_size)

    for idx, file_path in enumerate(filepaths):
        with open(file_path, "wb") as stream:
            file_size = stream.write(f"file {os.path.basename(file_path)}".encode())
            files.append((file_path, file_size))
    return files
