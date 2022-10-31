import os
import pytest

from lakedrive.localfs.handler import LocalFileHandler


TEMPDIR_SYNC = "/tmp/lakedrive-tests/sync"


@pytest.fixture(scope="class", autouse=True)  # type: ignore[misc]
def sync_source_handler(directory: str = "source") -> LocalFileHandler:
    os.makedirs(TEMPDIR_SYNC, exist_ok=True)
    assert os.path.exists(TEMPDIR_SYNC) is True
    return LocalFileHandler("/".join([TEMPDIR_SYNC, directory]))


@pytest.fixture(scope="class", autouse=True)  # type: ignore[misc]
def sync_target_handler(directory: str = "target") -> LocalFileHandler:
    os.makedirs(TEMPDIR_SYNC, exist_ok=True)
    assert os.path.exists(TEMPDIR_SYNC) is True
    return LocalFileHandler("/".join([TEMPDIR_SYNC, directory]))
