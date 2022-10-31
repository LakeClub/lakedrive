import os
import pytest

from typing import Iterator

from ..templates.cli_methods import (
    _TestCLICopy,
    _TestCLISync,
    _TestCLIFind,
    _TestCLIDelete,
)
from .test_localfs_handler import TEMPDIR_LOCALFS

from .helpers import cleanup_tempdir_localfs, create_files_localfs


TEMPDIR_LOCALFS_CLI = f"{TEMPDIR_LOCALFS}/cli"


@pytest.fixture(scope="session", autouse=True)  # type: ignore[misc]
def tempdir_localfs_cli_source() -> Iterator[str]:
    tempdir = f"{TEMPDIR_LOCALFS_CLI}/source"
    os.makedirs(tempdir, exist_ok=True)
    assert os.path.exists(tempdir) is True
    yield tempdir
    cleanup_tempdir_localfs(tempdir)


@pytest.fixture(scope="session", autouse=True)  # type: ignore[misc]
def tempdir_localfs_cli_destination() -> Iterator[str]:
    tempdir = f"{TEMPDIR_LOCALFS_CLI}/destination"
    os.makedirs(tempdir, exist_ok=True)
    assert os.path.exists(tempdir) is True
    yield tempdir
    cleanup_tempdir_localfs(tempdir)


class TestLocalFsCLICopy(_TestCLICopy):
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
        tempdir_localfs_cli_source: str,
        tempdir_localfs_cli_destination: str,
    ) -> None:
        self.source = tempdir_localfs_cli_source
        self.destination = tempdir_localfs_cli_destination
        setattr(self, "create_files", create_files_localfs)


class TestLocalFsCLISync(_TestCLISync):
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
        tempdir_localfs_cli_source: str,
        tempdir_localfs_cli_destination: str,
    ) -> None:
        self.source = tempdir_localfs_cli_source
        self.destination = tempdir_localfs_cli_destination
        setattr(self, "create_files", create_files_localfs)


class TestLocalFsCLIFind(_TestCLIFind):
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
        tempdir_localfs_cli_source: str,
    ) -> None:
        self.target = tempdir_localfs_cli_source
        setattr(self, "create_files", create_files_localfs)


class TestLocalFsCLIDelete(_TestCLIDelete):
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
        tempdir_localfs_cli_source: str,
    ) -> None:
        self.source = tempdir_localfs_cli_source
        setattr(self, "create_files", create_files_localfs)
