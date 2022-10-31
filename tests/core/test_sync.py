import pytest
import asyncio

from typing import List

from lakedrive.core.sync import execute_sync_update, get_sync_update, synchronise_paths
from lakedrive.core.objects import FileObject, FileUpdate
from lakedrive.core.handlers import ObjectStoreHandler
from lakedrive.localfs.handler import LocalFileHandler

from ..helpers.async_parsers import pytest_asyncio
from ..helpers.object_store import clear_object_store
from ..helpers.misc import splitlist, filter_regfiles


def assert_equal_file_objects(
    source_files: List[FileObject], target_files: List[FileObject]
) -> None:

    update = get_sync_update(
        source_files,
        target_files,
        params={
            "checksum": True,
            "skip_newer_on_target": True,
            "delete_extraneous": True,
        },
    )
    assert len(update) == 2

    action_copy, action_delete = update
    assert action_copy.action == "copy"
    assert action_delete.action == "delete"
    assert isinstance(action_copy.files, list)
    assert isinstance(action_delete.files, list)

    # given source- and destination-files consist of the same content,
    # expect nothing to be updated or deleted
    assert len(action_copy.files) == 0
    assert len(action_delete.files) == 0


class TestCoreSync:
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(
        self,
        mock_source_handler_files_random: ObjectStoreHandler,
        sync_source_handler: LocalFileHandler,
        sync_target_handler: LocalFileHandler,
    ) -> None:
        self.source = sync_source_handler
        self.target = sync_target_handler

        self.mock_source = mock_source_handler_files_random

        # ensure source and target are emptied
        asyncio.run(clear_object_store(self.source, delete_target=True))
        asyncio.run(clear_object_store(self.target, delete_target=True))

    def test_get_rsync_update(self) -> None:
        # get files differences. Note, source == target, i.e. no diff expected.
        mock_source_files = asyncio.run(self.mock_source.list_contents())
        assert_equal_file_objects(mock_source_files, mock_source_files)

    @pytest_asyncio
    async def test_execute_rsync_update_empty(self) -> None:
        await execute_sync_update(
            self.source, self.target, FileUpdate(action="copy", files=[])
        )
        await execute_sync_update(
            self.source, self.target, FileUpdate(action="delete", files=[])
        )

        with pytest.raises(ValueError) as error:
            await execute_sync_update(
                self.source, self.target, FileUpdate(action="dummy", files=[])
            )
        assert str(error.value) == "Invalid action: dummy"

    @pytest_asyncio
    async def test_execute_rsync_update(self) -> None:
        mock_source_files = await self.mock_source.list_contents()
        files_a, files_b = splitlist(mock_source_files, 2)

        # copy files_a to (localfs) source, and files_b to target
        await execute_sync_update(
            self.mock_source,
            self.source,
            FileUpdate(action="copy", files=files_a),
        )
        await execute_sync_update(
            self.mock_source,
            self.target,
            FileUpdate(action="copy", files=files_b),
        )

        # get list of new contents
        sync_source_files = await self.source.list_contents()
        sync_target_files = await self.target.list_contents()

        action_copy, action_delete = get_sync_update(
            sync_source_files,
            sync_target_files,
            params={
                "checksum": True,
                "skip_newer_on_target": True,
                "delete_extraneous": True,
            },
        )

        # expect all items on target to be deleted
        # filter by regfiles as directories may or may not exist on target
        assert len(filter_regfiles(sync_target_files)) == len(
            filter_regfiles(action_delete.files)
        )

        # expect all items on source to be copied
        # filter by regfiles as directories may or may not exist on target
        assert len(filter_regfiles(sync_source_files)) == len(
            filter_regfiles(action_copy.files)
        )

        await execute_sync_update(self.source, self.target, action_copy)
        await execute_sync_update(self.source, self.target, action_delete)

        # verify if source and target contents are equal
        assert_equal_file_objects(
            await self.source.list_contents(),
            await self.target.list_contents(),
        )

    @pytest_asyncio
    async def test_synchronise_paths(self) -> None:

        params = {
            "checksum": True,
            "skip_newer_on_target": True,
            "delete_extraneous": True,
            "dry_run": False,
        }
        # clear target
        await clear_object_store(self.target)

        # sync source to target
        await synchronise_paths(
            self.source,
            self.target,
            params=params,
        )

        # verify if source and target contents are equal
        assert_equal_file_objects(
            await self.source.list_contents(),
            await self.target.list_contents(),
        )

        # ensure source and target are emptied when done
        await clear_object_store(self.source)
        await clear_object_store(self.target)
