import os
import logging

from typing import List, Dict, Set

from ..core.handlers import ObjectStoreHandler, batch_stream_objects
from ..core.objects import FileObject, FileUpdate, FrozenFileObject
from ..utils.executors import run_async_processpool
from ..utils.helpers import splitlist


logger = logging.getLogger(__name__)


def freeze_file_objects(file_objects: List[FileObject]) -> List[FrozenFileObject]:
    frozen_objects = [
        FrozenFileObject(
            name=fo.name,
            mtime=fo.mtime,
            size=fo.size,
            hash=fo.hash,
        )
        for fo in file_objects
    ]
    return frozen_objects


def unfreeze_file_objects(
    frozen_file_objects: Set[FrozenFileObject],
) -> List[FileObject]:
    file_objects = [
        FileObject(
            name=ffo.name,
            size=ffo.size,
            hash=ffo.hash,
            mtime=ffo.mtime,
            tags=ffo.tags,
        )
        for ffo in frozen_file_objects
    ]
    return file_objects


def get_sync_update(
    objects_source: List[FileObject],
    objects_target: List[FileObject],
    params: Dict[str, bool],
) -> List[FileUpdate]:

    # create immutable objects to enable using set class functions
    objects_source_frozen = freeze_file_objects(objects_source)
    objects_target_frozen = freeze_file_objects(objects_target)

    checksum = params.get("checksum", False)
    skip_newer_on_target = params.get("skip_newer_on_target", False)
    delete_extraneous = params.get("delete_extraneous", False)

    if checksum is True:
        # skip based on checksum, not mtime (or size)
        objects_in_source = {(obj.name, obj.hash): obj for obj in objects_source_frozen}
        hashkeys_in_target = set(
            [(obj.name, obj.hash) for obj in objects_target_frozen]
        )
        hashkeys_in_source = set(objects_in_source.keys())
        copy_keys = hashkeys_in_source.difference(hashkeys_in_target)
        copy_to_target = set([objects_in_source[key] for key in copy_keys])

    else:
        copy_to_target = set(objects_source_frozen).difference(
            set(objects_target_frozen)
        )

    if checksum is False and skip_newer_on_target is True:
        # skip based on mod-time (unless skipped based on md5sum)
        filetimes_on_target = {obj.name: obj.mtime for obj in objects_target_frozen}
        # skip objects that exist on target, and have a newer mtime
        skippable_objects = [
            obj
            for obj in copy_to_target
            if obj.name in filetimes_on_target
            and obj.mtime < filetimes_on_target[obj.name]
        ]

        # substract skippable objects
        copy_to_target = copy_to_target.difference(skippable_objects)

    if delete_extraneous is True:
        # delete files found on target, but not on source
        objects_in_target = {obj.name: obj for obj in objects_target_frozen}
        keys_in_source = set([obj.name for obj in objects_source_frozen])
        keys_in_target = set(objects_in_target.keys())

        delete_keys = keys_in_target.difference(keys_in_source)
        delete_from_target = set(objects_in_target[key] for key in delete_keys)
    else:
        delete_from_target = set()

    fo_copy_to_target = unfreeze_file_objects(copy_to_target)
    fo_delete_from_target = unfreeze_file_objects(delete_from_target)

    return [
        FileUpdate(action="copy", files=fo_copy_to_target),
        FileUpdate(action="delete", files=fo_delete_from_target),
    ]


async def execute_sync_update(
    source_handler: ObjectStoreHandler,
    target_handler: ObjectStoreHandler,
    file_update: FileUpdate,
) -> None:

    if file_update.action == "copy":
        # 1 thread per cpu core, with min of 2 and max of 8
        workers = min(os.cpu_count() or 2, 8)

        arguments_list = [
            (source_handler, target_handler, chunk)
            for chunk in splitlist(file_update.files, workers * 1)
        ]
        await run_async_processpool(
            batch_stream_objects, arguments_list, max_workers=workers
        )

        return
    if file_update.action == "delete":
        await target_handler.delete_batch(file_objects=file_update.files)
        return

    raise ValueError(f"Invalid action: {str(file_update.action)}")


async def synchronise_paths(
    source: ObjectStoreHandler,
    destination: ObjectStoreHandler,
    params: Dict[str, bool] = {},
) -> None:

    skip_hidden = params.get("skip_hidden", False)
    checksum = params.get("checksum", False)
    dry_run = params.get("dry_run", False)

    objects_source = await source.list_contents(
        checksum=checksum, skip_hidden=skip_hidden
    )
    objects_target = await destination.list_contents(
        checksum=checksum, skip_hidden=skip_hidden
    )
    update_list = get_sync_update(objects_source, objects_target, params=params)

    if dry_run is False:
        for file_update in update_list:
            await execute_sync_update(
                source,
                destination,
                file_update,
            )
