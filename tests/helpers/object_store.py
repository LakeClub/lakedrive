import uuid
import asyncio

from lakedrive.core import get_scheme_handler
from lakedrive.core.handlers import ObjectStoreHandler


def create_object_store_s3(
    location: str = "",
    max_read_threads: int = 512,
    max_write_threads: int = 128,
) -> ObjectStoreHandler:
    if not location:
        location = f"s3://{str(uuid.uuid4())}"
    if not location.startswith("s3://"):
        location = f"s3://{location}"

    object_store = asyncio.run(
        get_scheme_handler(
            location,
            max_read_threads=max_read_threads,
            max_write_threads=max_write_threads,
        )
    )
    assert isinstance(object_store, ObjectStoreHandler)
    asyncio.run(object_store.create_storage_target())
    assert asyncio.run(object_store.storage_target_exists()) is True
    return object_store


async def clear_object_store(
    object_store: ObjectStoreHandler, delete_target: bool = False
) -> None:
    file_objects = await object_store.list_contents(
        checksum=False, include_directories=True
    )

    await object_store.delete_batch(file_objects=file_objects)

    # expect 0 files after delete all
    file_objects = await object_store.list_contents(
        checksum=False, include_directories=True
    )
    assert isinstance(file_objects, list)
    assert len(file_objects) == 0

    if delete_target is True:
        deleted = await object_store.delete_storage_target()
        assert isinstance(deleted, bool)
        assert deleted is True

        exists = await object_store.storage_target_exists()
        assert isinstance(exists, bool)
        assert exists is False
