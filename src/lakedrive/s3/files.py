import logging
import asyncio

from functools import partial
from typing import AsyncIterator, List, Dict

from ..core.objects import ByteStream, FileObject
from ..utils.helpers import split_in_chunks
from ..httplibs.objects import HttpResponse, HttpResponseError

from .bucket import bucket_objects
from .objects import S3Bucket, HttpRequestS3, HttpRequestS3Bucket

logger = logging.getLogger(__name__)


def parse_head_response_headers(response: HttpResponse) -> Dict[str, str]:
    if response.status_code == "200":
        return response.headers  # known to exist
    if response.status_code == "404":
        raise FileNotFoundError
    if response.status_code == "403":
        raise PermissionError
    raise HttpResponseError(response)


async def s3_head_file(
    bucket: S3Bucket,
    file_path: str,
) -> Dict[str, str]:
    async with HttpRequestS3Bucket(bucket, connections=1) as client:
        response = await client.head(resource=file_path)
    return parse_head_response_headers(response)


async def file_exists(
    bucket: S3Bucket,
    file_path: str,
) -> bool:
    if file_path[-1] == "/":
        # cant use head for virtual path
        # if no file_objects exists under path, virtual path wont exist either
        try:
            file_objects = await bucket_objects(
                bucket, file_path, max_keys_per_prefix=1
            )
            if len(file_objects) > 0:
                return True
            return False
        except HttpResponseError as error:
            error_msg = f"cant access 's3://{bucket.name}/{file_path}'"
            raise HttpResponseError(
                HttpResponse(status_code=error.status_code, error_msg=error_msg)
            )

    else:
        # object - use head
        async with HttpRequestS3Bucket(bucket, connections=1) as client:
            response = await client.head(resource=file_path)
        try:
            _ = parse_head_response_headers(response)
            return True  # no errors raised == OK
        except FileNotFoundError:
            return False  # known not to exist
        except Exception:
            response.error_msg = f"cant access 's3://{bucket.name}/{file_path}'"
            # any other http return code is un-expected
            raise HttpResponseError(response)


async def s3_batch_put(
    client: HttpRequestS3,
    batch: List[FileObject],
    prefix: str = "",
) -> List[FileObject]:
    """This function processes one batch of StreamObject items, and forwards
    each single item to a HTTP upstream client for execution.

    Return a list of failed items (or empy list if none failed),
    so failed items can be retried at a later time"""
    logger.debug(f"Uploading batch of {str(len(batch))} StreamObject items")

    responses = await asyncio.gather(
        *[
            client.upstream(
                f"{prefix}{file_object.name}",
                file_object.size,
                await file_object.byte_stream(),
                thread_id=thread_id,
            )
            for thread_id, file_object in enumerate(batch)
        ],
        return_exceptions=False,
    )

    items_failed = []
    for item_no, http_response in enumerate(responses):
        filename = batch[item_no].name
        logger.debug(f"http_response_type={type(http_response)}")
        logger.debug(http_response)
        if http_response.status_code == "200":
            logger.info(f"Succesful upload: {filename}")
            continue
        items_failed.append(batch[item_no])
        http_error = f"{http_response.error_msg} ({http_response.status_code})"
        logger.error(f"Failed upload: {filename},http_error={http_error})")

    return items_failed


async def s3_batch_delete(
    client: HttpRequestS3,
    batch: List[FileObject],
    prefix: str = "",
) -> List[FileObject]:
    """This function processes one batch of keynames, and forwards
    each single keyname to a HTTP client (HTTP DELETE) for execution.

    Return a list of failed items (or empy list if none failed),
    so failed items can be retried at a later time"""
    responses = await asyncio.gather(
        *[
            client.delete(
                f"{prefix}{fo.name}",
                thread_id=thread_id,
            )
            for thread_id, fo in enumerate(batch)
        ]
    )

    file_objects_failed: List[FileObject] = []
    for item_no, http_response in enumerate(responses):
        filename = batch[item_no]
        if http_response.status_code == "204":
            logger.info(f"Succesful delete: {filename}")
            continue
        file_objects_failed.append(batch[item_no])
        http_error = f"{http_response.error_msg} ({http_response.status_code})"
        logger.error(f"Failed delete: {filename},http_error={http_error})")

    return file_objects_failed


async def put_file_batched(
    bucket: S3Bucket,
    file_objects_batched: AsyncIterator[List[FileObject]],
    prefix: str = "",
    tcp_connections: int = 4,
) -> List[FileObject]:
    """Put files on S3 bucket batch by batch. This function sets up a HTTP config,
    yields batches of StreamObject items and forwards this to an upload processor.

    read_batch_iterator yields a list of StreamObject items, which are forwarded to
    an (HTTP) upstream client. Size of each batch should (ideally) be a multiple of
    connections (i.e. if threads_per_worker == 4, batch == 4, 8, 12, 16, ... items)

    Return list of FileObject (derived from StreamObject) that have failed"""
    file_objects_remaining: List[FileObject] = []
    client = await HttpRequestS3Bucket(
        bucket,
        connections=tcp_connections,
    ).__aenter__()

    async for batch in file_objects_batched:
        items_remaining = await s3_batch_put(client, batch, prefix=prefix)
        if items_remaining:
            file_objects_remaining += items_remaining
    await client.__aexit__(None, None, None)

    return file_objects_remaining


async def delete_file_batched(
    bucket: S3Bucket,
    file_objects: List[FileObject],
    prefix: str = "",
    max_connections_per_thread: int = 50,
) -> List[FileObject]:
    """Delete a list of files (by keyname) in a S3 bucket
    If an item delete fails, retry once"""

    if len(file_objects) == 0:
        # nothing to delete
        return []

    no_connections = min(len(file_objects), max_connections_per_thread)
    batches = split_in_chunks(file_objects, no_connections)

    file_objects_remaining: List[FileObject] = []

    async with HttpRequestS3Bucket(
        bucket,
        connections=no_connections,
    ) as client:
        for batch in batches:
            file_objects_remaining += await s3_batch_delete(
                client, batch, prefix=prefix
            )

    return file_objects_remaining


async def get_file_batched(
    bucket: S3Bucket,
    file_objects: List[FileObject],
    prefix: str = "",
    threads_per_worker: int = 16,
    chunk_size: int = 1024**2 * 16,
) -> AsyncIterator[List[FileObject]]:
    """Get files a S3 bucket in batches of StreamObjects. Each StreamObject within a
    batch must also be consumed over iteration (connection is opened/ re-used) as
    data gets read from the source.

    At the end ensure all connections are closed off"""
    client = await HttpRequestS3Bucket(
        bucket,
        connections=min(threads_per_worker, 50),
    ).__aenter__()

    batches: List[List[FileObject]] = split_in_chunks(file_objects, threads_per_worker)

    for batch in batches:
        for thread_id, file_object in enumerate(batch):
            file_object.content = None  # ensure reset
            file_object.source = ByteStream(
                stream=client.downstream(
                    resource=f"{prefix}{file_object.name}",
                    thread_id=thread_id,
                    chunk_size=chunk_size,
                ),
                chunk_size=chunk_size,
            )
        yield batch
    # ensure connections get closed
    await client.__aexit__()


async def get_file_bytestream(
    bucket: S3Bucket,
    file_name: str,
    prefix: str = "",
    chunk_size: int = 1024**2 * 16,
) -> ByteStream:
    http_client = await HttpRequestS3Bucket(
        bucket,
        connections=1,
    ).__aenter__()
    return ByteStream(
        stream=http_client.downstream(
            resource=f"{prefix}{file_name}", chunk_size=chunk_size
        ),
        chunk_size=chunk_size,
        http_client=http_client,
    )


async def get_file(
    bucket: S3Bucket,
    file_object: FileObject,
    prefix: str = "",
    chunk_size: int = 1024**2 * 16,
) -> FileObject:
    """Get a single file from a S3 bucket, return as a StreamObject iterator,
    note that calling function must close http_client when all data is read"""

    if file_object.size != 0:
        file_object.source = partial(
            get_file_bytestream,
            bucket,
            file_object.name,
            prefix=prefix,
            chunk_size=chunk_size,
        )
    else:
        logger.debug(f"Skip empty file: {bucket.name}/{file_object.name}")
        file_object.source = None

    return file_object


async def put_file(
    bucket: S3Bucket,
    file_path: str,
    body: bytes = b"",
) -> None:
    async with HttpRequestS3Bucket(bucket, connections=1) as client:
        response = await client.put(
            file_path,
            body=body,
        )
    if int(response.status_code) in [200, 201]:
        return

    response.error_msg = (
        response.error_msg or f"cant write to 's3://{bucket.name}/{file_path}'"
    )
    raise HttpResponseError(response)


async def put_file_stream(
    bucket: S3Bucket,
    file_object: FileObject,
    prefix: str = "",
) -> List[FileObject]:
    """Upload a single file (FileObject) to a S3 bucket"""
    async with HttpRequestS3Bucket(
        bucket,
        connections=1,
    ) as client:
        remaining = await s3_batch_put(client, [file_object], prefix=prefix)
    return remaining
