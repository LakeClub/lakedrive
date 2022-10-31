import re
import logging
import asyncio

from datetime import datetime
from typing import List, Dict, Optional, Tuple

from ..core.objects import FrozenFileObject, FileObject, HashTuple
from ..utils.helpers import path_filter_overlap
from ..httplibs.objects import HttpResponse, HttpResponseError
from ..s3.objects import S3Bucket, HttpRequestS3
from ..s3.xmlparse import parse_bucket_list_xml


LIST_MAX_KEYS = 1000


logger = logging.getLogger(__name__)


def rfc3339_to_epoch(timestamp: str) -> int:
    """Convert rf3339 formatted timestamp to epoch time"""
    return int(datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())


def aws_misc_to_epoch(timestamp: str) -> int:
    """Convert other known time-formats used by AWS to epoch"""
    try:
        # last-modified S3 HeadObject
        return int(
            datetime.strptime(timestamp, "%a, %d %b %Y %H:%M:%S GMT").timestamp()
        )
    except Exception:
        return -1


def verify_md5sum(etag: str) -> Optional[HashTuple]:
    """Currently only support SSE-S3 or Plaintext, given this can safely assume etag
    to be md5sum if matches regex. Future: add support for multipart uploads, that
    have format $CHECKSUM-$NO_PARTS"""
    if re.match("^[a-z0-9]{32}$", etag):
        return HashTuple(algorithm="md5", value=etag)
    return None


async def bucket_connect(bucket: S3Bucket) -> HttpResponse:
    async with HttpRequestS3(bucket, connections=1) as client:
        response = await client.head(resource="")
    return response


async def bucket_create(
    bucket: S3Bucket, credentials: Dict[str, str], raise_exist: bool = True
) -> S3Bucket:
    """Create S3 bucket. Optionally check if the bucket already exist
    If an S3 bucket either exists or is created, return the bucket object"""
    try:
        # check if bucket exists, if it does bucket.exists is set to True
        await bucket.validate(
            credentials,
            raise_not_found=True,
            raise_no_permission=True,
        )
    except FileNotFoundError:
        pass
    except PermissionError:
        raise

    if bucket.exists is True:
        if raise_exist is True:
            raise FileExistsError(f'Bucket "{bucket.name}" exists')
        return bucket

    # create new bucket
    body = f'\
<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\
<LocationConstraint>{bucket.region}</LocationConstraint>\
</CreateBucketConfiguration>'.encode()

    async with HttpRequestS3(bucket, connections=1) as client:
        response = await client.put("", body=body)
        assert response.status_code == "200"
    logger.info(f"S3 bucket created: {bucket.endpoint_url}")
    bucket.exists = True
    return bucket


async def bucket_delete(
    bucket: S3Bucket,
) -> bool:
    """Delete a S3 bucket"""
    try:
        async with HttpRequestS3(bucket, connections=1) as client:
            response = await client.delete("")

        _log_prefix = f'Bucket delete "{bucket.endpoint_url}"'
        if response.status_code == "204":
            logger.debug(f"{_log_prefix} succesful")
            bucket.exists = False
            return True
        logger.error(
            f"{_log_prefix} failed,E={response.status_code}:{response.error_msg}"
        )

    except ConnectionError as error:
        logger.error(str(error))

    return False


async def parse_bucket_list_response(
    response: HttpResponse,
    checksum: bool,
    skip_hidden: bool,
) -> Tuple[Dict[str, str], List[FrozenFileObject]]:
    """Parses the (xml) response of a bucket_list (v2) API call.
    Xml body itself is first parsed by another function that returns a dictionary,
    containing;
    1. meta-information (e.g. number of objects, a continuation-token),
    2. file_contents (> file_objects), and
    3. prefixes (virtual directories)

    meta-information is returned "as-is",
    file_contents are translated and returned as a list of FileObjects,
    prefixes are returned as an extension to starting prefix"""
    if response.status_code != "200":
        raise HttpResponseError(response)

    parsed_contents = parse_bucket_list_xml(response.body)
    results_meta = parsed_contents[".ListBucketResult"]
    file_contents = parsed_contents.get(".ListBucketResult.Contents", [])
    prefix_contents = parsed_contents.get(".ListBucketResult.CommonPrefixes", [])

    # additional checks required to guarantuee safe passage
    assert isinstance(results_meta, dict)
    assert isinstance(file_contents, list)
    assert isinstance(prefix_contents, list)

    frozen_file_objects = [
        FrozenFileObject(
            name=item["Key"],
            size=int(item["Size"]),
            mtime=rfc3339_to_epoch(item["LastModified"]),
            hash=(
                lambda: verify_md5sum(item["ETag"].strip('"')) if checksum else None
            )(),
            tags=b"",
        )
        for item in file_contents
        if isinstance(item, dict)
        and skip_hidden is False
        or (item["Key"][0] != "." and "/." not in item["Key"])
    ]

    frozen_file_objects += [
        FrozenFileObject(
            name=pre["Prefix"],
            size=0,
            mtime=-1,
            hash=None,
            tags=b"",
        )
        for pre in prefix_contents
    ]

    return results_meta, frozen_file_objects


async def bucket_list_loop(
    client: HttpRequestS3,
    parameter_str: str,
    thread_id: int,
    prefix: str,
    checksum: bool,
    skip_hidden: bool,
) -> List[FileObject]:
    """Execute a bucket list (v2) API call, continue as long as a NextContinuationToken
    is received back. Collect, merge and return (parsed) results"""

    _parameter_str = parameter_str
    file_objects_set = set()

    while True:
        response = await client.get(
            parameter_str=parameter_str,
            tid=thread_id,
        )
        results_meta, _frozen_file_objects = await parse_bucket_list_response(
            response, checksum, skip_hidden
        )

        file_objects_set.update(set(_frozen_file_objects))

        continuation_token = results_meta.get("NextContinuationToken", "")
        if (
            not continuation_token
            or results_meta.get("IsTruncated", "false") == "false"
        ):
            break

        logger.debug("ContinuationToken received after bucket_list, continuing loop")
        parameter_str = f"{_parameter_str}&continuation-token={continuation_token}"

    # convert to mutable version
    file_objects = [
        FileObject(
            name=ffo.name,
            size=ffo.size,
            hash=ffo.hash,
            mtime=ffo.mtime,
            tags=ffo.tags,
        )
        for ffo in file_objects_set
    ]
    return file_objects


async def bucket_list(
    bucket: S3Bucket,
    bucket_path: str,
    prefixes: List[str] = [],
    recursive: bool = True,
    checksum: bool = True,
    skip_hidden: bool = False,
    max_connections: int = 20,
    max_keys_per_prefix: Optional[int] = None,
) -> List[FileObject]:
    """Get contents of an S3 bucket.
    Each given prefix maps to its own query (/connection), and merged as a final step
    : checksum: if True, checksum is added to results
    : recursive: if True, queries recursive into ("virtual") directories
    : skip_hidden: if True, skip files and (virtual) directories starting with "."
    """

    if prefixes:
        prefixes = [
            f"{bucket_path}{prefix}" for prefix in path_filter_overlap(prefixes)
        ]
    else:
        prefixes = [bucket_path]

    if recursive:
        parameter_str = "list-type=2"
    else:
        # only show files in paths (prefixes) given
        parameter_str = "list-type=2&delimiter=/"

    max_keys_per_prefix = max_keys_per_prefix or LIST_MAX_KEYS
    parameter_str += f"&max-keys={str(max_keys_per_prefix)}"

    async with HttpRequestS3(
        bucket,
        connections=min(len(prefixes), max_connections),
    ) as client:
        responses = await asyncio.gather(
            *[
                bucket_list_loop(
                    client,
                    f"{parameter_str}&prefix={prefix}",
                    thread_id,
                    prefix,
                    checksum,
                    skip_hidden,
                )
                for thread_id, prefix in enumerate(prefixes)
            ]
        )

    merged: List[FileObject] = [item for sublist in responses for item in sublist]
    return merged
