import os
import pytest
import asyncio

from typing import List, Dict, Any

from lakedrive.core import get_scheme_handler
from lakedrive.core.objects import HashTuple
from lakedrive.httplibs.objects import HttpResponse, HttpResponseError

from lakedrive.s3.objects import S3Bucket
from lakedrive.s3.handler import S3Handler
from lakedrive.s3.bucket import (
    aws_misc_to_epoch,
    bucket_delete,
    verify_md5sum,
    parse_bucket_list_response,
)

from ..s3.fake_s3_objects import FakeHttpRequestS3Bucket
from ..helpers.generate_s3_list import generate_xml
from ..helpers.misc import get_random_string
from ..helpers.async_parsers import pytest_asyncio


def test_aws_misc_to_epoch() -> None:
    date_strings = [
        ("Mon, 22 Aug 2022 20:44:40 GMT", 1661201080),
        ("Wed, 15 Jul 2020 10:00:25 GMT", 1594807225),
        ("Sat, 10 Mar 2018 15:16:02 GMT", 1520694962),
        ("NaN", -1),
    ]
    for date_str, epoch_time in date_strings:
        response = aws_misc_to_epoch(date_str)
        assert isinstance(response, int)
        assert response == epoch_time


def test_verify_md5sum() -> None:
    random_strings = [
        get_random_string(32, special_chars_limited="") for _ in range(32)
    ]
    good_strings = [s[0:32].lower() for s in random_strings]

    # ensure bad_strings are guarantueed to be invalid
    bad_strings = [
        s[: i % 32] + "-" + s[i % 32 + 1 :] for i, s in enumerate(random_strings)
    ]

    # add bad_strings != 32 length
    bad_strings += [
        get_random_string(length, special_chars_limited="").lower()
        for length in range(20, 31)
    ]
    bad_strings += [
        get_random_string(length, special_chars_limited="").lower()
        for length in range(33, 40)
    ]

    # each bad_string should generate a None response (final list should be empty)
    assert list(filter(None, [verify_md5sum(bs) for bs in bad_strings])) == []

    hash_tuples = list(filter(None, [verify_md5sum(gs) for gs in good_strings]))
    assert isinstance(hash_tuples, list)
    assert len(hash_tuples) == len(good_strings)
    hash_tuples_verified: List[str] = list(
        filter(
            None,
            [
                isinstance(hs, HashTuple) and hs.value == good_strings[i]
                for i, hs in enumerate(hash_tuples)
            ],
        )
    )
    assert len(hash_tuples_verified) == len(good_strings)


def test_bucket_configure(monkeypatch: Any) -> None:
    fake_credentials = {
        "access_id": "ACCESS_ID",
        "secret_key": "SECRET_KEY",
    }
    bucket = S3Bucket("my-bucket-name")
    bucket = bucket.configure(
        credentials=fake_credentials,
        region="my-region",
        endpoint_url="https://my-endpoint",
    )
    assert isinstance(bucket, S3Bucket)
    assert bucket.name == "my-bucket-name"
    assert bucket.region == "my-region"
    assert bucket.endpoint_url == "https://my-endpoint/my-bucket-name"

    # temporary remove predefined AWS_ environment variables
    environment_original = dict(os.environ)
    for key in environment_original.keys():
        if key.startswith("AWS_"):
            del os.environ[key]

    bucket = bucket.configure(credentials=fake_credentials, region="my-region")
    assert bucket.endpoint_url == "https://my-bucket-name.s3.my-region.amazonaws.com"

    # restore original environment
    os.environ.update(environment_original)


def test_bucket_validate_301(monkeypatch: Any) -> None:
    credentials = {
        "access_id": os.environ["AWS_ACCESS_KEY_ID"],
        "secret_key": os.environ["AWS_SECRET_ACCESS_KEY"],
    }
    bucket = S3Bucket("my-bucket-name")
    bucket = bucket.configure(credentials=credentials, region="my-region")

    assert len(bucket.connection_args) > 0
    bucket.endpoint_url = bucket.endpoint_url + "/"

    custom_headers: Dict[str, str] = {}
    custom_status_code = "301"

    def _custom_response(_: Any, http_response: HttpResponse) -> HttpResponse:
        http_response.status_code = custom_status_code
        if custom_headers:
            http_response.headers.update(custom_headers)
        return http_response

    # override HttpRequest class with a mock version
    monkeypatch.setattr(
        "lakedrive.s3.bucket.HttpRequestS3Bucket", FakeHttpRequestS3Bucket
    )

    # insert (mock) custom response
    monkeypatch.setattr(
        __name__ + ".FakeHttpRequestS3Bucket.custom_response", _custom_response
    )

    with pytest.raises(ValueError) as error:
        asyncio.run(bucket.validate(credentials))
    assert (
        str(error.value) == "Unexpected response when checking bucket: my-bucket-name"
    )

    # configure (mock) response to redirect to alternate region
    custom_headers = {"x-amz-bucket-region": "earth"}
    with pytest.raises(HttpResponseError) as error:
        asyncio.run(bucket.validate(credentials))
    assert str(error.value) == "Retries exceeded (503)"

    # configure (mock) response to
    custom_headers = {}
    custom_status_code = "503"
    with pytest.raises(HttpResponseError) as error:
        asyncio.run(bucket.validate(credentials))
    assert str(error.value) == "Cant read S3 bucket (503)"


def test_bucket_delete() -> None:
    fake_credentials = {
        "access_id": "ACCESS_ID",
        "secret_key": "SECRET_KEY",
    }
    bucket = S3Bucket("my-bucket-name")
    bucket = bucket.configure(
        credentials=fake_credentials,
        region="my-region",
        endpoint_url=os.environ["AWS_S3_ENDPOINT"],
    )
    success = asyncio.run(bucket_delete(bucket))
    assert isinstance(success, bool)
    assert success is False

    # test Connection error catch
    bucket = bucket.configure(
        credentials=fake_credentials,
        region="my-region",
        endpoint_url="https://my-endpoint",
    )
    success = asyncio.run(bucket_delete(bucket))
    assert isinstance(success, bool)
    assert success is False


def test_bucket_parse_list_response() -> None:
    xml_content = generate_xml()

    http_response = HttpResponse(
        status_code="200",
        body=xml_content,
    )
    asyncio.run(
        parse_bucket_list_response(
            http_response,
            True,
            False,
        )
    )


def test_bucket_parse_list_response_403() -> None:
    http_response = HttpResponse(
        status_code="403",
        body=b"",
    )
    with pytest.raises(HttpResponseError):
        asyncio.run(
            parse_bucket_list_response(
                http_response,
                True,
                False,
            )
        )


def test_bucket_inaccessible() -> None:
    # empty access key to trigger PermissionError
    invalid_credentials = {
        "access_id": "dummy",
        "secret_key": "dummy",
    }
    s3_handler = get_scheme_handler("s3://bucket403", credentials=invalid_credentials)
    assert isinstance(s3_handler, S3Handler)
    with pytest.raises(PermissionError):
        asyncio.run(s3_handler.create_storage_target())


@pytest_asyncio
async def test_bucket_empty_name_storage_target_exists() -> None:
    s3_handler = get_scheme_handler("s3://")
    assert await s3_handler.storage_target_exists() is None


def test_bucket_delete_nonexistent() -> None:
    # create a bucket that is very unlikely to exist, 63 == max. length S3 bucket
    s3_random_bucket = f's3://{get_random_string(63, special_chars_limited="-_")}'
    s3_handler = get_scheme_handler(s3_random_bucket)

    assert isinstance(s3_handler, S3Handler)
    deleted = asyncio.run(s3_handler.delete_storage_target())
    assert isinstance(deleted, bool)
    assert deleted is True
