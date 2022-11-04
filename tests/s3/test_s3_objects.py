import os
import pytest

from lakedrive.s3.objects import S3Bucket, S3Connect, to_base16, hash_nobytes


class TestS3Bucket:
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(self) -> None:
        fake_credentials = {
            "access_id": "FAKE_ACCESS_ID",
            "secret_key": "FAKE_SECRET_KEY",
        }
        self.bucket = S3Bucket("dummy").configure(
            fake_credentials, "earth", "https://localhost"
        )

    def test___init__(self) -> None:
        assert isinstance(self.bucket, S3Bucket)
        assert self.bucket.exists is False


class TestS3Connect:
    @pytest.fixture(autouse=True)  # type: ignore[misc]
    def setup(self) -> None:
        self.fake_credentials = {
            "access_id": "FAKE_ACCESS_ID",
            "secret_key": "FAKE_SECRET_KEY",
        }

    def test_generate_headers_no_endpoint(self) -> None:
        """Default S3 tests are against Minio endpoint,
        this test ensures the no-endpoint path code runs"""
        bucket_name = "dummy"
        bucket_region = "earth"
        bucket_aws_host = f"{bucket_name}.s3.{bucket_region}.amazonaws.com"

        endpoint_env = os.environ.get("AWS_S3_ENDPOINT", "")  # store existing
        os.environ["AWS_S3_ENDPOINT"] = ""  # remove from env
        bucket = S3Bucket(bucket_name).configure(
            credentials=self.fake_credentials,
            region=bucket_region,
            endpoint_url="",
        )
        os.environ["AWS_S3_ENDPOINT"] = endpoint_env  # restore env

        assert isinstance(bucket.endpoint_url, str)
        assert bucket.endpoint_url == f"https://{bucket_aws_host}"

        http_connection = S3Connect(
            bucket.connection_args,
            bucket.s3_credentials,
            bucket.endpoint_url,
            bucket.region,
        )
        headers = http_connection.generate_headers("")
        assert isinstance(headers, dict)
        assert isinstance(headers["x-amz-date"], str)
        assert isinstance(headers["Authorization"], str)
        assert headers["host"] == bucket_aws_host
        assert headers["x-amz-content-sha256"] == "UNSIGNED-PAYLOAD"

    def test_generate_headers_streaming(self) -> None:
        """Testing the case where content_length < chunk_size,
        as these code-blocks will be skipped by the object store tests"""
        bucket = S3Bucket("dummy").configure(
            credentials=self.fake_credentials, region="earth"
        )
        http_connection = S3Connect(
            bucket.connection_args,
            bucket.s3_credentials,
            bucket.endpoint_url,
            bucket.region,
        )
        content_length = 1024
        chunk_size = 256 * 1024
        headers = http_connection.generate_headers_streaming(
            "/resource", content_length, chunk_size
        )
        assert isinstance(headers, dict)
        assert isinstance(headers["x-amz-date"], str)
        assert isinstance(headers["Authorization"], str)
        assert headers["x-amz-decoded-content-length"] == str(content_length)
        assert headers["Content-Encoding"] == "aws-chunked"
        assert headers["x-amz-content-sha256"] == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"


def test_to_base16() -> None:
    assert to_base16(0) == "0"
    assert to_base16(232) == "E8"
    assert to_base16(2416) == "970"


def test_hash_nobytes() -> None:
    sha256_nb = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    assert hash_nobytes() == sha256_nb
