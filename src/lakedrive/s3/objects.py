from __future__ import annotations, unicode_literals

import os
import re
import hmac
import logging
import asyncio
import hashlib
import datetime

from urllib.parse import quote, urlparse
from typing import Dict, List, Any, Optional

from ..httplibs.objects import (
    ConnectConfiguration,
    HttpConnectionMeta,
    HttpResponse,
    HttpResponseError,
)
from ..httplibs.request import HttpRequest
from ..httplibs.helpers import http_connection_args
from ..httplibs.request import HTTP_BASE_HEADERS
from .config import AWS_DEFAULT_REGION

logger = logging.getLogger(__name__)

CRLF = "\r\n"


def sign(key: bytes, msg: bytes) -> bytes:
    return hmac.new(key, msg, hashlib.sha256).digest()


def to_base16(n: int) -> str:
    """Convert base10 number to base16"""
    hex_chars = "0123456789ABCDEF"
    return "0" if not n else to_base16(n // 16).lstrip("0") + hex_chars[n % 16]


def hash_nobytes() -> str:
    return hashlib.sha256(b"").hexdigest()


class S3Bucket(ConnectConfiguration):
    def __init__(
        self,
        name: str,
    ):
        self.name: str = name
        self.endpoint_url = ""
        self.region = ""
        self.connection_args: List[Dict[str, Any]] = []
        self.s3_credentials: Dict[str, str] = {}

    def configure(
        self,
        credentials: Dict[str, str] = {},
        region: str = "",
        endpoint_url: Optional[str] = None,
    ) -> S3Bucket:
        """Create new S3 bucket object from a given bucket_name,
        Get region and endpoint from environment vars, or use defaults,
        Support default AWS, as well as custom endpoints (e.g. minio)"""
        bucket_name = self.name

        self.region = (
            region
            or os.environ.get("AWS_DEFAULT_REGION", AWS_DEFAULT_REGION)
            or self.region
        )

        endpoint_url = endpoint_url or os.environ.get("AWS_S3_ENDPOINT", "")
        if endpoint_url:
            # bucket_name must be in either domain or path
            if re.search(f"/{bucket_name}[\\./]|/{bucket_name}$", endpoint_url):
                # pass as-is
                self.endpoint_url = endpoint_url
            else:
                # path-based without buckt_name, append bucket_name
                self.endpoint_url = f"{endpoint_url}/{bucket_name}"
        else:
            # default to AWS (domain-based)
            self.endpoint_url = f"https://{bucket_name}.s3.{self.region}.amazonaws.com"

        self.s3_credentials = credentials or self.s3_credentials

        self.exists = False  # (re-)validation required after configure
        self.connection_args = http_connection_args(self.endpoint_url)
        return self

    async def validate(
        self,
        credentials: Dict[str, str],
        raise_not_found: bool = False,
        raise_no_permission: bool = False,
    ) -> S3Bucket:
        """Validate if S3 bucket is found and can be accessed based on the response
        status code;
        200: exists and can be accessed
        404: bucket does not exist
        403: authentication failed (bucket may, or may not exist)

        This function follows one redirect (HTTP status 301). A redirect is given when
        region is set incorrect (or not, and its different then default), AWS responds
        to this by given a 301 response with bucket region in the response-header"""

        self.configure(credentials=credentials)

        retries = 0
        while True:
            # continue loop as long as retries <
            retries += 1
            if retries >= 3:
                raise HttpResponseError(HttpResponse(error_msg="Retries exceeded"))

            from .bucket import bucket_connect

            response = await bucket_connect(self)
            status_code = int(response.status_code)

            if status_code != 301:
                break

            # redirect given to different endpoint
            logger.debug(f"http_response 301 on endpoint: {self.endpoint_url}")

            x_region = response.headers.get("x-amz-bucket-region", "")
            if not x_region:
                raise ValueError(
                    f"Unexpected response when checking bucket: {self.name}"
                )

            # retry with given region
            self.configure(
                region=x_region,
                endpoint_url=f"https://{self.name}.s3.{x_region}.amazonaws.com",
            )

        if status_code != 200:
            if status_code == 404:
                logger.debug(f"S3 bucket not found: {self.endpoint_url}")
                if raise_not_found is True:
                    raise FileNotFoundError
                self.exists = False
                return self

            if status_code == 403:
                logger.debug(f"No permission to access S3 bucket: {self.endpoint_url}")
                if raise_no_permission is True:
                    raise PermissionError
                self.exists = False  # cant be certain if bucket exists
                return self
            raise HttpResponseError(HttpResponse(error_msg="Cant read S3 bucket"))
        logger.info(f"S3 bucket validated: {self.endpoint_url}")
        self.exists = True
        return self


class S3Connect(HttpConnectionMeta):
    def __init__(
        self,
        connection_args: List[Dict[str, Any]],
        credentials: Dict[str, str],
        endpoint_url: str,
        region: str,
    ):
        self.connection_args = connection_args
        self.credentials = credentials
        self.endpoint_url = endpoint_url
        self.region = region
        self.base_headers = dict(HTTP_BASE_HEADERS)

        self._initialize()

    def _initialize(self) -> None:
        self.now = datetime.datetime.utcnow()
        self.dateStamp = self.now.strftime("%Y%m%d")
        self.credential_scope = f"{self.dateStamp}/{self.region}/s3/aws4_request"

        self._generate_signing_key()
        self.seed_signature = ""
        self.request_url = ""

    def _generate_signing_key(self) -> None:
        kDate = sign(
            ("AWS4" + self.credentials["secret_key"]).encode("utf-8"),
            self.dateStamp.encode("utf-8"),
        )
        kRegion = sign(kDate, self.region.encode("utf-8"))
        kService = sign(kRegion, b"s3")
        self.signing_key = sign(kService, b"aws4_request")

        self.headers: Dict[str, str] = {}

    def _get_canonical_headers(self, headers: Dict[str, str]) -> Dict[str, List[str]]:
        include_hdrs = set({"host", "content-type", "date", "x-amz-*"})
        # aws requires the header items to be sorted
        return {
            hdr: [val]
            for hdr, val in sorted(headers.items())
            if hdr in include_hdrs
            or (hdr.startswith("x-amz-") and not hdr == "x-amz-client-context")
        }

    def generate_headers(
        self,
        resource: str,
        query_string: str = "",
        method: str = "GET",
        headers: Dict[str, str] = {},
        payload_hash: str = "UNSIGNED-PAYLOAD",
    ) -> Dict[str, str]:
        """

        Implementation based on the following AWS documentation:
        https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
        https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
        """
        if not headers:
            headers = dict(self.base_headers)
        headers["x-amz-date"] = self.now.strftime("%Y%m%dT%H%M%SZ")
        headers["x-amz-content-sha256"] = payload_hash
        headers["x-amz-security-token"] = os.environ.get("AWS_SESSION_TOKEN", "")

        # Create canonical URI--the part of the URI from domain to query
        # if endpoint includes a path, add this to canonical_uri
        # ensure begin and end "/" is stripped of
        # canonical uri cant start with "/"
        endpoint_parsed = urlparse(self.endpoint_url)
        endpoint_path = endpoint_parsed.path.strip("/")
        if endpoint_path:
            canonical_uri = f'{endpoint_path}/{quote(resource, safe="/")}'
        else:
            canonical_uri = quote(resource, safe="/")

        # Create the canonical headers and signed headers. Header names
        # must be trimmed and lowercase, and sorted in code point order from
        # low to high. Note trailing \n in canonical_headers.
        # signed_headers is the list of headers that are being included
        # as part of the signing process. For requests that use query strings,
        # only "host" is included in the signed headers.
        # Flatten cano_headers dict to string and generate signed_headers
        headers["host"] = endpoint_parsed.netloc.split(":")[0]

        canonical_headers_dict = self._get_canonical_headers(headers)
        canonical_headers_str = ""
        for header in canonical_headers_dict:
            canonical_headers_str += (
                f"{header}:{','.join(sorted(canonical_headers_dict[header]))}\n"
            )
        signed_headers = ";".join(canonical_headers_dict.keys())

        # AWS handles "extreme" querystrings differently to urlparse
        # (see post-vanilla-query-nonunreserved test in aws_testsuite)
        _query_string_parts = [
            tuple(q.split("=", 1)) for q in sorted(query_string.split("&")) if q
        ]
        query_string_safe = "&".join(
            [f"{k}={quote(v, safe='')}" for k, v in _query_string_parts]
        )

        canonical_request = "\n".join(
            [
                method,
                f"/{canonical_uri}",
                query_string_safe,
                canonical_headers_str,
                signed_headers,
                payload_hash,
            ]
        )

        # Compute Authorization headers
        algorithm = "AWS4-HMAC-SHA256"
        string_to_sign = "\n".join(
            [
                algorithm,
                headers["x-amz-date"],
                self.credential_scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ]
        )

        self.seed_signature = hmac.new(
            self.signing_key,
            string_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        auth_header_str = ", ".join(
            [
                f'Credential={self.credentials["access_id"]}/{self.credential_scope}',
                f"SignedHeaders={signed_headers}",
                f"Signature={self.seed_signature}",
            ]
        )
        headers["Authorization"] = f"{algorithm} {auth_header_str}"

        self.headers = headers
        self.request_url = (
            f"{canonical_uri}?{query_string_safe}"
            if query_string_safe
            else canonical_uri
        )
        return headers

    def generate_headers_streaming(
        self,
        resource: str,
        content_length: int,
        chunk_size: int,
    ) -> Dict[str, str]:
        headers = dict(self.base_headers)

        headers["x-amz-decoded-content-length"] = str(content_length)
        headers["Content-Encoding"] = "aws-chunked"

        if 0 < chunk_size < content_length:
            no_full_chunks = content_length // chunk_size
            chunk_size_remainder = content_length % chunk_size
            sig_hexlength = len(to_base16(chunk_size)) * no_full_chunks + 1
        else:
            no_full_chunks = 1
            chunk_size_remainder = 0
            sig_hexlength = len(to_base16(content_length)) * no_full_chunks + 1

        if chunk_size_remainder > 0:
            chunks_to_write = no_full_chunks + 2
            sig_hexlength += len(to_base16(chunk_size_remainder))
        else:
            chunks_to_write = no_full_chunks + 1

        body_length = (
            content_length
            + sig_hexlength
            + (len(";chunk-signature=" + CRLF * 2) + 64) * chunks_to_write
        )
        headers["Content-Length"] = str(body_length)

        return self.generate_headers(
            resource,
            method="PUT",
            headers=headers,
            payload_hash="STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
        )

    def encapsulate_chunk(self, chunk: bytes) -> memoryview:
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256-PAYLOAD",
                self.headers["x-amz-date"],
                self.credential_scope,
                self.seed_signature,
                hash_nobytes(),
                hashlib.sha256(chunk).hexdigest(),
            ]
        )
        self.seed_signature = hmac.new(
            self.signing_key,
            string_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        sig = ";".join(
            [
                f"{to_base16(len(chunk))}",
                f"chunk-signature={self.seed_signature}{CRLF}",
            ]
        ).encode()
        return memoryview(sig + chunk + CRLF.encode())


class HttpRequestS3(HttpRequest):
    def __init__(
        self,
        connection_args: List[Dict[str, Any]],
        s3_credentials: Dict[str, str],
        endpoint_url: str,
        region: str,
        connections: int = 2,
    ) -> None:

        self.connection_args = connection_args
        self.credentials = s3_credentials
        self.endpoint_url = endpoint_url
        self.region = region

        super().__init__(
            self.connection_args, HttpConnectionMeta, connections=connections
        )

    async def __aenter__(self) -> HttpRequestS3:
        self._event_loop = asyncio.get_running_loop()
        return self

    def new_request(self) -> HttpConnectionMeta:
        return S3Connect(
            self.connection_args,
            self.credentials,
            self.endpoint_url,
            self.region,
        )


class HttpRequestS3Bucket(HttpRequestS3):
    def __init__(
        self,
        bucket: S3Bucket,
        connections: int = 2,
    ) -> None:
        super().__init__(
            bucket.connection_args,
            bucket.s3_credentials,
            bucket.endpoint_url,
            bucket.region,
            connections=connections,
        )
