from typing import Dict, List, Any


class ConnectConfiguration:
    def __init__(self) -> None:
        self.connection_args: List[Dict[str, Any]] = []


class HttpConnectionMeta:
    def __init__(self, config: ConnectConfiguration) -> None:
        self.config = config
        self.request_url: str = ""

    def generate_headers(
        self,
        resource: str,
        query_string: str = "",
        method: str = "GET",
        headers: Dict[str, str] = {},
        payload_hash: str = "",
    ) -> Dict[str, str]:
        return {}

    def generate_headers_streaming(
        self,
        resource: str,
        content_length: int,
        chunk_size: int,
    ) -> Dict[str, str]:
        return {}

    def encapsulate_chunk(self, chunk: bytes) -> memoryview:
        return memoryview(chunk)


class HttpResponse:
    def __init__(
        self,
        headers: Dict[str, str] = {},
        status_code: str = "503",
        body: bytes = b"",
        error_msg: str = "",
    ) -> None:
        self.status_code = status_code
        self.headers = headers
        self.body = body
        self.error_msg = error_msg


class HttpResponseError(Exception):
    """Exceptions related to HttpResponse"""

    def __init__(self, http_response: HttpResponse) -> None:
        exception_str = f"{http_response.error_msg} ({http_response.status_code})"
        super().__init__(exception_str)
        self.exception_str = exception_str
        self.status_code = http_response.status_code

    def __str__(self) -> str:
        return str(self.exception_str)
