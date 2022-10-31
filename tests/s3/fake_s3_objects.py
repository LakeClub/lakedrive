from __future__ import annotations
import json

from typing import Any

from lakedrive.httplibs.objects import HttpResponse

from ..helpers.http_connection import ConnectConfiguration, FakeHttpConnect


class FakeHttpRequestS3:
    """"""

    def __init__(
        self,
        config: ConnectConfiguration,
        connections: int = 2,
    ) -> None:
        self.config = config

    def new_request(self) -> FakeHttpConnect:
        return FakeHttpConnect(self.config)

    async def __aenter__(self) -> FakeHttpRequestS3:
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass

    def custom_response(self, response: HttpResponse) -> HttpResponse:
        return response

    async def head(
        self,
        resource: str = "",
        parameter_str: str = "",
        tid: int = 0,
    ) -> HttpResponse:
        http_connection = self.new_request()
        input_headers = http_connection.generate_headers(
            resource,
            query_string=parameter_str,
            method="HEAD",
        )
        response_headers = {
            "input_headers": json.dumps(input_headers, default=str),
        }
        return self.custom_response(
            HttpResponse(status_code="200", headers=response_headers)
        )
