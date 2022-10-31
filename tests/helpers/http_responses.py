from typing import AsyncIterator

from lakedrive.httplibs.objects import HttpResponse, HttpResponseError


RESPONSES = {
    "200_close": """\
HTTP/1.1 200 dummy
Connection: Close""",
    "200_content-length": """\
HTTP/1.1 200 dummy
Content-Length: 1024""",
    "200_chunked": """\
HTTP/1.1 200 dummy
""",
}


async def aiter_yield_httpresponse_403(
    input_bytes: bytes = b"",
) -> AsyncIterator[bytes]:
    for b in input_bytes:
        yield b.to_bytes(1, "little")
    raise HttpResponseError(HttpResponse(status_code="403"))


async def aiter_yield_httpresponse_404(
    input_bytes: bytes = b"",
) -> AsyncIterator[bytes]:
    for b in input_bytes:
        yield b.to_bytes(1, "little")
    raise HttpResponseError(HttpResponse(status_code="404"))


async def aiter_yield_httpresponse_500(
    input_bytes: bytes = b"",
) -> AsyncIterator[bytes]:
    for b in input_bytes:
        yield b.to_bytes(1, "little")
    raise HttpResponseError(HttpResponse(status_code="500"))
