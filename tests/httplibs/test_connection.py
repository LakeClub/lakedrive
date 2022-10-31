import pytest
import asyncio

from lakedrive.httplibs.connection import HttpConnection
from lakedrive.httplibs.objects import HttpResponse

from ..helpers.http_connection import FakeHttpConnection


class TestHttpConnectionFailures:
    """Tests aimed at triggering HttpConnection failures, to reach code that is
    not hit in a (succesful) use-case/ integration test"""

    def test_invalid_connection_args(self) -> None:
        http_conn = HttpConnection([], 0)
        with pytest.raises(ConnectionError) as error:
            asyncio.run(http_conn.get_connection())
        assert str(error.value) == "Cant setup ip(v4/v6) connection"

    def test_connection_response_incomplete_read(self) -> None:
        http_conn = FakeHttpConnection(
            read_response_exception=asyncio.IncompleteReadError
        )
        http_response = asyncio.run(http_conn.get_response_headers_safe())
        assert isinstance(http_response, HttpResponse)
        assert http_response.status_code == "408"
        assert http_response.error_msg == "IncompleteReadError"

    def test_connection_response_timeout(self) -> None:
        http_conn = FakeHttpConnection(read_response_exception=asyncio.TimeoutError)
        http_response = asyncio.run(http_conn.get_response_headers_safe())
        assert isinstance(http_response, HttpResponse)
        assert http_response.status_code == "408"
        assert http_response.error_msg == "TimeoutError"

    def test_connection_response_incomplete_response(self) -> None:
        http_conn = FakeHttpConnection()
        http_response = asyncio.run(http_conn.get_response_headers_safe())
        assert isinstance(http_response, HttpResponse)
        assert http_response.status_code == "408"
        assert http_response.error_msg == "IncompleteReadError"

    def test_connection_verify_connection(self) -> None:
        http_conn = FakeHttpConnection(reader_at_eof=True)
        assert http_conn._verify_connection() is False

        http_conn = FakeHttpConnection(writer_extra_info={})
        assert http_conn._verify_connection() is False

        http_conn = FakeHttpConnection()
        assert http_conn._verify_connection() is True

    def test_connection_execute_empty(self) -> None:
        http_conn = FakeHttpConnection()
        http_response = asyncio.run(http_conn.execute(b"", retries=1))
        assert isinstance(http_response, HttpResponse)
        assert http_response.status_code == "408"
        assert http_response.error_msg == "IncompleteReadError"
