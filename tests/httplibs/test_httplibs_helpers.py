import socket

from ipaddress import ip_address, IPv4Address, IPv6Address

from lakedrive.httplibs.helpers import headers_to_string, http_connection_args


def validate_connection_args(url_str: str, hostname: str, port: int) -> None:
    connection_args = http_connection_args(url_str)
    isinstance(connection_args, list)
    for ca in connection_args:
        assert ca["proto"] == 6  # tcp
        assert ca["server_hostname"] == hostname
        assert ca["port"] == port
        assert ca["family"] in [socket.AF_INET, socket.AF_INET6]
        assert isinstance(ca["host"], str)

        if ca["family"] == socket.AF_INET:
            assert type(ip_address(ca["host"])) == IPv4Address
        if ca["family"] == socket.AF_INET6:
            assert type(ip_address(ca["host"])) == IPv6Address

        assert ca["flags"] == socket.AI_NUMERICHOST | socket.AI_NUMERICSERV


def test_headers_to_string() -> None:
    headers = {
        "key_one": "value_one",
        "key_two": "value_two",
        "key_three": "value_three",
    }
    headers_str = "key_one: value_one\r\n\
key_two: value_two\r\n\
key_three: value_three\r\n"
    assert headers_to_string(headers) == headers_str


def test_http_connection_args() -> None:
    validate_connection_args("https://localhost:443/source", "localhost", 443)
    validate_connection_args("https://localhost/source", "localhost", 443)
    validate_connection_args("http://localhost/source", "localhost", 80)
    # add external host to test both ipv4 and ipv6
    validate_connection_args("https://example.com/source", "example.com", 443)
