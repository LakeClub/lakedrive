from lakedrive.httplibs.objects import HttpResponse, HttpResponseError


def test_dummy_http_response() -> None:
    headers = {
        "DummyKey": "Value",
        "DummyInt": "100",
    }
    http_response = HttpResponse(headers)
    assert http_response.headers == headers
    assert http_response.status_code == "503"
    assert http_response.body == b""
    assert http_response.error_msg == ""


def test_valid_http_response() -> None:
    headers = {
        "some_key": "some_value",
    }
    body = b"reply"

    http_response = HttpResponse(status_code="200", headers=headers, body=body)
    assert http_response.headers == headers
    assert http_response.status_code == "200"
    assert http_response.body == body
    assert http_response.error_msg == ""


def test_invalid_http_response() -> None:
    headers = {
        "some_key": "some_value",
    }
    error_msg = "Invalid credentials"

    http_response = HttpResponse(
        status_code="403", headers=headers, error_msg=error_msg
    )
    assert http_response.headers == headers
    assert http_response.status_code == "403"
    assert http_response.body == b""
    assert http_response.error_msg == error_msg


def test_http_response_exception() -> None:
    headers = {
        "some_key": "some_value",
    }
    error_msg = "Page not found"

    try:
        raise HttpResponseError(
            HttpResponse(status_code="404", headers=headers, error_msg=error_msg)
        )
    except HttpResponseError as error:
        assert error.__str__() == f"{error_msg} (404)"
