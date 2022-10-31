from lakedrive.api import Request


class MockApiRequestPermissionError(Request):
    def __init__(self, target: str):
        super().__init__(target)

    async def _parse_request(self) -> None:
        raise PermissionError("MockApiRequestPermissionError")
