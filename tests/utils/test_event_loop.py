import pytest

from lakedrive.utils.event_loop import run_on_event_loop

from ..helpers.async_parsers import pytest_asyncio


async def increment(start: int, step: int = 1) -> int:
    return start + step


def test_run_on_event_loop() -> None:
    assert run_on_event_loop(increment, 1) == 2
    assert run_on_event_loop(increment, 1, step=2) == 3


@pytest_asyncio
async def test_run_loop_in_loop() -> None:
    with pytest.raises(Exception) as error:
        run_on_event_loop(increment, 1)
    assert (
        str(error.value)
        == "Running eventloop detected. \
Try use 'await increment(...)' instead."
    )
