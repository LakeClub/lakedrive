from typing import Dict

from lakedrive.utils.event_loop import run_on_event_loop


async def async_function(dictionary: Dict[str, str]) -> None:
    dictionary["addedKey"] = "addedValue"


def test_run_on_event_loop() -> None:
    dictionary: Dict[str, str] = {}
    run_on_event_loop(async_function, dictionary)
    assert "addedKey" in dictionary
    assert dictionary["addedKey"] == "addedValue"
