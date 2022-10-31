import asyncio
from typing import Callable, Coroutine, Any


def run_on_event_loop(
    async_func: Callable[..., Coroutine[Any, Any, Any]],
    *args: Any,
    **kwargs: Any,
) -> Any:
    try:
        _ = asyncio.get_running_loop()
        error_msg = f"Running eventloop detected. \
Try use 'await {async_func.__name__}(...)' instead."
        raise Exception(error_msg)

    except RuntimeError:
        # no current event loop -- run async through asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        async_task = asyncio.ensure_future(async_func(*args, **kwargs))
        result = loop.run_until_complete(async_task)
        loop.close()
        return result
