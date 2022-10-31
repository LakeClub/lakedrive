import asyncio
import concurrent

from typing import Callable, List, Tuple, Any

from .event_loop import run_on_event_loop
from ..core.handlers import ObjectStoreHandler


async def run_async_threadpool(
    function: Callable[..., Any],
    arguments_list: List[Tuple[Any]],
    max_workers: int = 8,
    return_exceptions: bool = False,
) -> List[Any]:
    """Run process on multiple threads"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        loop = asyncio.get_running_loop()
        futures = [
            loop.run_in_executor(pool, function, *args) for args in arguments_list
        ]
        responses: List[Any] = await asyncio.gather(
            *futures, return_exceptions=return_exceptions
        )
    return responses


async def run_async_processpool(
    function: Callable[..., Any],
    arguments_list: List[Tuple[ObjectStoreHandler, ObjectStoreHandler, Any]],
    max_workers: int = 2,
) -> None:
    """Run async process on multiple processors"""
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as pool:
        loop = asyncio.get_running_loop()
        futures = [
            loop.run_in_executor(pool, run_on_event_loop, *((function,) + args))
            for args in arguments_list
        ]
        await asyncio.gather(*futures, return_exceptions=False)
