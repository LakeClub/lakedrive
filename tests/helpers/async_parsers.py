import asyncio

from io import BytesIO
from functools import wraps
from typing import List, AsyncIterator, Callable, Any


def pytest_asyncio(async_func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(async_func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return asyncio.run(async_func(*args, **kwargs))

    return wrapper


async def async_iterator_to_list(iterator: AsyncIterator[Any]) -> List[Any]:
    """Convert async iterator to a non async list"""
    return [item async for item in iterator]


async def list_to_async_iterator(item_list: List[Any]) -> AsyncIterator[Any]:
    """Iterate over list async"""
    for item in item_list:
        yield item


async def bytes_async_iterator(input_bytes: bytes) -> AsyncIterator[bytes]:
    """Iterate over bytes async"""
    for b in input_bytes:
        yield b.to_bytes(1, "little")


async def buffer_to_aiterator(
    buffer: BytesIO, chunk_size_kb: int = 128
) -> AsyncIterator[bytes]:
    while True:
        chunk = buffer.read(chunk_size_kb * 1024)
        if chunk:
            yield chunk
        else:
            break
