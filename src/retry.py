from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


async def with_backoff(operation: Callable[[], Awaitable[T]], attempts: int = 4, base_delay: float = 0.5) -> T:
    for attempt in range(attempts):
        try:
            return await operation()
        except Exception:
            if attempt == attempts - 1:
                raise
            await asyncio.sleep(base_delay * (2**attempt) + random.uniform(0, base_delay))
    raise AssertionError("unreachable")