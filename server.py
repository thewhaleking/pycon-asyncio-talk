import asyncio
import random
from typing import Any


class Server:
    @staticmethod
    async def get(wait_time: float | None = 0.5) -> int:
        if wait_time is None:
            wait_time = random.random()
        await asyncio.sleep(wait_time)
        return random.randint(1_000_000, 1_100_000)

    @staticmethod
    async def post(data: list[Any]) -> None:
        wait_time = 0.5 + (len(data) * 0.01)
        await asyncio.sleep(wait_time)
        return
