import asyncio
import random


class Server:
    async def get(self, wait_time: float | None = 0.5) -> int:
        if wait_time is None:
            wait_time = random.random()
        await asyncio.sleep(wait_time)
        return 1_000_000
