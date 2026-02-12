import asyncio
import random


class Server:
    async def get(self) -> int:
        await asyncio.sleep(0.5)
        return 1_000_000
