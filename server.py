import asyncio
import random
from typing import Any


class RecordKeeper:
    def __init__(self, page_size: int = 100, max_records: int = 1000):
        self.page_size = page_size
        self.max_records = max_records
        self.buffer = iter([])
        self.records = []

    def __aiter__(self):
        return self

    async def __anext__(self) -> int:
        record, successfully_retrieved = self._get_next_record()
        if successfully_retrieved:
            return record
        next_record = await self._get_next_page()
        if next_record is not None:
            return next_record
        raise StopAsyncIteration

    def _get_next_record(self) -> tuple[int | None, bool]:
        try:
            return next(self.buffer), True
        except StopIteration:
            return None, False

    async def _get_next_page(self) -> int | None:
        if len(self.records) >= self.max_records:
            return None
        print("Fetching next page...")
        await asyncio.sleep(0.5)
        retrieved = [random.randint(10_000, 1_000_000) for _ in range(self.page_size)]
        self.buffer = iter(retrieved)
        self.records.extend(retrieved)
        return next(self.buffer)


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

    @staticmethod
    async def records(page_size: int = 100, max_records: int = 1000) -> RecordKeeper:
        record_keeper = RecordKeeper(page_size=page_size, max_records=max_records)
        return record_keeper
