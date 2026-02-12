import asyncio
from contextlib import asynccontextmanager

import uvloop

from utils.go_fast import crunch as cy_crunch
from utils.pure import crunch as py_crunch
from server import Server


@asynccontextmanager
async def timeit():
    loop = asyncio.get_running_loop()
    start = loop.time()
    yield
    end = loop.time()
    print(f"{(end - start):.2f}", "seconds")


class Examples:
    def __init__(self):
        self.server = Server()

    async def example_0(self):
        async with timeit():
            data = await self.server.get()

    async def example_1(self):
        async with timeit():
            data = await self.server.get()
            data2 = await self.server.get()

    async def example_2(self):
        async with timeit():
            data, data2 = await asyncio.gather(self.server.get(), self.server.get())

    async def example_3(self):
        async with timeit():
            data1 = await self.server.get()
            py_crunch(data1)
            data2 = await self.server.get()
            py_crunch(data2)

    async def example_4(self):
        async with timeit():
            data1 = await self.server.get()
            cy_crunch(data1)
            data2 = await self.server.get()
            cy_crunch(data2)

    async def example_5(self):
        async with timeit():
            data, data2 = await asyncio.gather(self.server.get(), self.server.get())
            py_crunch(data2)
            py_crunch(data)

    async def example_6(self):
        async with timeit():
            data, data2 = await asyncio.gather(self.server.get(), self.server.get())
            cy_crunch(data2)
            cy_crunch(data)

    async def example_7(self):
        async def consumer(pro_q: asyncio.Queue[asyncio.Task], con_q: asyncio.Queue):
            """
            We use `None` in `pro_q` to indicate end
            """
            while True:
                data = await pro_q.get()
                if data is None:
                    break
                task = loop.create_task(asyncio.to_thread(py_crunch, await data))
                pro_q.task_done()
                await con_q.put(task)

        async with timeit():
            loop = asyncio.get_running_loop()
            results = []
            producer_queue = asyncio.Queue()
            consumer_queue = asyncio.Queue()
            consumer_task = loop.create_task(consumer(producer_queue, consumer_queue))
            for _ in range(101):
                to_send = loop.create_task(self.server.get())
                await producer_queue.put(to_send)
            for idx in range(101):
                results.append(await (await consumer_queue.get()))
                if idx == 100:
                    # to shut it down
                    await producer_queue.put(None)
            await consumer_task
            print(f"Completed {len(results)} tasks: {results}")

    async def example_8(self):
        async def consumer(pro_q: asyncio.Queue[asyncio.Task], con_q: asyncio.Queue):
            """
            We use `None` in `pro_q` to indicate end
            """
            while True:
                data = await pro_q.get()
                if data is None:
                    break
                task = loop.create_task(asyncio.to_thread(cy_crunch, await data))
                pro_q.task_done()
                await con_q.put(task)

        async with timeit():
            loop = asyncio.get_running_loop()
            results = []
            producer_queue = asyncio.Queue()
            consumer_queue = asyncio.Queue()
            consumer_task = loop.create_task(consumer(producer_queue, consumer_queue))
            for _ in range(101):
                to_send = loop.create_task(self.server.get())
                await producer_queue.put(to_send)
            for idx in range(101):
                results.append(await (await consumer_queue.get()))
                if idx == 100:
                    # to shut it down
                    await producer_queue.put(None)
            await consumer_task
            print(f"Completed {len(results)} tasks: {results}")


# TODO examples
# example_7 but sync/non-queue
# asyncio.as_completed
# others that I can't think of bc I am half asleep


if __name__ == "__main__":
    async_loop = uvloop.new_event_loop()
    asyncio.set_event_loop(async_loop)
    latest = 8
    examples = Examples()
    for i in dir(examples):
        if i.startswith("example_"):
            print("running", i)
            async_loop.run_until_complete(getattr(examples, i)())
            print()
