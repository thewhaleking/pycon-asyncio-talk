import asyncio
from concurrent.futures import ThreadPoolExecutor
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

    async def single(self):
        """
        What most people who start writing asyncio do
        """
        async with timeit():
            data = await self.server.get()

    async def sequential(self):
        """
        How most people continue writing asyncio
        """
        async with timeit():
            data = await self.server.get()
            data2 = await self.server.get()

    async def use_gather(self):
        """
        Gathering. The first asyncio optimisation you've probably used
        """
        async with timeit():
            data, data2 = await asyncio.gather(self.server.get(), self.server.get())

    async def py_processing(self):
        """
        How you might write some data processing with asyncio
        """
        async with timeit():
            data1 = await self.server.get()
            py_crunch(data1)
            data2 = await self.server.get()
            py_crunch(data2)

    async def cy_processing(self):
        """
        When you want CPU-bound speed, you write in Cython (or similar)
        """
        async with timeit():
            data1 = await self.server.get()
            cy_crunch(data1)
            data2 = await self.server.get()
            cy_crunch(data2)

    async def gather_and_py(self):
        """
        You've optimised your data pull, but it's not as fast as it could be
        """
        async with timeit():
            data, data2 = await asyncio.gather(self.server.get(), self.server.get())
            py_crunch(data2)
            py_crunch(data)

    async def gather_and_cy(self):
        """
        You've optimised your data pull, and the data processing. Can't get much faster, right?
        """
        async with timeit():
            data, data2 = await asyncio.gather(self.server.get(), self.server.get())
            cy_crunch(data2)
            cy_crunch(data)

    async def gather_and_cy_to_thread(self):
        """
        Any extension that releases the GIL will allow us to utilise threading to improve CPU-bound
        code
        """
        async with timeit():
            data, data2 = await asyncio.gather(self.server.get(), self.server.get())
            await asyncio.gather(asyncio.to_thread(cy_crunch, data), asyncio.to_thread(cy_crunch, data2))

    async def restrict_executor(self):
        """
        Demonstrates differently using the loop executor
        """
        async with timeit():
            loop = asyncio.get_running_loop()
            executor = ThreadPoolExecutor(max_workers=1)
            data, data2 = await asyncio.gather(self.server.get(), self.server.get())
            await asyncio.gather(loop.run_in_executor(executor, cy_crunch, data), loop.run_in_executor(executor, cy_crunch, data2))
        input()
        async with timeit():
            # Note we can also assign the executor to the whole loop
            loop.set_default_executor(executor)
            data, data2 = await asyncio.gather(self.server.get(), self.server.get())
            await asyncio.gather(asyncio.to_thread(cy_crunch, data), asyncio.to_thread(cy_crunch, data2))
        input()
        # there's virtually no speed difference here because we're only using 2 items. Let's increase that
        loop.set_default_executor(None)  # clear the default executor
        async with timeit():
            data_list = await asyncio.gather(*[self.server.get() for _ in range(1_000)])
            await asyncio.gather(*[asyncio.to_thread(cy_crunch, d) for d in data_list])
        # that's pretty fast. But we don't want to utilise the breadth of our threads
        input()
        async with timeit():
            loop.set_default_executor(executor)
            data_list = await asyncio.gather(*[self.server.get() for _ in range(1_000)])
            await asyncio.gather(*[asyncio.to_thread(cy_crunch, d) for d in data_list])


    async def use_queue(self):
        """
        Using queues to manage the data pulling and processing
        """
        async def consumer(pro_q: asyncio.Queue[asyncio.Task[int]], con_q: asyncio.Queue[asyncio.Task[float]]):
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

    async def tasks(self):
        loop = asyncio.get_running_loop()
        async with timeit():
            query_tasks = [loop.create_task(self.server.get()) for _ in range(100)]
            queried = await asyncio.gather(*query_tasks)
            for i in queried:
                cy_crunch(i)
        async with timeit():
            query_tasks = [loop.create_task(self.server.get()) for _ in range(100)]
            queried = await asyncio.gather(*query_tasks)
            await asyncio.gather(*[asyncio.to_thread(cy_crunch, i) for i in queried])

    # Python 3.13+

    async def as_completed(self):
        loop = asyncio.get_running_loop()
        # by giving a set wait time, the tasks will all be received linearly (0 - 9)
        query_tasks = [loop.create_task(self.server.get(0.5), name=str(i)) for i in range(10)]
        crunch_tasks = []
        task: asyncio.Task[int]
        print(len(query_tasks))
        async for task in asyncio.as_completed(query_tasks):
            print("Processing", task.get_name())
            crunch_tasks.append(asyncio.to_thread(cy_crunch, await task))
        await asyncio.gather(*crunch_tasks)
        input()
        # passing `None` to `self.server.get` gives us a random wait time (0-1)
        query_tasks = [loop.create_task(self.server.get(None), name=str(i)) for i in range(10)]
        crunch_tasks = []
        task: asyncio.Task[int]
        print(len(query_tasks))
        async for task in asyncio.as_completed(query_tasks):
            # demonstrates the non-linear order of completion
            print("Processing", task.get_name())
            crunch_tasks.append(asyncio.to_thread(cy_crunch, await task))
        await asyncio.gather(*crunch_tasks)

# TODO examples
# example_7 but sync/non-queue
# others that I can't think of bc I am half asleep


if __name__ == "__main__":
    async_loop = uvloop.new_event_loop()
    asyncio.set_event_loop(async_loop)
    examples = Examples()
    async_loop.run_until_complete(examples.restrict_executor())
    # for i in dir(examples):
    #     if i.startswith("example_"):
    #         print("running", i)
    #         async_loop.run_until_complete(getattr(examples, i)())
    #         print()
