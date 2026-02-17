import asyncio
import time
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
            await asyncio.gather(
                asyncio.to_thread(cy_crunch, data), asyncio.to_thread(cy_crunch, data2)
            )

    async def restrict_executor(self):
        """
        Demonstrates differently using the loop executor
        """
        async with timeit():
            loop = asyncio.get_running_loop()
            executor = ThreadPoolExecutor(max_workers=1)
            data, data2 = await asyncio.gather(self.server.get(), self.server.get())
            await asyncio.gather(
                loop.run_in_executor(executor, cy_crunch, data),
                loop.run_in_executor(executor, cy_crunch, data2),
            )
        input("Waiting")
        async with timeit():
            # Note we can also assign the executor to the whole loop
            loop.set_default_executor(executor)
            data, data2 = await asyncio.gather(self.server.get(), self.server.get())
            await asyncio.gather(
                asyncio.to_thread(cy_crunch, data), asyncio.to_thread(cy_crunch, data2)
            )
        input("Waiting")
        # there's virtually no speed difference here because we're only using 2 items. Let's increase that
        loop.set_default_executor(None)  # clear the default executor
        async with timeit():
            data_list = await asyncio.gather(*[self.server.get() for _ in range(1_000)])
            await asyncio.gather(*[asyncio.to_thread(cy_crunch, d) for d in data_list])
        # that's pretty fast. But we don't want to utilise the breadth of our threads
        input("Waiting")
        async with timeit():
            loop.set_default_executor(executor)
            data_list = await asyncio.gather(*[self.server.get() for _ in range(1_000)])
            await asyncio.gather(*[asyncio.to_thread(cy_crunch, d) for d in data_list])

    async def semaphore(self):
        """
        Use a semaphore to restrict your throughput
        """
        sema4 = asyncio.Semaphore(20)

        async def fetcher(sem: asyncio.Semaphore | None) -> int:
            if sem is None:
                return await self.server.get()
            else:
                async with sem:
                    return await self.server.get()

        print("Without semaphore")
        async with timeit():
            async with asyncio.TaskGroup() as tg:
                tasks = set()
                for _ in range(100):
                    tasks.add(tg.create_task(fetcher(None)))
        input("Waiting")
        print("With semaphore")
        async with timeit():
            async with asyncio.TaskGroup() as tg:
                tasks = set()
                for _ in range(100):
                    tasks.add(tg.create_task(fetcher(sema4)))

    async def use_queue(self):
        """
        Using queues to manage the data pulling and processing
        """

        async def consumer(
            pro_q: asyncio.Queue[asyncio.Task[int]],
            con_q: asyncio.Queue[asyncio.Task[float]],
        ):
            """
            We use `None` in `pro_q` to indicate end (called a 'sentinel')
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
        """
        Using tasks instead of just raw coroutines.

        Tasks begin execution (usually) at the next iteration of the event loop
        """
        loop = asyncio.get_running_loop()
        async with timeit():
            query_tasks = [loop.create_task(self.server.get()) for _ in range(100)]
            queried = await asyncio.gather(*query_tasks)
            for task in queried:
                cy_crunch(task)
        async with timeit():
            query_tasks = [loop.create_task(self.server.get()) for _ in range(100)]
            queried = await asyncio.gather(*query_tasks)
            await asyncio.gather(
                *[asyncio.to_thread(cy_crunch, task) for task in queried]
            )

    # Python 3.13+

    async def as_completed(self):
        """
        Using `as_completed` to perform an action on tasks, as they complete
        """
        loop = asyncio.get_running_loop()
        # by giving a set wait time, the tasks will all be received linearly (0 - 9)
        query_tasks = [
            loop.create_task(self.server.get(0.5), name=str(tsk)) for tsk in range(10)
        ]
        crunch_tasks = []
        task: asyncio.Task[int]
        print(len(query_tasks))
        async for task in asyncio.as_completed(query_tasks):
            print("Processing", task.get_name())
            crunch_tasks.append(asyncio.to_thread(cy_crunch, await task))
        await asyncio.gather(*crunch_tasks)
        input("Waiting")
        # passing `None` to `self.server.get` gives us a random wait time (0-1)
        query_tasks = [
            loop.create_task(self.server.get(None), name=str(tsk)) for tsk in range(10)
        ]
        crunch_tasks = []
        task: asyncio.Task[int]
        print(len(query_tasks))
        async for task in asyncio.as_completed(query_tasks):
            # demonstrates the non-linear order of completion
            print("Processing", task.get_name())
            crunch_tasks.append(asyncio.to_thread(cy_crunch, await task))
        await asyncio.gather(*crunch_tasks)

    async def queue_excels(self, num_fetchers: int | str = 100):
        """
        Queues excel at situations like this where you need to constantly fetch data, but not
        all at the same time

        :param num_fetchers: Should be determined through your own benchmarking/profiling
        """
        num_fetchers = int(num_fetchers)
        loop = asyncio.get_running_loop()

        async def producer(
            prod_q: asyncio.Queue[int | None],
            con_q: asyncio.Queue[asyncio.Task[int] | None],
        ) -> None:
            while True:
                from_q = await prod_q.get()
                if from_q is None:
                    print("Producer done")
                    return None
                r = await self.server.get()
                process_ = loop.create_task(asyncio.to_thread(cy_crunch, r))
                await con_q.put(process_)
                prod_q.task_done()

        async def consumer(
            con_q: asyncio.Queue[asyncio.Task[int] | None], send_threshold: int = 1_000
        ):
            async def send(data_):
                await self.server.post(data=data_)

            to_send = []
            while True:
                try:
                    recd = await asyncio.wait_for(con_q.get(), 0.5)
                    if recd is None:
                        if to_send:
                            await send(to_send)
                        print("Consumer done")
                        return None
                    data = await recd
                    to_send.append(data)
                    con_q.task_done()
                except TimeoutError:
                    if to_send:
                        await send(to_send)
                        to_send.clear()
                    continue

                if len(to_send) >= send_threshold:
                    await send(to_send)
                    to_send.clear()

        producer_queue = asyncio.Queue()
        consumer_queue = asyncio.Queue()

        producer_tasks = [
            loop.create_task(producer(producer_queue, consumer_queue))
            for _ in range(num_fetchers)
        ]
        consumer_task = loop.create_task(consumer(consumer_queue, send_threshold=1_000))

        async with timeit():
            for num in range(1_000):
                await producer_queue.put(num)
            for _ in range(num_fetchers):
                await producer_queue.put(None)
            for t in producer_tasks:
                await t
            await consumer_queue.put(None)
            await consumer_task

    async def async_for(self, page_size: int = 10, max_records: int = 100):
        """
        Having an interator that automatically runs a next async fetch can be highly useful
        """
        records = await self.server.records(page_size, max_records)
        async for record in records:
            print(record)


# TODO examples
# example_7 but sync/non-queue
# others that I can't think of bc I am half asleep


if __name__ == "__main__":
    import sys
    import inspect

    async_loop = uvloop.new_event_loop()
    asyncio.set_event_loop(async_loop)
    examples = Examples()
    try:
        selection = sys.argv[1]
        coro = getattr(examples, selection)
        print("Running", selection, coro.__doc__)
        time.sleep(5)
        args = sys.argv[2:]
        async_loop.run_until_complete(coro(*args))
    except Exception as e:
        if isinstance(e, IndexError):
            print(
                "You forgot the type the name of the example you want. You can choose:"
            )
        if isinstance(e, AttributeError):
            print(f"`{sys.argv[1]}` not recognised")
        for i in dir(examples):
            if inspect.iscoroutinefunction(getattr(examples, i)):
                print(" -", i)
