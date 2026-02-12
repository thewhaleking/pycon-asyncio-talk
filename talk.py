import asyncio
from contextlib import asynccontextmanager

import uvloop

from utils.go_fast import crunch as cy_crunch
from utils.pure import crunch as py_crunch
from server import Server

server = Server()


@asynccontextmanager
async def timeit():
    loop = asyncio.get_running_loop()
    start = loop.time()
    yield
    end = loop.time()
    print(f"{(end - start):.2f}", "seconds")


async def example_0():
    async with timeit():
        data = await server.get()


async def example_1():
    async with timeit():
        data = await server.get()
        data2 = await server.get()


async def example_2():
    async with timeit():
        data, data2 = await asyncio.gather(server.get(), server.get())


async def example_3():
    async with timeit():
        data1 = await server.get()
        py_crunch(data1)
        data2 = await server.get()
        py_crunch(data2)


async def example_4():
    async with timeit():
        data1 = await server.get()
        cy_crunch(data1)
        data2 = await server.get()
        cy_crunch(data2)


async def example_5():
    async with timeit():
        data, data2 = await asyncio.gather(server.get(), server.get())
        py_crunch(data2)
        py_crunch(data)


async def example_6():
    async with timeit():
        data, data2 = await asyncio.gather(server.get(), server.get())
        cy_crunch(data2)
        cy_crunch(data)


async def example_7():
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
            to_send = loop.create_task(server.get())
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
    uvloop.run(example_7())
