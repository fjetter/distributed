import asyncio
from unittest import mock

import pytest

from distributed import Nanny, wait
from distributed.core import CommClosedError
from distributed.utils_test import gen_cluster, inc, slowinc

pytestmark = pytest.mark.ci1


@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_abort_execution_release(c, s, w):
    fut = c.submit(slowinc, 1, delay=0.5, key="f1")

    async def wait_for_exec(dask_worker):
        while (
            fut.key not in dask_worker.tasks
            or dask_worker.tasks[fut.key].state != "executing"
        ):
            await asyncio.sleep(0.01)

    await c.run(wait_for_exec)

    fut.release()
    fut2 = c.submit(inc, 1, key="f2")

    async def observe(dask_worker):
        cancelled = False
        while (
            fut.key in dask_worker.tasks
            and dask_worker.tasks[fut.key].state != "released"
        ):
            if dask_worker.tasks[fut.key].state == "cancelled":
                cancelled = True
            await asyncio.sleep(0.005)
        return cancelled

    assert await c.run(observe)
    await fut2
    del fut2


@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny, timeout=3000)
async def test_abort_execution_reschedule(c, s, w):
    fut = c.submit(slowinc, 1, delay=1)

    async def wait_for_exec(dask_worker):
        while (
            fut.key not in dask_worker.tasks
            or dask_worker.tasks[fut.key].state != "executing"
        ):
            await asyncio.sleep(0.01)

    await c.run(wait_for_exec)

    fut.release()

    async def observe(dask_worker):
        while (
            fut.key in dask_worker.tasks
            and dask_worker.tasks[fut.key].state != "released"
        ):
            if dask_worker.tasks[fut.key].state == "cancelled":
                return
            await asyncio.sleep(0.005)

    assert await c.run(observe)
    fut = c.submit(slowinc, 1, delay=1)
    await fut


@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_abort_execution_add_as_dependency(c, s, w):
    fut = c.submit(slowinc, 1, delay=1)

    async def wait_for_exec(dask_worker):
        while (
            fut.key not in dask_worker.tasks
            or dask_worker.tasks[fut.key].state != "executing"
        ):
            await asyncio.sleep(0.01)

    await c.run(wait_for_exec)

    fut.release()

    async def observe(dask_worker):
        while (
            fut.key in dask_worker.tasks
            and dask_worker.tasks[fut.key].state != "released"
        ):
            if dask_worker.tasks[fut.key].state == "cancelled":
                return
            await asyncio.sleep(0.005)

    assert await c.run(observe)
    fut = c.submit(slowinc, 1, delay=1)
    fut = c.submit(slowinc, fut, delay=1)
    await fut


@gen_cluster(client=True, nthreads=[("", 1)] * 2, Worker=Nanny)
async def test_abort_execution_to_fetch(c, s, a, b):
    fut = c.submit(slowinc, 1, delay=2, key="f1", workers=[a.worker_address])

    async def wait_for_exec(dask_worker):
        while (
            fut.key not in dask_worker.tasks
            or dask_worker.tasks[fut.key].state != "executing"
        ):
            await asyncio.sleep(0.01)

    await c.run(wait_for_exec, workers=[a.worker_address])

    fut.release()

    async def observe(dask_worker):
        while (
            fut.key in dask_worker.tasks
            and dask_worker.tasks[fut.key].state != "released"
        ):
            if dask_worker.tasks[fut.key].state == "cancelled":
                return
            await asyncio.sleep(0.005)

    assert await c.run(observe)

    # While the first worker is still trying to compute f1, we'll resubmit it to
    # another worker with a smaller delay. The key is still the same
    fut = c.submit(slowinc, 1, delay=0, key="f1", workers=[b.worker_address])
    # then, a must switch the execute to fetch. Instead of doing so, it will
    # simply re-use the currently computing result.
    fut = c.submit(slowinc, fut, delay=1, workers=[a.worker_address], key="f2")
    await fut


@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_worker_find_missing(c, s, *workers):

    fut = c.submit(slowinc, 1, delay=0.5, workers=[workers[0].address])
    await wait(fut)
    # We do not want to use proper API since the API usually ensures that the cluster is informed properly. We want to
    del workers[0].data[fut.key]
    del workers[0].tasks[fut.key]

    # Actually no worker has the data, the scheduler is supposed to reschedule
    await c.submit(inc, fut, workers=[workers[1].address])


from distributed.utils_test import slow_deser


@gen_cluster(client=True, nthreads=[("", 1)] * 2, Worker=Nanny)
async def test_worker_stream_died_during_comm(c, s, *workers):

    fut = c.submit(slow_deser, 1, delay=2, workers=[workers[0].worker_address])
    await wait(fut)

    def _id(val):
        return val

    # Actually no worker has the data, the scheduler is supposed to reschedule
    res = c.submit(_id, fut, workers=[workers[1].worker_address])

    dep_key = fut.key
    remote_address = workers[0].worker_address

    async def observe(dask_worker):
        while (
            dep_key not in dask_worker.tasks
            or not dask_worker.tasks[dep_key].state == "flight"
            or not dask_worker.tasks[dep_key] in dask_worker._in_flight_tasks
        ):
            await asyncio.sleep(0.001)
        print("closing comms")
        for collection in [dask_worker.rpc.occupied, dask_worker.rpc.available]:
            while collection:
                addr, comms = collection.popitem()
                for comm in comms:
                    comm.abort()

    await c.run(observe, workers=[workers[1].worker_address])
    await res


# from contextlib import suppress


# @pytest.mark.repeat(100)
# @gen_cluster(client=True, Worker=Nanny)
# async def test_flight_to_executing_via_cancelled_resumed(c, s, a, b):
#     fut1 = c.submit(
#         slow_serialize, 1, delay=1, workers=[a.worker_address], allow_other_workers=True
#     )
#     await wait(fut1)

#     def _id(x):
#         return x.data

#     fut2 = c.submit(_id, fut1, workers=[b.worker_address])

#     async def observe(dask_worker):
#         while (
#             fut1.key not in dask_worker.tasks
#             or dask_worker.tasks[fut1.key].state != "flight"
#         ):
#             await asyncio.sleep(0)

#     await c.run(observe, workers=[b.worker_address])
#     proc = psutil.Process(a.pid)

#     # Close in scheduler to ensure we transition and reschedule task properly
#     await s.close_worker(worker=a.worker_address)

#     # Killing the process should cause a ""Worker stream died during communication" error
#     proc.kill()
#     assert await fut2 == 1

#     b_story = (
#         await c.run(
#             lambda dask_worker: dask_worker.story(fut1.key), workers=[b.worker_address]
#         )
#     )[b.worker_address]
#     assert any("receive-dep-failed" in msg for msg in b_story)
#     assert any("missing-dep" in msg for msg in b_story)
#     if not any("cancelled" in msg for msg in b_story):
#         breakpoint()
#     assert any("resumed" in msg for msg in b_story)

# @pytest.mark.repeat(100)
@gen_cluster(client=True)
async def test_flight_to_executing_via_cancelled_resumed(c, s, a, b):
    import asyncio

    import distributed

    lock = asyncio.Lock()
    await lock.acquire()

    async def wait_and_raise(*args, **kwargs):
        async with lock:
            raise CommClosedError()

    with mock.patch.object(
        distributed.worker,
        "get_data_from_worker",
        side_effect=wait_and_raise,
    ):
        fut1 = c.submit(inc, 1, workers=[a.address], allow_other_workers=True)

        fut2 = c.submit(inc, fut1, workers=[b.address])

        async def observe(dask_worker):
            while (
                fut1.key not in dask_worker.tasks
                or dask_worker.tasks[fut1.key].state != "flight"
            ):
                await asyncio.sleep(0)

        await c.run(observe, workers=[b.address])

        # Close in scheduler to ensure we transition and reschedule task properly
        await s.close_worker(worker=a.address)

        while fut1.key not in b.tasks or b.tasks[fut1.key].state != "resumed":
            await asyncio.sleep(0)

    lock.release()
    assert await fut2 == 3

    b_story = b.story(fut1.key)
    assert any("receive-dep-failed" in msg for msg in b_story)
    assert any("missing-dep" in msg for msg in b_story)
    if not any("cancelled" in msg for msg in b_story):
        breakpoint()
    assert any("resumed" in msg for msg in b_story)
