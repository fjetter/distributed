from __future__ import absolute_import, division, print_function

import pickle
from time import sleep

from distributed import Semaphore, get_client
from distributed.metrics import time
from distributed.utils_test import cluster_fixture, client, gen_cluster, loop  # noqa F401
from tornado import gen


@gen_cluster(client=True)
def test_semaphore(c, s, a, b):
    semaphore = Semaphore(value=2, name="x")
    result = yield semaphore.acquire()
    assert result is True

    second = yield semaphore.acquire()
    assert second is True
    start = time()
    result = yield semaphore.acquire(timeout=0.1)
    stop = time()
    assert stop - start < 0.3
    assert result is False


@gen_cluster(client=True)
def test_serializable(c, s, a, b):

    sem = Semaphore(value=2, name="x")
    res = yield sem.acquire()
    val = yield sem.value
    assert val == 1
    assert res
    sem2 = pickle.loads(pickle.dumps(sem))
    assert sem2.name == sem.name
    assert sem2.client.scheduler.address == sem.client.scheduler.address

    first = yield sem.value
    second = yield sem2.value
    assert first == second
    assert first == 1

    res = yield sem2.acquire()
    assert res

    # Ensure that both objects access the same semaphore
    res = yield sem.acquire(timeout=0)

    assert not res
    res = yield sem2.acquire(timeout=0)

    assert not res


@gen_cluster(client=True)
def test_release(c, s, a, b):

    def f(x, semaphore=None):
        with semaphore:
            assert semaphore.name == "x"
            return x + 1

    sem = Semaphore(value=2, name="x")
    futures = c.map(f, list(range(10)), semaphore=sem)
    yield c.gather(futures)


@gen_cluster(client=True)
def test_acquires_with_zero_timeout(c, s, a, b):
    sem = Semaphore(1, "x")
    yield sem.acquire(timeout=0)
    res = yield sem.acquire(timeout=0)
    assert res is False
    yield sem.release()

    yield sem.acquire(timeout=1)
    yield sem.release()
    yield sem.acquire(timeout=1)
    yield sem.release()


def test_timeout_sync(client):
    with Semaphore(name="x"):
        assert Semaphore(1, "x").acquire(timeout=0.05) is False


def test_lock_name_only(client):

    def f(x):
        with Semaphore(name="x"):
            client = get_client()
            assert client.get_metadata("locked") is False
            client.set_metadata("locked", True)
            sleep(0.05)
            assert client.get_metadata("locked") is True
            client.set_metadata("locked", False)

    client.set_metadata("locked", False)
    futures = client.map(f, range(10))
    client.gather(futures)


@gen_cluster(client=True)
def test_close_semaphore(c, s, a, b):
    sem = Semaphore()
    yield sem.acquire()
    assert sem.name in s.extensions["semaphores"].semaphores

    yield sem.close()
    yield gen.sleep(1)
    assert sem.name not in s.extensions["semaphores"].semaphores
