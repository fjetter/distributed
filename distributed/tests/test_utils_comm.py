from distributed.core import ConnectionPool
from distributed.utils_test import gen_cluster
from distributed.utils_comm import pack_data, gather_from_workers


def test_pack_data():
    data = {"x": 1}
    assert pack_data(("x", "y"), data) == (1, "y")
    assert pack_data({"a": "x", "b": "y"}, data) == {"a": 1, "b": "y"}
    assert pack_data({"a": ["x"], "b": "y"}, data) == {"a": [1], "b": "y"}


@gen_cluster(client=True)
def test_gather_from_workers_permissive(c, s, a, b):
    rpc = ConnectionPool()
    x = yield c.scatter({"x": 1}, workers=a.address)

    data, missing, bad_workers = yield gather_from_workers(
        {"x": [a.address], "y": [b.address]}, rpc=rpc
    )

    assert data == {"x": 1}
    assert list(missing) == ["y"]


class BrokenConnectionPool(ConnectionPool):
    def __init__(self, *args, failing_connections=0, **kwargs):
        self.cnn_count = 0
        self.failing_connections = failing_connections
        super(BrokenConnectionPool, self).__init__(*args, **kwargs)

    async def connect(self, addr, *args, **kwargs):
        self.cnn_count += 1
        comm = await super(BrokenConnectionPool, self).connect(addr, *args, **kwargs)

        async def raise_exc(self, *args, **kwargs):
            raise EnvironmentError

        if self.cnn_count <= self.failing_connections:
            comm.read = raise_exc
            comm.write = raise_exc

        self._created.add(comm)
        return comm


@gen_cluster(client=True)
def test_gather_from_workers_permissive_flaky(c, s, a, b):
    x = yield c.scatter({"x": 1}, workers=a.address)

    rpc = BrokenConnectionPool(failing_connections=100)
    data, missing, bad_workers = yield gather_from_workers({"x": [a.address]}, rpc=rpc)

    assert missing == {"x": [a.address]}
    assert bad_workers == [a.address]
