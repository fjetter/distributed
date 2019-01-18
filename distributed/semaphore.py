from __future__ import absolute_import, division, print_function

import uuid
from collections import defaultdict, deque
from functools import partial

import dask
import tornado.locks
import tornado.queues
from tornado import gen

from .client import Client, _get_global_client
from .utils import PeriodicCallback, log_errors, parse_timedelta
from .worker import get_client, get_worker
from toolz.dicttoolz import valmap
from .metrics import time


class _Watch(object):
    def __init__(self, duration=None):
        self.duration = duration
        self.started_at = None

    def start(self):
        self.started_at = time()

    def leftover(self):
        if self.duration is None:
            return None
        else:
            elapsed = time() - self.started_at
            return max(0, self.duration - elapsed)


class SemaphoreExtension(object):
    """ An extension for the scheduler to manage Semaphores

    This adds the following routes to the scheduler

    * semaphore_acquire
    * semaphore_release
    * semaphore_create
    """

    def __init__(self, scheduler):
        self.locks = defaultdict(tornado.locks.Lock)
        self.scheduler = scheduler
        self.leases = defaultdict(deque)
        self.events = defaultdict(tornado.locks.Event)
        self.max_leases = dict()
        self.leases_per_client = defaultdict(partial(defaultdict, deque))
        self.scheduler.handlers.update(
            {
                "semaphore_create": self.create,
                "semaphore_acquire": self.acquire,
                "semaphore_release": self.release,
            }
        )

        self.scheduler.extensions["semaphores"] = self
        self.pc_validate_leases = PeriodicCallback(
            self._validate_leases,
            parse_timedelta(
                dask.config.get("distributed.scheduler.locks.lease-validation-interval")
            ),
            io_loop=self.scheduler.loop,
        )
        self.pc_validate_leases.start()
        self._validation_running = False

    @gen.coroutine
    def create(
        self, stream=None, name=None, client=None, timeout=None, max_leases=None
    ):
        if name not in self.leases:
            assert isinstance(max_leases, int), max_leases
            self.max_leases[name] = max_leases
        else:
            if max_leases != self.max_leases[name]:
                raise ValueError(
                    "Inconsistent max leases: %s, expected: %s"
                    % (max_leases, self.max_leases[name])
                )

    @gen.coroutine
    def _get_lease(self, client, name, identifier):
        # We should make sure that the client is already properly registered with the scheduler
        # otherwise the lease validation will mop up every acquired release immediately
        while client not in self.scheduler.clients:
            yield
        with (yield self.locks[name].acquire()):
            result = True
            if len(self.leases[name]) < self.max_leases[name]:
                self.leases[name].append(identifier)
                self.leases_per_client[client][name].append(identifier)
            else:
                result = False
            raise gen.Return(result)

    @gen.coroutine
    def acquire(
        self, stream=None, name=None, client=None, timeout=None, identifier=None
    ):
        with log_errors():
            if isinstance(name, list):
                name = tuple(name)
            w = _Watch(timeout)
            w.start()

            while True:
                # Reset the event and try to get a release. The event will be set if the state
                # is changed and helps to identify when it is worth to retry an acquire
                self.events[name].clear()
                future = self._get_lease(client, name, identifier)
                if timeout is not None:
                    future = gen.with_timeout(w.leftover(), future)
                try:
                    result = yield future
                except gen.TimeoutError:
                    result = False

                # If acquiring fails, we wait for the event to be set, i.e. something has
                # been released and we can try to acquire again (continue loop)
                if not result:
                    future = self.events[name].wait()
                    if timeout is not None:
                        future = gen.with_timeout(w.leftover(), future)
                    try:
                        yield future
                        continue
                    except gen.TimeoutError:
                        result = False
                raise gen.Return(result)

    @gen.coroutine
    def release(self, stream=None, name=None, client=None, identifier=None):
        with log_errors():
            if isinstance(name, list):
                name = tuple(name)
            if name in self.leases and identifier in self.leases[name]:
                self.scheduler.loop.add_callback(
                    self._release_value, name, client, identifier
                )
            else:
                raise ValueError("Semaphore released too many times.")

    @gen.coroutine
    def _release_value(self, name, client, identifier):
        with (yield self.locks[name].acquire()):
            self.leases_per_client[client][name].remove(identifier)
            self.leases[name].remove(identifier)
            self.events[name].set()

    def _release_client(self, client):
        semaphore_names = list(self.leases_per_client[client])
        for name in semaphore_names:
            ids = list(self.leases_per_client[client][name])
            for _id in list(ids):
                self.scheduler.loop.add_callback(
                    self._release_value, name=name, client=client, identifier=_id
                )

    @gen.coroutine
    def _validate_leases(self):
        if not self._validation_running:
            self._validation_running = True
            known_clients = set(self.leases_per_client.keys())
            scheduler_clients = set(self.scheduler.clients.keys())
            for client in known_clients - scheduler_clients:
                client_has_leases = sum(
                    valmap(len, self.leases_per_client[client]).values()
                )
                if client_has_leases:
                    self._release_client(client)
            else:
                self._validation_running = False


class Semaphore(object):
    def __init__(self, max_leases=1, name=None, client=None):
        self.client = client or _get_global_client() or get_worker().client
        self.id = uuid.uuid4().hex
        self.name = name or "semaphore-" + uuid.uuid4().hex
        self.max_leases = max_leases

        if self.client.asynchronous:
            self._started = self.client.scheduler.semaphore_create(
                name=self.name, max_leases=max_leases
            )
        else:
            self.client.sync(
                self.client.scheduler.semaphore_create,
                name=self.name,
                max_leases=max_leases,
            )
            self._started = gen.moment

    def __await__(self):
        @gen.coroutine
        def _():
            yield self._started
            raise gen.Return(self)

        return _().__await__()

    def acquire(self, timeout=None):
        """
        Acquire a semaphore.

        If the internal counter is greater than zero, decrement it by one and return True immediately.
        If it is zero, wait until a release() is called and return True.
        """
        return self.client.sync(
            self.client.scheduler.semaphore_acquire,
            name=self.name,
            timeout=timeout,
            client=self.client.id,
            identifier=self.id,
        )

    def release(self):
        """
        Release a semaphore.

        Increment the internal counter by one.
        """

        """ Release the lock if already acquired """
        return self.client.sync(
            self.client.scheduler.semaphore_release,
            name=self.name,
            client=self.client.id,
            identifier=self.id,
        )

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *args, **kwargs):
        self.release()

    @gen.coroutine
    def __aenter__(self):
        yield self.acquire()
        raise gen.Return(self)

    @gen.coroutine
    def __aexit__(self, *args, **kwargs):
        yield self.release()

    def __getstate__(self):
        return (self.name, self.client.scheduler.address, self.max_leases)

    def __setstate__(self, state):
        name, address, max_leases = state
        try:
            client = get_client(address)
        except (AttributeError, AssertionError):
            client = Client(address, set_as_default=False)
        self.__init__(name=name, client=client, max_leases=max_leases)

    def close(self):
        self.client.sync(self.client.scheduler.semaphore_close, name=self.name)
