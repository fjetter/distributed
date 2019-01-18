from __future__ import print_function, division, absolute_import

from datetime import timedelta
import uuid

from tornado import gen
import tornado.locks

from .client import _get_global_client, Client
from .utils import log_errors
from .worker import get_worker, get_client


class SemaphoreExtension(object):
    """ An extension for the scheduler to manage Semaphores

    This adds the following routes to the scheduler

    *  semaphore_acquire
    *  semaphore_release
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.semaphores = dict()
        self.scheduler.handlers.update({'semaphore_create': self.create,
                                        'semaphore_acquire': self.acquire,
                                        'semaphore_release': self.release,
                                        'semaphore_value': self.value,
                                        'semaphore_close': self.close,
                                        })

        self.scheduler.extensions['semaphores'] = self

    @gen.coroutine
    def value(self, stream=None, client=None, name=None):
        if name in self.semaphores:
            raise gen.Return(self.semaphores[name]._value)

    def create(self, stream=None, name=None, client=None, value=1):
        if name not in self.semaphores:
            assert isinstance(value, int), value
            self.semaphores[name] = tornado.locks.Semaphore(value=int(value))

    @gen.coroutine
    def acquire(self, stream=None, name=None, client=None, timeout=None):
        with log_errors():

            if isinstance(name, list):
                name = tuple(name)
            future = self.semaphores[name].acquire(timeout=timeout)
            if timeout is not None:
                future = gen.with_timeout(timedelta(seconds=timeout), future)

            result = True
            try:
                yield future
            except gen.TimeoutError:
                result = False
            raise gen.Return(result)

    def release(self, stream=None, name=None, client=None):
        self.semaphores[name].release()

    def close(self, stream=None, name=None, client=None):
        if name in self.semaphores:
            del self.semaphores[name]


class Semaphore(object):

    def __init__(self, value=1, name=None, client=None):
        self.client = client or _get_global_client() or get_worker().client
        self.id = uuid.uuid4().hex
        self.name = name or 'semaphore-' + uuid.uuid4().hex

        if self.client.asynchronous:
            self._started = self.client.scheduler.semaphore_create(name=self.name,
                                                                   value=value)
        else:
            self.client.sync(self.client.scheduler.semaphore_create, name=self.name, value=value)
            self._started = gen.moment

    @property
    def value(self):
        return self.client.scheduler.semaphore_value(name=self.name)

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
        return self.client.sync(self.client.scheduler.semaphore_acquire,
                                name=self.name, timeout=timeout)

    def release(self):
        """
        Release a semaphore.

        Increment the internal counter by one.
        """

        """ Release the lock if already acquired """
        return self.client.sync(self.client.scheduler.semaphore_release,
                                name=self.name)

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
        return (self.name, self.client.scheduler.address)

    def __setstate__(self, state):
        name, address = state
        try:
            client = get_client(address)
        except (AttributeError, AssertionError):
            client = Client(address, set_as_default=False)
        self.__init__(name=name, client=client, value=0)

    def close(self):
        self.client.sync(self.client.scheduler.semaphore_close, name=self.name)
