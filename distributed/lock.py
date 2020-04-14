import logging

from .worker import get_client
from .semaphore import Semaphore

logger = logging.getLogger(__name__)


class Lock(Semaphore):
    """ Distributed Centralized Lock

    Parameters
    ----------
    name: string (optional)
        Name of the lock to acquire.  Choosing the same name allows two
        disconnected processes to coordinate a lock.  If not given, a random
        name will be generated.
    client: Client (optional)
        Client to use for communication with the scheduler.  If not given, the
        default global client will be used.

    Examples
    --------
    >>> lock = Lock('x')  # doctest: +SKIP
    >>> lock.acquire(timeout=1)  # doctest: +SKIP
    >>> # do things with protected resource
    >>> lock.release()  # doctest: +SKIP
    """

    def __init__(self, name=None, client=None):
        super().__init__(max_leases=1, name=name, client=client)

    def __getstate__(self):
        return self.name

    def __setstate__(self, name):
        client = get_client()
        self.__init__(name=name, client=client)

    def locked(self):
        # TODO: We should implement a num_leases or smth sicne this is not distributed in nature
        return len(self._leases) >= 1
