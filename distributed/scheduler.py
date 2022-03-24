import asyncio
import heapq
import inspect
import itertools
import json
import logging
import math
import operator
import os
import pickle
import random
import sys
import warnings
import weakref
from collections import defaultdict, deque
from collections.abc import (
    Callable,
    Collection,
    Container,
    Hashable,
    Iterable,
    Iterator,
    Mapping,
    Set,
)
from contextlib import suppress
from datetime import timedelta
from functools import partial
from numbers import Number
from typing import ClassVar, Literal

import psutil
from sortedcontainers import SortedDict
from tlz import compose, first, groupby, merge, merge_sorted, pluck, second, valmap
from tornado.ioloop import IOLoop, PeriodicCallback

import dask
from dask.highlevelgraph import HighLevelGraph
from dask.utils import format_bytes, format_time, parse_timedelta, tmpfile
from dask.widgets import get_template

from distributed.utils import recursive_to_dict

from . import preloading, profile
from . import versions as version_module
from .active_memory_manager import ActiveMemoryManagerExtension, RetireWorker
from .batched import BatchedSend
from .comm import (
    Comm,
    get_address_host,
    normalize_address,
    resolve_address,
    unparse_host_port,
)
from .comm.addressing import addresses_from_user_args
from .core import CommClosedError, Status, clean_exception, rpc, send_recv
from .diagnostics.memory_sampler import MemorySamplerExtension
from .diagnostics.plugin import SchedulerPlugin, _get_plugin_name
from .event import EventExtension
from .http import get_handlers
from .lock import LockExtension
from .metrics import time
from .multi_lock import MultiLockExtension
from .node import ServerNode
from .proctitle import setproctitle
from .protocol.pickle import dumps, loads
from .publish import PublishExtension
from .pubsub import PubSubSchedulerExtension
from .queues import QueueExtension
from .recreate_tasks import ReplayTaskScheduler
from .scheduler_state import (
    ALL_TASK_STATES,
    COMPILED,
    ClientState,
    MemoryState,
    SchedulerState,
    TaskState,
    WorkerState,
)
from .security import Security
from .semaphore import SemaphoreExtension
from .stealing import WorkStealing
from .utils import (
    All,
    TimeoutError,
    empty_context,
    get_fileno_limit,
    key_split,
    log_errors,
    no_default,
    validate_key,
)
from .utils_comm import gather_from_workers, retry_operation, scatter_to_workers
from .utils_perf import disable_gc_diagnosis, enable_gc_diagnosis
from .variable import VariableExtension

logger = logging.getLogger(__name__)


LOG_PDB = dask.config.get("distributed.admin.pdb-on-err")


DEFAULT_EXTENSIONS = [
    LockExtension,
    MultiLockExtension,
    PublishExtension,
    ReplayTaskScheduler,
    QueueExtension,
    VariableExtension,
    PubSubSchedulerExtension,
    SemaphoreExtension,
    EventExtension,
    ActiveMemoryManagerExtension,
    MemorySamplerExtension,
]

globals()["ALL_TASK_STATES"] = ALL_TASK_STATES
globals()["COMPILED"] = COMPILED


class _StateLegacyMapping(Mapping):
    """
    A mapping interface mimicking the former Scheduler state dictionaries.
    """

    def __init__(self, states, accessor):
        self._states = states
        self._accessor = accessor

    def __iter__(self):
        return iter(self._states)

    def __len__(self):
        return len(self._states)

    def __getitem__(self, key):
        return self._accessor(self._states[key])

    def __repr__(self):
        return f"{self.__class__}({dict(self)})"


class _OptionalStateLegacyMapping(_StateLegacyMapping):
    """
    Similar to _StateLegacyMapping, but a false-y value is interpreted
    as a missing key.
    """

    # For tasks etc.

    def __iter__(self):
        accessor = self._accessor
        for k, v in self._states.items():
            if accessor(v):
                yield k

    def __len__(self):
        accessor = self._accessor
        return sum(bool(accessor(v)) for v in self._states.values())

    def __getitem__(self, key):
        v = self._accessor(self._states[key])
        if v:
            return v
        else:
            raise KeyError


class _StateLegacySet(Set):
    """
    Similar to _StateLegacyMapping, but exposes a set containing
    all values with a true value.
    """

    # For loose_restrictions

    def __init__(self, states, accessor):
        self._states = states
        self._accessor = accessor

    def __iter__(self):
        return (k for k, v in self._states.items() if self._accessor(v))

    def __len__(self):
        return sum(map(bool, map(self._accessor, self._states.values())))

    def __contains__(self, k):
        st = self._states.get(k)
        return st is not None and bool(self._accessor(st))

    def __repr__(self):
        return f"{self.__class__}({set(self)})"


def _legacy_task_key_set(tasks):
    """
    Transform a set of task states into a set of task keys.
    """
    ts: TaskState
    return {ts._key for ts in tasks}


def _legacy_client_key_set(clients):
    """
    Transform a set of client states into a set of client keys.
    """
    cs: ClientState
    return {cs._client_key for cs in clients}


def _legacy_worker_key_set(workers):
    """
    Transform a set of worker states into a set of worker keys.
    """
    ws: WorkerState
    return {ws._address for ws in workers}


def _legacy_task_key_dict(task_dict: dict):
    """
    Transform a dict of {task state: value} into a dict of {task key: value}.
    """
    ts: TaskState
    return {ts._key: value for ts, value in task_dict.items()}


def _task_key_or_none(task: TaskState):
    return task._key if task is not None else None


class Scheduler(ServerNode):
    """Dynamic distributed task scheduler

    The scheduler tracks the current state of workers, data, and computations.
    The scheduler listens for events and responds by controlling workers
    appropriately.  It continuously tries to use the workers to execute an ever
    growing dask graph.

    All events are handled quickly, in linear time with respect to their input
    (which is often of constant size) and generally within a millisecond.  To
    accomplish this the scheduler tracks a lot of state.  Every operation
    maintains the consistency of this state.

    The scheduler communicates with the outside world through Comm objects.
    It maintains a consistent and valid view of the world even when listening
    to several clients at once.

    A Scheduler is typically started either with the ``dask-scheduler``
    executable::

         $ dask-scheduler
         Scheduler started at 127.0.0.1:8786

    Or within a LocalCluster a Client starts up without connection
    information::

        >>> c = Client()  # doctest: +SKIP
        >>> c.cluster.scheduler  # doctest: +SKIP
        Scheduler(...)

    Users typically do not interact with the scheduler directly but rather with
    the client object ``Client``.

    **State**

    The scheduler contains the following state variables.  Each variable is
    listed along with what it stores and a brief description.

    * **tasks:** ``{task key: TaskState}``
        Tasks currently known to the scheduler
    * **unrunnable:** ``{TaskState}``
        Tasks in the "no-worker" state

    * **workers:** ``{worker key: WorkerState}``
        Workers currently connected to the scheduler
    * **idle:** ``{WorkerState}``:
        Set of workers that are not fully utilized
    * **saturated:** ``{WorkerState}``:
        Set of workers that are not over-utilized

    * **host_info:** ``{hostname: dict}``:
        Information about each worker host

    * **clients:** ``{client key: ClientState}``
        Clients currently connected to the scheduler

    * **services:** ``{str: port}``:
        Other services running on this scheduler, like Bokeh
    * **loop:** ``IOLoop``:
        The running Tornado IOLoop
    * **client_comms:** ``{client key: Comm}``
        For each client, a Comm object used to receive task requests and
        report task status updates.
    * **stream_comms:** ``{worker key: Comm}``
        For each worker, a Comm object from which we both accept stimuli and
        report results
    * **task_duration:** ``{key-prefix: time}``
        Time we expect certain functions to take, e.g. ``{'sum': 0.25}``
    """

    default_port = 8786
    _instances: "ClassVar[weakref.WeakSet[Scheduler]]" = weakref.WeakSet()

    def __init__(
        self,
        loop=None,
        delete_interval="500ms",
        synchronize_worker_interval="60s",
        services=None,
        service_kwargs=None,
        allowed_failures=None,
        extensions=None,
        validate=None,
        scheduler_file=None,
        security=None,
        worker_ttl=None,
        idle_timeout=None,
        interface=None,
        host=None,
        port=0,
        protocol=None,
        dashboard_address=None,
        dashboard=None,
        http_prefix="/",
        preload=None,
        preload_argv=(),
        plugins=(),
        **kwargs,
    ):
        self._setup_logging(logger)

        # Attributes
        if allowed_failures is None:
            allowed_failures = dask.config.get("distributed.scheduler.allowed-failures")
        self.allowed_failures = allowed_failures
        if validate is None:
            validate = dask.config.get("distributed.scheduler.validate")
        self.proc = psutil.Process()
        self.delete_interval = parse_timedelta(delete_interval, default="ms")
        self.synchronize_worker_interval = parse_timedelta(
            synchronize_worker_interval, default="ms"
        )
        self.digests = None
        self.service_specs = services or {}
        self.service_kwargs = service_kwargs or {}
        self.services = {}
        self.scheduler_file = scheduler_file
        worker_ttl = worker_ttl or dask.config.get("distributed.scheduler.worker-ttl")
        self.worker_ttl = parse_timedelta(worker_ttl) if worker_ttl else None
        idle_timeout = idle_timeout or dask.config.get(
            "distributed.scheduler.idle-timeout"
        )
        if idle_timeout:
            self.idle_timeout = parse_timedelta(idle_timeout)
        else:
            self.idle_timeout = None
        self.idle_since = time()
        self.time_started = self.idle_since  # compatibility for dask-gateway
        self._lock = asyncio.Lock()
        self.bandwidth_workers = defaultdict(float)
        self.bandwidth_types = defaultdict(float)

        if not preload:
            preload = dask.config.get("distributed.scheduler.preload")
        if not preload_argv:
            preload_argv = dask.config.get("distributed.scheduler.preload-argv")
        self.preloads = preloading.process_preloads(self, preload, preload_argv)

        if isinstance(security, dict):
            security = Security(**security)
        self.security = security or Security()
        assert isinstance(self.security, Security)
        self.connection_args = self.security.get_connection_args("scheduler")
        self.connection_args["handshake_overrides"] = {  # common denominator
            "pickle-protocol": 4
        }

        self._start_address = addresses_from_user_args(
            host=host,
            port=port,
            interface=interface,
            protocol=protocol,
            security=security,
            default_port=self.default_port,
        )

        http_server_modules = dask.config.get("distributed.scheduler.http.routes")
        show_dashboard = dashboard or (dashboard is None and dashboard_address)
        # install vanilla route if show_dashboard but bokeh is not installed
        if show_dashboard:
            try:
                import distributed.dashboard.scheduler
            except ImportError:
                show_dashboard = False
                http_server_modules.append("distributed.http.scheduler.missing_bokeh")
        routes = get_handlers(
            server=self, modules=http_server_modules, prefix=http_prefix
        )
        self.start_http_server(routes, dashboard_address, default_port=8787)
        if show_dashboard:
            distributed.dashboard.scheduler.connect(
                self.http_application, self.http_server, self, prefix=http_prefix
            )

        # Communication state
        self.loop = loop or IOLoop.current()
        self.client_comms = {}
        self.stream_comms = {}
        self._worker_coroutines = []
        self._ipython_kernel = None

        # Task state
        tasks = {}
        for old_attr, new_attr, wrap in [
            ("priority", "priority", None),
            ("dependencies", "dependencies", _legacy_task_key_set),
            ("dependents", "dependents", _legacy_task_key_set),
            ("retries", "retries", None),
        ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr, _StateLegacyMapping(tasks, func))

        for old_attr, new_attr, wrap in [
            ("nbytes", "nbytes", None),
            ("who_wants", "who_wants", _legacy_client_key_set),
            ("who_has", "who_has", _legacy_worker_key_set),
            ("waiting", "waiting_on", _legacy_task_key_set),
            ("waiting_data", "waiters", _legacy_task_key_set),
            ("rprocessing", "processing_on", None),
            ("host_restrictions", "host_restrictions", None),
            ("worker_restrictions", "worker_restrictions", None),
            ("resource_restrictions", "resource_restrictions", None),
            ("suspicious_tasks", "suspicious", None),
            ("exceptions", "exception", None),
            ("tracebacks", "traceback", None),
            ("exceptions_blame", "exception_blame", _task_key_or_none),
        ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr, _OptionalStateLegacyMapping(tasks, func))

        for old_attr, new_attr, wrap in [
            ("loose_restrictions", "loose_restrictions", None)
        ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr, _StateLegacySet(tasks, func))

        self.generation = 0
        self._last_client = None
        self._last_time = 0
        unrunnable = set()

        self.datasets = {}

        # Prefix-keyed containers

        # Client state
        clients = {}
        for old_attr, new_attr, wrap in [
            ("wants_what", "wants_what", _legacy_task_key_set)
        ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr, _StateLegacyMapping(clients, func))

        # Worker state
        workers = SortedDict()
        for old_attr, new_attr, wrap in [
            ("nthreads", "nthreads", None),
            ("worker_bytes", "nbytes", None),
            ("worker_resources", "resources", None),
            ("used_resources", "used_resources", None),
            ("occupancy", "occupancy", None),
            ("worker_info", "metrics", None),
            ("processing", "processing", _legacy_task_key_dict),
            ("has_what", "has_what", _legacy_task_key_set),
        ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr, _StateLegacyMapping(workers, func))

        host_info = {}
        resources = {}
        aliases = {}

        self._task_state_collections: list = [unrunnable]

        self._worker_collections: list = [
            workers,
            host_info,
            resources,
            aliases,
        ]

        self.log = deque(
            maxlen=dask.config.get("distributed.scheduler.transition-log-length")
        )
        self.events = defaultdict(
            partial(
                deque, maxlen=dask.config.get("distributed.scheduler.events-log-length")
            )
        )
        self.event_counts = defaultdict(int)
        self.event_subscriber = defaultdict(set)
        self.worker_plugins = {}
        self.nanny_plugins = {}

        worker_handlers: dict = {
            "task-finished": self.handle_task_finished,
            "task-erred": self.handle_task_erred,
            "release-worker-data": self.release_worker_data,
            "add-keys": self.add_keys,
            "missing-data": self.handle_missing_data,
            "long-running": self.handle_long_running,
            "reschedule": self.reschedule,
            "keep-alive": lambda *args, **kwargs: None,
            "log-event": self.log_worker_event,
            "worker-status-change": self.handle_worker_status_change,
        }

        client_handlers: dict = {
            "update-graph": self.update_graph,
            "update-graph-hlg": self.update_graph_hlg,
            "client-desires-keys": self.client_desires_keys,
            "update-data": self.update_data,
            "report-key": self.report_on_key,
            "client-releases-keys": self.client_releases_keys,
            "heartbeat-client": self.client_heartbeat,
            "close-client": self.remove_client,
            "restart": self.restart,
            "subscribe-topic": self.subscribe_topic,
            "unsubscribe-topic": self.unsubscribe_topic,
        }

        self.handlers = {
            "register-client": self.add_client,
            "scatter": self.scatter,
            "register-worker": self.add_worker,
            "register_nanny": self.add_nanny,
            "unregister": self.remove_worker,
            "gather": self.gather,
            "cancel": self.stimulus_cancel,
            "retry": self.stimulus_retry,
            "feed": self.feed,
            "terminate": self.close,
            "broadcast": self.broadcast,
            "proxy": self.proxy,
            "ncores": self.get_ncores,
            "ncores_running": self.get_ncores_running,
            "has_what": self.get_has_what,
            "who_has": self.get_who_has,
            "processing": self.get_processing,
            "call_stack": self.get_call_stack,
            "profile": self.get_profile,
            "performance_report": self.performance_report,
            "get_logs": self.get_logs,
            "logs": self.get_logs,
            "worker_logs": self.get_worker_logs,
            "log_event": self.log_worker_event,
            "events": self.get_events,
            "nbytes": self.get_nbytes,
            "versions": self.versions,
            "add_keys": self.add_keys,
            "rebalance": self.rebalance,
            "replicate": self.replicate,
            "start_ipython": self.start_ipython,
            "run_function": self.run_function,
            "update_data": self.update_data,
            "set_resources": self.add_resources,
            "retire_workers": self.retire_workers,
            "get_metadata": self.get_metadata,
            "set_metadata": self.set_metadata,
            "set_restrictions": self.set_restrictions,
            "heartbeat_worker": self.heartbeat_worker,
            "get_task_status": self.get_task_status,
            "get_task_stream": self.get_task_stream,
            "get_task_prefix_states": self.get_task_prefix_states,
            "register_scheduler_plugin": self.register_scheduler_plugin,
            "register_worker_plugin": self.register_worker_plugin,
            "unregister_worker_plugin": self.unregister_worker_plugin,
            "register_nanny_plugin": self.register_nanny_plugin,
            "unregister_nanny_plugin": self.unregister_nanny_plugin,
            "adaptive_target": self.adaptive_target,
            "workers_to_close": self.workers_to_close,
            "subscribe_worker_status": self.subscribe_worker_status,
            "start_task_metadata": self.start_task_metadata,
            "stop_task_metadata": self.stop_task_metadata,
        }

        connection_limit = get_fileno_limit() / 2

        # Variables from dask.config, cached by __init__ for performance
        self.MEMORY_RECENT_TO_OLD_TIME = parse_timedelta(
            dask.config.get("distributed.worker.memory.recent-to-old-time")
        )
        self.MEMORY_REBALANCE_MEASURE = dask.config.get(
            "distributed.worker.memory.rebalance.measure"
        )
        self.MEMORY_REBALANCE_SENDER_MIN = dask.config.get(
            "distributed.worker.memory.rebalance.sender-min"
        )
        self.MEMORY_REBALANCE_RECIPIENT_MAX = dask.config.get(
            "distributed.worker.memory.rebalance.recipient-max"
        )
        self.MEMORY_REBALANCE_HALF_GAP = (
            dask.config.get("distributed.worker.memory.rebalance.sender-recipient-gap")
            / 2.0
        )

        self.state: SchedulerState = SchedulerState(
            aliases=aliases,
            clients=clients,
            workers=workers,
            host_info=host_info,
            resources=resources,
            tasks=tasks,
            unrunnable=unrunnable,
            validate=validate,
            plugins=plugins,
        )

        super().__init__(
            handlers=self.handlers,
            stream_handlers=merge(worker_handlers, client_handlers),
            io_loop=self.loop,
            connection_limit=connection_limit,
            deserialize=False,
            connection_args=self.connection_args,
            **kwargs,
        )

        if self.worker_ttl:
            pc = PeriodicCallback(self.check_worker_ttl, self.worker_ttl * 1000)
            self.periodic_callbacks["worker-ttl"] = pc

        if self.idle_timeout:
            pc = PeriodicCallback(self.check_idle, self.idle_timeout * 1000 / 4)
            self.periodic_callbacks["idle-timeout"] = pc

        if extensions is None:
            extensions = list(DEFAULT_EXTENSIONS)
            if dask.config.get("distributed.scheduler.work-stealing"):
                extensions.append(WorkStealing)
        for ext in extensions:
            ext(self)

        setproctitle("dask-scheduler [not started]")
        Scheduler._instances.add(self)
        self.rpc.allow_offload = False
        self.status = Status.undefined

    ####################
    # state properties #
    ####################

    @property
    def aliases(self):
        return self.state.aliases

    @property
    def bandwidth(self):
        return self.state.bandwidth

    @property
    def clients(self):
        return self.state.clients

    @property
    def extensions(self):
        return self.state.extensions

    @property
    def host_info(self):
        return self.state.host_info

    @property
    def idle(self):
        return self.state.idle

    @property
    def n_tasks(self):
        return self.state.n_tasks

    @property
    def plugins(self):
        return self.state.plugins

    @plugins.setter
    def plugins(self, val):
        self.state.plugins = val

    @property
    def resources(self):
        return self.state.resources

    @property
    def saturated(self):
        return self.state.saturated

    @property
    def tasks(self):
        return self.state.tasks

    @property
    def task_groups(self):
        return self.state.task_groups

    @property
    def task_prefixes(self):
        return self.state.task_prefixes

    @property
    def task_metadata(self):
        return self.state.task_metadata

    @property
    def total_nthreads(self):
        return self.state.total_nthreads

    @property
    def total_occupancy(self):
        return self.state.total_occupancy

    @total_occupancy.setter
    def total_occupancy(self, v: double):
        self.state.total_occupancy = v

    @property
    def transition_counter(self):
        return self.state.transition_counter

    @property
    def unknown_durations(self):
        return self.state.unknown_durations

    @property
    def unrunnable(self):
        return self.state.unrunnable

    @property
    def validate(self):
        return self.state.validate

    @validate.setter
    def validate(self, v: bint):
        self.state.validate = v

    @property
    def workers(self):
        return self.state.workers

    @property
    def memory(self) -> MemoryState:
        return MemoryState.sum(*(w.memory for w in self.state.workers.values()))

    @property
    def __pdict__(self):
        return self.state.__pdict__

    ##################
    # Administration #
    ##################

    def __repr__(self):
        return (
            f"<Scheduler {self.address!r}, "
            f"workers: {len(self.state.workers)}, "
            f"cores: {self.state.total_nthreads}, "
            f"tasks: {len(self.state.tasks)}>"
        )

    def _repr_html_(self):
        return get_template("scheduler.html.j2").render(
            address=self.address,
            workers=self.state.workers,
            threads=self.state.total_nthreads,
            tasks=self.state.tasks,
        )

    def identity(self):
        """Basic information about ourselves and our cluster"""
        d = {
            "type": type(self).__name__,
            "id": str(self.id),
            "address": self.address,
            "services": {key: v.port for (key, v) in self.services.items()},
            "started": self.time_started,
            "workers": {
                worker.address: worker.identity()
                for worker in self.state.workers.values()
            },
        }
        return d

    def _to_dict(
        self, comm: "Comm | None" = None, *, exclude: "Container[str]" = ()
    ) -> dict:
        """Dictionary representation for debugging purposes.
        Not type stable and not intended for roundtrips.

        See also
        --------
        Server.identity
        Client.dump_cluster_state
        distributed.utils.recursive_to_dict
        """
        info = super()._to_dict(exclude=exclude)
        extra = {
            "transition_log": self.state.transition_log,
            "log": self.log,
            "tasks": self.tasks,
            "task_groups": self.task_groups,
            # Overwrite dict of WorkerState.identity from info
            "workers": self.workers,
            "clients": self.clients,
            "memory": self.memory,
            "events": self.events,
            "extensions": self.extensions,
        }
        extra = {k: v for k, v in extra.items() if k not in exclude}
        info.update(recursive_to_dict(extra, exclude=exclude))
        return info

    def get_worker_service_addr(self, worker, service_name, protocol=False):
        """
        Get the (host, port) address of the named service on the *worker*.
        Returns None if the service doesn't exist.

        Parameters
        ----------
        worker : address
        service_name : str
            Common services include 'bokeh' and 'nanny'
        protocol : boolean
            Whether or not to include a full address with protocol (True)
            or just a (host, port) pair
        """
        ws: WorkerState = self.state.workers[worker]
        port = ws._services.get(service_name)
        if port is None:
            return None
        elif protocol:
            return "%(protocol)s://%(host)s:%(port)d" % {
                "protocol": ws._address.split("://")[0],
                "host": ws.host,
                "port": port,
            }
        else:
            return ws.host, port

    async def start(self):
        """Clear out old state and restart all running coroutines"""
        await super().start()
        assert self.status != Status.running

        enable_gc_diagnosis()

        self.clear_task_state()

        with suppress(AttributeError):
            for c in self._worker_coroutines:
                c.cancel()

        for addr in self._start_address:
            await self.listen(
                addr,
                allow_offload=False,
                handshake_overrides={"pickle-protocol": 4, "compression": None},
                **self.security.get_listen_args("scheduler"),
            )
            self.ip = get_address_host(self.listen_address)
            listen_ip = self.ip

            if listen_ip == "0.0.0.0":
                listen_ip = ""

        if self.address.startswith("inproc://"):
            listen_ip = "localhost"

        # Services listen on all addresses
        self.start_services(listen_ip)

        for listener in self.listeners:
            logger.info("  Scheduler at: %25s", listener.contact_address)
        for k, v in self.services.items():
            logger.info("%11s at: %25s", k, "%s:%d" % (listen_ip, v.port))

        self.loop.add_callback(self.reevaluate_occupancy)

        if self.scheduler_file:
            with open(self.scheduler_file, "w") as f:
                json.dump(self.identity(), f, indent=2)

            fn = self.scheduler_file  # remove file when we close the process

            def del_scheduler_file():
                if os.path.exists(fn):
                    os.remove(fn)

            weakref.finalize(self, del_scheduler_file)

        for preload in self.preloads:
            await preload.start()

        await asyncio.gather(
            *[plugin.start(self) for plugin in list(self.plugins.values())]
        )

        self.start_periodic_callbacks()

        setproctitle(f"dask-scheduler [{self.address}]")
        return self

    async def close(self, fast=False, close_workers=False):
        """Send cleanup signal to all coroutines then wait until finished

        See Also
        --------
        Scheduler.cleanup
        """
        if self.status in (Status.closing, Status.closed):
            await self.finished()
            return
        self.status = Status.closing

        logger.info("Scheduler closing...")
        setproctitle("dask-scheduler [closing]")

        for preload in self.preloads:
            await preload.teardown()

        if close_workers:
            await self.broadcast(msg={"op": "close_gracefully"}, nanny=True)
            for worker in self.state.workers:
                # Report would require the worker to unregister with the
                # currently closing scheduler. This is not necessary and might
                # delay shutdown of the worker unnecessarily
                self.worker_send(worker, {"op": "close", "report": False})
            for i in range(20):  # wait a second for send signals to clear
                if self.state.workers:
                    await asyncio.sleep(0.05)
                else:
                    break

        await asyncio.gather(
            *[plugin.close() for plugin in list(self.plugins.values())]
        )

        for pc in self.periodic_callbacks.values():
            pc.stop()
        self.periodic_callbacks.clear()

        self.stop_services()

        for ext in self.state.extensions.values():
            with suppress(AttributeError):
                ext.teardown()
        logger.info("Scheduler closing all comms")

        futures = []
        for w, comm in list(self.stream_comms.items()):
            if not comm.closed():
                comm.send({"op": "close", "report": False})
                comm.send({"op": "close-stream"})
            with suppress(AttributeError):
                futures.append(comm.close())

        for future in futures:  # TODO: do all at once
            await future

        for comm in self.client_comms.values():
            comm.abort()

        await self.rpc.close()

        self.status = Status.closed
        self.stop()
        await super().close()

        setproctitle("dask-scheduler [closed]")
        disable_gc_diagnosis()

    async def close_worker(self, worker: str, safe: bool = False):
        """Remove a worker from the cluster

        This both removes the worker from our local state and also sends a
        signal to the worker to shut down.  This works regardless of whether or
        not the worker has a nanny process restarting it
        """
        logger.info("Closing worker %s", worker)
        with log_errors():
            self.log_event(worker, {"action": "close-worker"})
            # FIXME: This does not handle nannies
            self.worker_send(worker, {"op": "close", "report": False})
            await self.remove_worker(address=worker, safe=safe)

    ###########
    # Stimuli #
    ###########

    def heartbeat_worker(
        self,
        comm=None,
        *,
        address,
        resolve_address: bool = True,
        now: float = None,
        resources: dict = None,
        host_info: dict = None,
        metrics: dict,
        executing: dict = None,
    ):
        address = self.coerce_address(address, resolve_address)
        address = normalize_address(address)
        ws: WorkerState = self.state.workers.get(address)  # type: ignore
        if ws is None:
            return {"status": "missing"}

        host = get_address_host(address)
        local_now = time()
        host_info = host_info or {}

        dh: dict = self.state.host_info.setdefault(host, {})
        dh["last-seen"] = local_now

        frac = 1 / len(self.state.workers)
        self.state.bandwidth = (
            self.state.bandwidth * (1 - frac) + metrics["bandwidth"]["total"] * frac
        )
        for other, (bw, count) in metrics["bandwidth"]["workers"].items():
            if (address, other) not in self.bandwidth_workers:
                self.bandwidth_workers[address, other] = bw / count
            else:
                alpha = (1 - frac) ** count
                self.bandwidth_workers[address, other] = self.bandwidth_workers[
                    address, other
                ] * alpha + bw * (1 - alpha)
        for typ, (bw, count) in metrics["bandwidth"]["types"].items():
            if typ not in self.bandwidth_types:
                self.bandwidth_types[typ] = bw / count
            else:
                alpha = (1 - frac) ** count
                self.bandwidth_types[typ] = self.bandwidth_types[typ] * alpha + bw * (
                    1 - alpha
                )

        ws._last_seen = local_now
        if executing is not None:
            ws._executing = {
                self.state.tasks[key]: duration
                for key, duration in executing.items()
                if key in self.state.tasks
            }

        ws._metrics = metrics

        # Calculate RSS - dask keys, separating "old" and "new" usage
        # See MemoryState for details
        max_memory_unmanaged_old_hist_age = local_now - self.MEMORY_RECENT_TO_OLD_TIME
        memory_unmanaged_old = ws._memory_unmanaged_old
        while ws._memory_other_history:
            timestamp, size = ws._memory_other_history[0]
            if timestamp >= max_memory_unmanaged_old_hist_age:
                break
            ws._memory_other_history.popleft()
            if size == memory_unmanaged_old:
                memory_unmanaged_old = 0  # recalculate min()

        # metrics["memory"] is None if the worker sent a heartbeat before its
        # SystemMonitor ever had a chance to run.
        # ws._nbytes is updated at a different time and sizeof() may not be accurate,
        # so size may be (temporarily) negative; floor it to zero.
        size = max(
            0,
            (metrics["memory"] or 0) - ws._nbytes + metrics["spilled_nbytes"]["memory"],
        )

        ws._memory_other_history.append((local_now, size))
        if not memory_unmanaged_old:
            # The worker has just been started or the previous minimum has been expunged
            # because too old.
            # Note: this algorithm is capped to 200 * MEMORY_RECENT_TO_OLD_TIME elements
            # cluster-wide by heartbeat_interval(), regardless of the number of workers
            ws._memory_unmanaged_old = min(map(second, ws._memory_other_history))
        elif size < memory_unmanaged_old:
            ws._memory_unmanaged_old = size

        if host_info:
            dh = self.state.host_info.setdefault(host, {})
            dh.update(host_info)

        if now:
            ws._time_delay = local_now - now

        if resources:
            self.add_resources(worker=address, resources=resources)

        self.log_event(address, merge({"action": "heartbeat"}, metrics))

        return {
            "status": "OK",
            "time": local_now,
            "heartbeat-interval": heartbeat_interval(len(self.state.workers)),
        }

    async def add_worker(
        self,
        comm=None,
        *,
        address: str,
        status: str,
        keys=(),
        nthreads=None,
        name=None,
        resolve_address=True,
        nbytes=None,
        types=None,
        now=None,
        resources=None,
        host_info=None,
        memory_limit=None,
        metrics=None,
        pid=0,
        services=None,
        local_directory=None,
        versions=None,
        nanny=None,
        extra=None,
    ):
        """Add a new worker to the cluster"""
        with log_errors():
            address = self.coerce_address(address, resolve_address)
            address = normalize_address(address)
            host = get_address_host(address)

            if address in self.state.workers:
                raise ValueError("Worker already exists %s" % address)

            if name in self.state.aliases:
                logger.warning(
                    "Worker tried to connect with a duplicate name: %s", name
                )
                msg = {
                    "status": "error",
                    "message": "name taken, %s" % name,
                    "time": time(),
                }
                if comm:
                    await comm.write(msg)
                return

            self.log_event(address, {"action": "add-worker"})
            self.log_event("all", {"action": "add-worker", "worker": address})

            ws: WorkerState
            self.state.workers[address] = ws = WorkerState(
                address=address,
                status=Status.lookup[status],  # type: ignore
                pid=pid,
                nthreads=nthreads,
                memory_limit=memory_limit or 0,
                name=name,
                local_directory=local_directory,
                services=services,
                versions=versions,
                nanny=nanny,
                extra=extra,
            )
            if ws._status == Status.running:
                self.state.running.add(ws)

            dh: dict = self.state.host_info.get(host)  # type: ignore
            if dh is None:
                self.state.host_info[host] = dh = {}

            dh_addresses: set = dh.get("addresses")  # type: ignore
            if dh_addresses is None:
                dh["addresses"] = dh_addresses = set()
                dh["nthreads"] = 0

            dh_addresses.add(address)
            dh["nthreads"] += nthreads

            self.state.total_nthreads += nthreads
            self.state.aliases[name] = address

            self.heartbeat_worker(
                address=address,
                resolve_address=resolve_address,
                now=now,
                resources=resources,
                host_info=host_info,
                metrics=metrics,
            )

            # Do not need to adjust self.state.total_occupancy as self.occupancy[ws] cannot
            # exist before this.
            self.state.check_idle_saturated(ws)

            # for key in keys:  # TODO
            #     self.mark_key_in_memory(key, [address])

            self.stream_comms[address] = BatchedSend(interval="5ms", loop=self.loop)

            if ws._nthreads > len(ws._processing):
                self.state.idle[ws._address] = ws

            for plugin in list(self.plugins.values()):
                try:
                    result = plugin.add_worker(scheduler=self, worker=address)
                    if inspect.isawaitable(result):
                        await result
                except Exception as e:
                    logger.exception(e)

            recommendations: dict = {}
            client_msgs: dict = {}
            worker_msgs: dict = {}
            if nbytes:
                assert isinstance(nbytes, dict)
                already_released_keys = []
                for key in nbytes:
                    ts: TaskState = self.state.tasks.get(key)  # type: ignore
                    if ts is not None and ts.state != "released":
                        if ts.state == "memory":
                            self.add_keys(worker=address, keys=[key])
                        else:
                            t: tuple = self.state.transition(
                                key,
                                "memory",
                                kwargs=dict(
                                    worker=address,
                                    nbytes=nbytes[key],
                                    typename=types[key],
                                ),
                            )
                            recommendations, client_msgs, worker_msgs = t
                            self.state.transitions(
                                recommendations, client_msgs, worker_msgs
                            )
                            recommendations = {}
                    else:
                        already_released_keys.append(key)
                if already_released_keys:
                    if address not in worker_msgs:
                        worker_msgs[address] = []
                    worker_msgs[address].append(
                        {
                            "op": "remove-replicas",
                            "keys": already_released_keys,
                            "stimulus_id": f"reconnect-already-released-{time()}",
                        }
                    )

            if ws._status == Status.running:
                for ts in self.state.unrunnable:
                    valid: set = self.state.valid_workers(ts)
                    if valid is None or ws in valid:
                        recommendations[ts._key] = "waiting"

            if recommendations:
                self.state.transitions(recommendations, client_msgs, worker_msgs)

            self.send_all(client_msgs, worker_msgs)

            logger.info("Register worker %s", ws)

            msg = {
                "status": "OK",
                "time": time(),
                "heartbeat-interval": heartbeat_interval(len(self.state.workers)),
                "worker-plugins": self.worker_plugins,
            }

            cs: ClientState
            version_warning = version_module.error_message(
                version_module.get_versions(),
                merge(
                    {w: ws._versions for w, ws in self.state.workers.items()},
                    {
                        c: cs._versions
                        for c, cs in self.state.clients.items()
                        if cs._versions
                    },
                ),
                versions,
                client_name="This Worker",
            )
            msg.update(version_warning)

            if comm:
                await comm.write(msg)

            await self.handle_worker(comm=comm, worker=address)

    async def add_nanny(self, comm):
        msg = {
            "status": "OK",
            "nanny-plugins": self.nanny_plugins,
        }
        return msg

    def get_task_duration(self, ts: TaskState) -> double:
        return self.state.get_task_duration(ts)

    def get_comm_cost(self, *args, **kwargs):
        return self.state.get_comm_cost(*args, **kwargs)

    def check_idle_saturated(self, *args, **kwargs):
        return self.state.check_idle_saturated(*args, **kwargs)

    def update_graph_hlg(
        self,
        client=None,
        hlg=None,
        keys=None,
        dependencies=None,
        restrictions=None,
        priority=None,
        loose_restrictions=None,
        resources=None,
        submitting_task=None,
        retries=None,
        user_priority=0,
        actors=None,
        fifo_timeout=0,
        code=None,
    ):
        unpacked_graph = HighLevelGraph.__dask_distributed_unpack__(hlg)
        dsk = unpacked_graph["dsk"]
        dependencies = unpacked_graph["deps"]
        annotations = unpacked_graph["annotations"]

        # Remove any self-dependencies (happens on test_publish_bag() and others)
        for k, v in dependencies.items():
            deps = set(v)
            if k in deps:
                deps.remove(k)
            dependencies[k] = deps

        if priority is None:
            # Removing all non-local keys before calling order()
            dsk_keys = set(dsk)  # intersection() of sets is much faster than dict_keys
            stripped_deps = {
                k: v.intersection(dsk_keys)
                for k, v in dependencies.items()
                if k in dsk_keys
            }
            priority = dask.order.order(dsk, dependencies=stripped_deps)

        return self.update_graph(
            client,
            dsk,
            keys,
            dependencies,
            restrictions,
            priority,
            loose_restrictions,
            resources,
            submitting_task,
            retries,
            user_priority,
            actors,
            fifo_timeout,
            annotations,
            code=code,
        )

    def update_graph(
        self,
        client=None,
        tasks=None,
        keys=None,
        dependencies=None,
        restrictions=None,
        priority=None,
        loose_restrictions=None,
        resources=None,
        submitting_task=None,
        retries=None,
        user_priority=0,
        actors=None,
        fifo_timeout=0,
        annotations=None,
        code=None,
    ):
        """
        Add new computations to the internal dask graph

        This happens whenever the Client calls submit, map, get, or compute.
        """
        start = time()
        fifo_timeout = parse_timedelta(fifo_timeout)
        keys = set(keys)
        if len(tasks) > 1:
            self.log_event(
                ["all", client], {"action": "update_graph", "count": len(tasks)}
            )

        # Remove aliases
        for k in list(tasks):
            if tasks[k] is k:
                del tasks[k]

        dependencies = dependencies or {}

        if self.state.total_occupancy > 1e-9 and self.state.computations:
            # Still working on something. Assign new tasks to same computation
            computation = cast(Computation, self.state.computations[-1])
        else:
            computation = Computation()
            self.state.computations.append(computation)

        if code and code not in computation._code:  # add new code blocks
            computation._code.add(code)

        n = 0
        while len(tasks) != n:  # walk through new tasks, cancel any bad deps
            n = len(tasks)
            for k, deps in list(dependencies.items()):
                if any(
                    dep not in self.state.tasks and dep not in tasks for dep in deps
                ):  # bad key
                    logger.info("User asked for computation on lost data, %s", k)
                    del tasks[k]
                    del dependencies[k]
                    if k in keys:
                        keys.remove(k)
                    self.report({"op": "cancelled-key", "key": k}, client=client)
                    self.client_releases_keys(keys=[k], client=client)

        # Avoid computation that is already finished
        ts: TaskState
        already_in_memory = set()  # tasks that are already done
        for k, v in dependencies.items():
            if v and k in self.state.tasks:
                ts = self.state.tasks[k]
                if ts._state in ("memory", "erred"):
                    already_in_memory.add(k)

        dts: TaskState
        if already_in_memory:
            dependents = dask.core.reverse_dict(dependencies)
            stack = list(already_in_memory)
            done = set(already_in_memory)
            while stack:  # remove unnecessary dependencies
                key = stack.pop()
                ts = self.state.tasks[key]
                try:
                    deps = dependencies[key]
                except KeyError:
                    deps = self.dependencies[key]
                for dep in deps:
                    if dep in dependents:
                        child_deps = dependents[dep]
                    else:
                        child_deps = self.dependencies[dep]
                    if all(d in done for d in child_deps):
                        if dep in self.state.tasks and dep not in done:
                            done.add(dep)
                            stack.append(dep)

            for d in done:
                tasks.pop(d, None)
                dependencies.pop(d, None)

        # Get or create task states
        stack = list(keys)
        touched_keys = set()
        touched_tasks = []
        while stack:
            k = stack.pop()
            if k in touched_keys:
                continue
            # XXX Have a method get_task_state(self, k) ?
            ts = self.state.tasks.get(k)
            if ts is None:
                ts = self.state.new_task(
                    k, tasks.get(k), "released", computation=computation
                )
            elif not ts._run_spec:
                ts._run_spec = tasks.get(k)

            touched_keys.add(k)
            touched_tasks.append(ts)
            stack.extend(dependencies.get(k, ()))

        self.client_desires_keys(keys=keys, client=client)

        # Add dependencies
        for key, deps in dependencies.items():
            ts = self.state.tasks.get(key)
            if ts is None or ts._dependencies:
                continue
            for dep in deps:
                dts = self.state.tasks[dep]
                ts.add_dependency(dts)

        # Compute priorities
        if isinstance(user_priority, Number):
            user_priority = {k: user_priority for k in tasks}

        annotations = annotations or {}
        restrictions = restrictions or {}
        loose_restrictions = loose_restrictions or []
        resources = resources or {}
        retries = retries or {}

        # Override existing taxonomy with per task annotations
        if annotations:
            if "priority" in annotations:
                user_priority.update(annotations["priority"])

            if "workers" in annotations:
                restrictions.update(annotations["workers"])

            if "allow_other_workers" in annotations:
                loose_restrictions.extend(
                    k for k, v in annotations["allow_other_workers"].items() if v
                )

            if "retries" in annotations:
                retries.update(annotations["retries"])

            if "resources" in annotations:
                resources.update(annotations["resources"])

            for a, kv in annotations.items():
                for k, v in kv.items():
                    # Tasks might have been culled, in which case
                    # we have nothing to annotate.
                    ts = self.state.tasks.get(k)
                    if ts is not None:
                        ts._annotations[a] = v

        # Add actors
        if actors is True:
            actors = list(keys)
        for actor in actors or []:
            ts = self.state.tasks[actor]
            ts._actor = True

        priority = priority or dask.order.order(
            tasks
        )  # TODO: define order wrt old graph

        if submitting_task:  # sub-tasks get better priority than parent tasks
            ts = self.state.tasks.get(submitting_task)
            if ts is not None:
                generation = ts._priority[0] - 0.01
            else:  # super-task already cleaned up
                generation = self.generation
        elif self._last_time + fifo_timeout < start:
            self.generation += 1  # older graph generations take precedence
            generation = self.generation
            self._last_time = start
        else:
            generation = self.generation

        for key in set(priority) & touched_keys:
            ts = self.state.tasks[key]
            if ts._priority is None:
                ts._priority = (-(user_priority.get(key, 0)), generation, priority[key])

        # Ensure all runnables have a priority
        runnables = [ts for ts in touched_tasks if ts._run_spec]
        for ts in runnables:
            if ts._priority is None and ts._run_spec:
                ts._priority = (self.generation, 0)

        if restrictions:
            # *restrictions* is a dict keying task ids to lists of
            # restriction specifications (either worker names or addresses)
            for k, v in restrictions.items():
                if v is None:
                    continue
                ts = self.state.tasks.get(k)
                if ts is None:
                    continue
                ts._host_restrictions = set()
                ts._worker_restrictions = set()
                # Make sure `v` is a collection and not a single worker name / address
                if not isinstance(v, (list, tuple, set)):
                    v = [v]
                for w in v:
                    try:
                        w = self.coerce_address(w)
                    except ValueError:
                        # Not a valid address, but perhaps it's a hostname
                        ts._host_restrictions.add(w)
                    else:
                        ts._worker_restrictions.add(w)

            if loose_restrictions:
                for k in loose_restrictions:
                    ts = self.state.tasks[k]
                    ts._loose_restrictions = True

        if resources:
            for k, v in resources.items():
                if v is None:
                    continue
                assert isinstance(v, dict)
                ts = self.state.tasks.get(k)
                if ts is None:
                    continue
                ts._resource_restrictions = v

        if retries:
            for k, v in retries.items():
                assert isinstance(v, int)
                ts = self.state.tasks.get(k)
                if ts is None:
                    continue
                ts._retries = v

        # Compute recommendations
        recommendations: dict = {}

        for ts in sorted(runnables, key=operator.attrgetter("priority"), reverse=True):
            if ts._state == "released" and ts._run_spec:
                recommendations[ts._key] = "waiting"

        for ts in touched_tasks:
            for dts in ts._dependencies:
                if dts._exception_blame:
                    ts._exception_blame = dts._exception_blame
                    recommendations[ts._key] = "erred"
                    break

        for plugin in list(self.plugins.values()):
            try:
                plugin.update_graph(
                    self,
                    client=client,
                    tasks=tasks,
                    keys=keys,
                    restrictions=restrictions or {},
                    dependencies=dependencies,
                    priority=priority,
                    loose_restrictions=loose_restrictions,
                    resources=resources,
                    annotations=annotations,
                )
            except Exception as e:
                logger.exception(e)

        self.transitions(recommendations)

        for ts in touched_tasks:
            if ts._state in ("memory", "erred"):
                self.report_on_key(ts=ts, client=client)

        end = time()
        if self.digests is not None:
            self.digests["update-graph-duration"].add(end - start)

        # TODO: balance workers

    def stimulus_task_finished(self, key=None, worker=None, **kwargs):
        """Mark that a task has finished execution on a particular worker"""
        logger.debug("Stimulus task finished %s, %s", key, worker)

        recommendations: dict = {}
        client_msgs: dict = {}
        worker_msgs: dict = {}

        ws: WorkerState = self.state.workers[worker]
        ts: TaskState = self.state.tasks.get(key)
        if ts is None or ts._state == "released":
            logger.debug(
                "Received already computed task, worker: %s, state: %s"
                ", key: %s, who_has: %s",
                worker,
                ts._state if ts else "forgotten",
                key,
                ts._who_has if ts else {},
            )
            worker_msgs[worker] = [
                {
                    "op": "free-keys",
                    "keys": [key],
                    "stimulus_id": f"already-released-or-forgotten-{time()}",
                }
            ]
        elif ts._state == "memory":
            self.add_keys(worker=worker, keys=[key])
        else:
            ts._metadata.update(kwargs["metadata"])
            r: tuple = self.state.transition(
                key,
                "memory",
                kwargs=dict(worker=worker, **kwargs),
            )
            recommendations, client_msgs, worker_msgs = r

            if ts._state == "memory":
                assert ws in ts._who_has
        return recommendations, client_msgs, worker_msgs

    def stimulus_task_erred(
        self, key=None, worker=None, exception=None, traceback=None, **kwargs
    ):
        """Mark that a task has erred on a particular worker"""
        logger.debug("Stimulus task erred %s, %s", key, worker)

        ts: TaskState = self.state.tasks.get(key)
        if ts is None or ts._state != "processing":
            return {}, {}, {}

        retries: Py_ssize_t = ts._retries
        if retries > 0:
            retries -= 1
            ts._retries = retries
            return self.state.transition(key, "waiting")
        else:
            return self.state.transition(
                key,
                "erred",
                kwargs=dict(
                    cause=key,
                    exception=exception,
                    traceback=traceback,
                    worker=worker,
                    **kwargs,
                ),
            )

    def stimulus_retry(self, keys, client=None):
        logger.info("Client %s requests to retry %d keys", client, len(keys))
        if client:
            self.log_event(client, {"action": "retry", "count": len(keys)})

        stack = list(keys)
        seen = set()
        roots = []
        ts: TaskState
        dts: TaskState
        while stack:
            key = stack.pop()
            seen.add(key)
            ts = self.state.tasks[key]
            erred_deps = [dts._key for dts in ts._dependencies if dts._state == "erred"]
            if erred_deps:
                stack.extend(erred_deps)
            else:
                roots.append(key)

        recommendations: dict = {key: "waiting" for key in roots}
        self.transitions(recommendations)

        if self.state.validate:
            for key in seen:
                assert not self.state.tasks[key].exception_blame

        return tuple(seen)

    async def remove_worker(self, address, safe=False, close=True):
        """
        Remove worker from cluster

        We do this when a worker reports that it plans to leave or when it
        appears to be unresponsive.  This may send its tasks back to a released
        state.
        """
        with log_errors():
            if self.status == Status.closed:
                return

            address = self.coerce_address(address)

            if address not in self.state.workers:
                return "already-removed"

            host = get_address_host(address)

            ws: WorkerState = self.state.workers[address]

            event_msg = {
                "action": "remove-worker",
                "processing-tasks": dict(ws._processing),
            }
            self.log_event(address, event_msg.copy())
            event_msg["worker"] = address
            self.log_event("all", event_msg)

            logger.info("Remove worker %s", ws)
            if close:
                with suppress(AttributeError, CommClosedError):
                    self.stream_comms[address].send({"op": "close", "report": False})

            self.remove_resources(address)

            dh: dict = self.state.host_info[host]
            dh_addresses: set = dh["addresses"]
            dh_addresses.remove(address)
            dh["nthreads"] -= ws._nthreads
            self.state.total_nthreads -= ws._nthreads
            if not dh_addresses:
                del self.state.host_info[host]

            self.rpc.remove(address)
            del self.stream_comms[address]
            del self.state.aliases[ws._name]
            self.state.idle.pop(ws._address, None)
            self.state.saturated.discard(ws)
            del self.state.workers[address]
            ws.status = Status.closed
            self.state.running.discard(ws)
            self.state.total_occupancy -= ws._occupancy

            recommendations: dict = {}

            ts: TaskState
            for ts in list(ws._processing):
                k = ts._key
                recommendations[k] = "released"
                if not safe:
                    ts._suspicious += 1
                    ts._prefix._suspicious += 1
                    if ts._suspicious > self.allowed_failures:
                        del recommendations[k]
                        e = pickle.dumps(
                            KilledWorker(task=k, last_worker=ws.clean()), protocol=4
                        )
                        r = self.transition(k, "erred", exception=e, cause=k)
                        recommendations.update(r)
                        logger.info(
                            "Task %s marked as failed because %d workers died"
                            " while trying to run it",
                            ts._key,
                            self.allowed_failures,
                        )

            for ts in list(ws._has_what):
                self.state.remove_replica(ts, ws)
                if not ts._who_has:
                    if ts._run_spec:
                        recommendations[ts._key] = "released"
                    else:  # pure data
                        recommendations[ts._key] = "forgotten"

            self.transitions(recommendations)

            for plugin in list(self.plugins.values()):
                try:
                    result = plugin.remove_worker(scheduler=self, worker=address)
                    if inspect.isawaitable(result):
                        await result
                except Exception as e:
                    logger.exception(e)

            if not self.state.workers:
                logger.info("Lost all workers")

            for w in self.state.workers:
                self.bandwidth_workers.pop((address, w), None)
                self.bandwidth_workers.pop((w, address), None)

            def remove_worker_from_events():
                # If the worker isn't registered anymore after the delay, remove from events
                if address not in self.state.workers and address in self.events:
                    del self.events[address]

            cleanup_delay = parse_timedelta(
                dask.config.get("distributed.scheduler.events-cleanup-delay")
            )
            self.loop.call_later(cleanup_delay, remove_worker_from_events)
            logger.debug("Removed worker %s", ws)

        return "OK"

    def stimulus_cancel(self, comm, keys=None, client=None, force=False):
        """Stop execution on a list of keys"""
        logger.info("Client %s requests to cancel %d keys", client, len(keys))
        if client:
            self.log_event(
                client, {"action": "cancel", "count": len(keys), "force": force}
            )
        for key in keys:
            self.cancel_key(key, client, force=force)

    def cancel_key(self, key, client, retries=5, force=False):
        """Cancel a particular key and all dependents"""
        # TODO: this should be converted to use the transition mechanism
        ts: TaskState = self.state.tasks.get(key)
        dts: TaskState
        try:
            cs: ClientState = self.state.clients[client]
        except KeyError:
            return
        if ts is None or not ts._who_wants:  # no key yet, lets try again in a moment
            if retries:
                self.loop.call_later(
                    0.2, lambda: self.cancel_key(key, client, retries - 1)
                )
            return
        if force or ts._who_wants == {cs}:  # no one else wants this key
            for dts in list(ts._dependents):
                self.cancel_key(dts._key, client, force=force)
        logger.info("Scheduler cancels key %s.  Force=%s", key, force)
        self.report({"op": "cancelled-key", "key": key})
        clients = list(ts._who_wants) if force else [cs]
        for cs in clients:
            self.client_releases_keys(keys=[key], client=cs._client_key)

    def client_desires_keys(self, keys=None, client=None):
        cs: ClientState = self.state.clients.get(client)
        if cs is None:
            # For publish, queues etc.
            self.state.clients[client] = cs = ClientState(client)
        ts: TaskState
        for k in keys:
            ts = self.state.tasks.get(k)
            if ts is None:
                # For publish, queues etc.
                ts = self.state.new_task(k, None, "released")
            ts._who_wants.add(cs)
            cs._wants_what.add(ts)

            if ts._state in ("memory", "erred"):
                self.report_on_key(ts=ts, client=client)

    def client_releases_keys(self, keys=None, client=None):
        """Remove keys from client desired list"""

        if not isinstance(keys, list):
            keys = list(keys)
        cs: ClientState = self.state.clients[client]
        recommendations: dict = {}

        self.state._client_releases_keys(
            keys=keys, cs=cs, recommendations=recommendations
        )
        self.transitions(recommendations)

    def client_heartbeat(self, client=None):
        """Handle heartbeats from Client"""
        cs: ClientState = self.state.clients[client]
        cs._last_seen = time()

    def validate_state(self):
        self.state.validate_state()

    ###################
    # Manage Messages #
    ###################

    def report(self, msg: dict, ts: TaskState = None, client: str = None):
        """
        Publish updates to all listening Queues and Comms

        If the message contains a key then we only send the message to those
        comms that care about the key.
        """
        if ts is None:
            msg_key = msg.get("key")
            if msg_key is not None:
                tasks: dict = self.state.tasks
                ts = tasks.get(msg_key)

        cs: ClientState
        client_comms: dict = self.client_comms
        client_keys: list
        if ts is None:
            # Notify all clients
            client_keys = list(client_comms)
        elif client is None:
            # Notify clients interested in key
            client_keys = [cs._client_key for cs in ts._who_wants]
        else:
            # Notify clients interested in key (including `client`)
            client_keys = [
                cs._client_key for cs in ts._who_wants if cs._client_key != client
            ]
            client_keys.append(client)

        k: str
        for k in client_keys:
            c = client_comms.get(k)
            if c is None:
                continue
            try:
                c.send(msg)
                # logger.debug("Scheduler sends message to client %s", msg)
            except CommClosedError:
                if self.status == Status.running:
                    logger.critical(
                        "Closed comm %r while trying to write %s", c, msg, exc_info=True
                    )

    async def add_client(self, comm, client=None, versions=None):
        """Add client to network

        We listen to all future messages from this Comm.
        """
        assert client is not None
        comm.name = "Scheduler->Client"
        logger.info("Receive client connection: %s", client)
        self.log_event(["all", client], {"action": "add-client", "client": client})
        self.state.clients[client] = ClientState(client, versions=versions)

        for plugin in list(self.plugins.values()):
            try:
                plugin.add_client(scheduler=self, client=client)
            except Exception as e:
                logger.exception(e)

        try:
            bcomm = BatchedSend(interval="2ms", loop=self.loop)
            bcomm.start(comm)
            self.client_comms[client] = bcomm
            msg = {"op": "stream-start"}
            ws: WorkerState
            version_warning = version_module.error_message(
                version_module.get_versions(),
                {w: ws._versions for w, ws in self.state.workers.items()},
                versions,
            )
            msg.update(version_warning)
            bcomm.send(msg)

            try:
                await self.handle_stream(comm=comm, extra={"client": client})
            finally:
                self.remove_client(client=client)
                logger.debug("Finished handling client %s", client)
        finally:
            if not comm.closed():
                self.client_comms[client].send({"op": "stream-closed"})
            try:
                if not sys.is_finalizing():
                    await self.client_comms[client].close()
                    del self.client_comms[client]
                    if self.status == Status.running:
                        logger.info("Close client connection: %s", client)
            except TypeError:  # comm becomes None during GC
                pass

    def remove_client(self, client=None):
        """Remove client from network"""
        if self.status == Status.running:
            logger.info("Remove client %s", client)
        self.log_event(["all", client], {"action": "remove-client", "client": client})
        try:
            cs: ClientState = self.state.clients[client]
        except KeyError:
            # XXX is this a legitimate condition?
            pass
        else:
            ts: TaskState
            self.client_releases_keys(
                keys=[ts._key for ts in cs._wants_what], client=cs._client_key
            )
            del self.state.clients[client]

            for plugin in list(self.plugins.values()):
                try:
                    plugin.remove_client(scheduler=self, client=client)
                except Exception as e:
                    logger.exception(e)

        def remove_client_from_events():
            # If the client isn't registered anymore after the delay, remove from events
            if client not in self.state.clients and client in self.events:
                del self.events[client]

        cleanup_delay = parse_timedelta(
            dask.config.get("distributed.scheduler.events-cleanup-delay")
        )
        self.loop.call_later(cleanup_delay, remove_client_from_events)

    def send_task_to_worker(self, worker, ts: TaskState, duration: double = -1):
        """Send a single computational task to a worker"""
        try:
            msg: dict = self.state._task_to_msg(ts, duration)
            self.worker_send(worker, msg)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def handle_uncaught_error(self, **msg):
        logger.exception(clean_exception(**msg)[1])

    def handle_task_finished(self, key=None, worker=None, **msg):
        if worker not in self.state.workers:
            return
        validate_key(key)

        recommendations: dict
        client_msgs: dict
        worker_msgs: dict

        r: tuple = self.stimulus_task_finished(key=key, worker=worker, **msg)
        recommendations, client_msgs, worker_msgs = r
        self.state.transitions(recommendations, client_msgs, worker_msgs)

        self.send_all(client_msgs, worker_msgs)

    def handle_task_erred(self, key=None, **msg):
        recommendations: dict
        client_msgs: dict
        worker_msgs: dict
        r: tuple = self.stimulus_task_erred(key=key, **msg)
        recommendations, client_msgs, worker_msgs = r
        self.state.transitions(recommendations, client_msgs, worker_msgs)

        self.send_all(client_msgs, worker_msgs)

    def handle_missing_data(self, key=None, errant_worker=None, **kwargs):
        """Signal that `errant_worker` does not hold `key`

        This may either indicate that `errant_worker` is dead or that we may be
        working with stale data and need to remove `key` from the workers
        `has_what`.

        If no replica of a task is available anymore, the task is transitioned
        back to released and rescheduled, if possible.

        Parameters
        ----------
        key : str, optional
            Task key that could not be found, by default None
        errant_worker : str, optional
            Address of the worker supposed to hold a replica, by default None
        """
        logger.debug("handle missing data key=%s worker=%s", key, errant_worker)
        self.log_event(errant_worker, {"action": "missing-data", "key": key})
        ts: TaskState = self.state.tasks.get(key)
        if ts is None:
            return
        ws: WorkerState = self.state.workers.get(errant_worker)

        if ws is not None and ws in ts._who_has:
            self.state.remove_replica(ts, ws)
        if ts.state == "memory" and not ts._who_has:
            if ts._run_spec:
                self.transitions({key: "released"})
            else:
                self.transitions({key: "forgotten"})

    def release_worker_data(self, key, worker):
        ws: WorkerState = self.state.workers.get(worker)
        ts: TaskState = self.state.tasks.get(key)
        if not ws or not ts:
            return
        recommendations: dict = {}
        if ws in ts._who_has:
            self.state.remove_replica(ts, ws)
            if not ts._who_has:
                recommendations[ts._key] = "released"
        if recommendations:
            self.transitions(recommendations)

    def handle_long_running(self, key=None, worker=None, compute_duration=None):
        """A task has seceded from the thread pool

        We stop the task from being stolen in the future, and change task
        duration accounting as if the task has stopped.
        """
        if key not in self.state.tasks:
            logger.debug("Skipping long_running since key %s was already released", key)
            return
        ts: TaskState = self.state.tasks[key]
        steal = self.state.extensions.get("stealing")
        if steal is not None:
            steal.remove_key_from_stealable(ts)

        ws: WorkerState = ts._processing_on
        if ws is None:
            logger.debug("Received long-running signal from duplicate task. Ignoring.")
            return

        if compute_duration:
            old_duration: double = ts._prefix._duration_average
            new_duration: double = compute_duration
            avg_duration: double
            if old_duration < 0:
                avg_duration = new_duration
            else:
                avg_duration = 0.5 * old_duration + 0.5 * new_duration

            ts._prefix._duration_average = avg_duration

        occ: double = ws._processing[ts]
        ws._occupancy -= occ
        self.state.total_occupancy -= occ
        # Cannot remove from processing since we're using this for things like
        # idleness detection. Idle workers are typically targeted for
        # downscaling but we should not downscale workers with long running
        # tasks
        ws._processing[ts] = 0
        ws._long_running.add(ts)
        self.state.check_idle_saturated(ws)

    def handle_worker_status_change(self, status: str, worker: str) -> None:
        ws: WorkerState = self.state.workers.get(worker)  # type: ignore
        if not ws:
            return
        prev_status = ws._status
        ws._status = Status.lookup[status]  # type: ignore
        if ws._status == prev_status:
            return

        self.log_event(
            ws._address,
            {
                "action": "worker-status-change",
                "prev-status": prev_status.name,
                "status": status,
            },
        )

        if ws._status == Status.running:
            self.state.running.add(ws)

            recs = {}
            ts: TaskState
            for ts in self.state.unrunnable:
                valid: set = self.state.valid_workers(ts)
                if valid is None or ws in valid:
                    recs[ts._key] = "waiting"
            if recs:
                client_msgs: dict = {}
                worker_msgs: dict = {}
                self.state.transitions(recs, client_msgs, worker_msgs)
                self.send_all(client_msgs, worker_msgs)

        else:
            self.state.running.discard(ws)

    async def handle_worker(self, comm=None, worker=None):
        """
        Listen to responses from a single worker

        This is the main loop for scheduler-worker interaction

        See Also
        --------
        Scheduler.handle_client: Equivalent coroutine for clients
        """
        comm.name = "Scheduler connection to worker"
        worker_comm = self.stream_comms[worker]
        worker_comm.start(comm)
        logger.info("Starting worker compute stream, %s", worker)
        try:
            await self.handle_stream(comm=comm, extra={"worker": worker})
        finally:
            if worker in self.stream_comms:
                worker_comm.abort()
                await self.remove_worker(address=worker)

    def add_plugin(
        self,
        plugin: SchedulerPlugin,
        *,
        idempotent: bool = False,
        name: "str | None" = None,
        **kwargs,
    ):
        """Add external plugin to scheduler.

        See https://distributed.readthedocs.io/en/latest/plugins.html

        Parameters
        ----------
        plugin : SchedulerPlugin
            SchedulerPlugin instance to add
        idempotent : bool
            If true, the plugin is assumed to already exist and no
            action is taken.
        name : str
            A name for the plugin, if None, the name attribute is
            checked on the Plugin instance and generated if not
            discovered.
        **kwargs
            Deprecated; additional arguments passed to the `plugin` class if it is
            not already an instance
        """
        if isinstance(plugin, type):
            warnings.warn(
                "Adding plugins by class is deprecated and will be disabled in a "
                "future release. Please add plugins by instance instead.",
                category=FutureWarning,
            )
            plugin = plugin(self, **kwargs)  # type: ignore
        elif kwargs:
            raise ValueError("kwargs provided but plugin is already an instance")

        if name is None:
            name = _get_plugin_name(plugin)

        if name in self.plugins:
            if idempotent:
                return
            warnings.warn(
                f"Scheduler already contains a plugin with name {name}; overwriting.",
                category=UserWarning,
            )

        self.plugins[name] = plugin

    def remove_plugin(
        self,
        name: "str | None" = None,
        plugin: "SchedulerPlugin | None" = None,
    ) -> None:
        """Remove external plugin from scheduler

        Parameters
        ----------
        name : str
            Name of the plugin to remove
        plugin : SchedulerPlugin
            Deprecated; use `name` argument instead. Instance of a
            SchedulerPlugin class to remove;
        """
        # TODO: Remove this block of code once removing plugins by value is disabled
        if bool(name) == bool(plugin):
            raise ValueError("Must provide plugin or name (mutually exclusive)")
        if isinstance(name, SchedulerPlugin):
            # Backwards compatibility - the sig used to be (plugin, name)
            plugin = name
            name = None
        if plugin is not None:
            warnings.warn(
                "Removing scheduler plugins by value is deprecated and will be disabled "
                "in a future release. Please remove scheduler plugins by name instead.",
                category=FutureWarning,
            )
            if hasattr(plugin, "name"):
                name = plugin.name  # type: ignore
            else:
                names = [k for k, v in self.plugins.items() if v is plugin]
                if not names:
                    raise ValueError(
                        f"Could not find {plugin} among the current scheduler plugins"
                    )
                if len(names) > 1:
                    raise ValueError(
                        f"Multiple instances of {plugin} were found in the current "
                        "scheduler plugins; we cannot remove this plugin."
                    )
                name = names[0]
        assert name is not None
        # End deprecated code

        try:
            del self.plugins[name]
        except KeyError:
            raise ValueError(
                f"Could not find plugin {name!r} among the current scheduler plugins"
            )

    async def register_scheduler_plugin(self, plugin, name=None, idempotent=None):
        """Register a plugin on the scheduler."""
        if not dask.config.get("distributed.scheduler.pickle"):
            raise ValueError(
                "Cannot register a scheduler plugin as the scheduler "
                "has been explicitly disallowed from deserializing "
                "arbitrary bytestrings using pickle via the "
                "'distributed.scheduler.pickle' configuration setting."
            )
        plugin = loads(plugin)

        if name is None:
            name = _get_plugin_name(plugin)

        if name in self.plugins and idempotent:
            return

        if hasattr(plugin, "start"):
            result = plugin.start(self)
            if inspect.isawaitable(result):
                await result

        self.add_plugin(plugin, name=name, idempotent=idempotent)

    def worker_send(self, worker, msg):
        """Send message to worker

        This also handles connection failures by adding a callback to remove
        the worker on the next cycle.
        """
        stream_comms: dict = self.stream_comms
        try:
            stream_comms[worker].send(msg)
        except (CommClosedError, AttributeError):
            self.loop.add_callback(self.remove_worker, address=worker)

    def client_send(self, client, msg):
        """Send message to client"""
        client_comms: dict = self.client_comms
        c = client_comms.get(client)
        if c is None:
            return
        try:
            c.send(msg)
        except CommClosedError:
            if self.status == Status.running:
                logger.critical(
                    "Closed comm %r while trying to write %s", c, msg, exc_info=True
                )

    def send_all(self, client_msgs: dict, worker_msgs: dict):
        """Send messages to client and workers"""
        client_comms: dict = self.client_comms
        stream_comms: dict = self.stream_comms
        msgs: list

        for client, msgs in client_msgs.items():
            c = client_comms.get(client)
            if c is None:
                continue
            try:
                c.send(*msgs)
            except CommClosedError:
                if self.status == Status.running:
                    logger.critical(
                        "Closed comm %r while trying to write %s",
                        c,
                        msgs,
                        exc_info=True,
                    )

        for worker, msgs in worker_msgs.items():
            try:
                w = stream_comms[worker]
                w.send(*msgs)
            except KeyError:
                # worker already gone
                pass
            except (CommClosedError, AttributeError):
                self.loop.add_callback(self.remove_worker, address=worker)

    ############################
    # Less common interactions #
    ############################

    async def scatter(
        self,
        comm=None,
        data=None,
        workers=None,
        client=None,
        broadcast=False,
        timeout=2,
    ):
        """Send data out to workers

        See also
        --------
        Scheduler.broadcast:
        """
        ws: WorkerState

        start = time()
        while True:
            if workers is None:
                wss = self.state.running
            else:
                workers = [self.coerce_address(w) for w in workers]
                wss = {self.state.workers[w] for w in workers}
                wss = {ws for ws in wss if ws._status == Status.running}

            if wss:
                break
            if time() > start + timeout:
                raise TimeoutError("No valid workers found")
            await asyncio.sleep(0.1)

        nthreads = {ws._address: ws.nthreads for ws in wss}

        assert isinstance(data, dict)

        keys, who_has, nbytes = await scatter_to_workers(
            nthreads, data, rpc=self.rpc, report=False
        )

        self.update_data(who_has=who_has, nbytes=nbytes, client=client)

        if broadcast:
            n = len(nthreads) if broadcast is True else broadcast
            await self.replicate(keys=keys, workers=workers, n=n)

        self.log_event(
            [client, "all"], {"action": "scatter", "client": client, "count": len(data)}
        )
        return keys

    async def gather(self, keys, serializers=None):
        """Collect data from workers to the scheduler"""
        ws: WorkerState
        keys = list(keys)
        who_has = {}
        for key in keys:
            ts: TaskState = self.state.tasks.get(key)
            if ts is not None:
                who_has[key] = [ws._address for ws in ts._who_has]
            else:
                who_has[key] = []

        data, missing_keys, missing_workers = await gather_from_workers(
            who_has, rpc=self.rpc, close=False, serializers=serializers
        )
        if not missing_keys:
            result = {"status": "OK", "data": data}
        else:
            missing_states = [
                (self.state.tasks[key].state if key in self.state.tasks else None)
                for key in missing_keys
            ]
            logger.exception(
                "Couldn't gather keys %s state: %s workers: %s",
                missing_keys,
                missing_states,
                missing_workers,
            )
            result = {"status": "error", "keys": missing_keys}
            with log_errors():
                # Remove suspicious workers from the scheduler but allow them to
                # reconnect.
                await asyncio.gather(
                    *(
                        self.remove_worker(address=worker, close=False)
                        for worker in missing_workers
                    )
                )
                recommendations: dict
                client_msgs: dict = {}
                worker_msgs: dict = {}
                for key, workers in missing_keys.items():
                    # Task may already be gone if it was held by a
                    # `missing_worker`
                    ts: TaskState = self.state.tasks.get(key)
                    logger.exception(
                        "Workers don't have promised key: %s, %s",
                        str(workers),
                        str(key),
                    )
                    if not workers or ts is None:
                        continue
                    recommendations: dict = {key: "released"}
                    for worker in workers:
                        ws = self.state.workers.get(worker)
                        if ws is not None and ws in ts._who_has:
                            self.state.remove_replica(ts, ws)
                            self.state.transitions(
                                recommendations, client_msgs, worker_msgs
                            )
                self.send_all(client_msgs, worker_msgs)

        self.log_event("all", {"action": "gather", "count": len(keys)})
        return result

    def clear_task_state(self):
        # XXX what about nested state such as ClientState.wants_what
        # (see also fire-and-forget...)
        logger.info("Clear task state")
        for collection in self._task_state_collections:
            collection.clear()

    async def restart(self, client=None, timeout=30):
        """Restart all workers. Reset local state."""
        with log_errors():

            n_workers = len(self.state.workers)

            logger.info("Send lost future signal to clients")
            cs: ClientState
            ts: TaskState
            for cs in self.state.clients.values():
                self.client_releases_keys(
                    keys=[ts._key for ts in cs._wants_what], client=cs._client_key
                )

            ws: WorkerState
            nannies = {addr: ws._nanny for addr, ws in self.state.workers.items()}

            for addr in list(self.state.workers):
                try:
                    # Ask the worker to close if it doesn't have a nanny,
                    # otherwise the nanny will kill it anyway
                    await self.remove_worker(address=addr, close=addr not in nannies)
                except Exception:
                    logger.info(
                        "Exception while restarting.  This is normal", exc_info=True
                    )

            self.clear_task_state()

            for plugin in list(self.plugins.values()):
                try:
                    plugin.restart(self)
                except Exception as e:
                    logger.exception(e)

            logger.debug("Send kill signal to nannies: %s", nannies)

            nannies = [
                rpc(nanny_address, connection_args=self.connection_args)
                for nanny_address in nannies.values()
                if nanny_address is not None
            ]

            resps = All(
                [
                    nanny.restart(
                        close=True, timeout=timeout * 0.8, executor_wait=False
                    )
                    for nanny in nannies
                ]
            )
            try:
                resps = await asyncio.wait_for(resps, timeout)
            except TimeoutError:
                logger.error(
                    "Nannies didn't report back restarted within "
                    "timeout.  Continuuing with restart process"
                )
            else:
                if not all(resp == "OK" for resp in resps):
                    logger.error(
                        "Not all workers responded positively: %s", resps, exc_info=True
                    )
            finally:
                await asyncio.gather(*[nanny.close_rpc() for nanny in nannies])

            self.clear_task_state()

            with suppress(AttributeError):
                for c in self._worker_coroutines:
                    c.cancel()

            self.log_event([client, "all"], {"action": "restart", "client": client})
            start = time()
            while time() < start + 10 and len(self.state.workers) < n_workers:
                await asyncio.sleep(0.01)

            self.report({"op": "restart"})

    async def broadcast(
        self,
        comm=None,
        *,
        msg: dict,
        workers: "list[str] | None" = None,
        hosts: "list[str] | None" = None,
        nanny: bool = False,
        serializers=None,
        on_error: "Literal['raise', 'return', 'return_pickle', 'ignore']" = "raise",
    ) -> dict:  # dict[str, Any]
        """Broadcast message to workers, return all results"""
        if workers is True:
            warnings.warn(
                "workers=True is deprecated; pass workers=None or omit instead",
                category=FutureWarning,
            )
            workers = None
        if workers is None:
            if hosts is None:
                workers = list(self.state.workers)
            else:
                workers = []
        if hosts is not None:
            for host in hosts:
                dh: dict = self.state.host_info.get(host)  # type: ignore
                if dh is not None:
                    workers.extend(dh["addresses"])
        # TODO replace with worker_list

        if nanny:
            addresses = [self.state.workers[w].nanny for w in workers]
        else:
            addresses = workers

        ERROR = object()

        async def send_message(addr):
            try:
                comm = await self.rpc.connect(addr)
                comm.name = "Scheduler Broadcast"
                try:
                    resp = await send_recv(
                        comm, close=True, serializers=serializers, **msg
                    )
                finally:
                    self.rpc.reuse(addr, comm)
                return resp
            except Exception as e:
                logger.error(f"broadcast to {addr} failed: {e.__class__.__name__}: {e}")
                if on_error == "raise":
                    raise
                elif on_error == "return":
                    return e
                elif on_error == "return_pickle":
                    return dumps(e, protocol=4)
                elif on_error == "ignore":
                    return ERROR
                else:
                    raise ValueError(
                        "on_error must be 'raise', 'return', 'return_pickle', "
                        f"or 'ignore'; got {on_error!r}"
                    )

        results = await All(
            [send_message(address) for address in addresses if address is not None]
        )

        return {k: v for k, v in zip(workers, results) if v is not ERROR}

    async def proxy(self, comm=None, msg=None, worker=None, serializers=None):
        """Proxy a communication through the scheduler to some other worker"""
        d = await self.broadcast(
            comm=comm, msg=msg, workers=[worker], serializers=serializers
        )
        return d[worker]

    async def gather_on_worker(
        self, worker_address: str, who_has: "dict[str, list[str]]"
    ) -> set:
        """Peer-to-peer copy of keys from multiple workers to a single worker

        Parameters
        ----------
        worker_address: str
            Recipient worker address to copy keys to
        who_has: dict[Hashable, list[str]]
            {key: [sender address, sender address, ...], key: ...}

        Returns
        -------
        returns:
            set of keys that failed to be copied
        """
        try:
            result = await retry_operation(
                self.rpc(addr=worker_address).gather, who_has=who_has
            )
        except OSError as e:
            # This can happen e.g. if the worker is going through controlled shutdown;
            # it doesn't necessarily mean that it went unexpectedly missing
            logger.warning(
                f"Communication with worker {worker_address} failed during "
                f"replication: {e.__class__.__name__}: {e}"
            )
            return set(who_has)

        ws: WorkerState = self.state.workers.get(worker_address)  # type: ignore

        if ws is None:
            logger.warning(f"Worker {worker_address} lost during replication")
            return set(who_has)
        elif result["status"] == "OK":
            keys_failed = set()
            keys_ok: Set = who_has.keys()
        elif result["status"] == "partial-fail":
            keys_failed = set(result["keys"])
            keys_ok = who_has.keys() - keys_failed
            logger.warning(
                f"Worker {worker_address} failed to acquire keys: {result['keys']}"
            )
        else:  # pragma: nocover
            raise ValueError(f"Unexpected message from {worker_address}: {result}")

        for key in keys_ok:
            ts: TaskState = self.state.tasks.get(key)  # type: ignore
            if ts is None or ts._state != "memory":
                logger.warning(f"Key lost during replication: {key}")
                continue
            if ws not in ts._who_has:
                self.state.add_replica(ts, ws)

        return keys_failed

    async def delete_worker_data(
        self, worker_address: str, keys: "Collection[str]"
    ) -> None:
        """Delete data from a worker and update the corresponding worker/task states

        Parameters
        ----------
        worker_address: str
            Worker address to delete keys from
        keys: list[str]
            List of keys to delete on the specified worker
        """
        try:
            await retry_operation(
                self.rpc(addr=worker_address).free_keys,
                keys=list(keys),
                stimulus_id=f"delete-data-{time()}",
            )
        except OSError as e:
            # This can happen e.g. if the worker is going through controlled shutdown;
            # it doesn't necessarily mean that it went unexpectedly missing
            logger.warning(
                f"Communication with worker {worker_address} failed during "
                f"replication: {e.__class__.__name__}: {e}"
            )
            return

        ws: WorkerState = self.state.workers.get(worker_address)  # type: ignore
        if ws is None:
            return

        for key in keys:
            ts: TaskState = self.state.tasks.get(key)  # type: ignore
            if ts is not None and ws in ts._who_has:
                assert ts._state == "memory"
                self.state.remove_replica(ts, ws)
                if not ts._who_has:
                    # Last copy deleted
                    self.transitions({key: "released"})

        self.log_event(ws._address, {"action": "remove-worker-data", "keys": keys})

    async def rebalance(
        self,
        comm=None,
        keys: "Iterable[Hashable]" = None,
        workers: "Iterable[str]" = None,
    ) -> dict:
        """Rebalance keys so that each worker ends up with roughly the same process
        memory (managed+unmanaged).

        .. warning::
           This operation is generally not well tested against normal operation of the
           scheduler. It is not recommended to use it while waiting on computations.

        **Algorithm**

        #. Find the mean occupancy of the cluster, defined as data managed by dask +
           unmanaged process memory that has been there for at least 30 seconds
           (``distributed.worker.memory.recent-to-old-time``).
           This lets us ignore temporary spikes caused by task heap usage.

           Alternatively, you may change how memory is measured both for the individual
           workers as well as to calculate the mean through
           ``distributed.worker.memory.rebalance.measure``. Namely, this can be useful
           to disregard inaccurate OS memory measurements.

        #. Discard workers whose occupancy is within 5% of the mean cluster occupancy
           (``distributed.worker.memory.rebalance.sender-recipient-gap`` / 2).
           This helps avoid data from bouncing around the cluster repeatedly.
        #. Workers above the mean are senders; those below are recipients.
        #. Discard senders whose absolute occupancy is below 30%
           (``distributed.worker.memory.rebalance.sender-min``). In other words, no data
           is moved regardless of imbalancing as long as all workers are below 30%.
        #. Discard recipients whose absolute occupancy is above 60%
           (``distributed.worker.memory.rebalance.recipient-max``).
           Note that this threshold by default is the same as
           ``distributed.worker.memory.target`` to prevent workers from accepting data
           and immediately spilling it out to disk.
        #. Iteratively pick the sender and recipient that are farthest from the mean and
           move the *least recently inserted* key between the two, until either all
           senders or all recipients fall within 5% of the mean.

           A recipient will be skipped if it already has a copy of the data. In other
           words, this method does not degrade replication.
           A key will be skipped if there are no recipients available with enough memory
           to accept the key and that don't already hold a copy.

        The least recently insertd (LRI) policy is a greedy choice with the advantage of
        being O(1), trivial to implement (it relies on python dict insertion-sorting)
        and hopefully good enough in most cases. Discarded alternative policies were:

        - Largest first. O(n*log(n)) save for non-trivial additional data structures and
          risks causing the largest chunks of data to repeatedly move around the
          cluster like pinballs.
        - Least recently used (LRU). This information is currently available on the
          workers only and not trivial to replicate on the scheduler; transmitting it
          over the network would be very expensive. Also, note that dask will go out of
          its way to minimise the amount of time intermediate keys are held in memory,
          so in such a case LRI is a close approximation of LRU.

        Parameters
        ----------
        keys: optional
            allowlist of dask keys that should be considered for moving. All other keys
            will be ignored. Note that this offers no guarantee that a key will actually
            be moved (e.g. because it is unnecessary or because there are no viable
            recipient workers for it).
        workers: optional
            allowlist of workers addresses to be considered as senders or recipients.
            All other workers will be ignored. The mean cluster occupancy will be
            calculated only using the allowed workers.
        """
        with log_errors():
            wss: "Collection[WorkerState]"
            if workers is not None:
                wss = [self.state.workers[w] for w in workers]
            else:
                wss = self.state.workers.values()
            if not wss:
                return {"status": "OK"}

            if keys is not None:
                if not isinstance(keys, Set):
                    keys = set(keys)  # unless already a set-like
                if not keys:
                    return {"status": "OK"}
                missing_data = [
                    k
                    for k in keys
                    if k not in self.state.tasks or not self.state.tasks[k].who_has
                ]
                if missing_data:
                    return {"status": "partial-fail", "keys": missing_data}

            msgs = self._rebalance_find_msgs(keys, wss)
            if not msgs:
                return {"status": "OK"}

            async with self._lock:
                result = await self._rebalance_move_data(msgs)
                if result["status"] == "partial-fail" and keys is None:
                    # Only return failed keys if the client explicitly asked for them
                    result = {"status": "OK"}
                return result

    def _rebalance_find_msgs(
        self,
        keys: "Set[Hashable] | None",
        workers: "Iterable[WorkerState]",
    ) -> "list[tuple[WorkerState, WorkerState, TaskState]]":
        """Identify workers that need to lose keys and those that can receive them,
        together with how many bytes each needs to lose/receive. Then, pair a sender
        worker with a recipient worker for each key, until the cluster is rebalanced.

        This method only defines the work to be performed; it does not start any network
        transfers itself.

        The big-O complexity is O(wt + ke*log(we)), where

        - wt is the total number of workers on the cluster (or the number of allowed
          workers, if explicitly stated by the user)
        - we is the number of workers that are eligible to be senders or recipients
        - kt is the total number of keys on the cluster (or on the allowed workers)
        - ke is the number of keys that need to be moved in order to achieve a balanced
          cluster

        There is a degenerate edge case O(wt + kt*log(we)) when kt is much greater than
        the number of allowed keys, or when most keys are replicated or cannot be
        moved for some other reason.

        Returns list of tuples to feed into _rebalance_move_data:

        - sender worker
        - recipient worker
        - task to be transferred
        """
        ts: TaskState
        ws: WorkerState

        # Heaps of workers, managed by the heapq module, that need to send/receive data,
        # with how many bytes each needs to send/receive.
        #
        # Each element of the heap is a tuple constructed as follows:
        # - snd_bytes_max/rec_bytes_max: maximum number of bytes to send or receive.
        #   This number is negative, so that the workers farthest from the cluster mean
        #   are at the top of the smallest-first heaps.
        # - snd_bytes_min/rec_bytes_min: minimum number of bytes after sending/receiving
        #   which the worker should not be considered anymore. This is also negative.
        # - arbitrary unique number, there just to to make sure that WorkerState objects
        #   are never used for sorting in the unlikely event that two processes have
        #   exactly the same number of bytes allocated.
        # - WorkerState
        # - iterator of all tasks in memory on the worker (senders only), insertion
        #   sorted (least recently inserted first).
        #   Note that this iterator will typically *not* be exhausted. It will only be
        #   exhausted if, after moving away from the worker all keys that can be moved,
        #   is insufficient to drop snd_bytes_min above 0.
        senders: "list[tuple[int, int, int, WorkerState, Iterator[TaskState]]]" = []
        recipients: "list[tuple[int, int, int, WorkerState]]" = []

        # Output: [(sender, recipient, task), ...]
        msgs: "list[tuple[WorkerState, WorkerState, TaskState]]" = []

        # By default, this is the optimistic memory, meaning total process memory minus
        # unmanaged memory that appeared over the last 30 seconds
        # (distributed.worker.memory.recent-to-old-time).
        # This lets us ignore temporary spikes caused by task heap usage.
        memory_by_worker = [
            (ws, getattr(ws.memory, self.MEMORY_REBALANCE_MEASURE)) for ws in workers
        ]
        mean_memory = sum(m for _, m in memory_by_worker) // len(memory_by_worker)

        for ws, ws_memory in memory_by_worker:
            if ws.memory_limit:
                half_gap = int(self.MEMORY_REBALANCE_HALF_GAP * ws.memory_limit)
                sender_min = self.MEMORY_REBALANCE_SENDER_MIN * ws.memory_limit
                recipient_max = self.MEMORY_REBALANCE_RECIPIENT_MAX * ws.memory_limit
            else:
                half_gap = 0
                sender_min = 0.0
                recipient_max = math.inf

            if (
                ws._has_what
                and ws_memory >= mean_memory + half_gap
                and ws_memory >= sender_min
            ):
                # This may send the worker below sender_min (by design)
                snd_bytes_max = mean_memory - ws_memory  # negative
                snd_bytes_min = snd_bytes_max + half_gap  # negative
                # See definition of senders above
                senders.append(
                    (snd_bytes_max, snd_bytes_min, id(ws), ws, iter(ws._has_what))
                )
            elif ws_memory < mean_memory - half_gap and ws_memory < recipient_max:
                # This may send the worker above recipient_max (by design)
                rec_bytes_max = ws_memory - mean_memory  # negative
                rec_bytes_min = rec_bytes_max + half_gap  # negative
                # See definition of recipients above
                recipients.append((rec_bytes_max, rec_bytes_min, id(ws), ws))

        # Fast exit in case no transfers are necessary or possible
        if not senders or not recipients:
            self.log_event(
                "all",
                {
                    "action": "rebalance",
                    "senders": len(senders),
                    "recipients": len(recipients),
                    "moved_keys": 0,
                },
            )
            return []

        heapq.heapify(senders)
        heapq.heapify(recipients)

        snd_ws: WorkerState
        rec_ws: WorkerState

        while senders and recipients:
            snd_bytes_max, snd_bytes_min, _, snd_ws, ts_iter = senders[0]

            # Iterate through tasks in memory, least recently inserted first
            for ts in ts_iter:
                if keys is not None and ts.key not in keys:
                    continue
                nbytes = ts.nbytes
                if nbytes + snd_bytes_max > 0:
                    # Moving this task would cause the sender to go below mean and
                    # potentially risk becoming a recipient, which would cause tasks to
                    # bounce around. Move on to the next task of the same sender.
                    continue

                # Find the recipient, farthest from the mean, which
                # 1. has enough available RAM for this task, and
                # 2. doesn't hold a copy of this task already
                # There may not be any that satisfies these conditions; in this case
                # this task won't be moved.
                skipped_recipients = []
                use_recipient = False
                while recipients and not use_recipient:
                    rec_bytes_max, rec_bytes_min, _, rec_ws = recipients[0]
                    if nbytes + rec_bytes_max > 0:
                        # recipients are sorted by rec_bytes_max.
                        # The next ones will be worse; no reason to continue iterating
                        break
                    use_recipient = ts not in rec_ws._has_what
                    if not use_recipient:
                        skipped_recipients.append(heapq.heappop(recipients))

                for recipient in skipped_recipients:
                    heapq.heappush(recipients, recipient)

                if not use_recipient:
                    # This task has no recipients available. Leave it on the sender and
                    # move on to the next task of the same sender.
                    continue

                # Schedule task for transfer from sender to recipient
                msgs.append((snd_ws, rec_ws, ts))

                # *_bytes_max/min are all negative for heap sorting
                snd_bytes_max += nbytes
                snd_bytes_min += nbytes
                rec_bytes_max += nbytes
                rec_bytes_min += nbytes

                # Stop iterating on the tasks of this sender for now and, if it still
                # has bytes to lose, push it back into the senders heap; it may or may
                # not come back on top again.
                if snd_bytes_min < 0:
                    # See definition of senders above
                    heapq.heapreplace(
                        senders,
                        (snd_bytes_max, snd_bytes_min, id(snd_ws), snd_ws, ts_iter),
                    )
                else:
                    heapq.heappop(senders)

                # If recipient still has bytes to gain, push it back into the recipients
                # heap; it may or may not come back on top again.
                if rec_bytes_min < 0:
                    # See definition of recipients above
                    heapq.heapreplace(
                        recipients,
                        (rec_bytes_max, rec_bytes_min, id(rec_ws), rec_ws),
                    )
                else:
                    heapq.heappop(recipients)

                # Move to next sender with the most data to lose.
                # It may or may not be the same sender again.
                break

            else:  # for ts in ts_iter
                # Exhausted tasks on this sender
                heapq.heappop(senders)

        return msgs

    async def _rebalance_move_data(
        self, msgs: "list[tuple[WorkerState, WorkerState, TaskState]]"
    ) -> dict:
        """Perform the actual transfer of data across the network in rebalance().
        Takes in input the output of _rebalance_find_msgs(), that is a list of tuples:

        - sender worker
        - recipient worker
        - task to be transferred

        FIXME this method is not robust when the cluster is not idle.
        """
        snd_ws: WorkerState
        rec_ws: WorkerState
        ts: TaskState

        to_recipients: "defaultdict[str, defaultdict[str, list[str]]]" = defaultdict(
            lambda: defaultdict(list)
        )
        for snd_ws, rec_ws, ts in msgs:
            to_recipients[rec_ws.address][ts._key].append(snd_ws.address)
        failed_keys_by_recipient = dict(
            zip(
                to_recipients,
                await asyncio.gather(
                    *(
                        # Note: this never raises exceptions
                        self.gather_on_worker(w, who_has)
                        for w, who_has in to_recipients.items()
                    )
                ),
            )
        )

        to_senders = defaultdict(list)
        for snd_ws, rec_ws, ts in msgs:
            if ts._key not in failed_keys_by_recipient[rec_ws.address]:
                to_senders[snd_ws.address].append(ts._key)

        # Note: this never raises exceptions
        await asyncio.gather(
            *(self.delete_worker_data(r, v) for r, v in to_senders.items())
        )

        for r, v in to_recipients.items():
            self.log_event(r, {"action": "rebalance", "who_has": v})
        self.log_event(
            "all",
            {
                "action": "rebalance",
                "senders": valmap(len, to_senders),
                "recipients": valmap(len, to_recipients),
                "moved_keys": len(msgs),
            },
        )

        missing_keys = {k for r in failed_keys_by_recipient.values() for k in r}
        if missing_keys:
            return {"status": "partial-fail", "keys": list(missing_keys)}
        else:
            return {"status": "OK"}

    async def replicate(
        self,
        comm=None,
        keys=None,
        n=None,
        workers=None,
        branching_factor=2,
        delete=True,
        lock=True,
    ):
        """Replicate data throughout cluster

        This performs a tree copy of the data throughout the network
        individually on each piece of data.

        Parameters
        ----------
        keys: Iterable
            list of keys to replicate
        n: int
            Number of replications we expect to see within the cluster
        branching_factor: int, optional
            The number of workers that can copy data in each generation.
            The larger the branching factor, the more data we copy in
            a single step, but the more a given worker risks being
            swamped by data requests.

        See also
        --------
        Scheduler.rebalance
        """
        ws: WorkerState
        wws: WorkerState
        ts: TaskState

        assert branching_factor > 0
        async with self._lock if lock else empty_context:
            if workers is not None:
                workers = {self.state.workers[w] for w in self.workers_list(workers)}
                workers = {ws for ws in workers if ws._status == Status.running}
            else:
                workers = self.state.running

            if n is None:
                n = len(workers)
            else:
                n = min(n, len(workers))
            if n == 0:
                raise ValueError("Can not use replicate to delete data")

            tasks = {self.state.tasks[k] for k in keys}
            missing_data = [ts._key for ts in tasks if not ts._who_has]
            if missing_data:
                return {"status": "partial-fail", "keys": missing_data}

            # Delete extraneous data
            if delete:
                del_worker_tasks = defaultdict(set)
                for ts in tasks:
                    del_candidates = tuple(ts._who_has & workers)
                    if len(del_candidates) > n:
                        for ws in random.sample(
                            del_candidates, len(del_candidates) - n
                        ):
                            del_worker_tasks[ws].add(ts)

                # Note: this never raises exceptions
                await asyncio.gather(
                    *[
                        self.delete_worker_data(ws._address, [t.key for t in tasks])
                        for ws, tasks in del_worker_tasks.items()
                    ]
                )

            # Copy not-yet-filled data
            while tasks:
                gathers = defaultdict(dict)
                for ts in list(tasks):
                    if ts._state == "forgotten":
                        # task is no longer needed by any client or dependant task
                        tasks.remove(ts)
                        continue
                    n_missing = n - len(ts._who_has & workers)
                    if n_missing <= 0:
                        # Already replicated enough
                        tasks.remove(ts)
                        continue

                    count = min(n_missing, branching_factor * len(ts._who_has))
                    assert count > 0

                    for ws in random.sample(tuple(workers - ts._who_has), count):
                        gathers[ws._address][ts._key] = [
                            wws._address for wws in ts._who_has
                        ]

                await asyncio.gather(
                    *(
                        # Note: this never raises exceptions
                        self.gather_on_worker(w, who_has)
                        for w, who_has in gathers.items()
                    )
                )
                for r, v in gathers.items():
                    self.log_event(r, {"action": "replicate-add", "who_has": v})

            self.log_event(
                "all",
                {
                    "action": "replicate",
                    "workers": list(workers),
                    "key-count": len(keys),
                    "branching-factor": branching_factor,
                },
            )

    def workers_to_close(
        self,
        comm=None,
        memory_ratio: "int | float | None" = None,
        n: "int | None" = None,
        key: "Callable[[WorkerState], Hashable] | None" = None,
        minimum: "int | None" = None,
        target: "int | None" = None,
        attribute: str = "address",
    ) -> "list[str]":
        """
        Find workers that we can close with low cost

        This returns a list of workers that are good candidates to retire.
        These workers are not running anything and are storing
        relatively little data relative to their peers.  If all workers are
        idle then we still maintain enough workers to have enough RAM to store
        our data, with a comfortable buffer.

        This is for use with systems like ``distributed.deploy.adaptive``.

        Parameters
        ----------
        memory_ratio : Number
            Amount of extra space we want to have for our stored data.
            Defaults to 2, or that we want to have twice as much memory as we
            currently have data.
        n : int
            Number of workers to close
        minimum : int
            Minimum number of workers to keep around
        key : Callable(WorkerState)
            An optional callable mapping a WorkerState object to a group
            affiliation. Groups will be closed together. This is useful when
            closing workers must be done collectively, such as by hostname.
        target : int
            Target number of workers to have after we close
        attribute : str
            The attribute of the WorkerState object to return, like "address"
            or "name".  Defaults to "address".

        Examples
        --------
        >>> scheduler.workers_to_close()
        ['tcp://192.168.0.1:1234', 'tcp://192.168.0.2:1234']

        Group workers by hostname prior to closing

        >>> scheduler.workers_to_close(key=lambda ws: ws.host)
        ['tcp://192.168.0.1:1234', 'tcp://192.168.0.1:4567']

        Remove two workers

        >>> scheduler.workers_to_close(n=2)

        Keep enough workers to have twice as much memory as we we need.

        >>> scheduler.workers_to_close(memory_ratio=2)

        Returns
        -------
        to_close: list of worker addresses that are OK to close

        See Also
        --------
        Scheduler.retire_workers
        """
        if target is not None and n is None:
            n = len(self.state.workers) - target
        if n is not None:
            if n < 0:
                n = 0
            target = len(self.state.workers) - n

        if n is None and memory_ratio is None:
            memory_ratio = 2

        ws: WorkerState
        with log_errors():
            if not n and all([ws._processing for ws in self.state.workers.values()]):
                return []

            if key is None:
                key = operator.attrgetter("address")
            if isinstance(key, bytes) and dask.config.get(
                "distributed.scheduler.pickle"
            ):
                key = pickle.loads(key)

            groups = groupby(key, self.state.workers.values())

            limit_bytes = {
                k: sum([ws._memory_limit for ws in v]) for k, v in groups.items()
            }
            group_bytes = {k: sum([ws._nbytes for ws in v]) for k, v in groups.items()}

            limit = sum(limit_bytes.values())
            total = sum(group_bytes.values())

            def _key(group):
                wws: WorkerState
                is_idle = not any([wws._processing for wws in groups[group]])
                bytes = -group_bytes[group]
                return (is_idle, bytes)

            idle = sorted(groups, key=_key)

            to_close = []
            n_remain = len(self.state.workers)

            while idle:
                group = idle.pop()
                if n is None and any([ws._processing for ws in groups[group]]):
                    break

                if minimum and n_remain - len(groups[group]) < minimum:
                    break

                limit -= limit_bytes[group]

                if (
                    n is not None and n_remain - len(groups[group]) >= cast(int, target)
                ) or (memory_ratio is not None and limit >= memory_ratio * total):
                    to_close.append(group)
                    n_remain -= len(groups[group])

                else:
                    break

            result = [getattr(ws, attribute) for g in to_close for ws in groups[g]]
            if result:
                logger.debug("Suggest closing workers: %s", result)

            return result

    async def retire_workers(
        self,
        comm=None,
        *,
        workers: "list[str] | None" = None,
        names: "list | None" = None,
        close_workers: bool = False,
        remove: bool = True,
        **kwargs,
    ) -> dict:
        """Gracefully retire workers from cluster

        Parameters
        ----------
        workers: list[str] (optional)
            List of worker addresses to retire.
        names: list (optional)
            List of worker names to retire.
            Mutually exclusive with ``workers``.
            If neither ``workers`` nor ``names`` are provided, we call
            ``workers_to_close`` which finds a good set.
        close_workers: bool (defaults to False)
            Whether or not to actually close the worker explicitly from here.
            Otherwise we expect some external job scheduler to finish off the
            worker.
        remove: bool (defaults to True)
            Whether or not to remove the worker metadata immediately or else
            wait for the worker to contact us
        **kwargs: dict
            Extra options to pass to workers_to_close to determine which
            workers we should drop

        Returns
        -------
        Dictionary mapping worker ID/address to dictionary of information about
        that worker for each retired worker.

        See Also
        --------
        Scheduler.workers_to_close
        """
        ws: WorkerState
        ts: TaskState
        with log_errors():
            # This lock makes retire_workers, rebalance, and replicate mutually
            # exclusive and will no longer be necessary once rebalance and replicate are
            # migrated to the Active Memory Manager.
            # Note that, incidentally, it also prevents multiple calls to retire_workers
            # from running in parallel - this is unnecessary.
            async with self._lock:
                if names is not None:
                    if workers is not None:
                        raise TypeError("names and workers are mutually exclusive")
                    if names:
                        logger.info("Retire worker names %s", names)
                    # Support cases where names are passed through a CLI and become
                    # strings
                    names_set = {str(name) for name in names}
                    wss = {
                        ws
                        for ws in self.state.workers.values()
                        if str(ws._name) in names_set
                    }
                elif workers is not None:
                    wss = {
                        self.state.workers[address]
                        for address in workers
                        if address in self.state.workers
                    }
                else:
                    wss = {
                        self.state.workers[address]
                        for address in self.workers_to_close(**kwargs)
                    }
                if not wss:
                    return {}

                stop_amm = False
                amm: ActiveMemoryManagerExtension = self.extensions["amm"]
                if not amm.running:
                    amm = ActiveMemoryManagerExtension(
                        self, policies=set(), register=False, start=True, interval=2.0
                    )
                    stop_amm = True

                try:
                    coros = []
                    for ws in wss:
                        logger.info("Retiring worker %s", ws._address)

                        policy = RetireWorker(ws._address)
                        amm.add_policy(policy)

                        # Change Worker.status to closing_gracefully. Immediately set
                        # the same on the scheduler to prevent race conditions.
                        prev_status = ws.status
                        ws.status = Status.closing_gracefully
                        self.state.running.discard(ws)
                        self.stream_comms[ws.address].send(
                            {"op": "worker-status-change", "status": ws.status.name}
                        )

                        coros.append(
                            self._track_retire_worker(
                                ws,
                                policy,
                                prev_status=prev_status,
                                close_workers=close_workers,
                                remove=remove,
                            )
                        )

                    # Give the AMM a kick, in addition to its periodic running. This is
                    # to avoid unnecessarily waiting for a potentially arbitrarily long
                    # time (depending on interval settings)
                    amm.run_once()

                    workers_info = dict(await asyncio.gather(*coros))
                    workers_info.pop(None, None)
                finally:
                    if stop_amm:
                        amm.stop()

            self.log_event("all", {"action": "retire-workers", "workers": workers_info})
            self.log_event(list(workers_info), {"action": "retired"})

            return workers_info

    async def _track_retire_worker(
        self,
        ws: WorkerState,
        policy: RetireWorker,
        prev_status: Status,
        close_workers: bool,
        remove: bool,
    ) -> tuple:  # tuple[str | None, dict]
        while not policy.done():
            if policy.no_recipients:
                # Abort retirement. This time we don't need to worry about race
                # conditions and we can wait for a scheduler->worker->scheduler
                # round-trip.
                self.stream_comms[ws.address].send(
                    {"op": "worker-status-change", "status": prev_status.name}
                )
                return None, {}

            # Sleep 0.01s when there are 4 tasks or less
            # Sleep 0.5s when there are 200 or more
            poll_interval = max(0.01, min(0.5, len(ws.has_what) / 400))
            await asyncio.sleep(poll_interval)

        logger.debug(
            "All unique keys on worker %s have been replicated elsewhere", ws._address
        )

        if close_workers and ws._address in self.state.workers:
            await self.close_worker(worker=ws._address, safe=True)
        if remove:
            await self.remove_worker(address=ws._address, safe=True)

        logger.info("Retired worker %s", ws._address)
        return ws._address, ws.identity()

    def add_keys(self, worker=None, keys=(), stimulus_id=None):
        """
        Learn that a worker has certain keys

        This should not be used in practice and is mostly here for legacy
        reasons.  However, it is sent by workers from time to time.
        """
        if worker not in self.state.workers:
            return "not found"
        ws: WorkerState = self.state.workers[worker]
        redundant_replicas = []
        for key in keys:
            ts: TaskState = self.state.tasks.get(key)
            if ts is not None and ts._state == "memory":
                if ws not in ts._who_has:
                    self.state.add_replica(ts, ws)
            else:
                redundant_replicas.append(key)

        if redundant_replicas:
            if not stimulus_id:
                stimulus_id = f"redundant-replicas-{time()}"
            self.worker_send(
                worker,
                {
                    "op": "remove-replicas",
                    "keys": redundant_replicas,
                    "stimulus_id": stimulus_id,
                },
            )

        return "OK"

    def update_data(
        self,
        *,
        who_has: dict,
        nbytes: dict,
        client=None,
    ):
        """
        Learn that new data has entered the network from an external source

        See Also
        --------
        Scheduler.mark_key_in_memory
        """
        with log_errors():
            who_has = {
                k: [self.coerce_address(vv) for vv in v] for k, v in who_has.items()
            }
            logger.debug("Update data %s", who_has)

            for key, workers in who_has.items():
                ts: TaskState = self.state.tasks.get(key)  # type: ignore
                if ts is None:
                    ts = self.state.new_task(key, None, "memory")
                ts.state = "memory"
                ts_nbytes = nbytes.get(key, -1)
                if ts_nbytes >= 0:
                    ts.set_nbytes(ts_nbytes)

                for w in workers:
                    ws: WorkerState = self.state.workers[w]
                    if ws not in ts._who_has:
                        self.state.add_replica(ts, ws)
                self.report(
                    {"op": "key-in-memory", "key": key, "workers": list(workers)}
                )

            if client:
                self.client_desires_keys(keys=list(who_has), client=client)

    def report_on_key(self, key: str = None, ts: TaskState = None, client: str = None):
        if ts is None:
            ts = self.state.tasks.get(key)
        elif key is None:
            key = ts._key
        else:
            assert False, (key, ts)
            return

        report_msg: dict
        if ts is None:
            report_msg = {"op": "cancelled-key", "key": key}
        else:
            report_msg = self.state._task_to_report_msg(ts)
        if report_msg is not None:
            self.report(report_msg, ts=ts, client=client)

    async def feed(
        self, comm, function=None, setup=None, teardown=None, interval="1s", **kwargs
    ):
        """
        Provides a data Comm to external requester

        Caution: this runs arbitrary Python code on the scheduler.  This should
        eventually be phased out.  It is mostly used by diagnostics.
        """
        if not dask.config.get("distributed.scheduler.pickle"):
            logger.warn(
                "Tried to call 'feed' route with custom functions, but "
                "pickle is disallowed.  Set the 'distributed.scheduler.pickle'"
                "config value to True to use the 'feed' route (this is mostly "
                "commonly used with progress bars)"
            )
            return

        interval = parse_timedelta(interval)
        with log_errors():
            if function:
                function = pickle.loads(function)
            if setup:
                setup = pickle.loads(setup)
            if teardown:
                teardown = pickle.loads(teardown)
            state = setup(self) if setup else None
            if inspect.isawaitable(state):
                state = await state
            try:
                while self.status == Status.running:
                    if state is None:
                        response = function(self)
                    else:
                        response = function(self, state)
                    await comm.write(response)
                    await asyncio.sleep(interval)
            except OSError:
                pass
            finally:
                if teardown:
                    teardown(self, state)

    def log_worker_event(self, worker=None, topic=None, msg=None):
        self.log_event(topic, msg)

    def subscribe_worker_status(self, comm=None):
        WorkerStatusPlugin(self, comm)
        ident = self.identity()
        for v in ident["workers"].values():
            del v["metrics"]
            del v["last_seen"]
        return ident

    def get_processing(self, workers=None):
        ws: WorkerState
        ts: TaskState
        if workers is not None:
            workers = set(map(self.coerce_address, workers))
            return {
                w: [ts._key for ts in self.state.workers[w].processing] for w in workers
            }
        else:
            return {
                w: [ts._key for ts in ws._processing]
                for w, ws in self.state.workers.items()
            }

    def get_who_has(self, keys=None):
        ws: WorkerState
        ts: TaskState
        if keys is not None:
            return {
                k: [ws._address for ws in self.state.tasks[k].who_has]
                if k in self.state.tasks
                else []
                for k in keys
            }
        else:
            return {
                key: [ws._address for ws in ts._who_has]
                for key, ts in self.state.tasks.items()
            }

    def get_has_what(self, workers=None):
        ws: WorkerState
        ts: TaskState
        if workers is not None:
            workers = map(self.coerce_address, workers)
            return {
                w: [ts._key for ts in self.state.workers[w].has_what]
                if w in self.state.workers
                else []
                for w in workers
            }
        else:
            return {
                w: [ts._key for ts in ws.has_what]
                for w, ws in self.state.workers.items()
            }

    def get_ncores(self, workers=None):
        ws: WorkerState
        if workers is not None:
            workers = map(self.coerce_address, workers)
            return {
                w: self.state.workers[w].nthreads
                for w in workers
                if w in self.state.workers
            }
        else:
            return {w: ws._nthreads for w, ws in self.state.workers.items()}

    def get_ncores_running(self, workers=None):
        ncores = self.get_ncores(workers=workers)
        return {
            w: n
            for w, n in ncores.items()
            if self.state.workers[w].status == Status.running
        }

    async def get_call_stack(self, keys=None):
        ts: TaskState
        dts: TaskState
        if keys is not None:
            stack = list(keys)
            processing = set()
            while stack:
                key = stack.pop()
                ts = self.state.tasks[key]
                if ts._state == "waiting":
                    stack.extend([dts._key for dts in ts._dependencies])
                elif ts._state == "processing":
                    processing.add(ts)

            workers = defaultdict(list)
            for ts in processing:
                if ts._processing_on:
                    workers[ts._processing_on.address].append(ts._key)
        else:
            workers = {w: None for w in self.state.workers}

        if not workers:
            return {}

        results = await asyncio.gather(
            *(self.rpc(w).call_stack(keys=v) for w, v in workers.items())
        )
        response = {w: r for w, r in zip(workers, results) if r}
        return response

    def get_nbytes(self, keys=None, summary=True):
        ts: TaskState
        with log_errors():
            if keys is not None:
                result = {k: self.state.tasks[k].nbytes for k in keys}
            else:
                result = {
                    k: ts._nbytes
                    for k, ts in self.state.tasks.items()
                    if ts._nbytes >= 0
                }

            if summary:
                out = defaultdict(lambda: 0)
                for k, v in result.items():
                    out[key_split(k)] += v
                result = dict(out)

            return result

    def run_function(self, comm, function, args=(), kwargs=None, wait=True):
        """Run a function within this process

        See Also
        --------
        Client.run_on_scheduler
        """
        from .worker import run

        if not dask.config.get("distributed.scheduler.pickle"):
            raise ValueError(
                "Cannot run function as the scheduler has been explicitly disallowed from "
                "deserializing arbitrary bytestrings using pickle via the "
                "'distributed.scheduler.pickle' configuration setting."
            )
        kwargs = kwargs or {}
        self.log_event("all", {"action": "run-function", "function": function})
        return run(self, comm, function=function, args=args, kwargs=kwargs, wait=wait)

    def set_metadata(self, keys=None, value=None):
        metadata = self.state.task_metadata
        for key in keys[:-1]:
            if key not in metadata or not isinstance(metadata[key], (dict, list)):
                metadata[key] = {}
            metadata = metadata[key]
        metadata[keys[-1]] = value

    def get_metadata(self, keys, default=no_default):
        metadata = self.state.task_metadata
        for key in keys[:-1]:
            metadata = metadata[key]
        try:
            return metadata[keys[-1]]
        except KeyError:
            if default != no_default:
                return default
            else:
                raise

    def set_restrictions(self, worker: "dict[str, Collection[str] | str]"):
        ts: TaskState
        for key, restrictions in worker.items():
            ts = self.tasks[key]
            if isinstance(restrictions, str):
                restrictions = {restrictions}
            ts._worker_restrictions = set(restrictions)

    def get_task_prefix_states(self):
        with log_errors():
            state = {}

            for tp in self.task_prefixes.values():
                active_states = tp.active_states
                if any(
                    active_states.get(s)
                    for s in {"memory", "erred", "released", "processing", "waiting"}
                ):
                    state[tp.name] = {
                        "memory": active_states["memory"],
                        "erred": active_states["erred"],
                        "released": active_states["released"],
                        "processing": active_states["processing"],
                        "waiting": active_states["waiting"],
                    }

        return state

    def get_task_status(self, keys=None):
        return {
            key: (self.state.tasks[key].state if key in self.state.tasks else None)
            for key in keys
        }

    def get_task_stream(self, start=None, stop=None, count=None):
        from distributed.diagnostics.task_stream import TaskStreamPlugin

        if TaskStreamPlugin.name not in self.plugins:
            self.add_plugin(TaskStreamPlugin(self))

        plugin = self.plugins[TaskStreamPlugin.name]

        return plugin.collect(start=start, stop=stop, count=count)

    def start_task_metadata(self, name=None):
        plugin = CollectTaskMetaDataPlugin(scheduler=self, name=name)
        self.add_plugin(plugin)

    def stop_task_metadata(self, name=None):
        plugins = [
            p
            for p in list(self.plugins.values())
            if isinstance(p, CollectTaskMetaDataPlugin) and p.name == name
        ]
        if len(plugins) != 1:
            raise ValueError(
                "Expected to find exactly one CollectTaskMetaDataPlugin "
                f"with name {name} but found {len(plugins)}."
            )

        plugin = plugins[0]
        self.remove_plugin(name=plugin.name)
        return {"metadata": plugin.metadata, "state": plugin.state}

    async def register_worker_plugin(self, comm, plugin, name=None):
        """Registers a worker plugin on all running and future workers"""
        self.worker_plugins[name] = plugin

        responses = await self.broadcast(
            msg=dict(op="plugin-add", plugin=plugin, name=name)
        )
        return responses

    async def unregister_worker_plugin(self, comm, name):
        """Unregisters a worker plugin"""
        try:
            self.worker_plugins.pop(name)
        except KeyError:
            raise ValueError(f"The worker plugin {name} does not exists")

        responses = await self.broadcast(msg=dict(op="plugin-remove", name=name))
        return responses

    async def register_nanny_plugin(self, comm, plugin, name=None):
        """Registers a setup function, and call it on every worker"""
        self.nanny_plugins[name] = plugin

        responses = await self.broadcast(
            msg=dict(op="plugin_add", plugin=plugin, name=name),
            nanny=True,
        )
        return responses

    async def unregister_nanny_plugin(self, comm, name):
        """Unregisters a worker plugin"""
        try:
            self.nanny_plugins.pop(name)
        except KeyError:
            raise ValueError(f"The nanny plugin {name} does not exists")

        responses = await self.broadcast(
            msg=dict(op="plugin_remove", name=name), nanny=True
        )
        return responses

    def transition(self, key, finish: str, *args, **kwargs):
        """Transition a key from its current state to the finish state

        Examples
        --------
        >>> self.transition('x', 'waiting')
        {'x': 'processing'}

        Returns
        -------
        Dictionary of recommendations for future transitions

        See Also
        --------
        Scheduler.transitions: transitive version of this function
        """
        recommendations: dict
        worker_msgs: dict
        client_msgs: dict
        a: tuple = self.state.transition(key, finish, args=args, kwargs=kwargs)
        recommendations, client_msgs, worker_msgs = a
        self.send_all(client_msgs, worker_msgs)
        return recommendations

    def transitions(self, recommendations: dict):
        """Process transitions until none are left

        This includes feedback from previous transitions and continues until we
        reach a steady state
        """
        client_msgs: dict = {}
        worker_msgs: dict = {}
        self.state.transitions(recommendations, client_msgs, worker_msgs)
        self.send_all(client_msgs, worker_msgs)

    def story(self, *keys):
        """Get all transitions that touch one of the input keys"""
        keys = {key.key if isinstance(key, TaskState) else key for key in keys}
        return [
            t
            for t in self.state.transition_log
            if t[0] in keys or keys.intersection(t[3])
        ]

    transition_story = story

    def reschedule(self, key=None, worker=None):
        """Reschedule a task

        Things may have shifted and this task may now be better suited to run
        elsewhere
        """
        ts: TaskState
        try:
            ts = self.state.tasks[key]
        except KeyError:
            logger.warning(
                "Attempting to reschedule task {}, which was not "
                "found on the scheduler. Aborting reschedule.".format(key)
            )
            return
        if ts._state != "processing":
            return
        if worker and ts._processing_on.address != worker:
            return
        self.transitions({key: "released"})

    #####################
    # Utility functions #
    #####################

    def add_resources(self, worker: str, resources=None):
        ws: WorkerState = self.state.workers[worker]
        if resources:
            ws._resources.update(resources)
        ws._used_resources = {}
        for resource, quantity in ws._resources.items():
            ws._used_resources[resource] = 0
            dr: dict = self.state.resources.get(resource, None)
            if dr is None:
                self.state.resources[resource] = dr = {}
            dr[worker] = quantity
        return "OK"

    def remove_resources(self, worker):
        ws: WorkerState = self.state.workers[worker]
        for resource, quantity in ws._resources.items():
            dr: dict = self.state.resources.get(resource, None)
            if dr is None:
                self.state.resources[resource] = dr = {}
            del dr[worker]

    def coerce_address(self, addr, resolve=True):
        """
        Coerce possible input addresses to canonical form.
        *resolve* can be disabled for testing with fake hostnames.

        Handles strings, tuples, or aliases.
        """
        # XXX how many address-parsing routines do we have?
        if addr in self.state.aliases:
            addr = self.state.aliases[addr]
        if isinstance(addr, tuple):
            addr = unparse_host_port(*addr)
        if not isinstance(addr, str):
            raise TypeError(f"addresses should be strings or tuples, got {addr!r}")

        if resolve:
            addr = resolve_address(addr)
        else:
            addr = normalize_address(addr)

        return addr

    def workers_list(self, workers):
        """
        List of qualifying workers

        Takes a list of worker addresses or hostnames.
        Returns a list of all worker addresses that match
        """
        if workers is None:
            return list(self.state.workers)

        out = set()
        for w in workers:
            if ":" in w:
                out.add(w)
            else:
                out.update(
                    {ww for ww in self.state.workers if w in ww}
                )  # TODO: quadratic
        return list(out)

    def start_ipython(self):
        """Start an IPython kernel

        Returns Jupyter connection info dictionary.
        """
        from ._ipython_utils import start_ipython

        if self._ipython_kernel is None:
            self._ipython_kernel = start_ipython(
                ip=self.ip, ns={"scheduler": self}, log=logger
            )
        return self._ipython_kernel.get_connection_info()

    async def get_profile(
        self,
        comm=None,
        workers=None,
        scheduler=False,
        server=False,
        merge_workers=True,
        start=None,
        stop=None,
        key=None,
    ):
        if workers is None:
            workers = self.state.workers
        else:
            workers = set(self.state.workers) & set(workers)

        if scheduler:
            return profile.get_profile(self.io_loop.profile, start=start, stop=stop)

        results = await asyncio.gather(
            *(
                self.rpc(w).profile(start=start, stop=stop, key=key, server=server)
                for w in workers
            ),
            return_exceptions=True,
        )

        results = [r for r in results if not isinstance(r, Exception)]

        if merge_workers:
            response = profile.merge(*results)
        else:
            response = dict(zip(workers, results))
        return response

    async def get_profile_metadata(
        self,
        comm=None,
        workers=None,
        merge_workers=True,
        start=None,
        stop=None,
        profile_cycle_interval=None,
    ):
        dt = profile_cycle_interval or dask.config.get(
            "distributed.worker.profile.cycle"
        )
        dt = parse_timedelta(dt, default="ms")

        if workers is None:
            workers = self.state.workers
        else:
            workers = set(self.state.workers) & set(workers)
        results = await asyncio.gather(
            *(self.rpc(w).profile_metadata(start=start, stop=stop) for w in workers),
            return_exceptions=True,
        )

        results = [r for r in results if not isinstance(r, Exception)]
        counts = [v["counts"] for v in results]
        counts = itertools.groupby(merge_sorted(*counts), lambda t: t[0] // dt * dt)
        counts = [(time, sum(pluck(1, group))) for time, group in counts]

        keys = set()
        for v in results:
            for t, d in v["keys"]:
                for k in d:
                    keys.add(k)
        keys = {k: [] for k in keys}

        groups1 = [v["keys"] for v in results]
        groups2 = list(merge_sorted(*groups1, key=first))

        last = 0
        for t, d in groups2:
            tt = t // dt * dt
            if tt > last:
                last = tt
                for k, v in keys.items():
                    v.append([tt, 0])
            for k, v in d.items():
                keys[k][-1][1] += v

        return {"counts": counts, "keys": keys}

    async def performance_report(
        self, start: float, last_count: int, code="", mode=None
    ):
        stop = time()
        # Profiles
        compute, scheduler, workers = await asyncio.gather(
            *[
                self.get_profile(start=start),
                self.get_profile(scheduler=True, start=start),
                self.get_profile(server=True, start=start),
            ]
        )
        from . import profile

        def profile_to_figure(state):
            data = profile.plot_data(state)
            figure, source = profile.plot_figure(data, sizing_mode="stretch_both")
            return figure

        compute, scheduler, workers = map(
            profile_to_figure, (compute, scheduler, workers)
        )

        # Task stream
        task_stream = self.get_task_stream(start=start)
        total_tasks = len(task_stream)
        timespent: "defaultdict[str, float]" = defaultdict(float)
        for d in task_stream:
            for x in d["startstops"]:
                timespent[x["action"]] += x["stop"] - x["start"]
        tasks_timings = ""
        for k in sorted(timespent.keys()):
            tasks_timings += f"\n<li> {k} time: {format_time(timespent[k])} </li>"

        from .dashboard.components.scheduler import task_stream_figure
        from .diagnostics.task_stream import rectangles

        rects = rectangles(task_stream)
        source, task_stream = task_stream_figure(sizing_mode="stretch_both")
        source.data.update(rects)

        # Bandwidth
        from distributed.dashboard.components.scheduler import (
            BandwidthTypes,
            BandwidthWorkers,
        )

        bandwidth_workers = BandwidthWorkers(self, sizing_mode="stretch_both")
        bandwidth_workers.update()
        bandwidth_types = BandwidthTypes(self, sizing_mode="stretch_both")
        bandwidth_types.update()

        # System monitor
        from distributed.dashboard.components.shared import SystemMonitor

        sysmon = SystemMonitor(self, last_count=last_count, sizing_mode="stretch_both")
        sysmon.update()

        # Scheduler logs
        from distributed.dashboard.components.scheduler import SchedulerLogs

        logs = SchedulerLogs(self, start=start)

        from bokeh.models import Div, Panel, Tabs

        import distributed

        # HTML
        ws: WorkerState
        html = """
        <h1> Dask Performance Report </h1>

        <i> Select different tabs on the top for additional information </i>

        <h2> Duration: {time} </h2>
        <h2> Tasks Information </h2>
        <ul>
         <li> number of tasks: {ntasks} </li>
         {tasks_timings}
        </ul>

        <h2> Scheduler Information </h2>
        <ul>
          <li> Address: {address} </li>
          <li> Workers: {nworkers} </li>
          <li> Threads: {threads} </li>
          <li> Memory: {memory} </li>
          <li> Dask Version: {dask_version} </li>
          <li> Dask.Distributed Version: {distributed_version} </li>
        </ul>

        <h2> Calling Code </h2>
        <pre>
{code}
        </pre>
        """.format(
            time=format_time(stop - start),
            ntasks=total_tasks,
            tasks_timings=tasks_timings,
            address=self.address,
            nworkers=len(self.state.workers),
            threads=sum([ws._nthreads for ws in self.state.workers.values()]),
            memory=format_bytes(
                sum([ws._memory_limit for ws in self.state.workers.values()])
            ),
            code=code,
            dask_version=dask.__version__,
            distributed_version=distributed.__version__,
        )
        html = Div(
            text=html,
            style={
                "width": "100%",
                "height": "100%",
                "max-width": "1920px",
                "max-height": "1080px",
                "padding": "12px",
                "border": "1px solid lightgray",
                "box-shadow": "inset 1px 0 8px 0 lightgray",
                "overflow": "auto",
            },
        )

        html = Panel(child=html, title="Summary")
        compute = Panel(child=compute, title="Worker Profile (compute)")
        workers = Panel(child=workers, title="Worker Profile (administrative)")
        scheduler = Panel(child=scheduler, title="Scheduler Profile (administrative)")
        task_stream = Panel(child=task_stream, title="Task Stream")
        bandwidth_workers = Panel(
            child=bandwidth_workers.root, title="Bandwidth (Workers)"
        )
        bandwidth_types = Panel(child=bandwidth_types.root, title="Bandwidth (Types)")
        system = Panel(child=sysmon.root, title="System")
        logs = Panel(child=logs.root, title="Scheduler Logs")

        tabs = Tabs(
            tabs=[
                html,
                task_stream,
                system,
                logs,
                compute,
                workers,
                scheduler,
                bandwidth_workers,
                bandwidth_types,
            ]
        )

        from bokeh.core.templates import get_env
        from bokeh.plotting import output_file, save

        with tmpfile(extension=".html") as fn:
            output_file(filename=fn, title="Dask Performance Report", mode=mode)
            template_directory = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "dashboard", "templates"
            )
            template_environment = get_env()
            template_environment.loader.searchpath.append(template_directory)
            template = template_environment.get_template("performance_report.html")
            save(tabs, filename=fn, template=template)

            with open(fn) as f:
                data = f.read()

        return data

    async def get_worker_logs(self, n=None, workers=None, nanny=False):
        results = await self.broadcast(
            msg={"op": "get_logs", "n": n}, workers=workers, nanny=nanny
        )
        return results

    def log_event(self, name, msg):
        event = (time(), msg)
        if isinstance(name, (list, tuple)):
            for n in name:
                self.events[n].append(event)
                self.event_counts[n] += 1
                self._report_event(n, event)
        else:
            self.events[name].append(event)
            self.event_counts[name] += 1
            self._report_event(name, event)

    def _report_event(self, name, event):
        for client in self.event_subscriber[name]:
            self.report(
                {
                    "op": "event",
                    "topic": name,
                    "event": event,
                },
                client=client,
            )

    def subscribe_topic(self, topic, client):
        self.event_subscriber[topic].add(client)

    def unsubscribe_topic(self, topic, client):
        self.event_subscriber[topic].discard(client)

    def get_events(self, topic=None):
        if topic is not None:
            return tuple(self.events[topic])
        else:
            return valmap(tuple, self.events)

    async def get_worker_monitor_info(self, recent=False, starts=None):
        if starts is None:
            starts = {}
        results = await asyncio.gather(
            *(
                self.rpc(w).get_monitor_info(recent=recent, start=starts.get(w, 0))
                for w in self.state.workers
            )
        )
        return dict(zip(self.state.workers, results))

    ###########
    # Cleanup #
    ###########

    def reevaluate_occupancy(self, worker_index: Py_ssize_t = 0):
        """Periodically reassess task duration time

        The expected duration of a task can change over time.  Unfortunately we
        don't have a good constant-time way to propagate the effects of these
        changes out to the summaries that they affect, like the total expected
        runtime of each of the workers, or what tasks are stealable.

        In this coroutine we walk through all of the workers and re-align their
        estimates with the current state of tasks.  We do this periodically
        rather than at every transition, and we only do it if the scheduler
        process isn't under load (using psutil.Process.cpu_percent()).  This
        lets us avoid this fringe optimization when we have better things to
        think about.
        """
        try:
            if self.status == Status.closed:
                return
            last = time()
            next_time = timedelta(seconds=0.1)

            if self.proc.cpu_percent() < 50:
                workers: list = list(self.state.workers.values())
                nworkers: Py_ssize_t = len(workers)
                i: Py_ssize_t
                for i in range(nworkers):
                    ws: WorkerState = workers[worker_index % nworkers]
                    worker_index += 1
                    try:
                        if ws is None or not ws._processing:
                            continue
                        self.state.reevaluate_occupancy_worker(ws)
                    finally:
                        del ws  # lose ref

                    duration = time() - last
                    if duration > 0.005:  # 5ms since last release
                        next_time = timedelta(seconds=duration * 5)  # 25ms gap
                        break

            self.loop.add_timeout(
                next_time, self.reevaluate_occupancy, worker_index=worker_index
            )

        except Exception:
            logger.error("Error in reevaluate occupancy", exc_info=True)
            raise

    async def check_worker_ttl(self):
        ws: WorkerState
        now = time()
        for ws in self.state.workers.values():
            if (ws._last_seen < now - self.worker_ttl) and (
                ws._last_seen < now - 10 * heartbeat_interval(len(self.state.workers))
            ):
                logger.warning(
                    "Worker failed to heartbeat within %s seconds. Closing: %s",
                    self.worker_ttl,
                    ws,
                )
                await self.remove_worker(address=ws._address)

    def check_idle(self):
        ws: WorkerState
        if (
            any([ws._processing for ws in self.state.workers.values()])
            or self.state.unrunnable
        ):
            self.idle_since = None
            return
        elif not self.idle_since:
            self.idle_since = time()

        if time() > self.idle_since + self.idle_timeout:
            logger.info(
                "Scheduler closing after being idle for %s",
                format_time(self.idle_timeout),
            )
            self.loop.add_callback(self.close)

    def adaptive_target(self, target_duration=None):
        """Desired number of workers based on the current workload

        This looks at the current running tasks and memory use, and returns a
        number of desired workers.  This is often used by adaptive scheduling.

        Parameters
        ----------
        target_duration : str
            A desired duration of time for computations to take.  This affects
            how rapidly the scheduler will ask to scale.

        See Also
        --------
        distributed.deploy.Adaptive
        """
        if target_duration is None:
            target_duration = dask.config.get("distributed.adaptive.target-duration")
        target_duration = parse_timedelta(target_duration)

        # CPU
        cpu = math.ceil(
            self.state.total_occupancy / target_duration
        )  # TODO: threads per worker

        # Avoid a few long tasks from asking for many cores
        ws: WorkerState
        tasks_processing = 0
        for ws in self.state.workers.values():
            tasks_processing += len(ws._processing)

            if tasks_processing > cpu:
                break
        else:
            cpu = min(tasks_processing, cpu)

        if self.state.unrunnable and not self.state.workers:
            cpu = max(1, cpu)

        # add more workers if more than 60% of memory is used
        limit = sum([ws._memory_limit for ws in self.state.workers.values()])
        used = sum([ws._nbytes for ws in self.state.workers.values()])
        memory = 0
        if used > 0.6 * limit and limit > 0:
            memory = 2 * len(self.state.workers)

        target = max(memory, cpu)
        if target >= len(self.state.workers):
            return target
        else:  # Scale down?
            to_close = self.workers_to_close()
            return len(self.state.workers) - len(to_close)

    def request_acquire_replicas(self, addr: str, keys: list, *, stimulus_id: str):
        """Asynchronously ask a worker to acquire a replica of the listed keys from
        other workers. This is a fire-and-forget operation which offers no feedback for
        success or failure, and is intended for housekeeping and not for computation.
        """
        ws: WorkerState
        ts: TaskState

        who_has = {}
        for key in keys:
            ts = self.state.tasks[key]
            who_has[key] = {ws._address for ws in ts._who_has}

        self.stream_comms[addr].send(
            {
                "op": "acquire-replicas",
                "keys": keys,
                "who_has": who_has,
                "stimulus_id": stimulus_id,
            },
        )

    def request_remove_replicas(self, addr: str, keys: list, *, stimulus_id: str):
        """Asynchronously ask a worker to discard its replica of the listed keys.
        This must never be used to destroy the last replica of a key. This is a
        fire-and-forget operation, intended for housekeeping and not for computation.

        The replica disappears immediately from TaskState.who_has on the Scheduler side;
        if the worker refuses to delete, e.g. because the task is a dependency of
        another task running on it, it will (also asynchronously) inform the scheduler
        to re-add itself to who_has. If the worker agrees to discard the task, there is
        no feedback.
        """
        ws: WorkerState = self.state.workers[addr]
        validate = self.validate

        # The scheduler immediately forgets about the replica and suggests the worker to
        # drop it. The worker may refuse, at which point it will send back an add-keys
        # message to reinstate it.
        for key in keys:
            ts: TaskState = self.state.tasks[key]
            if validate:
                # Do not destroy the last copy
                assert len(ts._who_has) > 1
            self.state.remove_replica(ts, ws)

        self.stream_comms[addr].send(
            {
                "op": "remove-replicas",
                "keys": keys,
                "stimulus_id": stimulus_id,
            }
        )


def heartbeat_interval(n):
    """
    Interval in seconds that we desire heartbeats based on number of workers
    """
    if n <= 10:
        return 0.5
    elif n < 50:
        return 1
    elif n < 200:
        return 2
    else:
        # no more than 200 hearbeats a second scaled by workers
        return n / 200 + 1


class KilledWorker(Exception):
    def __init__(self, task, last_worker):
        super().__init__(task, last_worker)
        self.task = task
        self.last_worker = last_worker


class WorkerStatusPlugin(SchedulerPlugin):
    """
    An plugin to share worker status with a remote observer

    This is used in cluster managers to keep updated about the status of the
    scheduler.
    """

    name = "worker-status"

    def __init__(self, scheduler, comm):
        self.bcomm = BatchedSend(interval="5ms")
        self.bcomm.start(comm)

        self.scheduler = scheduler
        self.scheduler.add_plugin(self)

    def add_worker(self, worker=None, **kwargs):
        ident = self.scheduler.workers[worker].identity()
        del ident["metrics"]
        del ident["last_seen"]
        try:
            self.bcomm.send(["add", {"workers": {worker: ident}}])
        except CommClosedError:
            self.scheduler.remove_plugin(name=self.name)

    def remove_worker(self, worker=None, **kwargs):
        try:
            self.bcomm.send(["remove", worker])
        except CommClosedError:
            self.scheduler.remove_plugin(name=self.name)

    def teardown(self):
        self.bcomm.close()


class CollectTaskMetaDataPlugin(SchedulerPlugin):
    def __init__(self, scheduler, name):
        self.scheduler = scheduler
        self.name = name
        self.keys = set()
        self.metadata = {}
        self.state = {}

    def update_graph(self, scheduler, dsk=None, keys=None, restrictions=None, **kwargs):
        self.keys.update(keys)

    def transition(self, key, start, finish, *args, **kwargs):
        if finish == "memory" or finish == "erred":
            ts: TaskState = self.scheduler.tasks.get(key)
            if ts is not None and ts._key in self.keys:
                self.metadata[key] = ts._metadata
                self.state[key] = finish
                self.keys.discard(key)
