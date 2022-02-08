
```python
from __future__ import annotations
from asyncio import Future, Task
from collections.abc import Collection, Callable

from dataclasses import dataclass

class TaskState:
    pass

# TODO: The dataclasses below may just as well be defined as dictionary

class Event:
    pass

@dataclass
class TimedEvent(Event):
    start: float
    end: float

@dataclass
class ErrorEvent(Event):
    exception: Exception
    traceback: Traceback
    exception_text: str
    traceback_text: str


@dataclass
class ExecuteSuccess(TimedEvent):
    ts: TaskState
    data: object
    # The below is currently also executed in the thread. This is likely not
    # strictly required
    type: type
    nbytes: int


@dataclass
class ExecuteError(ErrorEvent, TimedEvent):
    ts: TaskState


@dataclass
class GatherRemoteBusy(Event):
    tasks: Collection[TaskState]
    worker: str

@dataclass
class GatherSuccess(TimedEvent):
    tasks: Collection[TaskState]
    worker: str
    data: object

@dataclass
class GatherError(Event):
    tasks: Collection[TaskState]
    worker: str

@dataclass
class GatherMissing(TimedEvent):
    tasks: Collection[TaskState]
    worker: str

class WorkerState:

    _transition_table: dict[tuple[str, str], Callable]
    tasks: dict[str, TaskState]

    def stimulus(self, event: Event) -> list[dict]:
        pass

class WorkerControlLoop:
    _events: Queue[Event]
    _async_tasks = list[Task]


    async def _control_loop(self):
        while self.status == "running":
            event = await self._events.get()
            decisions = self.state.stimulus(event)
            for dec in decisions:
                if isinstance(dec, Execute):
                    coro = self._execute(dec)
                elif isinstance(dec, Execute):
                    coro = self._gather_remote_data(dec)
                else:
                    raise Exception

                # TODO: Do we "fire-and-forget" these?
                self._async_tasks.append(
                    asyncio.create_task(coro)
                )

    async def _execute(self, ts: TaskState) -> ExecuteSuccess | ExecuteError:
        try:
            result = await do_work(ts)
            event = ExecuteSuccess(result)
        except Exception as err:
            event = ExecuteError(err)
        return event

    async def _gather_remote_data(self, worker: str, to_gather: Collection[TaskState]) -> Collection[GatherSuccess, GatherError, GatherMissing] | GatherRemoteBusy:
        try:
            result = await do_work(worker, to_gather)
            if result == "busy":
                event = GatherRemoteBusy()
            else:
                event = GatherSuccess(result)
        except Exception as err:
            event = GatherMissing(err)
        return event


class Worker1:
    _events: Queue[Event]
    _async_tasks = list[Task]

    async def _control_loop(self):
        while self.status == "running":
            event = await self._events.get()
            decisions = self.state.stimulus(event)
            for dec in decisions:
                if isinstance(dec, Execute):
                    coro = self._execute(dec)
                elif isinstance(dec, Execute):
                    coro = self._gather_remote_data(dec)
                else:
                    raise Exception

                # TODO: Do we want to "fire-and-forget" these?
                self._async_tasks.append(
                    asyncio.create_task(coro)
                )

    async def _execute(self, ts: TaskState) -> ExecuteSuccess | ExecuteError:
        try:
            result = await do_work(ts)
            event = ExecuteSuccess(result)
        except Exception as err:
            event = ExecuteError(err)
        return event

    async def _gather_remote_data(self, worker: str, to_gather: Collection[TaskState]) -> Collection[GatherSuccess, GatherError, GatherMissing] | GatherRemoteBusy:
        try:
            result = await do_work(worker, to_gather)
            if result == "busy":
                event = GatherRemoteBusy()
            else:
                event = GatherSuccess(result)
        except Exception as err:
            event = GatherMissing(err)
        return event


class Worker2:
    async def _control_loop(self):
        while True:
            try:
                # TODO: If as_complete can handle a mutating collection we don't need the timeout
                for event_handle in asyncio.as_completed(self._async_tasks, "100ms"):
                    event = await event_handle
                    decision = self.state.stimulus(event)
                    schedule_tasks(decisions)
            except asyncio.TimeoutError:
                continue

# Is it possible w/out controll loop?

# Tradtitional worker pattern where we're using N queues + worker threads/coroutines to work off tasks is not extremely well suited for Worker.execute since we do want to distinguish tasks form "ready" and "executing" within the worker since otherwise we cannot steal them.
# Given a Priority queue that would support discarding a specific item efficiently, this would be possible. Our UniqueTaskHeap could be extended to support this (not super efficiently but it would be possible)
# By transition, this applies to the gather_dep tasks as well since a stolen task will no longer need its dependents
# IFF we want to separate this task state from the execution lane, we cannot do any "is in checks" since this would breach owenership lines


# The functions/coros should not communicate over a queue since this would render mocking/faking harder since the interface is not that clean.
class WorkerNoLoop:

    def _handle_event(self, event):
        decisions = self.state.stimulus(event)
        for dec in decisions:
            schedule_callback(dec)

    def handle_free_keys(self, keys):
        events = self.state.free_keys(keys)

        # or
        events = [
            self.state.stimulus(FreeKey(key))
        ]
        for event in events:
            self.schedule_callback(event)


```

