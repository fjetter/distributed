from __future__ import annotations

import abc
import asyncio
from dataclasses import dataclass
from typing import Collection, Iterable, Literal

from distributed.spill import SpillBuffer
from distributed.worker import TaskState


@dataclass
class Instruction:
    instruction_id: str


@dataclass
class GatherDep(Instruction):
    worker: str
    to_gather: Collection[TaskState]


@dataclass
class FindMissing(Instruction):
    ...


@dataclass
class Execute(Instruction):
    ...


@dataclass
class SendMsg(Instruction):
    payload: dict
    # One of the following remote actions
    # Messages emitted by the executor
    # - task-erred; This is technically not emitted by the state machine but by the executor
    # - reschedule; This one is also emitted by the executor
    # - task-finished; Executor

    # - release-worker-data; State machine / remove ACK/confirm
    # - long-running; Basically a user trigger during execution
    # - add-keys; Manual update_data, fix scheduler state if remove-replica fails (StateMachine),


@dataclass
class StateMachineEvent:
    stimulus_id: str


@dataclass
class HandleComputeTask(StateMachineEvent):
    key: str
    who_has: dict
    priority: tuple


@dataclass
class RemoveReplicas(StateMachineEvent):
    keys: Collection[str]


@dataclass
class GatherDepSuccess(StateMachineEvent):
    data: object


@dataclass
class GatherDepError(StateMachineEvent):
    data: object


@dataclass
class GatherBusy(StateMachineEvent):
    attempt: int


@dataclass
class RemoteDead(StateMachineEvent):
    worker: str
    exception: Exception


@dataclass
class ExecuteSuccess(StateMachineEvent):
    ts: TaskState
    data: object


@dataclass
class ExecuteFailure(StateMachineEvent):
    ts: TaskState
    exception: Exception


@dataclass
class Reschedule(StateMachineEvent):
    ...


class WorkerState:

    data: SpillBuffer

    def handle_stimulus(self, event: StateMachineEvent) -> Collection[Instruction]:
        if isinstance(event, RemoveReplicas):
            return self.handle_remove_replicas(event)
        else:
            raise RuntimeError(f"Unknown stimulus {event}")

    def handle_remove_replicas(self, event: RemoveReplicas) -> Collection:
        ...

    def handle_compute_task(
        self, event: HandleComputeTask
    ) -> Collection[Execute | GatherDep | FindMissing]:
        ...


class WorkerBase(abc.ABC):
    def __init__(self):
        self.queue = asyncio.Queue()
        self.state = WorkerState()
        self.log: list[StateMachineEvent] = []
        self.paused_reconciler = asyncio.Event()
        self.paused_reconciler.set()
        self.event_handlers = {GatherDep: self.gather_dep}

    async def reconciler(self):
        while True:  # status == running

            # Can be used to freeze the state machine in place
            await self.paused_reconciler.wait()

            event = await self.queue.get()
            self.log.append(event)
            # TODO: This is a great place for low level plugins
            instructions = self.state.handle_stimulus(event)
            for inst in instructions:
                self.implement_instruction(inst)

    def implement_instruction(self, inst: Instruction):
        try:
            handler = self.event_handlers[inst]
            coro = handler(inst)
        except KeyError:
            raise RuntimeError("Unknown instruction")
        fut = asyncio.ensure_future(coro)
        fut.add_done_callback(self._instruction_done)

    def _instruction_done(self, fut):
        try:
            res = fut.result()
        except Exception:
            raise RuntimeError(
                "Handlers must not raise exceptions. Please report this to <link>"
            )
        if isinstance(res, Iterable):
            for item in res:
                self.queue.put_nowait(item)
        else:
            self.queue.put_nowait(res)
        self.queue.task_done()

    def __await__(self):
        return self

    @abc.abstractmethod
    async def execute(
        self, inst: Execute
    ) -> ExecuteSuccess | ExecuteFailure | Reschedule:
        ...

    @abc.abstractmethod
    async def gather_dep(
        self, inst: GatherDep
    ) -> Collection[GatherDepSuccess] | GatherBusy | RemoteDead | GatherDepError:
        raise NotImplementedError


class Worker(WorkerBase):
    def __init__(self):
        stream_handlers = {
            "remove-replicas": self.handle_remove_replicas,
            "compute-task": self.handle_compute_task,
        }
        stream_handlers

    def handle_remove_replicas(
        self, keys: Collection[str], stimulus_id: str
    ) -> Literal["OK"]:
        event = RemoveReplicas(stimulus_id, keys)
        self.queue.put_nowait(event)

        return "OK"

    async def gather_dep(
        self, inst: GatherDep
    ) -> Collection[GatherDepSuccess] | GatherBusy | GatherDepError:
        async def get_data(inst):
            ...

        try:
            await get_data(inst)
        except Exception:
            return GatherDepError("new-stim", "foo")
        return [GatherDepSuccess("new-stim", "foo")]

    def handle_compute_task(
        self, key: str, who_has: dict, priority: tuple, stimulus_id
    ) -> Literal["OK"]:
        event = HandleComputeTask(stimulus_id, key, who_has, priority)
        self.queue.put_nowait(event)
        return "OK"


class FakeWorker(WorkerBase):
    """This class can be used for testing to simulate a busy worker A"""

    async def gather_dep(
        self, inst: GatherDep
    ) -> Collection[GatherDepSuccess] | GatherBusy | RemoteDead | GatherDepError:
        if inst.worker == "A":
            return GatherBusy("foo", 2)
        else:
            return [GatherDepSuccess("foo", "bar") for _ in inst.to_gather]


async def get_worker_from_history(history: Collection[StateMachineEvent]):
    state = WorkerState()
    for event in history:
        _ = state.handle_stimulus(event)  # ignore instructions

    # This _should_ now be a functional worker at the given time
    return await Worker(..., state=state)  # type: ignore
