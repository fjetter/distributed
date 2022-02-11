from __future__ import annotations

import abc
import asyncio
from dataclasses import dataclass
from typing import Collection

from distributed.batched import BatchedSend
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
    worker: str
    exc: Exception


@dataclass
class GatherDepMissing(StateMachineEvent):
    key: str


@dataclass
class Pause(StateMachineEvent):
    ...


@dataclass
class UnPause(StateMachineEvent):
    ...


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


_TRANSITION_RETURN = tuple[dict, Collection[Instruction]]


class WorkerState:

    data: SpillBuffer
    state_event_log: list[StateMachineEvent]

    def handle_stimulus(self, event: StateMachineEvent) -> Collection[Instruction]:
        self.state_event_log.append(event)
        if isinstance(event, RemoveReplicas):
            return self._handle_remove_replicas(event)
        else:
            raise RuntimeError(f"Unknown stimulus {event}")

    def _handle_remove_replicas(self, event: RemoveReplicas) -> Collection[Instruction]:
        # This is where the current transition logic would reside and the
        # transition chain would be triggered. No new logic other than
        # unwrapping the event dataclasses if we were to use them
        ...

    def handle_compute_task(self, event: HandleComputeTask) -> Collection[Instruction]:
        ...

    # This is effectively the logic of ``ensure_communicating`` but the
    # transition logic is separated from coroutine scheduling. It is also only
    # called during transition OR when unpausing
    def _ensure_communicating(self, stimulus_id: str) -> _TRANSITION_RETURN:
        recommendations = {}
        instructions = []

        # This is pseudo/simplified code of what is currently in
        # ensure_communicating
        while self.data_needed and (
            len(self.in_flight_workers) < self.total_out_connections
            or self.comm_nbytes < self.comm_threshold_bytes
        ):
            next_ = self.data_needed.pop()
            worker = self.worker_to_fetch_from(next_)
            to_gather = self.select_keys_for_gather(worker, next_)
            recommendations.update({k: ("flight", worker) for k in to_gather})
            instructions.append(GatherDep(stimulus_id, worker, to_gather))

        return recommendations, instructions

    def transition_released_fetch(
        self, ts: TaskState, *, stimulus_id: str
    ) -> _TRANSITION_RETURN:
        for w in ts.who_has:
            self.pending_data_per_worker[w].push(ts)
        ts.state = "fetch"
        ts.done = False
        self.data_needed.push(ts)
        # The current released->fetch does never return any content
        return self._ensure_communicating(stimulus_id)

    async def memory_monitor(self):
        def check_pause(memory):
            if unpaused:  # type: ignore
                self.handle_stimulus(UnPause())

        check_pause(42)


class WorkerBase(abc.ABC):
    batched_stream: BatchedSend
    state: WorkerState
    instruction_history: list[StateMachineEvent]

    def _handle_stimulus_from_future(self, fut):
        try:
            stim = fut.result()
        except Exception:
            # This must never happen and the handlers must implement exception handling.
            # If we implement error handling here, this should raise some exception that
            # can be immediately filed as a bug report
            raise
        for s in stim:
            self._handle_stimulus(s)

    def _handle_stimulus(self, stim: StateMachineEvent):
        self.instruction_history.append(stim)
        instructions = self.state.handle_stimulus(stim)
        for inst in instructions:
            fut = None
            # TODO: collect all futures and await/cancel when closing?
            if isinstance(inst, GatherDep):
                fut = asyncio.ensure_future(self._gather_data(inst))
            elif isinstance(inst, Execute):
                fut = asyncio.ensure_future(self.execute(inst))
            elif isinstance(inst, SendMsg):
                self.batched_stream.send(inst.payload)
            else:
                raise RuntimeError("Unknown instruction")
            if fut:
                fut.add_done_callback(self._handle_stimulus_from_future)

    @abc.abstractmethod
    async def execute(self, inst: Execute) -> Collection[Instruction]:
        raise NotImplementedError

    @abc.abstractmethod
    async def _gather_data(self, inst: GatherDep) -> Collection[Instruction]:
        raise NotImplementedError


class Worker(WorkerBase):
    async def _gather_data(self, inst: GatherDep) -> Collection[StateMachineEvent]:
        async def get_data(inst):
            ...

        try:
            await get_data(inst)
        except Exception as exc:
            return [GatherDepError("new-stim", "foo", exc)]
        return [GatherDepSuccess("new-stim", "foo")]


class FakeWorker(WorkerBase):
    """This class can be used for testing to simulate a busy worker A"""

    async def _gather_data(self, inst: GatherDep) -> Collection[StateMachineEvent]:
        if inst.worker == "A":
            return [GatherBusy("foo", 2)]
        else:
            return [GatherDepSuccess("foo", "bar") for _ in inst.to_gather]


async def get_worker_from_history(history: Collection[StateMachineEvent]):
    state = WorkerState()
    for event in history:
        _ = state.handle_stimulus(event)  # ignore instructions

    # This _should_ now be a functional worker at the given time
    return await Worker(..., state=state)  # type: ignore
