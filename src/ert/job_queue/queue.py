"""
Module implementing a queue for managing external jobs.

"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
import pathlib
import ssl
import threading
import time
from collections import deque
from dataclasses import dataclass
import queue
from threading import BoundedSemaphore, Semaphore
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

from cloudevents.conversion import to_json
from cloudevents.http import CloudEvent
from websockets.client import WebSocketClientProtocol, connect
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosed

from ert.constant_filenames import CERT_FILE, JOBS_FILE, ERROR_file, STATUS_file
from ert.job_queue.job_status import JobStatus
from ert.job_queue.queue_differ import QueueDiffer
from ert.job_queue.thread_status import ThreadStatus

if TYPE_CHECKING:
    from ert.ensemble_evaluator import Realization
    from ert.run_arg import RunArg

    from .driver import Driver


logger = logging.getLogger(__name__)

LONG_RUNNING_FACTOR = 1.25
"""If STOP_LONG_RUNNING is true, realizations taking more time than the average
times this ￼factor will be killed."""
CONCURRENT_INTERNALIZATION = 1
"""How many realizations allowed to be concurrently internalized using
threads."""

EVTYPE_REALIZATION_FAILURE = "com.equinor.ert.realization.failure"
EVTYPE_REALIZATION_PENDING = "com.equinor.ert.realization.pending"
EVTYPE_REALIZATION_RUNNING = "com.equinor.ert.realization.running"
EVTYPE_REALIZATION_SUCCESS = "com.equinor.ert.realization.success"
EVTYPE_REALIZATION_UNKNOWN = "com.equinor.ert.realization.unknown"
EVTYPE_REALIZATION_WAITING = "com.equinor.ert.realization.waiting"
EVTYPE_REALIZATION_TIMEOUT = "com.equinor.ert.realization.timeout"

_queue_state_to_event_type_map = {
    "NOT_ACTIVE": EVTYPE_REALIZATION_WAITING,
    "WAITING": EVTYPE_REALIZATION_WAITING,
    "SUBMITTED": EVTYPE_REALIZATION_WAITING,
    "PENDING": EVTYPE_REALIZATION_PENDING,
    "RUNNING": EVTYPE_REALIZATION_RUNNING,
    "DONE": EVTYPE_REALIZATION_RUNNING,
    "EXIT": EVTYPE_REALIZATION_RUNNING,
    "IS_KILLED": EVTYPE_REALIZATION_FAILURE,
    "DO_KILL": EVTYPE_REALIZATION_FAILURE,
    "SUCCESS": EVTYPE_REALIZATION_SUCCESS,
    "STATUS_FAILURE": EVTYPE_REALIZATION_UNKNOWN,
    "FAILED": EVTYPE_REALIZATION_FAILURE,
    "DO_KILL_NODE_FAILURE": EVTYPE_REALIZATION_FAILURE,
    "UNKNOWN": EVTYPE_REALIZATION_UNKNOWN,
}


def _queue_state_event_type(state: str) -> str:
    return _queue_state_to_event_type_map[state]


@dataclass
class ExecutableRealization:  # Aka "Job" or previously "JobQueueNode"
    # status: JobStatus  # property of the driver
    job_script: pathlib.Path
    num_cpu: int
    status_file: str
    exit_file: str
    run_arg: "RunArg"
    max_runtime: Optional[int] = None
    callback_timeout: Optional[Callable[[int], None]] = None

    def __hash__(self):
        # Elevate iens up to two levels? Check if it can be removed from run_arg
        return self.run_arg.iens

    def __repr__(self):
        return str(self.run_arg.iens)


class JobQueue:
    """Represents a queue of realizations (aka Jobs) to be executed on a
    cluster."""

    def __init__(self, driver: "Driver", max_running_jobs: int = 0, resubmits: int = 1):
        self.job_list: List[ExecutableRealization] = []
        self.driver = driver  # We should maybe initialize the driver here, not outside.

        self._statuses: Dict[ExecutableRealization, JobStatus] = {}
        self._waiting_realizations: queue.Queue = queue.Queue()
        self._queue_stopped = False

        self._differ = QueueDiffer()
        self._resubmits = resubmits  # How many retries in case a job fails
        self._max_running_jobs: int = max_running_jobs  # 0 means infinite

        self._ens_id = None
        self._ee_connection = None

    @property
    def max_running_job(self) -> Optional[int]:
        return self._max_running_jobs

    @property
    def resubmits(self) -> int:
        return self._resubmits

    def is_active(self) -> bool:
        print(f" <is_active> {self._statuses.values()}")
        return any(
            job_status in (JobStatus.WAITING, JobStatus.PENDING, JobStatus.RUNNING)
            for job_status in self._statuses.values()
        )

    def count_status(self, status: JobStatus) -> int:
        print("count_satus is not impelemented")
        return len([job for job in self.job_list if job.queue_status == status])

    @property
    def stopped(self) -> bool:
        return self._queue_stopped

    def kill_all_jobs(self) -> None:
        self._stopped = True

    @property
    def queue_size(self) -> int:
        return len(self.job_list)

    @property
    def exit_file(self) -> str:
        return ERROR_file

    @property
    def status_file(self) -> str:
        return STATUS_file

    def add_job(self, job: ExecutableRealization) -> None:
        self.job_list.append(job)
        self._waiting_realizations.put(job)
        self._statuses[job] = JobStatus.WAITING

    def count_running(self) -> int:
        return sum(status == JobStatus.RUNNING for status in self._statuses.values)

    def max_running(self) -> int:
        if self._max_running() == 0:
            return len(self.job_list)
        else:
            return self.get_max_running()

    def available_capacity(self) -> bool:
        if self._max_running_jobs == 0:
            # A value of zero means infinite capacity
            return True
        return self.count_running() < self._max_running_jobs

    async def stop_jobs(self) -> None:
        killtasks = []
        for job in self.job_list:
            killtasks.append(asyncio.create_task(self.driver.kill(job)))
        await asyncio.gather(killtasks)

    def assert_complete(self) -> None:
        for job, job_status in self._statuses:
            if job_status != JobStatus.DONE:
                raise AssertionError(
                    "Unexpected job status type after "
                    f"running job: {job.run_arg.iens} with JobStatus: {job_status}"
                )

    async def get_statuses(self) -> Dict["ExecutableRealization", JobStatus]:
        # This has no side-effects like updating the queues map of statuses
        # It only gets from driver, while this class might also update the state.
        return await self.driver.poll_statuses()

    async def launch_jobs(self) -> None:
        # Will return without guaranteeing launch succcess,
        while self.available_capacity():
            try:
                job = self._waiting_realizations.get(block=False)
                asyncio.create_task(self.driver.submit(job))
            except queue.Empty:
                return

    def execute_queue(self, evaluators: Optional[Iterable[Callable[[], None]]]) -> None:
        while self.is_active() and not self.stopped:
            self.launch_jobs()
            time.sleep(1)

            if evaluators is not None:
                for func in evaluators:
                    func()

        if self.stopped:
            self.stop_jobs()

        self.assert_complete()

    @staticmethod
    def _translate_change_to_cloudevent(
        ens_id: str, real_id: int, status: str
    ) -> CloudEvent:
        return CloudEvent(
            {
                "type": _queue_state_event_type(status),
                "source": f"/ert/ensemble/{ens_id}/real/{real_id}",
                "datacontenttype": "application/json",
            },
            {
                "queue_event_type": status,
            },
        )

    @staticmethod
    async def _publish_changes(
        ens_id: str,
        changes: Dict[int, str],
        ee_connection: WebSocketClientProtocol,
    ) -> None:
        print(ens_id)
        print(changes)
        print(ee_connection)
        events = deque(
            [
                JobQueue._translate_change_to_cloudevent(ens_id, iens, status)
                for iens, status in changes.items()
            ]
        )
        while events:
            await ee_connection.send(to_json(events[0]))
            events.popleft()

    async def _execution_loop_queue_via_websockets(
        self,
        ee_connection: WebSocketClientProtocol,
        ens_id: str,
        pool_sema: threading.BoundedSemaphore,
        evaluators: List[Callable[..., Any]],
    ) -> None:
        self._ens_id = ens_id
        self._ee_connection = ee_connection
        while True:
            print("_execution_loop_queue_via_websockets")
            await self.launch_jobs()  # Move jobs from WAITING stage
            print(f" <exc_loop> {self._statuses.values()}")

            await asyncio.sleep(0.5)
            print("<heartbeat>")
            for func in evaluators:
                func()

            # Do polling
            new_state = await self.get_statuses()  # will not modify self.statuses
            # This will not work, we may also have state updates from this class, not
            # only the driver!
            changes: Dict[int, str] = self._differ.diff_states(
                self._statuses, new_state
            )
            # print(f"{self._statuses=}")
            # print(f"{new_state=}")
            # print(f"{changes=}")
            # logically not necessary the way publish changes is implemented at the
            # moment, but highly relevant before, and might be relevant in the
            # future in case publish changes becomes expensive again
            if len(changes) > 0:
                # print("***********************")
                # print(f"we found changes, {changes=}")
                await JobQueue._publish_changes(
                    ens_id,
                    changes,
                    ee_connection,
                )
                self._statuses = copy.copy(new_state)

                for job, newstatus in changes.items():
                    if newstatus == "DONE":
                        asyncio.create_task(self.handle_done_status(job))

            if self.stopped:
                print(" <exc_loop> stopped")
                raise asyncio.CancelledError

            print(" <exc_loop> are we active?")
            if not self.is_active():
                print(" <exc_loop> not active any longer")
                # for x in range(10):
                #    await asyncio.sleep(1)
                break
            print(" <exc_loop> looping")

    async def handle_done_status(self, job: ExecutableRealization):
        # MOCKED
        print(f"Handling DONE status for realization {job}")
        self._statuses[
            job
        ] = JobStatus.SUCCESS  # does not work, not hashed the same way!
        print(self._statuses)
        await JobQueue._publish_changes(
            self._ens_id,
            {job: "SUCCESS"},
            self._ee_connection,
        )

    async def execute_queue_via_websockets(
        self,
        ee_uri: str,
        ens_id: str,
        pool_sema: threading.BoundedSemaphore,
        evaluators: List[Callable[..., Any]],
        ee_cert: Optional[Union[str, bytes]] = None,
        ee_token: Optional[str] = None,
    ) -> None:
        if evaluators is None:
            evaluators = []
        ee_ssl_context: Optional[Union[ssl.SSLContext, bool]]
        if ee_cert is not None:
            ee_ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ee_ssl_context.load_verify_locations(cadata=ee_cert)
        else:
            ee_ssl_context = True if ee_uri.startswith("wss") else None
        ee_headers = Headers()
        if ee_token is not None:
            ee_headers["token"] = ee_token

        try:
            # initial publish
            async with connect(
                ee_uri,
                ssl=ee_ssl_context,
                extra_headers=ee_headers,
                open_timeout=60,
                ping_timeout=60,
                ping_interval=60,
                close_timeout=60,
            ) as ee_connection:
                await JobQueue._publish_changes(
                    ens_id, self._differ.snapshot(), ee_connection
                )
            # loop
            async for ee_connection in connect(
                ee_uri,
                ssl=ee_ssl_context,
                extra_headers=ee_headers,
                open_timeout=60,
                ping_timeout=60,
                ping_interval=60,
                close_timeout=60,
            ):
                try:
                    await self._execution_loop_queue_via_websockets(
                        ee_connection, ens_id, pool_sema, evaluators
                    )
                except ConnectionClosed:
                    logger.warning(
                        "job queue dropped connection to ensemble evaluator - "
                        "going to try and reconnect"
                    )
                    continue
                if not self.is_active():
                    break

        except asyncio.CancelledError:
            logger.debug("queue cancelled, stopping jobs...")
            await self.stop_jobs_async()
            logger.debug("jobs stopped, re-raising CancelledError")
            raise

        except Exception as exc:
            import traceback

            print(exc)
            print(traceback.format_exc())
            logger.exception(
                "unexpected exception in queue",
                exc_info=True,
            )
            await self.stop_jobs_async()
            logger.debug("jobs stopped, re-raising exception")
            raise

        self.assert_complete()
        self._differ.transition(self._statuses)
        # final publish
        async with connect(
            ee_uri,
            ssl=ee_ssl_context,
            extra_headers=ee_headers,
            open_timeout=60,
            ping_timeout=60,
            ping_interval=60,
            close_timeout=60,
        ) as ee_connection:
            await JobQueue._publish_changes(
                ens_id, self._differ.snapshot(), ee_connection
            )

    # pylint: disable=too-many-arguments
    def add_job_from_run_arg(
        self,
        run_arg: "RunArg",
        job_script: str,
        max_runtime: Optional[int],
        num_cpu: int,
    ) -> None:
        job = ExecutableRealization(
            iens=run_arg.iens,
            job_script=job_script,
            num_cpu=num_cpu,
            status_file=self.status_file,
            exit_file=self.exit_file,
            run_arg=run_arg,
            max_runtime=max_runtime,
        )
        # Everest uses this queue_index?
        run_arg.queue_index = self.add_job(job)

    def add_realization(
        self,
        real: Realization,  # ensemble_evaluator.Realization
        callback_timeout: Optional[Callable[[int], None]] = None,
    ) -> None:
        job = ExecutableRealization(
            job_script=real.job_script,
            num_cpu=real.num_cpu,
            status_file=self.status_file,
            exit_file=self.exit_file,
            run_arg=real.run_arg,
            max_runtime=real.max_runtime,
            callback_timeout=callback_timeout,
        )
        # Everest uses this queue_index?
        real.run_arg.queue_index = self.add_job(job)

    def stop_long_running_jobs(self, minimum_required_realizations: int) -> None:
        completed_jobs = [
            job for job in self.job_list if job.queue_status == JobStatus.DONE
        ]
        finished_realizations = len(completed_jobs)

        if not finished_realizations:
            job_nodes_status = ""
            for job in self.job_list:
                job_nodes_status += str(job)
            logger.error(
                f"Attempted to stop finished jobs when none was found in queue"
                f"{str(self)}, {job_nodes_status}"
            )
            return

        if finished_realizations < minimum_required_realizations:
            return

        average_runtime = (
            sum(job.runtime for job in completed_jobs) / finished_realizations
        )

        for job in self.job_list:
            if job.runtime > LONG_RUNNING_FACTOR * average_runtime:
                job.stop()

    def snapshot(self) -> Optional[Dict[int, str]]:
        """Return the whole state, or None if there was no snapshot."""
        return self._differ.snapshot()

    async def get_changes_without_transition(
        self,
    ) -> Tuple[Dict[int, str], List[JobStatus]]:
        old_state = copy.copy(self._statuses)
        # Poll:
        new_state = await self.get_statuses()  # will not modify self.statuses
        # print(f"{new_state=}")
        # old_state, new_state = self._differ.get_old_and_new_state(self._statuses)
        return self._differ.diff_states(self._statuses, new_state), new_state

    def add_dispatch_information_to_jobs_file(
        self,
        ens_id: str,
        dispatch_url: str,
        cert: Optional[str],
        token: Optional[str],
        experiment_id: Optional[str] = None,
    ) -> None:
        for job in self.job_list:
            cert_path = f"{job.run_arg.runpath}/{CERT_FILE}"
            if cert is not None:
                with open(cert_path, "w", encoding="utf-8") as cert_file:
                    cert_file.write(cert)
            with open(
                f"{job.run_arg.runpath}/{JOBS_FILE}", "r+", encoding="utf-8"
            ) as jobs_file:
                data = json.load(jobs_file)

                data["ens_id"] = ens_id
                data["real_id"] = job.run_arg.iens
                data["dispatch_url"] = dispatch_url
                data["ee_token"] = token
                data["ee_cert_path"] = cert_path if cert is not None else None
                data["experiment_id"] = experiment_id

                jobs_file.seek(0)
                jobs_file.truncate()
                json.dump(data, jobs_file, indent=4)
