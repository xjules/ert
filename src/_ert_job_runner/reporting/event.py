import asyncio
import collections
import logging
import queue
import threading
from pathlib import Path
from typing import Any, Dict

import websockets
from cloudevents.conversion import to_json
from cloudevents.http import CloudEvent
from websockets.exceptions import ConnectionClosed

from _ert_job_runner.client import Client
from _ert_job_runner.reporting.base import Reporter
from _ert_job_runner.reporting.message import (
    _JOB_EXIT_FAILED_STRING,
    Exited,
    Finish,
    Init,
    Message,
    Running,
    Start,
)
from _ert_job_runner.reporting.statemachine import StateMachine

_FM_JOB_START = "com.equinor.ert.forward_model_job.start"
_FM_JOB_RUNNING = "com.equinor.ert.forward_model_job.running"
_FM_JOB_SUCCESS = "com.equinor.ert.forward_model_job.success"
_FM_JOB_FAILURE = "com.equinor.ert.forward_model_job.failure"

_CONTENT_TYPE = "datacontenttype"
_JOB_MSG_TYPE = "type"
_JOB_SOURCE = "source"

logger = logging.getLogger(__name__)


class Event(Reporter):
    # pylint: disable=too-many-instance-attributes
    def __init__(self, evaluator_url, token=None, cert_path=None):
        self._evaluator_url = evaluator_url
        self._token = token
        if cert_path is not None:
            with open(cert_path, encoding="utf-8") as f:
                self._cert = f.read()
        else:
            self._cert = None

        self._statemachine = StateMachine()
        self._statemachine.add_handler((Init,), self._init_handler)
        self._statemachine.add_handler((Start, Running, Exited), self._job_handler)
        self._statemachine.add_handler((Finish,), self._finished_handler)

        self._ens_id = None
        self._real_id = None
        self._step_id = None
        self._event_queue = queue.Queue()
        # the events are being sent from this one
        self._publish_queue = collections.deque()
        self._event_publisher_future = None
        self._sentinel = object()  # notifying the queue's ended
        # running the publisher queue
        self._loop = asyncio.get_event_loop()
        self._running = self._loop.create_future()
        self._finish_task = self._loop.create_future()

    async def _finish_queue_timeout(self, timeout_seconds: float):
        try:
            await asyncio.wait_for(
                self._running, timeout=timeout_seconds, loop=self._loop
            )
        except TimeoutError:
            self._running.set_result(None)

    async def _async_publish_event(self):
        logger.debug("Publishing event.")
        client = Client(
            url=self._evaluator_url,
            token=self._token,
            cert=self._cert,
            ping_interval=40,
            ping_timeout=40,
        )
        while True:
            async with client:
                try:
                    while self._running:
                        await self._update_publish_queue()
                        while self._publish_queue:
                            event = self._publish_queue[0]
                            if event is self._sentinel:
                                if not self._running.done():
                                    self._running.set_result(None)
                            else:
                                await client._send(to_json(event).decode())
                            self._publish_queue.popleft()
                    return
                except ConnectionClosed:
                    continue

    async def _update_publish_queue(self):
        while not self._event_queue.empty():
            event = self._event_queue.get()
            if event is self._sentinel:
                self._finish_task = asyncio.create_task(self._finish_queue_timeout, 60)
            self._publish_queue.append(event)
            self._event_queue.task_done()

    def report(self, msg):
        self._statemachine.transition(msg)

    def _dump_event(self, attributes: Dict[str, str], data: Any = None):
        if data is None and _CONTENT_TYPE in attributes:
            attributes.pop(_CONTENT_TYPE)

        event = CloudEvent(attributes=attributes, data=data)
        logger.debug(f'Schedule {type(event)} "{event["type"]}" for delivery')
        self._event_queue.put(event)

    def _step_path(self):
        return f"/ert/ensemble/{self._ens_id}/real/{self._real_id}/step/{self._step_id}"

    def _init_handler(self, msg):
        self._ens_id = msg.ens_id
        self._real_id = msg.real_id
        self._step_id = msg.step_id
        # self._event_publisher_thread.start()
        self._event_publisher_future = asyncio.run_coroutine_threadsafe(
            self._async_publish_event(), self._loop
        )

    def _job_handler(self, msg: Message):
        job_name = msg.job.name()
        job_msg_attrs = {
            _JOB_SOURCE: (
                f"{self._step_path()}/job/{msg.job.index}" f"/index/{msg.job.index}"
            ),
            _CONTENT_TYPE: "application/json",
        }
        if isinstance(msg, Start):
            logger.debug(f"Job {job_name} was successfully started")
            self._dump_event(
                attributes={_JOB_MSG_TYPE: _FM_JOB_START, **job_msg_attrs},
                data={
                    "stdout": str(Path(msg.job.std_out).resolve()),
                    "stderr": str(Path(msg.job.std_err).resolve()),
                },
            )
            if not msg.success():
                logger.error(f"Job {job_name} FAILED to start")
                self._dump_event(
                    attributes={_JOB_MSG_TYPE: _FM_JOB_FAILURE, **job_msg_attrs},
                    data={
                        "error_msg": msg.error_message,
                    },
                )

        elif isinstance(msg, Exited):
            data = None
            if msg.success():
                logger.debug(f"Job {job_name} exited successfully")
                attributes = {_JOB_MSG_TYPE: _FM_JOB_SUCCESS, **job_msg_attrs}
            else:
                logger.error(
                    _JOB_EXIT_FAILED_STRING.format(
                        job_name=msg.job.name(),
                        exit_code=msg.exit_code,
                        error_message=msg.error_message,
                    )
                )
                attributes = {_JOB_MSG_TYPE: _FM_JOB_FAILURE, **job_msg_attrs}
                data = {
                    "exit_code": msg.exit_code,
                    "error_msg": msg.error_message,
                }
            self._dump_event(attributes=attributes, data=data)

        elif isinstance(msg, Running):
            logger.debug(f"{job_name} job is running")
            self._dump_event(
                attributes={_JOB_MSG_TYPE: _FM_JOB_RUNNING, **job_msg_attrs},
                data={
                    "max_memory_usage": msg.max_memory_usage,
                    "current_memory_usage": msg.current_memory_usage,
                },
            )

    async def _finish_publisher(self):
        # await self._running
        # await self._publish_queue.join()
        await self._finish_task

    def _finished_handler(self, msg):
        self._event_queue.put(self._sentinel)
        self._loop.run_until_complete(asyncio.wait_for(self._finish_publisher(), 10))
        # self._loop.run_until_complete(asyncio.wait_for(self._finish_publisher, 10))
        # self._event_queue.join()
        # self._event_publisher_thread.join()
