import asyncio
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from threading import BoundedSemaphore
from typing import Any, Dict, Optional
from unittest.mock import patch

from res._lib.model_callbacks import LoadStatus

try:
    from unittest.mock import AsyncMock
except ImportError:
    # Python 3.6:
    from mock import AsyncMock

import pytest
from res.job_queue import Driver, JobQueue, JobQueueNode, JobStatusType, QueueDriverEnum
from websockets.exceptions import ConnectionClosedError

from ...libres_utils import wait_for


def dummy_exit_callback(args):
    print(args)


DUMMY_CONFIG: Dict[str, Any] = {
    "job_script": "job_script.py",
    "num_cpu": 1,
    "job_name": "dummy_job_{}",
    "run_path": "dummy_path_{}",
    "ok_callback": lambda _: (LoadStatus.LOAD_SUCCESSFUL, ""),
    "exit_callback": dummy_exit_callback,
}

SIMPLE_SCRIPT = """#!/usr/bin/env python
print('hello')
"""

NEVER_ENDING_SCRIPT = """#!/usr/bin/env python
import time
while True:
    time.sleep(0.5)
"""

FAILING_SCRIPT = """#!/usr/bin/env python
import sys
sys.exit(1)
"""


@dataclass
class RunArg:
    iens: int


def create_local_queue(
    executable_script: str,
    max_submit: int = 1,
    max_runtime: Optional[int] = None,
    callback_timeout: Optional[int] = None,
):
    driver = Driver(driver_type=QueueDriverEnum.LOCAL_DRIVER, max_running=5)
    job_queue = JobQueue(driver, max_submit=max_submit)

    scriptpath = Path(DUMMY_CONFIG["job_script"])
    scriptpath.write_text(executable_script, encoding="utf-8")
    scriptpath.chmod(stat.S_IRWXU | stat.S_IRWXO | stat.S_IRWXG)

    for iens in range(10):
        Path(DUMMY_CONFIG["run_path"].format(iens)).mkdir(exist_ok=False)
        job = JobQueueNode(
            job_script=DUMMY_CONFIG["job_script"],
            job_name=DUMMY_CONFIG["job_name"].format(iens),
            run_path=DUMMY_CONFIG["run_path"].format(iens),
            num_cpu=DUMMY_CONFIG["num_cpu"],
            status_file=job_queue.status_file,
            ok_file=job_queue.ok_file,
            exit_file=job_queue.exit_file,
            done_callback_function=DUMMY_CONFIG["ok_callback"],
            exit_callback_function=DUMMY_CONFIG["exit_callback"],
            callback_arguments=[RunArg(iens)],
            max_runtime=max_runtime,
            callback_timeout=callback_timeout,
        )

        job_queue.add_job(job, iens)

    return job_queue


def start_all(job_queue, sema_pool):
    job = job_queue.fetch_next_waiting()
    while job is not None:
        job.run(job_queue.driver, sema_pool, job_queue.max_submit)
        job = job_queue.fetch_next_waiting()


def test_kill_jobs(tmpdir):
    os.chdir(tmpdir)
    job_queue = create_local_queue(NEVER_ENDING_SCRIPT)

    assert job_queue.queue_size == 10
    assert job_queue.is_active()

    pool_sema = BoundedSemaphore(value=10)
    start_all(job_queue, pool_sema)

    # Make sure NEVER_ENDING_SCRIPT has started:
    wait_for(job_queue.is_active)

    # Ask the job to stop:
    for job in job_queue.job_list:
        job.stop()

    wait_for(job_queue.is_active, target=False)

    # pylint: disable=protected-access
    job_queue._differ.transition(job_queue.job_list)

    for q_index, job in enumerate(job_queue.job_list):
        assert job.status == JobStatusType.JOB_QUEUE_IS_KILLED
        iens = job_queue._differ.qindex_to_iens(q_index)
        assert job_queue.snapshot()[iens] == str(JobStatusType.JOB_QUEUE_IS_KILLED)

    for job in job_queue.job_list:
        job.wait_for()


def test_add_jobs(tmpdir):
    os.chdir(tmpdir)
    job_queue = create_local_queue(SIMPLE_SCRIPT)

    assert job_queue.queue_size == 10
    assert job_queue.is_active()
    assert job_queue.fetch_next_waiting() is not None

    pool_sema = BoundedSemaphore(value=10)
    start_all(job_queue, pool_sema)

    for job in job_queue.job_list:
        job.stop()

    wait_for(job_queue.is_active, target=False)

    for job in job_queue.job_list:
        job.wait_for()


def test_failing_jobs(tmpdir):
    os.chdir(tmpdir)
    job_queue = create_local_queue(FAILING_SCRIPT, max_submit=1)

    assert job_queue.queue_size == 10
    assert job_queue.is_active()

    pool_sema = BoundedSemaphore(value=10)
    start_all(job_queue, pool_sema)

    wait_for(job_queue.is_active, target=False)

    for job in job_queue.job_list:
        job.wait_for()

    # pylint: disable=protected-access
    job_queue._differ.transition(job_queue.job_list)

    assert job_queue.fetch_next_waiting() is None

    for q_index, job in enumerate(job_queue.job_list):
        assert job.status == JobStatusType.JOB_QUEUE_FAILED
        iens = job_queue._differ.qindex_to_iens(q_index)
        assert job_queue.snapshot()[iens] == str(JobStatusType.JOB_QUEUE_FAILED)


def test_timeout_jobs(tmpdir):
    os.chdir(tmpdir)
    job_numbers = set()

    def callback(arg):
        nonlocal job_numbers
        job_numbers.add(arg[0].iens)

    job_queue = create_local_queue(
        NEVER_ENDING_SCRIPT,
        max_submit=1,
        max_runtime=5,
        callback_timeout=callback,
    )

    assert job_queue.queue_size == 10
    assert job_queue.is_active()

    pool_sema = BoundedSemaphore(value=10)
    start_all(job_queue, pool_sema)

    # Make sure NEVER_ENDING_SCRIPT jobs have started:
    wait_for(job_queue.is_active)

    # Wait for the timeout to kill them:
    wait_for(job_queue.is_active, target=False)

    # pylint: disable=protected-access
    job_queue._differ.transition(job_queue.job_list)

    for q_index, job in enumerate(job_queue.job_list):
        assert job.status == JobStatusType.JOB_QUEUE_IS_KILLED
        iens = job_queue._differ.qindex_to_iens(q_index)
        assert job_queue.snapshot()[iens] == str(JobStatusType.JOB_QUEUE_IS_KILLED)

    assert job_numbers == set(range(10))

    for job in job_queue.job_list:
        job.wait_for()


def test_add_ensemble_evaluator_info(tmpdir):
    os.chdir(tmpdir)
    job_queue = create_local_queue(SIMPLE_SCRIPT)
    ee_id = "some_id"
    dispatch_url = "wss://example.org"
    experiment_ingest_uri = "wss://example.org/experiment_ingest"
    cert = "My very nice cert"
    token = "my_super_secret_token"
    cert_file = ".ee.pem"
    runpaths = [Path(DUMMY_CONFIG["run_path"].format(iens)) for iens in range(10)]
    for runpath in runpaths:
        (runpath / "jobs.json").write_text(json.dumps({}), encoding="utf-8")
    job_queue.add_ensemble_evaluator_information_to_jobs_file(
        experiment_id="experiment_id",
        experiment_url=experiment_ingest_uri,
        ee_id=ee_id,
        dispatch_url=dispatch_url,
        cert=cert,
        token=token,
    )

    for runpath in runpaths:
        job_file_path = runpath / "jobs.json"
        content: dict = json.loads(job_file_path.read_text(encoding="utf-8"))
        assert content["step_id"] == 0
        assert content["dispatch_url"] == dispatch_url
        assert content["ee_token"] == token

        assert content["ee_cert_path"] == str(runpath / cert_file)
        assert (runpath / cert_file).read_text(encoding="utf-8") == cert


def test_add_ensemble_evaluator_info_cert_none(tmpdir):
    os.chdir(tmpdir)
    job_queue = create_local_queue(SIMPLE_SCRIPT)
    ee_id = "some_id"
    dispatch_url = "wss://example.org"
    experiment_ingest_uri = "wss://example.org/experiment_ingest"
    cert = None
    token = None
    cert_file = ".ee.pem"
    runpaths = [Path(DUMMY_CONFIG["run_path"].format(iens)) for iens in range(10)]
    for runpath in runpaths:
        (runpath / "jobs.json").write_text(json.dumps({}), encoding="utf-8")
    job_queue.add_ensemble_evaluator_information_to_jobs_file(
        experiment_id="experiment_id",
        experiment_url=experiment_ingest_uri,
        ee_id=ee_id,
        dispatch_url=dispatch_url,
        cert=cert,
        token=token,
    )

    for runpath in runpaths:
        job_file_path = runpath / "jobs.json"
        content: dict = json.loads(job_file_path.read_text(encoding="utf-8"))
        assert content["step_id"] == 0
        assert content["dispatch_url"] == dispatch_url
        assert content["ee_token"] == token

        assert content["ee_cert_path"] is None
        assert not (runpath / cert_file).exists()


@pytest.mark.timeout(20)
@pytest.mark.asyncio
async def test_retry_on_closed_connection(tmpdir):
    os.chdir(tmpdir)
    job_queue = create_local_queue(SIMPLE_SCRIPT, max_submit=1)
    pool_sema = BoundedSemaphore(value=10)

    with patch("res.job_queue.queue.connect") as queue_connection:
        websocket_mock = AsyncMock()
        queue_connection.side_effect = [
            ConnectionClosedError(1006, "expected close"),
            websocket_mock,
        ]

        # the queue ate both the exception, and the websocket_mock, trying to
        # consume a third item from the mock causes an (expected) exception
        with pytest.raises(RuntimeError, match="coroutine raised StopIteration"):
            await job_queue.execute_queue_via_websockets(
                ws_uri="ws://example.org",
                ee_id="",
                pool_sema=pool_sema,
                evaluators=[],
            )

        # one fails, the next is a mock, the third is a StopIteration
        assert (
            queue_connection.call_count == 3
        ), "unexpected number of connect calls {f.call_count}"

        # there will be many send calls in here, but the main point it tried
        assert len(websocket_mock.mock_calls) > 0, "the websocket was never called"

    # job_queue cannot go out of scope before queue has completed
    await job_queue.stop_jobs_async()
    while job_queue.isRunning:
        await asyncio.sleep(0.1)
