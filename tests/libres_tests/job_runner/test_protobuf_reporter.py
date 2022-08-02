import os

import pytest
from ert.experiment_server._schema_pb2 import (
    JOB_FAILURE,
    JOB_RUNNING,
    JOB_START,
    JOB_SUCCESS,
)
from ert.experiment_server._schema_pb2 import Job as PJob
from libres_utils import _mock_ws_thread

from job_runner.job import Job
from job_runner.reporting import Protobuf
from job_runner.reporting.message import Exited, Finish, Init, Running, Start
from job_runner.reporting.statemachine import TransitionError


def test_report_with_successful_start_message_argument(unused_tcp_port):
    host = "localhost"
    url = f"ws://{host}:{unused_tcp_port}"
    reporter = Protobuf(experiment_url=url)
    job1 = Job({"name": "job1", "stdout": "stdout", "stderr": "stderr"}, 0)
    lines = []
    with _mock_ws_thread(host, unused_tcp_port, lines):
        reporter.report(
            Init(
                [job1],
                1,
                19,
                experiment_id="experiment_id",
                ee_id="ee_id",
                real_id=0,
                step_id=0,
            )
        )
        reporter.report(Start(job1))
        reporter.report(Finish())

    assert len(lines) == 1
    event = PJob()
    event.ParseFromString(lines[0])
    assert event.status == JOB_START
    assert event.id.index == 0
    assert event.id.step.step == 0
    assert event.id.step.realization.realization == 0
    assert event.id.step.realization.ensemble.id == "ee_id"
    assert event.id.step.realization.ensemble.experiment.id == "experiment_id"
    assert os.path.basename(event.stdout) == "stdout"
    assert os.path.basename(event.stderr) == "stderr"


def test_report_with_failed_start_message_argument(unused_tcp_port):
    host = "localhost"
    url = f"ws://{host}:{unused_tcp_port}"
    reporter = Protobuf(experiment_url=url)

    job1 = Job({"name": "job1", "stdout": "stdout", "stderr": "stderr"}, 0)

    lines = []
    with _mock_ws_thread(host, unused_tcp_port, lines):
        reporter.report(
            Init(
                [job1],
                1,
                19,
                ee_id="ee_id",
                real_id=0,
                step_id=0,
                experiment_id="experiment_id",
            )
        )

        msg = Start(job1).with_error("massive_failure")

        reporter.report(msg)
        reporter.report(Finish())

    assert len(lines) == 2
    event = PJob()
    event.ParseFromString(lines[1])
    assert event.status == JOB_FAILURE
    assert event.error == "massive_failure"


def test_report_with_successful_exit_message_argument(unused_tcp_port):
    host = "localhost"
    url = f"ws://{host}:{unused_tcp_port}"
    reporter = Protobuf(experiment_url=url)
    job1 = Job({"name": "job1", "stdout": "stdout", "stderr": "stderr"}, 0)

    lines = []
    with _mock_ws_thread(host, unused_tcp_port, lines):
        reporter.report(
            Init(
                [job1],
                1,
                19,
                ee_id="ee_id",
                real_id=0,
                step_id=0,
                experiment_id="experiment_id",
            )
        )
        reporter.report(Exited(job1, 0))
        reporter.report(Finish().with_error("failed"))

    assert len(lines) == 1
    event = PJob()
    event.ParseFromString(lines[0])
    assert event.status == JOB_SUCCESS


def test_report_with_failed_exit_message_argument(unused_tcp_port):
    host = "localhost"
    url = f"ws://{host}:{unused_tcp_port}"
    reporter = Protobuf(experiment_url=url)
    job1 = Job({"name": "job1", "stdout": "stdout", "stderr": "stderr"}, 0)

    lines = []
    with _mock_ws_thread(host, unused_tcp_port, lines):
        reporter.report(
            Init(
                [job1],
                1,
                19,
                ee_id="ee_id",
                real_id=0,
                step_id=0,
                experiment_id="experiment_id",
            )
        )
        reporter.report(Exited(job1, 1).with_error("massive_failure"))
        reporter.report(Finish())

    assert len(lines) == 1
    event = PJob()
    event.ParseFromString(lines[0])
    assert event.status == JOB_FAILURE
    assert event.error == "massive_failure"
    assert event.exit_code == 1


def test_report_with_running_message_argument(unused_tcp_port):
    host = "localhost"
    url = f"ws://{host}:{unused_tcp_port}"
    reporter = Protobuf(experiment_url=url)
    job1 = Job({"name": "job1", "stdout": "stdout", "stderr": "stderr"}, 0)

    lines = []
    with _mock_ws_thread(host, unused_tcp_port, lines):
        reporter.report(
            Init(
                [job1],
                1,
                19,
                ee_id="ee_id",
                real_id=0,
                step_id=0,
                experiment_id="experiment_id",
            )
        )
        reporter.report(Running(job1, 100, 10))
        reporter.report(Finish())

    assert len(lines) == 1
    event = PJob()
    event.ParseFromString(lines[0])
    assert event.status == JOB_RUNNING
    assert event.max_memory == 100
    assert event.current_memory == 10


def test_report_only_job_running_for_successful_run(unused_tcp_port):
    host = "localhost"
    url = f"ws://{host}:{unused_tcp_port}"
    reporter = Protobuf(experiment_url=url)
    job1 = Job({"name": "job1", "stdout": "stdout", "stderr": "stderr"}, 0)

    lines = []
    with _mock_ws_thread(host, unused_tcp_port, lines):
        reporter.report(
            Init(
                [job1],
                1,
                19,
                ee_id="ee_id",
                real_id=0,
                step_id=0,
                experiment_id="experiment_id",
            )
        )
        reporter.report(Running(job1, 100, 10))
        reporter.report(Finish())

    assert len(lines) == 1


def test_report_with_failed_finish_message_argument(unused_tcp_port):
    host = "localhost"
    url = f"ws://{host}:{unused_tcp_port}"
    reporter = Protobuf(experiment_url=url)
    job1 = Job({"name": "job1", "stdout": "stdout", "stderr": "stderr"}, 0)

    lines = []
    with _mock_ws_thread(host, unused_tcp_port, lines):
        reporter.report(
            Init(
                [job1],
                1,
                19,
                ee_id="ee_id",
                real_id=0,
                step_id=0,
                experiment_id="experiment_id",
            )
        )
        reporter.report(Running(job1, 100, 10))
        reporter.report(Finish().with_error("massive_failure"))

    assert len(lines) == 1


def test_report_inconsistent_events(unused_tcp_port):
    host = "localhost"
    url = f"ws://{host}:{unused_tcp_port}"
    reporter = Protobuf(experiment_url=url)

    lines = []
    with _mock_ws_thread(host, unused_tcp_port, lines):
        with pytest.raises(
            TransitionError,
            match=r"Illegal transition None -> \(MessageType<Finish>,\)",
        ):
            reporter.report(Finish())
