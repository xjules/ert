import json
import os
import os.path
import stat

import pytest

from _ert_job_runner.reporting.message import Exited, Start
from _ert_job_runner.runner import JobRunner
from ert._c_wrappers.util import SubstitutionList
from ert.config import ErtConfig, ExtJob

# Test data generated by ForwardModel
JSON_STRING = """
{
  "run_id"    : "ERT_RUN_ID",
  "jobList" : [ {"name" : "PERLIN",
  "executable" : "perlin.py",
  "target_file" : "my_target_file",
  "error_file" : "error_file",
  "start_file" : "some_start_file",
  "stdout" : "perlin.stdoit",
  "stderr" : "perlin.stderr",
  "stdin" : "intput4thewin",
  "argList" : ["-speed","hyper"],
  "environment" : {"TARGET" : "flatland"},
  "license_path" : "this/is/my/license/PERLIN",
  "max_running_minutes" : 12,
  "max_running" : 30
},
{"name" : "PERGEN",
  "executable" : "pergen.py",
  "target_file" : "my_target_file",
  "error_file" : "error_file",
  "start_file" : "some_start_file",
  "stdout" : "perlin.stdoit",
  "stderr" : "perlin.stderr",
  "stdin" : "intput4thewin",
  "argList" : ["-speed","hyper"],
  "environment" : {"TARGET" : "flatland"},
  "license_path" : "this/is/my/license/PERGEN",
  "max_running_minutes" : 12,
  "max_running" : 30
}]
}
"""


def create_jobs_json(job_list):
    return {"jobList": job_list}


@pytest.fixture(autouse=True)
def set_up_environ():
    if "ERT_RUN_ID" in os.environ:
        del os.environ["ERT_RUN_ID"]

    yield

    keys = (
        "KEY_ONE",
        "KEY_TWO",
        "KEY_THREE",
        "KEY_FOUR",
        "PATH104",
        "ERT_RUN_ID",
    )

    for key in keys:
        if key in os.environ:
            del os.environ[key]


@pytest.mark.usefixtures("use_tmpdir")
def test_missing_joblist_json():
    with pytest.raises(KeyError):
        JobRunner({})


@pytest.mark.usefixtures("use_tmpdir")
def test_run_output_rename():
    job = {
        "name": "TEST_JOB",
        "executable": "/bin/mkdir",
        "stdout": "out",
        "stderr": "err",
    }
    joblist = [job, job, job, job, job]

    jobm = JobRunner(create_jobs_json(joblist))

    for status in enumerate(jobm.run([])):
        if isinstance(status, Start):
            assert status.job.std_err == f"err.{status.job.index}"
            assert status.job.std_out == f"out.{status.job.index}"


@pytest.mark.usefixtures("use_tmpdir")
def test_run_multiple_ok():
    joblist = []
    dir_list = ["1", "2", "3", "4", "5"]
    for job_index in dir_list:
        job = {
            "name": "MKDIR",
            "executable": "/bin/mkdir",
            "stdout": f"mkdir_out.{job_index}",
            "stderr": f"mkdir_err.{job_index}",
            "argList": ["-p", "-v", job_index],
        }
        joblist.append(job)

    jobm = JobRunner(create_jobs_json(joblist))

    statuses = [s for s in list(jobm.run([])) if isinstance(s, Exited)]

    assert len(statuses) == 5
    for status in statuses:
        assert status.exit_code == 0

    for dir_number in dir_list:
        assert os.path.isdir(dir_number)
        assert os.path.isfile(f"mkdir_out.{dir_number}")
        assert os.path.isfile(f"mkdir_err.{dir_number}")
        assert os.path.getsize(f"mkdir_err.{dir_number}") == 0


@pytest.mark.usefixtures("use_tmpdir")
def test_run_multiple_fail_only_runs_one():
    joblist = []
    for index in range(1, 6):
        job = {
            "name": "exit",
            "executable": "/bin/bash",
            "stdout": "exit_out",
            "stderr": "exit_err",
            # produces something on stderr, and exits with
            "argList": [
                "-c",
                f'echo "failed with {index}" 1>&2 ; exit {index}',
            ],
        }
        joblist.append(job)

    jobm = JobRunner(create_jobs_json(joblist))

    statuses = [s for s in list(jobm.run([])) if isinstance(s, Exited)]

    assert len(statuses) == 1
    for i, status in enumerate(statuses):
        assert status.exit_code == i + 1


@pytest.mark.usefixtures("use_tmpdir")
def test_exec_env():
    with open("exec_env.py", "w", encoding="utf-8") as f:
        f.write(
            """#!/usr/bin/env python\n
import os
import json
with open("exec_env_exec_env.json") as f:
 exec_env = json.load(f)
assert exec_env["TEST_ENV"] == "123"
            """
        )
    os.chmod("exec_env.py", stat.S_IEXEC + stat.S_IREAD)

    with open("EXEC_ENV", "w", encoding="utf-8") as f:
        f.write("EXECUTABLE exec_env.py\n")
        f.write("EXEC_ENV TEST_ENV 123\n")

    ext_job = ExtJob.from_config_file(name=None, config_file="EXEC_ENV")

    with open("jobs.json", mode="w", encoding="utf-8") as fptr:
        json.dump(
            ErtConfig(forward_model_list=[ext_job]).forward_model_data_to_json(
                "run_id"
            ),
            fptr,
        )

    with open("jobs.json", "r", encoding="utf-8") as f:
        jobs_json = json.load(f)

    for msg in list(JobRunner(jobs_json).run([])):
        if isinstance(msg, Start):
            with open("exec_env_exec_env.json", encoding="utf-8") as f:
                exec_env = json.load(f)
                assert exec_env["TEST_ENV"] == "123"
        if isinstance(msg, Exited):
            assert msg.exit_code == 0


@pytest.mark.usefixtures("use_tmpdir")
def test_env_var_available_inside_job_context():
    with open("run_me.py", "w", encoding="utf-8") as f:
        f.write(
            """#!/usr/bin/env python\n
import os
assert os.environ["TEST_ENV"] == "123"
            """
        )
    os.chmod("run_me.py", stat.S_IEXEC + stat.S_IREAD)

    with open("RUN_ENV", "w", encoding="utf-8") as f:
        f.write("EXECUTABLE run_me.py\n")
        f.write("ENV TEST_ENV 123\n")

    job = ExtJob.from_config_file(name=None, config_file="RUN_ENV")
    with open("jobs.json", mode="w", encoding="utf-8") as fptr:
        json.dump(
            ErtConfig(forward_model_list=[job]).forward_model_data_to_json("run_id"),
            fptr,
        )

    with open("jobs.json", "r", encoding="utf-8") as f:
        jobs_json = json.load(f)

    # Check ENV variable not available outside of job context
    assert "TEST_ENV" not in os.environ

    for msg in list(JobRunner(jobs_json).run([])):
        if isinstance(msg, Exited):
            assert msg.exit_code == 0

    # Check ENV variable not available outside of job context
    assert "TEST_ENV" not in os.environ


@pytest.mark.usefixtures("use_tmpdir")
def test_default_env_variables_available_inside_job_context():
    with open("run_me.py", "w", encoding="utf-8") as f:
        f.write(
            """#!/usr/bin/env python\n
import os
assert os.environ["_ERT_ITERATION_NUMBER"] == "0"
assert os.environ["_ERT_REALIZATION_NUMBER"] == "0"
assert os.environ["_ERT_RUNPATH"] == "./"
            """
        )
    os.chmod("run_me.py", stat.S_IEXEC + stat.S_IREAD)

    with open("RUN_ENV", "w", encoding="utf-8") as f:
        f.write("EXECUTABLE run_me.py\n")

    job = ExtJob.from_config_file(name=None, config_file="RUN_ENV")
    with open("jobs.json", mode="w", encoding="utf-8") as fptr:
        json.dump(
            ErtConfig(
                forward_model_list=[job],
                substitution_list=SubstitutionList.from_dict(
                    {"DEFINE": [["<RUNPATH>", "./"]]}
                ),
            ).forward_model_data_to_json(
                "run_id",
            ),
            fptr,
        )

    with open("jobs.json", "r", encoding="utf-8") as f:
        jobs_json = json.load(f)

    # Check default ENV variable not available outside of job context
    for k in ExtJob.default_env:
        assert k not in os.environ

    for msg in list(JobRunner(jobs_json).run([])):
        if isinstance(msg, Exited):
            assert msg.exit_code == 0

    # Check default ENV variable not available outside of job context
    for k in ExtJob.default_env:
        assert k not in os.environ
