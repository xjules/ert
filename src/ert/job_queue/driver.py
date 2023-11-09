import asyncio
import shlex
import shutil
import subprocess
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from ert.config import QueueConfig, QueueSystem
from ert.job_queue.job_status import JobStatus

if TYPE_CHECKING:
    from ert.job_queue import ExecutableRealization


class Driver(ABC):
    def __init__(
        self,
        driver_type: QueueSystem,
        options: Optional[List[Tuple[str, str]]] = None,
    ):
        self._driver_type = driver_type
        self._options = {}

        if options:
            for key, value in options:
                self.set_option(key, value)

    def set_option(self, option: str, value: str) -> bool:
        self._options.update({option: value})

    def get_option(self, option_key: str) -> str:
        return self._options[option_key]

    @abstractmethod
    async def submit(self, job: "ExecutableRealization"):
        pass

    @abstractmethod
    async def poll_statuses(self):
        pass

    @abstractmethod
    async def kill(self, job: "ExecutableRealization"):
        pass

    @classmethod
    def create_driver(cls, queue_config: QueueConfig) -> "Driver":
        if queue_config.queue_system == QueueSystem.LOCAL:
            return LocalDriver(queue_config.queue_options)
        elif queue_config.queue_system == QueueSystem.LSF:
            return LSFDriver(queue_config.queue_options)
        raise NotImplementedError


class LocalDriver(Driver):
    def __init__(self, options):
        super().__init__(options)
        self._processes: Dict["ExecutableRealization", asyncio.subprocess.Process] = {}

        # This status map only contains the states that the driver
        # can recognize and is thus not authorative for JobQueue.
        self._statuses: Dict["ExecutableRealization", JobStatus] = {}

        self._statuspoll_mutex = False

    async def submit(self, job):
        """Submit and *actually (a)wait* for the process to finish."""
        process = await asyncio.create_subprocess_exec(
            job.job_script,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=job.run_arg.runpath,
        )
        if process.returncode is None:
            self._statuses[job] = JobStatus.RUNNING
        else:
            # Hmm, can it return so fast that we have a zero return code here?
            raise RuntimeError
        print(f"Started realization {job.run_arg.iens} with pid {process.pid}")
        self._processes[job] = process

        # Wait for process to finish:
        output, error = await process.communicate()

        if process.returncode == 0:
            self._statuses[job] = JobStatus.DONE
        else:
            self._statuses[job] = JobStatus.FAILED
            # TODO: fetch stdout/stderr

    async def poll_statuses(self):
        return self._statuses

    async def kill(self, job):
        self._processes[job].kill()


bjobs_state_to_jobstatus = {
    "RUN": JobStatus.RUNNING,
    "PEND": JobStatus.PENDING,
    "DONE": JobStatus.DONE,
}


class LSFDriver(Driver):
    def __init__(self, queue_options):
        super().__init__(queue_options)

        self._job_to_lsfid: Dict["ExecutableRealization", str] = {}
        self._lsfid_to_job: Dict[str, "ExecutableRealization"] = {}
        self._submit_processes: Dict[
            "ExecutableRealization", asyncio.subprocess.Process
        ] = {}

        # This status map only contains the states that the driver
        # can recognize and is thus not authorative for JobQueue.
        self._statuses: Dict["ExecutableRealization", JobStatus] = {}

    async def submit(self, job):
        submit_cmd = [
            "bsub",
            "-J",
            f"poly_{job.run_arg.iens}",
            job.job_script,
            job.run_arg.runpath,
        ]
        assert shutil.which(submit_cmd[0])  # does not propagate back..
        process = await asyncio.create_subprocess_exec(
            *submit_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self._submit_processes[job] = process

        # Wait for submit process to finish:
        output, error = await process.communicate()
        # print(output)
        # print(error)

        lsf_id = str(output).split(" ")[1].replace("<", "").replace(">", "")
        self._job_to_lsfid[job] = lsf_id
        self._lsfid_to_job[lsf_id] = job
        self._statuses[job] = JobStatus.SUBMITTED
        print(f"Submitted job {job} and got LSF JOBID {lsf_id}")

    async def poll_statuses(self) -> Dict["ExecutableRealization", JobStatus]:
        if self._statuspoll_mutex:
            # Don't repeat if we are called too often.
            # So easy in async..
            return self._statuses
        self._statuspoll_mutex = True
        if not self._job_to_lsfid:
            # We know nothing new yet.
            return self._statuses

        poll_cmd = ["bjobs"] + list(self._job_to_lsfid.values())
        print(f"{poll_cmd=}")
        assert shutil.which(poll_cmd[0])  # does not propagate back..
        process = await asyncio.create_subprocess_exec(
            *poll_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        output, error = await process.communicate()
        for line in output.decode(encoding="utf-8").split("\n"):
            if "JOBID" in line:
                continue
            tokens = shlex.split(
                line
            )  # (shlex parsing is actually wrong, positions are fixed)
            if not tokens:
                continue
            if tokens[0] not in self._lsfid_to_job:
                # A LSF id we know nothing of
                continue
            self._statuses[self._lsfid_to_job[tokens[0]]] = bjobs_state_to_jobstatus[
                tokens[2]
            ]
        self._statuspoll_mutex = False
        return self._statuses

    async def kill(self, job):
        print(f"would like to kill {job}")
        pass
