import asyncio
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

    @classmethod
    def create_driver(cls, queue_config: QueueConfig) -> "Driver":
        if queue_config.queue_system == QueueSystem.LOCAL:
            return LocalDriver(queue_config.queue_options)
        elif queue_config.queue_system == QueueSystem.LSF:
            raise LSFDriver(queue_config.queue_options)
        raise NotImplementedError


class LocalDriver(Driver):
    def __init__(self, options):
        super().__init__(options)
        self._processes: Dict["ExecutableRealization", asyncio.subprocess.Process] = {}
        self._statuses: Dict["ExecutableRealization", JobStatus] = {}

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
        print(" *** a realization is finished")
        print(f"{output=}")
        print(f"{error=}")
        if process.returncode == 0:
            self._statuses[job] = JobStatus.DONE
        else:
            self._statuses[job] = JobStatus.FAILED
            # TODO: fetch stdout/stderr

    async def poll_statuses(self):
        return self._statuses

    def get_statuses(self):
        # auto-poll here or not?
        return self._statuses

    def kill(self, job):
        self._processes[job].kill()


class LSFDriver(Driver):
    def __init__(self):
        self._job_to_lsfid = {}

    async def submit(self, job):
        pass
        # lsf_id = subprocess.run(["bsub", job.job_script])
        # self._job_to_lsfid[job.id] = lsf_id
