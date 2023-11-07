from typing import List, Optional, Tuple


from ert.config import QueueConfig, QueueSystem
from queue import ExecutableRealization


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
    def submit(job: ExecutableRealization):
        pass


    @classmethod
    def create_driver(cls, queue_config: QueueConfig) -> "Driver":
        driver = Driver(queue_config.queue_system)
        if queue_config.queue_system in queue_config.queue_options:
            for setting in queue_config.queue_options[queue_config.queue_system]:
                driver.set_option(*setting)
        return driver


class LocalDriver(Driver):
    def __init__(self, options):
        super(QueueSystem.LOCAL, options)
        self._popen_handles: Dict[int, subprocess.Popen] = {}
        self._statuses: Dict[int, JobStatus] = {}

    def submit(self.job):
        self._job_to_popen_handles[job.id] = subprocess.Popen(executable=job.job_script)  # must return immediately
        self._statuses[job.id] = JobStatus.SUBMITTED

    def poll_statuses(self):
        for job_id, popen_handle in self._popen_handles:
            return_code = popen_handle.poll()
            if return_code is None:
                continue
            elif return_code == 0:
                self._statuses[job_id] = JobStatus.DONE
                # TODO: fetch stdout/stderr
            else:
                self._statuses[job_id] = JobStatus.FAILED
                # TODO: fetch stdout/stderr

class LSFDriver(Driver):
    def __init__():
        self._job_to_lsfid = {}

    def submit(job):
        lsf_id = subprocess.run(["bsub", job.job_script])
        self._job_to_lsfid[job.id] = lsf_id