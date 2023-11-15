import asyncio
from enum import Enum, auto

# from statemachine import StateMachine, states
# from transitions import Machine
from transitions.extensions.asyncio import AsyncMachine


class JobStatus(Enum):
    # This value is used in external query routines - for jobs which are
    # (currently) not active.
    NOT_ACTIVE = auto()

    WAITING = auto()  # A node which is waiting in the internal queue.

    # (should) place it as pending or running.
    SUBMITTED = auto()

    # A node which is pending - a status returned by the external system. I.e LSF
    PENDING = auto()

    RUNNING = auto()  # The job is running

    # The job is done - but we have not yet checked if the target file is
    # produced:
    DONE = auto()

    # The job has exited - check attempts to determine if we retry or go to
    # complete_fail
    EXIT = auto()

    # The the job should be killed, either due to user request, or automated
    # measures - the job can NOT be restarted..
    DO_KILL = auto()

    # The job has been killed, following a DO_KILL - can restart.
    IS_KILLED = auto()

    # Validation went fine:
    SUCCESS = auto()

    STATUS_FAILURE = auto()  # Temporary failure, should not be a reachable state

    FAILED = auto()  # No more retries
    DO_KILL_NODE_FAILURE = auto()  # Compute node should be blocked
    UNKNOWN = auto()

    def __str__(self):
        return super().__str__().replace("JobStatus.", "")


transitions = [
    {
        "trigger": "activate",
        "source": JobStatus.NOT_ACTIVE,
        "dest": JobStatus.WAITING,
    },
    {
        "trigger": "submit",
        "source": JobStatus.WAITING,
        "dest": JobStatus.SUBMITTED,
        "after": "on_submit",
    },
    {
        "trigger": "pc",
        "source": [
            JobStatus.SUBMITTED,
            JobStatus.PENDING,
            JobStatus.RUNNING,
            JobStatus.SUCCESS,
            JobStatus.FAILED,
        ],
        "dest": "*",
        "after": "publish_changes",
    },
    {"trigger": "allocate", "source": JobStatus.UNKNOWN, "dest": JobStatus.NOT_ACTIVE},
    {
        "trigger": "accept",
        "source": JobStatus.SUBMITTED,
        "dest": JobStatus.PENDING,
    },  # from driver
    {
        "trigger": "start",
        "source": JobStatus.PENDING,
        "dest": JobStatus.RUNNING,
    },  # from driver
    {
        "trigger": "runend",
        "source": JobStatus.RUNNING,
        "dest": JobStatus.DONE,
    },  # from driver
    {
        "trigger": "runfail",
        "source": JobStatus.RUNNING,
        "dest": JobStatus.EXIT,
    },  # from driver
    {"trigger": "retry", "source": JobStatus.EXIT, "dest": JobStatus.SUBMITTED},
    {
        "trigger": "dokill",
        "source": [JobStatus.SUBMITTED, JobStatus.PENDING, JobStatus.RUNNING],
        "dest": JobStatus.DO_KILL,
    },
    {
        "trigger": "verify_kill",
        "source": JobStatus.DO_KILL,
        "dest": JobStatus.IS_KILLED,
    },
    {
        "trigger": "ack_killfailure",
        "source": JobStatus.DO_KILL,
        "dest": JobStatus.DO_KILL_NODE_FAILURE,
    },  # do we want to track this?
    {"trigger": "validate", "source": JobStatus.DONE, "dest": JobStatus.SUCCESS},
    {"trigger": "invalidate", "source": JobStatus.DONE, "dest": JobStatus.FAILED},
    {
        "trigger": "somethingwentwrong",
        "source": [
            JobStatus.NOT_ACTIVE,
            JobStatus.WAITING,
            JobStatus.SUBMITTED,
            JobStatus.PENDING,
            JobStatus.RUNNING,
            JobStatus.DONE,
            JobStatus.EXIT,
            JobStatus.DO_KILL,
        ],
        "dest": JobStatus.UNKNOWN,
    },
    {
        "trigger": "donotgohere",
        "source": JobStatus.UNKNOWN,
        "dest": JobStatus.STATUS_FAILURE,
    },
]


class JobStatusModel:
    def __init__(self, jobqueue, iens, retries: int = 1):
        self.jobqueue = jobqueue
        self.iens: int = iens
        self.retries_left: int = retries
        self.machine = AsyncMachine(
            model=self,
            states=JobStatus,
            transitions=transitions,
            initial=JobStatus.NOT_ACTIVE,
        )

    async def on_submit(self):
        print(f"submitting job to driver {self.iens}")
        await self.jobqueue.driver_submit(self.iens)

    async def on_enter_SUBMITTED(self):
        await self.jobqueue.publish_change(self.iens, self.state)

    async def on_enter_PENDING(self):
        await self.jobqueue.publish_change(self.iens, self.state)

    async def on_enter_RUNNING(self):
        await self.jobqueue.publish_change(self.iens, self.state)

    async def on_enter_SUCCESS(self):
        await self.jobqueue.publish_change(self.iens, self.state)

    async def on_enter_FAILED(self):
        await self.jobqueue.publish_change(self.iens, self.state)

    async def on_enter_EXIT(self):
        if self.retries_left > 0:
            await self.retry()
            self.retries_left -= 1
        else:
            await self.invalidate()

    async def on_runend(self):
        await self.jobqueue.run_done_callback(self.iens)

    async def on_enter_DO_KILL(self):
        await self.jobqueue.driver_kill(self.iens)


class JobQueue:
    def __init__(self):
        # Should probably only hand over necessary callbacks...
        self.reals = []
        for iens in range(3):
            self.reals.append(JobStatusModel(self, iens=iens))

    async def execute_loop(self):
        for real in self.reals:
            await real.activate()

        for real in self.reals:
            await real.submit()

        now = 0
        while True:
            # our execution loop
            print(f"{now=}")
            await asyncio.sleep(0.1)
            now += 1
            await self.poll(now)

            if now == 25:
                # max_runtime says we should kill iens=1
                await self.reals[1].dokill()
            if now > 30:
                break
            await asyncio.sleep(0)
        print(self.reals)

    async def driver_submit(self, iens):
        print(f"asking the driver to submit {iens=}")
        await asyncio.sleep(0.5)  # Mocking the response time of the cluster
        await self.reals[iens].accept()

    async def driver_kill(self, iens):
        if await asyncio.sleep(0.5):  # Mocking the response time of the cluster
            await self.reals[iens].verify_kill()
        else:
            await self.reals[iens].ack_killfailure()

    async def run_done_callback(self, iens):
        print(f"running done callback for {iens}")
        await asyncio.sleep(0.2)  # slow summary file reading..
        if iens < 1:
            await self.reals[iens].validate()
        else:
            await self.reals[iens].invalidate()  # failed reading summary or something

    async def publish_change(self, iens, newstate):
        print(
            f"sending cloudevent over websocket for {iens=} with new state {newstate}"
        )

    async def poll(self, time):
        if time == 10:
            await self.reals[0].start()  # mocked driver
            await self.reals[1].start()  # mocked driver
            await self.reals[2].start()  # mocked driver
        if time == 20:
            await self.reals[0].runend()  # mocked driver
            await self.reals[1].runfail()  # mocked driver
            await self.reals[2].runend()  # mocked driver


async def amain():
    jobqueue = JobQueue()
    await jobqueue.execute_loop()


if __name__ == "__main__":
    asyncio.run(amain())
