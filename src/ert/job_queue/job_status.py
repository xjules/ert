import asyncio
from enum import Enum, auto

from statemachine import StateMachine, states
from transitions import Machine
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
    ["activate", JobStatus.NOT_ACTIVE, JobStatus.WAITING],
    {
        "trigger": "submit",
        "source": JobStatus.WAITING,
        "dest": JobStatus.SUBMITTED,
        "before": "on_submit",
    },
    ["allocate", JobStatus.UNKNOWN, JobStatus.NOT_ACTIVE],
    ["accept", JobStatus.SUBMITTED, JobStatus.PENDING],  # from driver
    ["start", JobStatus.PENDING, JobStatus.RUNNING],  # from driver
    ["runend", JobStatus.RUNNING, JobStatus.DONE],  # from driver
    ["runfail", JobStatus.RUNNING, JobStatus.EXIT],  # from driver
    ["retry", JobStatus.EXIT, JobStatus.SUBMITTED],
    [
        "dokill",
        [JobStatus.SUBMITTED, JobStatus.PENDING, JobStatus.RUNNING],
        JobStatus.DO_KILL,
    ],
    ["verify_kill", JobStatus.DO_KILL, JobStatus.IS_KILLED],
    [
        "ack_killfailure",
        JobStatus.DO_KILL,
        JobStatus.DO_KILL_NODE_FAILURE,
    ],  # do we want to track this?
    ["validate", JobStatus.DONE, JobStatus.SUCCESS],
    ["invalidate", JobStatus.DONE, JobStatus.FAILED],
    [
        "somethingwentwrong",
        [
            JobStatus.NOT_ACTIVE,
            JobStatus.WAITING,
            JobStatus.SUBMITTED,
            JobStatus.PENDING,
            JobStatus.RUNNING,
            JobStatus.DONE,
            JobStatus.EXIT,
            JobStatus.DO_KILL,
        ],
        JobStatus.UNKNOWN,
    ],
    ["donotgohere", JobStatus.UNKNOWN, JobStatus.STATUS_FAILURE],
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

    async def on_submit(self, event, state):
        self.jobqueue.driver_submit(self.iens)

    async def on_enter_state(self, event, state):
        if state in [
            self.SUBMITTED,
            self.PENDING,
            self.RUNNING,
            self.SUCCESS,
            self.FAILED,
        ]:
            self.jobqueue.publish_change(self.iens, state.id)

    async def on_enter_EXIT(self):
        if self.retries_left > 0:
            self.retry()
            self.retries_left -= 1
        else:
            self.invalidate()

    async def on_runend(self):
        self.jobqueue.run_done_callback(self.iens)

    async def on_enter_DO_KILL(self):
        self.jobqueue.driver_kill(self.iens)


class JobStatusMachine(StateMachine):
    def __init__(self, jobqueue, iens, retries: int = 1):
        self.jobqueue = jobqueue
        self.iens: int = iens
        self.retries_left: int = retries
        super().__init__()

    _ = states.States.from_enum(
        JobStatus,
        initial=JobStatus.NOT_ACTIVE,
        final={
            JobStatus.SUCCESS,
            JobStatus.FAILED,
            JobStatus.IS_KILLED,
            JobStatus.DO_KILL_NODE_FAILURE,
        },
    )

    allocate = _.UNKNOWN.to(_.NOT_ACTIVE)

    activate = _.NOT_ACTIVE.to(_.WAITING)
    submit = _.WAITING.to(_.SUBMITTED)  # from jobqueue
    accept = _.SUBMITTED.to(_.PENDING)  # from driver
    start = _.PENDING.to(_.RUNNING)  # from driver
    runend = _.RUNNING.to(_.DONE)  # from driver
    runfail = _.RUNNING.to(_.EXIT)  # from driver
    retry = _.EXIT.to(_.SUBMITTED)

    dokill = _.DO_KILL.from_(_.SUBMITTED, _.PENDING, _.RUNNING)

    verify_kill = _.DO_KILL.to(_.IS_KILLED)

    ack_killfailure = _.DO_KILL.to(_.DO_KILL_NODE_FAILURE)  # do we want to track this?

    validate = _.DONE.to(_.SUCCESS)
    invalidate = _.DONE.to(_.FAILED)

    somethingwentwrong = _.UNKNOWN.from_(
        _.NOT_ACTIVE,
        _.WAITING,
        _.SUBMITTED,
        _.PENDING,
        _.RUNNING,
        _.DONE,
        _.EXIT,
        _.DO_KILL,
    )

    donotgohere = _.UNKNOWN.to(_.STATUS_FAILURE)

    def on_submit(self, event, state):
        asyncio.create_task(self.jobqueue.driver_submit(self.iens))

    def on_enter_state(self, event, state):
        if state in [
            self.SUBMITTED,
            self.PENDING,
            self.RUNNING,
            self.SUCCESS,
            self.FAILED,
        ]:
            asyncio.create_task(self.jobqueue.publish_change(self.iens, state.id))

    def on_enter_EXIT(self):
        if self.retries_left > 0:
            self.retry()
            self.retries_left -= 1
        else:
            self.invalidate()

    def on_runend(self):
        asyncio.create_task(self.jobqueue.run_done_callback(self.iens))

    def on_enter_DO_KILL(self):
        asyncio.create_task(self.jobqueue.driver_kill(self.iens))


class JobQueue:
    def __init__(self):
        # Should probably only hand over necessary callbacks...
        self.reals = []
        for iens in range(3):
            self.reals.append(JobStatusMachine(self, iens=iens))

    async def execute_loop(self):
        for real in self.reals:
            real.activate()

        for real in self.reals:
            real.submit()

        now = 0
        while True:
            # our execution loop
            print(f"{now=}")
            await asyncio.sleep(0.1)
            now += 1
            await self.poll(now)

            if now == 25:
                # max_runtime says we should kill iens=1
                self.reals[1].dokill()
            if now > 30:
                break
            await asyncio.sleep(0)
        print(self.reals)

    async def driver_submit(self, iens):
        print(f"asking the driver to submit {iens=}")
        await asyncio.sleep(0.5)  # Mocking the response time of the cluster
        self.reals[iens].accept()

    async def driver_kill(self, iens):
        if await asyncio.sleep(0.5):  # Mocking the response time of the cluster
            self.reals[iens].verify_kill()
        else:
            self.reals[iens].ack_killfailure()

    async def run_done_callback(self, iens):
        print(f"running done callback for {iens}")
        await asyncio.sleep(0.2)  # slow summary file reading..
        if iens < 1:
            self.reals[iens].validate()
        else:
            self.reals[iens].invalidate()  # failed reading summary or something

    async def publish_change(self, iens, newstate):
        print(
            f"sending cloudevent over websocket for {iens=} with new state {newstate}"
        )

    async def poll(self, time):
        if time == 10:
            self.reals[0].start()  # mocked driver
            self.reals[1].start()  # mocked driver
            self.reals[2].start()  # mocked driver
        if time == 20:
            self.reals[0].runend()  # mocked driver
            self.reals[1].runfail()  # mocked driver
            self.reals[2].runend()  # mocked driver


async def amain():
    jobqueue = JobQueue()
    await jobqueue.execute_loop()


if __name__ == "__main__":
    asyncio.run(amain())
