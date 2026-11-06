import asyncio
from datetime import timedelta

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleSpec,
)
from workflows import Reconcile


async def main() -> None:
    client = await Client.connect("localhost:7233")

    await client.create_schedule(
        "reconcile-minute",
        Schedule(
            action=ScheduleActionStartWorkflow(
                Reconcile.run,
                task_queue="email",
                id="reconcile-minute-2",
            ),
            spec=ScheduleSpec(
                intervals=[ScheduleIntervalSpec(every=timedelta(minutes=2))],
            ),
        ),
    )


asyncio.run(main())
