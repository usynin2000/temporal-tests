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
        "reconcile-hourly",
        Schedule(
            action=ScheduleActionStartWorkflow(
                Reconcile.run,
                task_queue="email",
            ),
            spec=ScheduleSpec(
                intervals=[ScheduleIntervalSpec(every=timedelta(minutes=1))],
            ),
        ),
    )


asyncio.run(main())
