import asyncio

from temporalio.client import Client
from temporalio.worker import Worker
from workflows import ConfirmEmail, Reconcile, send_email


async def main() -> None:
    client = await Client.connect("localhost:7233")
    worker = Worker(
        client,
        task_queue="email",
        workflows=[ConfirmEmail, Reconcile],
        activities=[send_email],
    )
    await worker.run()


asyncio.run(main())
