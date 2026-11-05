import asyncio

from temporalio.client import Client
from workflows import ConfirmEmail


async def main() -> None:
    client = await Client.connect("localhost:7233")

    handle = await client.start_workflow(
        ConfirmEmail.run,
        "user@example.com",
        id="confirm-user-42",
        task_queue="email",
    )

    # позже, когда пользователь нажал кнопку
    await handle.signal(ConfirmEmail.confirm)

    result = await handle.result()  # "подтверждено"
    print(result)


asyncio.run(main())
