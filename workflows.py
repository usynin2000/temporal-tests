from datetime import timedelta

from temporalio import activity, workflow


@activity.defn
async def send_email(address: str) -> None:
    print(f"письмо на {address}: подтверди адрес")


@workflow.defn
class ConfirmEmail:
    def __init__(self) -> None:
        self.confirmed = False

    @workflow.signal
    def confirm(self) -> None:
        self.confirmed = True

    @workflow.run
    async def run(self, address: str) -> str:
        await workflow.execute_activity(
            send_email,
            address,
            start_to_close_timeout=timedelta(seconds=10),
        )
        await workflow.wait_condition(lambda: self.confirmed)
        return "подтверждено"


@workflow.defn
class Reconcile:
    @workflow.run
    async def run(self) -> str:
        return "сверено"
