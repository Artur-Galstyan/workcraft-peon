import asyncio
import time

from loguru import logger

from workcraft.models import TaskPayload, Workcraft


stronghold_url = "http://localhost:6112"
api_key = "abcd"
workcraft = Workcraft(stronghold_url, api_key)

global_counter = 0


# @workcraft.setup_handler()
# def setup_handler():
#     global global_counter
#     global_counter = 1000
#     logger.info("Setting up the worker!")


@workcraft.task("simple_task")
def simple_task(task_id: str, a: str) -> int:
    print(task_id, len(a))
    time.sleep(10)
    # raise ValueError("This is a test error mon")

    return 0


@workcraft.postrun_handler()
def postrun_handler(task_id, task_name, result, status):
    logger.info(
        f"PR called for {task_id} and {task_name}! Got {result} and status {status}"
    )


async def main():
    n_tasks = 10
    for _ in range(n_tasks):
        workcraft.send_task_sync(
            task_name="simple_task",
            payload=TaskPayload(
                task_args=["aaaaaa"],
            ),
            retry_on_failure=True,
        )


if __name__ == "__main__":
    asyncio.run(main())
