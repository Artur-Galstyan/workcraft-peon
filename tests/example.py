import asyncio
import json
import logging
import random
import string
import sys
import time

from tqdm import tqdm

from workcraft.models import TaskPayload, Workcraft


def calculate_entry_size() -> int:
    """Calculate the size of a single entry including JSON overhead."""
    sample_entry = {
        "id": "a" * 10,  # Fixed length of 10
        "value": "a" * 100,  # Fixed length of 100
        "timestamp": "2024-02-11T12:00:00Z",
        "type": "type1",
        "status": "active",
    }
    # Add 2 for the comma and newline that will be added between entries
    return len(json.dumps(sample_entry).encode("utf-8")) + 2


def generate_mb_json(n: int) -> dict:
    """
    Generate a JSON object of approximately n megabytes.
    Uses pre-calculation and batch generation for better performance.

    Args:
        n: Target size in megabytes
    Returns:
        Dictionary that will serialize to approximately n MB of JSON
    """
    target_bytes = n * 1048576

    # Calculate fixed overhead
    base_obj = {
        "metadata": {
            "size": f"{n}MB",
            "timestamp": "2024-02-11T12:00:00Z",
            "type": "test-data",
        },
        "data": [],
    }
    base_size = len(json.dumps(base_obj).encode("utf-8"))

    entry_size = calculate_entry_size()
    num_entries = (target_bytes - base_size) // entry_size

    entries = [
        {
            "id": "".join(random.choices(string.ascii_letters + string.digits, k=10)),
            "value": "".join(
                random.choices(string.ascii_letters + string.digits, k=100)
            ),
            "timestamp": "2024-02-11T12:00:00Z",
            "type": random.choice(["type1", "type2", "type3"]),
            "status": random.choice(["active", "inactive", "pending"]),
        }
        for _ in range(num_entries)
    ]

    base_obj["data"] = entries
    return base_obj


# Example usage:
data = generate_mb_json(5)  # Generates ~5MB JSON
json_str = json.dumps(data)  # Convert to JSON string if needed


stronghold_url = "http://localhost:6112"
api_key = "abcd"
workcraft = Workcraft(stronghold_url, api_key)

global_counter = 0


# @workcraft.setup_handler()
# def setup_handler():
#     global global_counter
#     global_counter = 1000
#     print("Setting up the worker!")


@workcraft.task("simple_task")
def simple_task(task_id: str, a: str) -> int:
    # raise ValueError("This is a test error mon")
    print(task_id, len(a))
    print("Regular print")
    logging.info("Standard logging")
    sys.stderr.write("Direct stderr write\n")
    time.sleep(100)
    # if random.randint(0, 10) < 2:
    #     raise ValueError("This is a test error mon")

    return 0


@workcraft.prerun_handler()
def prerun_handler(task_id, task_name):
    print(f"PR called for {task_id} and {task_name}!")


@workcraft.postrun_handler()
def postrun_handler(task_id, task_name, result, status):
    print(f"PR called for {task_id} and {task_name}! Got {result} and status {status}")


async def main():
    n_tasks = 1
    for _ in tqdm(range(n_tasks)):
        workcraft.send_task_sync(
            task_name="simple_task",
            payload=TaskPayload(task_args=[json_str]),
            retry_on_failure=True,
            retry_limit=3,
            # queue=random.choice(["A", "B", "C"]),
            queue="D",
        )


if __name__ == "__main__":
    asyncio.run(main())
