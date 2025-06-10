import datetime
import json
import threading
import time
import uuid
from queue import Empty, Queue

import requests
from loguru import logger
from requests.adapters import HTTPAdapter

from workcraft.models import Separators, Task, Workcraft
from workcraft.utils import capture_all_output, tenacious_request


class Peon:
    def __init__(
        self,
        workcraft: Workcraft,
        id: str | None = None,
        queues: list[str] | None = None,
    ) -> None:
        self.id = id or uuid.uuid4().hex
        self.queues = queues
        self.workcraft = workcraft
        self.seen_tasks_in_memory = set()
        self.current_task = None

        self.working = True
        self.queue = Queue()
        self.connected = False
        self._sse_thread = threading.Thread(target=self._run_sse, daemon=True)
        self._heartbeat_thread = threading.Thread(target=self._heartbeat, daemon=True)
        self._processor_thread = threading.Thread(target=self._process, daemon=True)
        self._statistics_thread = threading.Thread(target=self._statistics, daemon=True)

        self._stop_event = threading.Event()
        self._task_cancelled = threading.Event()

        self.last_heartbeat_print = datetime.datetime.now()
        self.last_heartbeat_length = 0
        self.heartbeat_messages = []

    def _sync(self, data: dict) -> None:
        if self.connected:
            try:
                res = tenacious_request(
                    lambda: requests.post(
                        self.workcraft.stronghold_url + f"/api/peon/{self.id}/update",
                        headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                        json=data,
                    )
                )

                if "current_task_set" in data and "current_task" in data:
                    self.current_task = data["current_task"]

                if 200 <= res.status_code < 300:
                    pass
                else:
                    logger.error(
                        f"Failed to update peon: {res.status_code} - {res.text}"
                    )
            except Exception as e:
                logger.error(f"Failed to send peon update: {e}")

    def work(self) -> None:
        logger.info("Starting peon...")

        self._sse_thread.start()
        logger.info("Started SSE thread")

        self._heartbeat_thread.start()
        logger.info("Started heartbeat thread")
        self._processor_thread.start()
        logger.info("Started processor thread")

        self._statistics_thread.start()
        logger.info("Started statistics thread")
        logger.info(f"Peon ID {self.id}")
        available_tasks = self.workcraft.tasks.keys()
        logger.info("Available Tasks:")
        for task in available_tasks:
            logger.info(f" - {task}")

        logger.success("Zug Zug. Ready to work!")
        try:
            while not self._stop_event.is_set():
                self._stop_event.wait(1)  # Wait with timeout
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
            self.stop()

    def _cancel_task_in_queue(self, task_id: str) -> None:
        for i in range(self.queue.qsize()):
            task = self.queue.get()
            if task.id != task_id:
                self.queue.put(task)
        logger.info(f"Task {task_id} removed from queue")

    def _statistics(self) -> None:
        while self.working and not self._stop_event.is_set():
            try:
                res = tenacious_request(
                    lambda: requests.post(
                        self.workcraft.stronghold_url
                        + f"/api/peon/{self.id}/statistics",
                        json={
                            "type": "queue",
                            "value": {
                                "size": self.queue.qsize(),
                            },
                            "peon_id": self.id,
                        },
                        headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                    )
                )

                if 200 <= res.status_code < 300:
                    # logger.info("Statistics sent successfully")
                    pass
                else:
                    logger.error(f"Failed to send statistics: {res.text}")
            except Exception as e:
                logger.error(f"Failed to send statistics: {e}")

            self._stop_event.wait(5)

    def _process(self) -> None:
        while self.working and not self._stop_event.is_set():
            try:
                task = self.queue.get(timeout=1)
            except Empty as _:
                self._sync(
                    {
                        "current_task": None,
                        "current_task_set": True,
                        "status": "IDLE",
                        "status_set": True,
                    }
                )
                continue

            try:
                logger.info(f"Processing task {task.id}")
                self._sync(
                    {
                        "current_task": task.id,
                        "current_task_set": True,
                        "status": "WORKING",
                        "status_set": True,
                    }
                )

                res = tenacious_request(
                    lambda: requests.post(
                        f"{self.workcraft.stronghold_url}/api/task/{task.id}/update",
                        headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                        json={"status": "RUNNING"},
                    )
                )

                if 200 <= res.status_code < 300:
                    logger.info(f"Task {task.id} set to RUNNING")
                else:
                    raise Exception(f"Failed to set task to RUNNING: {res.text}")

                self._task_cancelled.clear()
                result_queue = Queue()

                def execute_task(_task):
                    with capture_all_output(self.workcraft, task.id):
                        result = self.workcraft.execute(_task)
                        result_queue.put(result)
                        return

                task_thread = threading.Thread(target=execute_task, args=(task,))
                task_thread.start()

                # Monitor for cancellation or completion
                cancelled = False
                while task_thread.is_alive():
                    if self._task_cancelled.is_set() or self._stop_event.is_set():
                        logger.info("Task cancellation requested")
                        task_thread.join(timeout=5)
                        if task_thread.is_alive():
                            logger.warning("Task did not stop gracefully")
                        task.status = "CANCELLED"
                        cancelled = True
                        break
                    task_thread.join(timeout=1)

                if not cancelled:
                    try:
                        updated_task = result_queue.get_nowait()
                    except Empty as e:
                        logger.error(
                            f"Failed to get task result: {e} because queue is empty"
                        )
                        task.status = "FAILURE"
                        task.result = (
                            f"Task failed to complete. No result available. Error: {e}"
                        )
                        updated_task = task
                else:
                    updated_task = task  # Use the cancelled task

                try:
                    res = tenacious_request(
                        lambda: requests.post(
                            f"{self.workcraft.stronghold_url}/api/task/{task.id}/update",
                            headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                            json=Task.to_stronghold(updated_task),
                        )
                    )

                    if 200 <= res.status_code < 300:
                        logger.info(f"Task updated with status: {updated_task.status}")
                    else:
                        logger.error(
                            f"Failed to update task: {res.status_code} - {res.text}"
                        )
                except Exception as e:
                    logger.error(f"Failed to send task update: {e}")

            except Exception as e:
                logger.error(f"Failed to process task: {e}")
            finally:
                self._sync(
                    {
                        "current_task": None,
                        "current_task_set": True,
                        "status": "IDLE",
                        "status_set": True,
                    }
                )

                self.seen_tasks_in_memory.remove(task.id)
                self.queue.task_done()

            # Break the loop if we're stopping
            if self._stop_event.is_set():
                logger.info("Stopping processor thread")
                break

    def _heartbeat(self) -> None:
        while self.working and not self._stop_event.is_set():
            try:
                self._sync(
                    {
                        "last_heartbeat": datetime.datetime.now().isoformat(),
                        "last_heartbeat_set": True,
                    }
                )
            except Exception as e:
                logger.error(f"Failed to send ping: {e}")
            self._stop_event.wait(5)

    def _queue_to_stronghold(self) -> str:
        if self.queues is None:
            return "[]"
        return "['" + "','".join(self.queues) + "']"

    def _run_sse(self):
        logger.info("Starting SSE thread")
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=1, pool_maxsize=1, max_retries=10, pool_block=False
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        while self.working and not self._stop_event.is_set():
            print("Connecting to server after 5 seconds...")
            self._stop_event.wait(5)
            try:
                logger.info(f"Attempting connection to {self.workcraft.stronghold_url}")
                response = session.get(
                    f"{self.workcraft.stronghold_url}/events?type=peon&peon_id={self.id}&queues={self._queue_to_stronghold()}",
                    stream=True,
                    headers={
                        "WORKCRAFT_API_KEY": self.workcraft.api_key,
                        "Connection": "keep-alive",
                        "Keep-Alive": "timeout=60, max=1000",
                    },
                    timeout=(5, 30),
                )
                if response.status_code != 200:
                    logger.error(f"Failed to connect to server: {response.text}")
                    self._stop_event.wait(5)
                    continue
                buffer = ""
                buffer_last_update = time.time()
                max_buffer_age = 60
                for line in response.iter_content(chunk_size=None):
                    if line:
                        try:
                            current_time = time.time()
                            if (
                                buffer
                                and current_time - buffer_last_update > max_buffer_age
                            ):
                                logger.warning(
                                    f"Discarding stale buffer after {max_buffer_age} seconds: {buffer[:100]}..."
                                )
                                buffer = ""
                            buffer += line.decode()
                            buffer_last_update = current_time
                            if not buffer.endswith(
                                Separators.WORKCRAFT_SSE_SEPARATOR_END
                            ):
                                continue

                            if not buffer.startswith(
                                Separators.WORKCRAFT_SSE_SEPARATOR_START
                            ) and not buffer.endswith(
                                Separators.WORKCRAFT_SSE_SEPARATOR_END
                            ):
                                raise ValueError("Invalid buffer data received")
                            msg = buffer.split(
                                Separators.WORKCRAFT_SSE_SEPARATOR_START
                            )[1].split(Separators.WORKCRAFT_SSE_SEPARATOR_END)[0]
                            msg = json.loads(msg)
                            buffer = ""
                            if msg["type"] == "new_task" and self.connected:
                                try:
                                    task = Task.model_validate(msg["data"])
                                except Exception as e:
                                    logger.error(
                                        f"Failed to validate task: {e}, malformed."
                                        " Setting task to INVALID"
                                    )
                                    task_id = msg["payload"]["id"]
                                    if not task_id:
                                        logger.error("Task ID is missing")
                                        continue

                                    res = tenacious_request(
                                        lambda: requests.post(
                                            f"{self.workcraft.stronghold_url}/api/task/{task_id}/update",
                                            headers={
                                                "WORKCRAFT_API_KEY": self.workcraft.api_key
                                            },
                                            json={
                                                "status": "INVALID",
                                                "result": f"Task is invalid: {e}",
                                            },
                                        )
                                    )

                                    if 200 <= res.status_code < 300:
                                        logger.info(f"Task {task_id} set to INVALID")
                                    else:
                                        logger.error(
                                            f"Failed to set task to INVALID: {res.text}"
                                        )
                                    continue

                                if task.id in self.seen_tasks_in_memory:
                                    logger.info(
                                        f"Task {task.id} already seen, skipping"
                                    )
                                    continue

                                try:
                                    res = tenacious_request(
                                        lambda: requests.post(
                                            self.workcraft.stronghold_url
                                            + f"/api/task/{task.id}/update",
                                            headers={
                                                "WORKCRAFT_API_KEY": self.workcraft.api_key
                                            },
                                            json={
                                                "peon_id": self.id,
                                                "status": "ACKNOWLEDGED",
                                            },
                                        )
                                    )

                                    if 200 <= res.status_code < 300:
                                        logger.info(
                                            "Task acknowledgement sent successfully"
                                        )
                                    else:
                                        logger.error(
                                            f"Failed to send task ack: {res.text}"
                                        )

                                    self._sync(
                                        {
                                            "current_task": task.id,
                                            "current_task_set": True,
                                            "status": "PREPARING",
                                            "status_set": True,
                                        }
                                    )
                                    task.peon_id = self.id
                                    self.queue.put(task)
                                    self.seen_tasks_in_memory.add(task.id)

                                except Exception as e:
                                    logger.error(
                                        f"Failed to send task acknowledgement: {e}"
                                    )
                            elif msg["type"] == "cancel_task" and self.connected:
                                logger.info(f"Received message: {msg}")
                                task_id = msg["task_id"]
                                if self.current_task and self.current_task == task_id:
                                    self._task_cancelled.set()
                                    logger.info("Task cancellation acknowledged")
                                else:
                                    self._cancel_task_in_queue(task_id)
                            elif msg["type"] == "connected":
                                self.connected = True
                                logger.info("Connected to server")
                            elif msg["type"] == "heartbeat":
                                self.heartbeat_messages.append(msg)
                                time_since_last_print = (
                                    datetime.datetime.now() - self.last_heartbeat_print
                                ).seconds

                                if time_since_last_print > 30:
                                    logger.info(
                                        f"Got {len(self.heartbeat_messages) - self.last_heartbeat_length}"
                                        " heartbeats in the last 30 seconds"
                                    )
                                    self.last_heartbeat_print = datetime.datetime.now()
                                    self.last_heartbeat_length = len(
                                        self.heartbeat_messages
                                    )

                        except IndexError:
                            logger.debug(f"Received non-event line: {line.decode()}")
                            self._stop_event.wait(5)
                            continue
                        except json.JSONDecodeError:
                            logger.warning(f"Received invalid JSON: {msg}")
                            self._stop_event.wait(5)
                            continue
            except requests.exceptions.ConnectionError as e:
                logger.info(
                    f"Failed to retrieve stream, likely because server is offline. "
                    f"Raw error: {e}"
                )
                self._stop_event.wait(5)
            except Exception as e:
                logger.error(f"Failed to receive message: {e}")
                continue
        logger.info("SSE thread stopped")

    def stop(self):
        if not self.working:
            logger.info("Peon already shutting down...")
        else:
            logger.info("Initiating shutdown...")
            self.working = False
            self._stop_event.set()
            self._task_cancelled.set()
            # Set a timeout for joining threads
            timeout = 5
            threads = [
                self._sse_thread,
                self._heartbeat_thread,
                self._processor_thread,
                self._statistics_thread,
            ]

            for thread in threads:
                thread.join(timeout=timeout)
                if thread.is_alive():
                    logger.warning(
                        f"Thread {thread.name} did not terminate within {timeout}s"
                    )

            # clean up the queue and set tasks back to PENDING

            while not self.queue.empty():
                try:
                    task = self.queue.get(timeout=1)
                    task.status = "PENDING"
                    task.peon_id = None

                    res = tenacious_request(
                        lambda: requests.post(
                            f"{self.workcraft.stronghold_url}/api/task/{task.id}/update",
                            headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                            json=Task.to_stronghold(task),
                        )
                    )

                    if 200 <= res.status_code < 300:
                        logger.info(f"Task {task.id} reset to PENDING")
                    else:
                        logger.error(f"Failed to reset task {task.id}: {res.text}")

                    self.queue.task_done()

                except Exception as e:
                    logger.error(f"Failed to reset task: {e}")

            logger.info("Stopped peon")
