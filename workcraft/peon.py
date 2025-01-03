import json
import threading
import uuid
from queue import Empty, Queue

import requests
from loguru import logger

from workcraft.models import State, Task, Workcraft


class Peon:
    def __init__(
        self,
        workcraft: Workcraft,
        id: str | None = None,
        queues: list[str] | None = None,
    ) -> None:
        self.id = id or uuid.uuid4().hex
        self.state = State(id=self.id, status="IDLE", queues=queues)

        self.workcraft = workcraft
        self.seen_tasks_in_memory = set()

        self.working = True
        self.queue = Queue()
        self.connected = False
        self._sse_thread = threading.Thread(target=self._run_sse, daemon=True)
        self._heartbeat_thread = threading.Thread(target=self._heartbeat, daemon=True)
        self._processor_thread = threading.Thread(target=self._process, daemon=True)
        self._statistics_thread = threading.Thread(target=self._statistics, daemon=True)

        self._stop_event = threading.Event()
        self._task_cancelled = threading.Event()

    def _update_and_send_state(self, **kwargs) -> None:
        if self.connected:
            try:
                self.state = self.state.update_and_return(**kwargs)
                res = requests.post(
                    self.workcraft.stronghold_url + f"/api/peon/{self.id}/update",
                    headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                    json=self.state.to_stronghold(),
                )

                if 200 <= res.status_code < 300:
                    pass
                    # logger.info(f"Peon {self.id} updated successfully")
                else:
                    logger.error(
                        f"Failed to update peon: {res.status_code} - {res.text}"
                    )
            except Exception as e:
                logger.error(f"Failed to send peon update: {e}")

    def work(self) -> None:
        """
        Main work method that starts all worker threads and maintains the main loop.
        """

        try:
            logger.info("Starting peon...")

            # Start all threads
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

            # Main loop with proper signal handling
            while not self._stop_event.is_set():
                try:
                    for _ in range(10):  # 10 * 0.1 = 1 second total
                        if self._stop_event.is_set():
                            break
                        self._stop_event.wait(0.1)
                except KeyboardInterrupt:
                    logger.info("Received keyboard interrupt in main thread")
                    break

        except Exception as e:
            logger.error(f"Error in main work loop: {e}")

    def _cancel_task_in_queue(self, task_id: str) -> None:
        # Remove task from queue
        for i in range(self.queue.qsize()):
            task = self.queue.get()
            if task.id != task_id:
                self.queue.put(task)
            else:
                logger.info(f"Removing task {task_id} from queue")
                task.status = "CANCELLED"

                res = requests.post(
                    f"{self.workcraft.stronghold_url}/api/task/{task.id}/update",
                    headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                    json=Task.to_stronghold(task),
                )

                if 200 <= res.status_code < 300:
                    logger.info(f"Task {task_id} removed from queue")
                else:
                    logger.error(f"Failed to remove task from queue: {res.text}")

                self.queue.task_done()
                break
        logger.info(f"Task {task_id} removed from queue")

    def _statistics(self) -> None:
        while self.working and not self._stop_event.is_set():
            try:
                res = requests.post(
                    self.workcraft.stronghold_url + f"/api/peon/{self.id}/statistics",
                    json={
                        "type": "queue",
                        "value": {
                            "size": self.queue.qsize(),
                        },
                        "peon_id": self.id,
                    },
                    headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
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
                if self.state.current_task:
                    self.state = self.state.update_and_return(
                        current_task=None, status="IDLE"
                    )
                    self._update_and_send_state()
                continue

            try:
                # logger.info(f"Processing task {task}, type: {type(task)}")
                self._update_and_send_state(current_task=task.id, status="WORKING")

                res = requests.post(
                    f"{self.workcraft.stronghold_url}/api/task/{task.id}/update",
                    headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                    json={"status": "RUNNING"},
                )

                if 200 <= res.status_code < 300:
                    logger.info(f"Task {task.id} set to RUNNING")
                else:
                    raise Exception(f"Failed to set task to RUNNING: {res.text}")

                self._task_cancelled.clear()
                result_queue = Queue()

                def execute_task(_task):
                    try:
                        result = self.workcraft.execute(_task)
                        result_queue.put(result)
                    except Exception as e:
                        result_queue.put(e)

                task_thread = threading.Thread(target=execute_task, args=(task,))
                task_thread.start()

                # Monitor for cancellation or completion
                cancelled = False
                while task_thread.is_alive():
                    if self._task_cancelled.is_set() or self._stop_event.is_set():
                        if self._task_cancelled.is_set():
                            logger.info("Task cancellation requested")
                            task.status = "CANCELLED"
                        task_thread.join(timeout=5)
                        if task_thread.is_alive():
                            logger.warning("Task did not stop gracefully")
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

                # Always try to send the final status, even if cancelled
                try:
                    res = requests.post(
                        f"{self.workcraft.stronghold_url}/api/task/{task.id}/update",
                        headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                        json=Task.to_stronghold(updated_task),
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
                self._update_and_send_state(current_task=None, status="IDLE")
                self.seen_tasks_in_memory.remove(task.id)
                self.queue.task_done()

            # Break the loop if we're stopping
            if self._stop_event.is_set():
                logger.info("Stopping processor thread")
                break

    def _heartbeat(self) -> None:
        while self.working and not self._stop_event.is_set():
            try:
                self._update_and_send_state()
            except Exception as e:
                logger.error(f"Failed to send ping: {e}")
            self._stop_event.wait(5)  # Replace sleep with event wait

    def _run_sse(self):
        logger.info("Starting SSE thread")
        retry_delay = 1
        max_retry_delay = 60

        while self.working and not self._stop_event.is_set():
            try:
                logger.info(f"Attempting connection to {self.workcraft.stronghold_url}")
                response = requests.get(
                    f"{self.workcraft.stronghold_url}/events?type=peon&peon_id={self.id}&queues={self.state.queue_to_stronghold()}",
                    stream=True,
                    headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                    timeout=30,
                )

                if response.status_code != 200:
                    logger.error(f"Failed to connect to server: {response.text}")
                    self._handle_connection_failure(retry_delay)
                    retry_delay = min(
                        retry_delay * 2, max_retry_delay
                    )  # Exponential backoff
                    continue

                retry_delay = 5
                self.connected = False

                # Process the stream
                for line in response.iter_lines(decode_unicode=True):
                    if self._stop_event.is_set():
                        logger.info("Stop event detected during SSE processing")
                        return

                    if not line:
                        continue

                    if not line.startswith("data:"):
                        continue

                    try:
                        msg = json.loads(line[5:])  # Skip 'data:' prefix
                        logger.info(f"Received message: {msg}")

                        if msg["type"] == "new_task":
                            self._handle_new_task(msg)
                        elif msg["type"] == "cancel_task":
                            self._handle_cancel_task(msg)
                        elif msg["type"] == "connected":
                            self.connected = True
                            logger.info("Connected to server")

                    except json.JSONDecodeError:
                        logger.warning(f"Received invalid JSON: {line}")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        continue

            except requests.exceptions.ConnectionError as e:
                logger.error(f"Connection error: {e}")
                self._handle_connection_failure(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)
            except requests.exceptions.Timeout:
                logger.error("Connection timed out")
                self._handle_connection_failure(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)
            except Exception as e:
                logger.error(f"Unexpected error in SSE thread: {e}")
                self._handle_connection_failure(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)

        logger.info("SSE thread stopped")

    def stop(self):
        if not self.working:
            logger.info("Peon already shutting down...")
        else:
            logger.info("Initiating shutdown...")
            self.working = False
            self._stop_event.set()
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

            def _reset_task_to_pending(task_id, json_data):
                try:
                    res = requests.post(
                        f"{self.workcraft.stronghold_url}/api/task/{task_id}/update",
                        headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                        json=json_data,
                    )

                    if 200 <= res.status_code < 300:
                        logger.info(f"Task {task_id} reset to PENDING")
                    else:
                        logger.error(f"Failed to reset task {task_id}: {res.text}")
                except Exception as e:
                    logger.error(f"Failed to reset task: {e}")

            if self.state.current_task:
                self._update_and_send_state(current_task=None)
                # set task back to pending
                _reset_task_to_pending(
                    self.state.current_task, {"status": "PENDING", "peon_id": None}
                )

            # clean up the queue and set tasks back to PENDING

            while not self.queue.empty():
                try:
                    task = self.queue.get(timeout=1)
                    task.status = "PENDING"
                    task.peon_id = None

                    _reset_task_to_pending(task.id, Task.to_stronghold(task))

                    self.queue.task_done()

                except Exception as e:
                    logger.error(f"Failed to reset task: {e}")

            logger.info("Stopped peon")

    def _handle_connection_failure(self, delay):
        """Handle connection failures with appropriate delay and state updates."""
        self.connected = False
        logger.info(f"Connection lost. Retrying in {delay} seconds...")
        self._stop_event.wait(delay)

    def _handle_new_task(self, msg):
        """Handle new task messages."""
        try:
            task = Task.model_validate(msg["data"])
        except Exception as e:
            self._handle_invalid_task(msg, e)
            return

        if task.id in self.seen_tasks_in_memory:
            logger.info(f"Task {task.id} already seen, skipping")
            return

        self._process_new_task(task)

    def _handle_invalid_task(self, msg, error):
        """Handle invalid task messages."""
        logger.error(
            f"Failed to validate task: {error}, likely malformed. Setting task to INVALID"
        )
        try:
            task_id = msg["data"]["id"]
            if not task_id:
                logger.error("Task ID is missing")
                return

            res = requests.post(
                f"{self.workcraft.stronghold_url}/api/task/{task_id}/update",
                headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                json={
                    "status": "INVALID",
                    "result": f"Task is invalid: {error}",
                },
            )

            if 200 <= res.status_code < 300:
                logger.info(f"Task {task_id} set to INVALID")
            else:
                logger.error(f"Failed to set task to INVALID: {res.text}")
        except Exception as e:
            logger.error(f"Error handling invalid task: {e}")

    def _process_new_task(self, task):
        """Process a new valid task."""
        task.peon_id = self.id
        self.queue.put(task)
        self.seen_tasks_in_memory.add(task.id)

        try:
            res = requests.post(
                f"{self.workcraft.stronghold_url}/api/task/{task.id}/update",
                headers={"WORKCRAFT_API_KEY": self.workcraft.api_key},
                json={
                    "peon_id": self.id,
                    "status": "ACKNOWLEDGED",
                },
            )

            if 200 <= res.status_code < 300:
                logger.info("Task acknowledgement sent successfully")
            else:
                logger.error(f"Failed to send task acknowledgement: {res.text}")
        except Exception as e:
            logger.error(f"Failed to send task acknowledgement: {e}")

    def _handle_cancel_task(self, msg):
        """Handle cancel task messages."""
        task_id = msg["data"]
        if self.state.current_task and self.state.current_task == task_id:
            self._task_cancelled.set()
            logger.info("Task cancellation acknowledged")
        else:
            self._cancel_task_in_queue(task_id)
