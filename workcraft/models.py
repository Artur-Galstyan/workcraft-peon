import json
import uuid
from collections.abc import Callable
from datetime import datetime
from typing import ClassVar

import requests
from beartype.typing import Any, Literal, Protocol
from loguru import logger
from pydantic import BaseModel, Field


class Separators(BaseModel):
    WORKCRAFT_SSE_SEPARATOR_START: ClassVar = "###WORKCRAFT_MESSAGE_START###"
    WORKCRAFT_SSE_SEPARATOR_END: ClassVar = "###WORKCRAFT_MESSAGE_END###"


class WebsocketMessage(BaseModel):
    type: str
    payload: Any


class TaskPayload(BaseModel):
    task_args: list = Field(default_factory=list)
    task_kwargs: dict = Field(default_factory=dict)
    prerun_handler_args: list = Field(default_factory=list)
    prerun_handler_kwargs: dict = Field(default_factory=dict)
    postrun_handler_args: list = Field(default_factory=list)
    postrun_handler_kwargs: dict = Field(default_factory=dict)


class Task(BaseModel):
    id: str
    task_name: str
    status: Literal[
        "PENDING", "RUNNING", "SUCCESS", "FAILURE", "INVALID", "CANCELLED"
    ] = Field(default="PENDING")
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    peon_id: str | None = None
    queue: str = Field(default="DEFAULT")
    payload: TaskPayload
    result: Any | None = None
    retry_on_failure: bool = Field(default=False)
    retry_count: int = Field(default=0)
    retry_limit: int = Field(default=3)

    @classmethod
    def to_stronghold(cls, task: "Task") -> dict:
        """Converts a Task instance to a dictionary for Stronghold."""
        return {
            "id": task.id,
            "task_name": task.task_name,
            "status": task.status,
            "created_at": task.created_at.isoformat() + "Z",
            "updated_at": task.updated_at.isoformat() + "Z",
            "peon_id": task.peon_id,
            "queue": task.queue,
            "payload": task.payload.model_dump_json(),
            "result": json.dumps(task.result),
            "retry_on_failure": task.retry_on_failure,
            "retry_count": task.retry_count,
            "retry_limit": task.retry_limit,
        }


class SetupHandlerFn(Protocol):
    def __call__(self): ...


class TaskHandlerFn(Protocol):
    def __call__(
        self,
        task_id: str,
        *args,
        **kwargs,
    ) -> Any: ...


class PostRunHandlerFn(Protocol):
    def __call__(
        self,
        task_id: str,
        task_name: str,
        result: Any,
        status: Literal["FAILURE", "SUCCESS", "RUNNING", "PENDING"],
        *args,
        **kwargs,
    ): ...


class PreRunHandlerFn(Protocol):
    def __call__(
        self,
        task_id: str,
        task_name: str,
        *args,
        **kwargs,
    ): ...


class Workcraft:
    """Workcraft: A simple distributed task system."""

    def __init__(self, stronghold_url: str, api_key: str):
        self.stronghold_url = stronghold_url
        self.api_key = api_key

        self.tasks: dict[str, Callable] = {}
        self.setup_handler_fn: Callable | None = None
        self.prerun_handler_fn: Callable | None = None
        self.postrun_handler_fn: Callable | None = None

    def task(self, name: str):
        def decorator(func: TaskHandlerFn):
            self.tasks[name] = func
            return func

        return decorator

    def prerun_handler(self):
        def decorator(func: PreRunHandlerFn):
            self.prerun_handler_fn = func
            return func

        return decorator

    def postrun_handler(self):
        def decorator(func: PostRunHandlerFn):
            self.postrun_handler_fn = func
            return func

        return decorator

    def setup_handler(self):
        def decorator(func: SetupHandlerFn):
            self.setup_handler_fn = func
            return func

        return decorator

    def execute(self, task: Task) -> Task:
        if self.prerun_handler_fn:
            try:
                self.prerun_handler_fn(
                    task.id,
                    task.task_name,
                    *task.payload.prerun_handler_args,
                    **task.payload.prerun_handler_kwargs,
                )
            except Exception as e:
                import traceback

                print(traceback.format_exc())
                logger.warning(f"Failed to execute prerun handler: {e}")
                logger.warning(
                    f"Task {task.id} will still be executed, "
                    "but the prerun handler failed. This may cause issues "
                    "with the task execution. Please check the logs."
                )
        try:
            task_fn = self.tasks[task.task_name]
            result = task_fn(
                task.id, *task.payload.task_args, **task.payload.task_kwargs
            )
            task.result = json.dumps(result)
            task.status = "SUCCESS"

        except Exception as e:
            import traceback

            print(traceback.format_exc())
            logger.error(f"Failed to execute task: {e}")
            task.status = "FAILURE"
            task.result = str(e)
        finally:
            task.updated_at = datetime.now()
            if self.postrun_handler_fn:
                try:
                    self.postrun_handler_fn(
                        task.id,
                        task.task_name,
                        task.result,
                        task.status,
                        *task.payload.postrun_handler_args,
                        **task.payload.postrun_handler_kwargs,
                    )
                except Exception as e:
                    import traceback

                    print(traceback.format_exc())
                    logger.warning(f"Failed to execute postrun handler: {e}")
                    logger.warning(
                        f"Task {task.id} was executed successfully, "
                        "but the postrun handler failed. Please check the logs."
                    )
            return task

    def send_task_sync(
        self,
        payload: TaskPayload,
        task_name: str,
        queue: str = "DEFAULT",
        retry_on_failure: bool = False,
        retry_limit: int = 3,
    ) -> str:
        id = str(uuid.uuid4())
        try:
            task = {
                "id": id,
                "task_name": task_name,
                "payload": payload.model_dump(),
                "queue": queue,
                "retry_on_failure": retry_on_failure,
                "retry_limit": retry_limit,
            }
            requests.post(
                f"{self.stronghold_url}/api/task",
                json=task,
                headers={
                    "Content-Type": "application/json",
                    "WORKCRAFT_API_KEY": self.api_key,
                },
            )
        except Exception as e:
            logger.error(f"Failed to send task: {e}")
            raise e
        return id


class State(BaseModel):
    id: str
    status: Literal["IDLE", "PREPARING", "WORKING", "OFFLINE", "PREPARING"]
    current_task: str | None = None
    queues: list[str] | None = None
    last_heartbeat: datetime = Field(default_factory=datetime.now)

    def update_and_return(self, **kwargs) -> "State":
        for key, value in kwargs.items():
            setattr(self, key, value)
        setattr(self, "last_heartbeat", datetime.now())
        return self

    def queue_to_stronghold(self) -> str:
        if self.queues is None:
            return "[]"
        return "['" + "','".join(self.queues) + "']"

    def to_stronghold(self) -> dict:
        queues = None
        if self.queues is not None:
            queues = "['" + "','".join(self.queues) + "']"

        update_json = {
            "current_task": self.current_task if self.current_task else None,
            "current_task_set": bool(self.current_task),
            "status": self.status,
            "status_set": True,
            "last_heartbeat": self.last_heartbeat.isoformat(),
            "last_heartbeat_set": True,
            "queues": queues,
            "queues_set": bool(self.queues),
            "id": self.id,
        }
        return update_json
