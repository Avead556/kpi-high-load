class TaskNotFound(Exception):
    def __init__(self, task_id: int):
        super().__init__(f"task {task_id} not found")
        self.task_id = task_id


class TaskFinished(Exception):
    def __init__(self, task_id: int, status: str):
        super().__init__(f"task {task_id} is already {status}")
        self.task_id = task_id
        self.status = status
