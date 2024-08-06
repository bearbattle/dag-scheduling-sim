from .dag_task import DAGTask
from .dag_subtask import Subtask
from .event import Event

EType = Event.Type


class Node:
    def __init__(self, id_: int, resource: list[int]) -> None:
        # index of this operation within its operation
        self.id_ = id_

        # Resource provided by this node
        self.resource = resource

        # Subtasks that this node is assigned
        self.subtasks: list[Subtask] = []

        # ids of current subtasks that this executor is local to, if any
        self.subtask_ids: list[int] = []

        # Tasks that this node is assigned by subtask
        self.dags: list[DAGTask] = []

        # ids of current DAGTask that this executor is local to, if any
        self.dag_ids: list[int] = []

        # list of pairs [t, job_id], where `t` is the wall time that this executor
        # was released from job with id `job_id`, or `None` if it has not been released
        # yet. `job_id` is -1 if the executor is at the general pool.
        # NOTE: only used for rendering
        self.history: list[list] = [[None, -1, -1, None]]

    @property
    def is_idle(self) -> bool:
        return len(self.subtasks) == 0

    @property
    def get_dag_count(self) -> int:
        return len(self.dag_ids)

    @property
    def get_subtask_count(self) -> int:
        return len(self.subtask_ids)

    def check_add_subtask(self, subtask: Subtask) -> bool:
        if subtask.id_ in self.subtask_ids:
            return True
        if subtask.resource_req <= self.resource:
            return True
        return False

    def add_subtask(self, wall_time: float, subtask: Subtask) -> bool:
        if not self.check_add_subtask(subtask):
            return False
        if subtask.id_ in self.subtask_ids:
            return True
        self.resource -= subtask.resource_req
        self.subtasks.append(subtask)
        self.subtask_ids.append(subtask.id_)
        if subtask.dag_id not in self.dag_ids:
            self.dag_ids.append(subtask.dag_id)
        self.add_history(wall_time, subtask.id_, subtask.dag_id, EType.DAG_SUBTASK_ASSIGNED)
        return True

    def remove_subtask(self, wall_time: float, subtask: Subtask) -> None:
        self.resource += subtask.resource_req
        self.subtasks.remove(subtask)
        self.subtask_ids.remove(subtask.id_)
        # Remove DAGTask if no subtask of this DAG is assigned to this node
        if all([subtask.dag_id != task.id_ for task in self.subtasks]):
            self.dag_ids.remove(subtask.dag_id)
        self.add_history(wall_time, subtask.id_, subtask.dag_id, EType.DAG_SUBTASK_RESIGNED)

    def is_at_task(self, task_id: int) -> bool:
        return task_id in self.dag_ids

    def is_at_subtask(self, subtask_id: int) -> bool:
        return subtask_id in self.subtask_ids

    def add_history(self, wall_time: float, subtask_id: int, task_id: int, event_type: EType) -> None:
        """should be called whenever this executor is released from a job"""
        if self.history is None:
            self.history = []

        if len(self.history) > 0:
            # add release time to most recent history
            self.history[-1][0] = wall_time

        # add new history
        self.history += [[None, subtask_id, task_id, event_type]]
