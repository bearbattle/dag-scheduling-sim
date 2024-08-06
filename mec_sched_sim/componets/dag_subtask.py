from dataclasses import dataclass

from .node import Node
from .resource import ResourceList

import numpy as np


@dataclass
class Subtask:
    id_: int
    dag_id: int
    node_id: int | None = None
    t_assigned: float = np.inf
    t_completed: float = np.inf

    # resource_req: tuple[int, int] = (CPU, RAM)
    # Can extend this to include other resources
    resource_req: ResourceList = None

    @property
    def __unique_id(self) -> tuple[int, int]:
        return self.dag_id, self.id_

    @property
    def assigned(self) -> bool:
        return self.node_id is not None

    def __hash__(self) -> int:
        return hash(self.__unique_id)

    def __eq__(self, other) -> bool:
        if type(other) is type(self):
            return self.__unique_id == other.__unique_id
        else:
            return False

    def assign_node(self, node: Node, t: float) -> None:
        if self.assigned:
            raise ValueError("Subtask already assigned to a node")
        if not node.add_subtask(t, self):
            raise ValueError("Node cannot accept subtask")
        self.node_id = node.id_
        self.t_assigned = t
