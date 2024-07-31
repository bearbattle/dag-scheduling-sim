from collections.abc import Generator
import networkx as nx
import numpy as np

from .dag_subtask import Subtask


class DAGTask:

    def __init__(self, id_: int, subtasks: list[Subtask], dag: nx.DiGraph, t_arrival: float):
        # Identifier of this DAG Task
        self.id_ = id_
        # List of all subtask of this DAG Task
        self.subtasks = subtasks
        # List of all unassigned subtasks
        self.pending_subtasks = subtasks.copy()
        # List of all subtasks whose parents have been assigned
        self.frontier_subtasks: set[Subtask] = set()
        # Networkx DAG storing the subtask dependencies
        self.dag = dag
        # Time that this DAG Task arrived into the system
        self.t_arrival = t_arrival
        # Time that this DAG Task completed, i.e. when the last subtask completed
        self.t_completed = np.inf
        # Time that this DAG predicted time cost of executing this DAG Task
        self.t_predicted = np.inf
        # Set of nodes that are local to this DAG Task
        self.assigned_nodes: dict[int, int] = {}

    @property
    def pool_key(self) -> tuple[int, None]:
        return self.id_, None

    @property
    def fully_assigned(self) -> bool:
        return len(self.pending_subtasks) == 0

    @property
    def num_subtasks(self) -> int:
        return len(self.subtasks)

    @property
    def num_pending_subtasks(self) -> int:
        return len(self.pending_subtasks)

    def record_stage_completion(self, subtask: Subtask) -> bool:
        """increments the count of completed stages"""
        self.pending_subtasks.remove(subtask)
        self.frontier_subtasks.remove(subtask)

        new_subtasks = self._find_new_frontier_stages(subtask)
        self.frontier_subtasks |= new_subtasks

        return bool(new_subtasks)

    def get_children_stages(self, stage: Subtask) -> Generator[Subtask, None, None]:
        return (self.stages[stage_id] for stage_id in self.dag.successors(stage.id_))

    def get_parent_stages(self, stage: Subtask) -> Generator[Subtask, None, None]:
        return (self.stages[stage_id] for stage_id in self.dag.predecessors(stage.id_))

    def attach_node(self, subtask: Subtask, node: Node) -> None:
        # TODO)) Check if the node can satisfy the requirements of the subtask
        # TODO)) subtask.assign_node(node)
        # TODO)) self.pending_subtasks.remove(subtask)
        # self.assigned_nodes[subtask.id_] = node.id_
        # node.job_id = self.id_
        pass

    def detach_node(self, subtask: Subtask, node: Node) -> None:
        # TODO)) Release resources from node
        # TODO)) self.local_nodes.remove(node.id_)
        pass

    # internal methods

    def _init_frontier(self) -> None:
        """returns a set containing all the stages which are
        source nodes in the dag, i.e. which have no dependencies
        """
        assert not self.frontier_subtasks
        self.frontier_subtasks |= self._get_source_subtasks()

    def _check_dependencies(self, subtask_id: int) -> bool:
        """searches to see if all the dependencies of stage with id `stage_id` are satisfied."""
        for dep_id in self.dag.predecessors(subtask_id):
            if not self.subtasks[dep_id]:
                return False

        return True

    def _get_source_subtasks(self) -> set[Subtask]:
        return set(
            self.stages[node] for node, in_deg in self.dag.in_degree() if in_deg == 0
        )

    def _find_new_frontier_stages(self, stage: Subtask) -> set[Subtask]:
        """if ` stage` is completed, returns all of its successors whose other dependencies are also
        completed, if any exist.
        """
        if not stage.completed:
            return set()

        new_stages = set()
        # search through stage's children
        for suc_stage_id in self.dag.successors(stage.id_):
            # if all dependencies are satisfied, then add this child to the frontier
            new_stage = self.stages[suc_stage_id]
            if not new_stage.completed and self._check_dependencies(suc_stage_id):
                new_stages.add(new_stage)

        return new_stages
