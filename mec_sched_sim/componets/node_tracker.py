class NodeTracker:
    """
    Maintains all SubTask-Task-Node assignment. These include:
        - current load of all nodes
        - commitment of task assigning and resigning, and
        - further information about the nodes, like multi-instance of a task on different/same nodes.
    """
    def __init__(self, num_nodes: int) -> None:
        self._m_node_dags = None
        self._m_node_subtasks = None
        self.num_nodes = num_nodes

    def reset(self) -> None:
        self._m_node_dags: dict[int, list[int]] = {
            node_id: [] for node_id in range(self.num_nodes)
        }

        self._m_node_subtasks: dict[int, list[int]] = {
            node_id: [] for node_id in range(self.num_nodes)
        }