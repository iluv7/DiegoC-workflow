"""Workflow JSON parsing, graph validation, and runtime edge-state helpers."""

from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from nodes.base import Node

    from .node_factory import NodeFactory


class EdgeState(str, Enum):
    UNKNOWN = "unknown"
    TAKEN = "taken"
    SKIPPED = "skipped"


@dataclass(slots=True)
class Edge:
    """Parsed edge plus the state it acquires during one graph execution."""

    id: str
    source: str
    target: str
    source_handle: str = "source"
    target_handle: str = "target"
    state: EdgeState = EdgeState.UNKNOWN

    @classmethod
    def from_config(cls, config: dict[str, Any], index: int) -> "Edge":
        if not isinstance(config, dict):
            raise ValueError("边配置必须是对象")
        source = config.get("source")
        target = config.get("target")
        if not isinstance(source, str) or not source or not isinstance(target, str) or not target:
            raise ValueError("每条边都必须有非空的 source 和 target")
        return cls(
            id=str(config.get("id") or f"{source}-{target}-{index}"),
            source=source,
            target=target,
            source_handle=str(config.get("sourceHandle") or "source"),
            target_handle=str(config.get("targetHandle") or "target"),
        )


RuntimeEdge = Edge


class WorkflowGraph:
    """A validated per-run DAG built from concrete nodes and parsed edges."""

    def __init__(self, nodes: dict[str, "Node"], edges: list[Edge | dict[str, Any]]) -> None:
        self.nodes = nodes
        self.edges = [
            edge if isinstance(edge, Edge) else Edge.from_config(edge, index)
            for index, edge in enumerate(edges)
        ]
        self._outgoing: dict[str, list[Edge]] = {node_id: [] for node_id in nodes}
        self._incoming: dict[str, list[Edge]] = {node_id: [] for node_id in nodes}
        self._build()

    @classmethod
    def init(cls, graph_config: dict[str, Any], node_factory: "NodeFactory") -> "WorkflowGraph":
        """Turn JSON-compatible node and edge dictionaries into graph objects."""
        if not isinstance(graph_config, dict):
            raise ValueError("graph_config 必须是对象")
        nodes_config = graph_config.get("nodes", [])
        edges_config = graph_config.get("edges", [])
        if not isinstance(nodes_config, list) or not isinstance(edges_config, list):
            raise ValueError("graph_config.nodes 和 graph_config.edges 必须是数组")

        nodes: dict[str, Node] = {}
        for node_config in nodes_config:
            node = node_factory.create_node(node_config)
            if node.node_id in nodes:
                raise ValueError(f"节点 ID 不能重复: {node.node_id}")
            nodes[node.node_id] = node

        graph = cls(nodes, [Edge.from_config(config, i) for i, config in enumerate(edges_config)])
        graph.validate()
        return graph

    def _build(self) -> None:
        seen: set[tuple[str, str, str, str]] = set()
        for edge in self.edges:
            if edge.source not in self.nodes or edge.target not in self.nodes:
                raise ValueError(f"边引用了不存在的节点: {edge.source} → {edge.target}")
            key = (edge.source, edge.target, edge.source_handle, edge.target_handle)
            if key in seen:
                raise ValueError(f"存在重复边: {edge.source} → {edge.target} ({edge.source_handle})")
            seen.add(key)
            self._outgoing[edge.source].append(edge)
            self._incoming[edge.target].append(edge)

    def validate(self) -> None:
        if not self.nodes:
            raise ValueError("工作流至少需要一个节点")
        order = self.topological_sort()
        if len(order) != len(self.nodes):
            remaining = set(self.nodes) - set(order)
            raise ValueError(f"工作流存在循环依赖: {remaining}")

        starts = [node_id for node_id, node in self.nodes.items() if node.node_type == "start"]
        ends = [node_id for node_id, node in self.nodes.items() if node.node_type == "end"]
        if len(starts) != 1:
            raise ValueError("工作流必须且只能有一个 start 节点")
        if not ends:
            raise ValueError("工作流至少需要一个 end 节点")
        if self._incoming[starts[0]]:
            raise ValueError("start 节点不能有入边")
        for node_id in ends:
            if self._outgoing[node_id]:
                raise ValueError(f"end 节点不能有出边: {node_id}")

        reachable = {starts[0]}
        for node_id in order:
            if node_id in reachable:
                reachable.update(edge.target for edge in self._outgoing[node_id])
        unreachable = set(self.nodes) - reachable
        if unreachable:
            raise ValueError(f"存在从 start 不可达的节点: {sorted(unreachable)}")

        ancestors: dict[str, set[str]] = {node_id: set() for node_id in self.nodes}
        for node_id in order:
            for edge in self._outgoing[node_id]:
                ancestors[edge.target].update(ancestors[node_id] | {node_id})
        for node_id, node in self.nodes.items():
            errors = node.validate_config()
            if errors:
                raise ValueError(f"节点 {node_id} 配置错误: {'; '.join(errors)}")
            for ref in node.input_mapping.values():
                if "." not in ref:
                    raise ValueError(f"节点 {node_id} 的变量引用格式错误: {ref}")
                upstream = ref.split(".", 1)[0]
                if upstream in {"sys", "env"}:
                    continue
                if upstream not in self.nodes:
                    raise ValueError(f"节点 {node_id} 引用了不存在的节点: {upstream}")
                if upstream not in ancestors[node_id]:
                    raise ValueError(f"节点 {node_id} 只能引用拓扑上游节点: {upstream}")

    @property
    def root_node_id(self) -> str:
        return next(node_id for node_id, node in self.nodes.items() if node.node_type == "start")

    def topological_sort(self) -> list[str]:
        indegree = {node_id: len(edges) for node_id, edges in self._incoming.items()}
        queue = deque(node_id for node_id, degree in indegree.items() if degree == 0)
        order: list[str] = []
        while queue:
            node_id = queue.popleft()
            order.append(node_id)
            for edge in self._outgoing[node_id]:
                indegree[edge.target] -= 1
                if indegree[edge.target] == 0:
                    queue.append(edge.target)
        return order

    def roots(self) -> list[str]:
        return [node_id for node_id, edges in self._incoming.items() if not edges]

    def incoming(self, node_id: str) -> list[Edge]:
        return self._incoming[node_id]

    def outgoing(self, node_id: str) -> list[Edge]:
        return self._outgoing[node_id]

    def resolve_outgoing(self, node_id: str, selected_handle: str | None) -> set[str]:
        """Resolve outgoing states and return affected target ids."""
        affected: set[str] = set()
        for edge in self.outgoing(node_id):
            if selected_handle is None:
                edge.state = EdgeState.SKIPPED if edge.source_handle == "fail-branch" else EdgeState.TAKEN
            else:
                edge.state = EdgeState.TAKEN if edge.source_handle == selected_handle else EdgeState.SKIPPED
            affected.add(edge.target)
        return affected

    def readiness(self, node_id: str) -> str:
        """Return ready, wait, or skip according to three-state incoming edges."""
        edges = self.incoming(node_id)
        if not edges:
            return "ready"
        if any(edge.state == EdgeState.UNKNOWN for edge in edges):
            return "wait"
        if any(edge.state == EdgeState.TAKEN for edge in edges):
            return "ready"
        return "skip"

    def bfs_levels(self) -> list[list[str]]:
        """Return static levels for compatibility and graph visualization."""
        indegree = {node_id: len(edges) for node_id, edges in self._incoming.items()}
        current = deque(node_id for node_id, degree in indegree.items() if degree == 0)
        levels: list[list[str]] = []
        while current:
            levels.append(list(current))
            nxt: deque[str] = deque()
            for node_id in current:
                for edge in self._outgoing[node_id]:
                    indegree[edge.target] -= 1
                    if indegree[edge.target] == 0:
                        nxt.append(edge.target)
            current = nxt
        return levels
