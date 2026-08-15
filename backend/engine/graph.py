"""Workflow graph validation and runtime edge-state helpers."""

from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nodes.base import Node


class EdgeState(str, Enum):
    UNKNOWN = "unknown"
    TAKEN = "taken"
    SKIPPED = "skipped"


@dataclass(slots=True)
class RuntimeEdge:
    source: str
    target: str
    source_handle: str = "source"
    target_handle: str = "target"
    state: EdgeState = EdgeState.UNKNOWN


class WorkflowGraph:
    """Validated DAG with per-run, three-state edges."""

    def __init__(self, nodes: dict[str, "Node"], edges: list[dict[str, str]]) -> None:
        self.nodes = nodes
        self.edges = [
            RuntimeEdge(
                source=e["source"],
                target=e["target"],
                source_handle=e.get("sourceHandle") or "source",
                target_handle=e.get("targetHandle") or "target",
            )
            for e in edges
        ]
        self._outgoing = {nid: [] for nid in nodes}
        self._incoming = {nid: [] for nid in nodes}
        self._build()

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
        starts = [nid for nid, n in self.nodes.items() if n.node_type == "start"]
        ends = [nid for nid, n in self.nodes.items() if n.node_type == "end"]
        if len(starts) != 1:
            raise ValueError("工作流必须且只能有一个 start 节点")
        if not ends:
            raise ValueError("工作流至少需要一个 end 节点")
        if self._incoming[starts[0]]:
            raise ValueError("start 节点不能有入边")
        for nid in ends:
            if self._outgoing[nid]:
                raise ValueError(f"end 节点不能有出边: {nid}")
        ancestors: dict[str, set[str]] = {nid: set() for nid in self.nodes}
        for nid in order:
            for edge in self._outgoing[nid]:
                ancestors[edge.target].update(ancestors[nid] | {nid})
        for nid, node in self.nodes.items():
            errors = node.validate_config()
            if errors:
                raise ValueError(f"节点 {nid} 配置错误: {'; '.join(errors)}")
            for ref in node.input_mapping.values():
                if "." not in ref:
                    raise ValueError(f"节点 {nid} 的变量引用格式错误: {ref}")
                upstream = ref.split(".", 1)[0]
                if upstream not in self.nodes:
                    raise ValueError(f"节点 {nid} 引用了不存在的节点: {upstream}")
                if upstream not in ancestors[nid]:
                    raise ValueError(f"节点 {nid} 只能引用拓扑上游节点: {upstream}")

    def topological_sort(self) -> list[str]:
        indegree = {nid: len(edges) for nid, edges in self._incoming.items()}
        queue = deque(nid for nid, degree in indegree.items() if degree == 0)
        order: list[str] = []
        while queue:
            nid = queue.popleft()
            order.append(nid)
            for edge in self._outgoing[nid]:
                indegree[edge.target] -= 1
                if indegree[edge.target] == 0:
                    queue.append(edge.target)
        return order

    def roots(self) -> list[str]:
        return [nid for nid, edges in self._incoming.items() if not edges]

    def incoming(self, node_id: str) -> list[RuntimeEdge]:
        return self._incoming[node_id]

    def outgoing(self, node_id: str) -> list[RuntimeEdge]:
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

    # Kept for compatibility and documentation examples.
    def bfs_levels(self) -> list[list[str]]:
        indegree = {nid: len(edges) for nid, edges in self._incoming.items()}
        current = deque(nid for nid, degree in indegree.items() if degree == 0)
        levels: list[list[str]] = []
        while current:
            levels.append(list(current))
            nxt: deque[str] = deque()
            for nid in current:
                for edge in self._outgoing[nid]:
                    indegree[edge.target] -= 1
                    if indegree[edge.target] == 0:
                        nxt.append(edge.target)
            current = nxt
        return levels
