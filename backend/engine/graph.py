"""工作流图 — DAG 的拓扑排序和 BFS 分层。

核心算法:
  - topological_sort: Kahn 算法 (BFS)，返回串行执行顺序
  - bfs_levels: BFS 分层，同层节点无依赖关系，可并行
"""

from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .node_factory import NodeFactory
    from nodes.base import Node


@dataclass(frozen=True, slots=True)
class Edge:
    id: str
    source: str
    target: str
    source_handle: str = "source"
    target_handle: str = "target"

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


class WorkflowGraph:
    """从 nodes dict + edges list 构建 DAG，提供拓扑排序和分层。"""

    def __init__(
        self,
        nodes: dict[str, "Node"],
        edges: list[Edge | dict[str, Any]],
    ) -> None:
        self.nodes = nodes
        self.edges = [edge if isinstance(edge, Edge) else Edge.from_config(edge, i) for i, edge in enumerate(edges)]
        self._adj: dict[str, list[str]] = {}
        self._in_degree: dict[str, int] = {}
        self._build()

    @classmethod
    def init(cls, graph_config: dict[str, Any], node_factory: "NodeFactory") -> "WorkflowGraph":
        """Parse workflow JSON into a validated static graph."""
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
        edges = [Edge.from_config(config, i) for i, config in enumerate(edges_config)]
        graph = cls(nodes, edges)
        graph.validate()
        return graph

    def _build(self) -> None:
        """构建邻接表和入度表"""
        self._adj = {nid: [] for nid in self.nodes}
        self._in_degree = {nid: 0 for nid in self.nodes}

        for edge in self.edges:
            src, tgt = edge.source, edge.target
            if src not in self._adj or tgt not in self._adj:
                raise ValueError(f"边引用了不存在的节点: {src} → {tgt}")
            self._adj[src].append(tgt)
            self._in_degree[tgt] += 1

    def validate(self) -> None:
        """验证 DAG 合法性: 节点角色、引用和环。"""
        if not self.nodes:
            raise ValueError("工作流至少需要一个节点")
        order = self.topological_sort()
        if len(order) != len(self.nodes):
            # 有环 — 找出剩余节点
            remaining = set(self.nodes.keys()) - set(order)
            raise ValueError(f"工作流存在循环依赖，以下节点无法排序: {remaining}")
        starts = [node for node in self.nodes.values() if node.node_type == "start"]
        if len(starts) != 1:
            raise ValueError("工作流必须且只能有一个 start 节点")
        if self._in_degree[starts[0].node_id] != 0:
            raise ValueError("start 节点不能有入边")

        reachable = {starts[0].node_id}
        for node_id in order:
            if node_id not in reachable:
                continue
            reachable.update(self._adj[node_id])
        unreachable = set(self.nodes) - reachable
        if unreachable:
            raise ValueError(f"存在从 start 不可达的节点: {sorted(unreachable)}")

    @property
    def root_node_id(self) -> str:
        return next(node.node_id for node in self.nodes.values() if node.node_type == "start")

    def topological_sort(self) -> list[str]:
        """Kahn 算法: 不断取入度为 0 的节点，返回串行执行顺序"""
        in_degree = dict(self._in_degree)
        queue = deque([nid for nid, d in in_degree.items() if d == 0])
        order: list[str] = []

        while queue:
            nid = queue.popleft()
            order.append(nid)
            for neighbor in self._adj[nid]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        return order

    def bfs_levels(self) -> list[list[str]]:
        """BFS 分层: 同层节点之间无直接依赖，可并行执行。

        Example:
            A → B → D
            A → C → D

            第0层: [A]      (入度为0)
            第1层: [B, C]   (只依赖A，彼此无依赖)
            第2层: [D]      (依赖B和C)
        """
        remaining_in = dict(self._in_degree)
        current_level = deque([nid for nid, d in remaining_in.items() if d == 0])
        levels: list[list[str]] = []

        while current_level:
            levels.append(list(current_level))
            next_level: deque[str] = deque()

            for nid in current_level:
                for neighbor in self._adj[nid]:
                    remaining_in[neighbor] -= 1
                    if remaining_in[neighbor] == 0:
                        next_level.append(neighbor)

            current_level = next_level

        # 检查是否有环
        total = sum(len(lv) for lv in levels)
        if total != len(self.nodes):
            remaining = set(self.nodes.keys()) - {n for lv in levels for n in lv}
            raise ValueError(f"工作流存在循环依赖: {remaining}")

        return levels
