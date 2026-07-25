"""工作流图 — DAG 的拓扑排序和 BFS 分层。

核心算法:
  - topological_sort: Kahn 算法 (BFS)，返回串行执行顺序
  - bfs_levels: BFS 分层，同层节点无依赖关系，可并行
"""

from collections import deque
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from nodes.base import Node


class WorkflowGraph:
    """从 nodes dict + edges list 构建 DAG，提供拓扑排序和分层。"""

    def __init__(
        self,
        nodes: dict[str, "Node"],
        edges: list[dict[str, str]],
    ) -> None:
        self.nodes = nodes
        self.edges = edges
        self._adj: dict[str, list[str]] = {}
        self._in_degree: dict[str, int] = {}
        self._build()

    def _build(self) -> None:
        """构建邻接表和入度表"""
        self._adj = {nid: [] for nid in self.nodes}
        self._in_degree = {nid: 0 for nid in self.nodes}

        for edge in self.edges:
            src, tgt = edge["source"], edge["target"]
            if src not in self._adj or tgt not in self._adj:
                raise ValueError(f"边引用了不存在的节点: {src} → {tgt}")
            self._adj[src].append(tgt)
            self._in_degree[tgt] += 1

    def validate(self) -> None:
        """验证 DAG 合法性: 检测环、检测孤立节点"""
        order = self.topological_sort()
        if len(order) != len(self.nodes):
            # 有环 — 找出剩余节点
            remaining = set(self.nodes.keys()) - set(order)
            raise ValueError(f"工作流存在循环依赖，以下节点无法排序: {remaining}")

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
