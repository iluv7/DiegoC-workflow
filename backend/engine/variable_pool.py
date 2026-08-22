"""变量池 — 节点间数据传递的共享空间。

每个节点执行完后将输出写入 pool，下游节点通过 input_mapping
从 pool 中取上游值。数据流:  node output → pool[node_id] → downstream input
"""

from typing import Any


class VariablePool:
    """命名空间字典: {node_id: {field_name: value}}"""

    def __init__(self) -> None:
        self._data: dict[str, dict[str, Any]] = {}

    def set(self, node_id: str, outputs: dict[str, Any]) -> None:
        """节点执行完后，将输出写入 pool"""
        self._data[node_id] = outputs

    def get(self, node_id: str, field: str) -> Any:
        """按 node_id.field 读取单个值"""
        ns = self._data.get(node_id)
        if ns is None:
            raise KeyError(f"节点 '{node_id}' 还没有输出")
        if field not in ns:
            raise KeyError(f"节点 '{node_id}' 没有字段 '{field}'，可用: {list(ns.keys())}")
        return ns[field]

    def namespace(self, node_id: str) -> dict[str, Any]:
        """Return a copy of one namespace, such as the root node inputs."""
        ns = self._data.get(node_id)
        if ns is None:
            raise KeyError(f"变量命名空间 '{node_id}' 不存在")
        return dict(ns)

    def resolve(self, input_mapping: dict[str, str], missing_nodes: set[str] | None = None) -> dict[str, Any]:
        """根据 input_mapping 从 pool 取上游值，组装成输入 dict。

        input_mapping 格式: {"本地变量名": "上游节点ID.字段名"}
        返回: {"本地变量名": 实际值}

        Example:
            pool._data = {"start": {"question": "什么是RAG?"}}
            pool.resolve({"prompt": "start.question"})
            → {"prompt": "什么是RAG?"}
        """
        resolved: dict[str, Any] = {}
        for local_name, ref in input_mapping.items():
            if "." not in ref:
                raise ValueError(f"变量引用格式错误: '{ref}'，需要 '节点ID.字段名'")
            upstream_node, field = ref.split(".", 1)
            if missing_nodes and upstream_node in missing_nodes:
                resolved[local_name] = None
            else:
                resolved[local_name] = self.get(upstream_node, field)
        return resolved

    def flatten(self) -> dict[str, Any]:
        """扁平化为 {node_id.field: value}，供 Jinja2 模板渲染使用"""
        flat: dict[str, Any] = {}
        for node_id, fields in self._data.items():
            for field, value in fields.items():
                flat[f"{node_id}.{field}"] = value
        return flat

    def snapshot(self) -> dict[str, dict[str, Any]]:
        """返回只读快照"""
        return dict(self._data)
