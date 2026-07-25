"""节点注册表 — 管理节点类型的注册和实例化。"""

from typing import Any

from nodes.base import Node


class NodeRegistry:
    """节点类型注册表，支持通过 type string 创建节点实例。"""

    _registry: dict[str, type[Node]] = {}

    @classmethod
    def register(cls, node_type: str, node_class: type[Node]) -> None:
        """注册节点类型"""
        cls._registry[node_type] = node_class

    @classmethod
    def create(cls, node_type: str, node_id: str, config: dict[str, Any], input_mapping: dict[str, str]) -> Node:
        """根据类型创建节点实例"""
        if node_type not in cls._registry:
            raise ValueError(f"未知节点类型: '{node_type}'，可用: {list(cls._registry.keys())}")
        node_class = cls._registry[node_type]
        return node_class(
            node_id=node_id,
            config=config,
            input_mapping=input_mapping,
        )

    @classmethod
    def list_types(cls) -> list[dict[str, Any]]:
        """返回所有已注册节点类型的元信息"""
        result: list[dict[str, Any]] = []
        for node_type, node_class in cls._registry.items():
            result.append({
                "type": node_type,
                "label": getattr(node_class, "label", node_type),
                "description": getattr(node_class, "description", ""),
                "config_schema": getattr(node_class, "config_schema", {}),
                "color": getattr(node_class, "color", "#6b7280"),
            })
        return result
