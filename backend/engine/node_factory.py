"""Turn a node JSON object into a concrete Node instance."""

from typing import Any

from nodes.base import Node

from .node_registry import NodeRegistry
from .runtime import GraphRuntimeState


class NodeFactory:
    """Resolve node type and inject dependencies shared by a workflow run."""

    def __init__(self, graph_runtime_state: GraphRuntimeState) -> None:
        self.graph_runtime_state = graph_runtime_state

    def create_node(self, node_config: dict[str, Any]) -> Node:
        if not isinstance(node_config, dict):
            raise ValueError("节点配置必须是对象")
        node_id = node_config.get("id")
        if not isinstance(node_id, str) or not node_id:
            raise ValueError("每个节点都必须有非空字符串 ID")

        data = node_config.get("data") or {}
        if not isinstance(data, dict):
            raise ValueError(f"节点 {node_id} 的 data 必须是对象")
        config = data.get("config") or {}
        input_mapping = data.get("input_mapping") or {}
        if not isinstance(config, dict):
            raise ValueError(f"节点 {node_id} 的 config 必须是对象")
        if not isinstance(input_mapping, dict):
            raise ValueError(f"节点 {node_id} 的 input_mapping 必须是对象")
        if any(not isinstance(key, str) or not isinstance(value, str) for key, value in input_mapping.items()):
            raise ValueError(f"节点 {node_id} 的 input_mapping 键和值必须是字符串")

        return NodeRegistry.create(
            node_type=data.get("type", ""),
            node_id=node_id,
            config=config,
            input_mapping=input_mapping,
            graph_runtime_state=self.graph_runtime_state,
        )
