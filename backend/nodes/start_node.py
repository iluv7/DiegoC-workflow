"""起始节点 — 工作流的入口，定义输入变量。"""

from typing import Any

from .base import Node


class StartNode(Node):
    node_type = "start"
    label = "开始"
    description = "工作流的入口，定义输入变量"
    color = "#22c55e"
    config_schema = {
        "variables": {
            "type": "array",
            "description": "输入变量定义",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "type": {"type": "string", "enum": ["string", "number"]},
                },
            },
            "default": [{"name": "question", "type": "string"}],
        },
    }

    async def run(self, inputs: dict[str, Any]) -> dict[str, Any]:
        """Start 节点直接透传上游输入（通常来自用户初始输入）"""
        return dict(inputs)
