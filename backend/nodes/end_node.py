"""结束节点 — 工作流的出口，组装最终输出。"""

from typing import Any

from .base import Node


class EndNode(Node):
    node_type = "end"
    label = "结束"
    description = "工作流的出口，输出最终结果"
    color = "#ef4444"
    config_schema = {
        "output_fields": {
            "type": "array",
            "description": "要输出的字段（从上游变量中选择）",
            "items": {"type": "string"},
            "default": [],
        },
    }

    async def run(self, inputs: dict[str, Any]) -> dict[str, Any]:
        output_fields = self.config.get("output_fields", [])

        if output_fields:
            # 只输出指定的字段
            result = {}
            for field in output_fields:
                if field in inputs:
                    result[field] = inputs[field]
            return result
        else:
            # 未指定则透传全部
            return dict(inputs)

    def validate_config(self) -> list[str]:
        fields = self.config.get("output_fields", [])
        return [] if isinstance(fields, list) else ["output_fields 必须是数组"]
