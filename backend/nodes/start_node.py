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
        for variable in self.config.get("variables", []):
            name = variable["name"]
            if variable.get("required", True) and name not in inputs:
                raise ValueError(f"缺少必填输入: {name}")
            if name not in inputs:
                continue
            expected = variable.get("type", "string")
            expected_type = {"string": str, "number": (int, float), "boolean": bool}.get(expected)
            if expected_type and not isinstance(inputs[name], expected_type):
                raise TypeError(f"输入 {name} 类型应为 {expected}")
        return dict(inputs)

    def validate_config(self) -> list[str]:
        errors = []
        seen = set()
        for variable in self.config.get("variables", []):
            if not isinstance(variable, dict) or not variable.get("name"):
                errors.append("variables 中每项都必须包含 name")
                continue
            if variable["name"] in seen:
                errors.append(f"输入变量重复: {variable['name']}")
            seen.add(variable["name"])
            if variable.get("type", "string") not in {"string", "number", "boolean"}:
                errors.append(f"输入变量 {variable['name']} 类型不受支持")
        return errors
