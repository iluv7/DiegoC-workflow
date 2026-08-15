"""Simple conditional branch node."""

from typing import Any

from .base import Node


class IfElseNode(Node):
    node_type = "if_else"
    label = "条件分支"
    description = "根据输入选择 true 或 false 出口"
    color = "#a855f7"
    config_schema = {
        "variable": {"type": "string"},
        "operator": {"type": "string", "enum": ["equals", "not_equals", "truthy", "contains", "gt", "gte", "lt", "lte"]},
        "value": {},
    }

    async def run(self, inputs: dict[str, Any]) -> dict[str, Any]:
        name = self.config["variable"]
        actual = inputs.get(name)
        expected = self.config.get("value")
        operator = self.config.get("operator", "truthy")
        checks = {
            "equals": lambda: actual == expected,
            "not_equals": lambda: actual != expected,
            "truthy": lambda: bool(actual),
            "contains": lambda: expected in actual,
            "gt": lambda: actual > expected,
            "gte": lambda: actual >= expected,
            "lt": lambda: actual < expected,
            "lte": lambda: actual <= expected,
        }
        matched = bool(checks[operator]())
        return {"matched": matched, "value": actual, "_selected_handle": "true" if matched else "false"}

    def validate_config(self) -> list[str]:
        errors = []
        if not self.config.get("variable"):
            errors.append("variable 不能为空")
        if self.config.get("operator", "truthy") not in {"equals", "not_equals", "truthy", "contains", "gt", "gte", "lt", "lte"}:
            errors.append("operator 不受支持")
        return errors
