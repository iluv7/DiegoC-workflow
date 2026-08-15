"""Node 基类 — 所有工作流节点的抽象父类。"""

from abc import ABC, abstractmethod
from typing import Any


class Node(ABC):
    """工作流节点的基类。

    每个节点声明:
      - node_id: 唯一标识
      - node_type: 类型字符串，用于注册表 lookup
      - input_mapping: {"本地变量名": "上游节点ID.字段名"}
      - config: 节点特定配置
    """

    # --- 子类必须定义 ---
    node_type: str = ""       # 类型字符串: "start", "llm", "code", "http", "end"
    label: str = ""           # 前端显示名称
    description: str = ""     # 前端显示的描述
    color: str = "#6b7280"    # 画布上节点的颜色
    config_schema: dict[str, Any] = {}  # 配置项的 JSON Schema

    def __init__(
        self,
        node_id: str,
        config: dict[str, Any],
        input_mapping: dict[str, str],
    ) -> None:
        self.node_id = node_id
        self.config = config
        self.input_mapping = input_mapping
        self.execution_context: dict[str, Any] = {}

    @abstractmethod
    async def run(self, inputs: dict[str, Any]) -> dict[str, Any]:
        """执行节点逻辑，返回 output dict。

        Args:
            inputs: 从 VariablePool.resolve() 得到的已解析输入

        Returns:
            节点的输出，会被写入 pool[self.node_id]
        """
        ...

    def validate_config(self) -> list[str]:
        """验证 config 是否合法，返回错误列表（空列表 = 合法）"""
        return []
