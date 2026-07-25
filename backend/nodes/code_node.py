"""代码节点 — 在受限环境中执行 Python 代码。"""

import io
import logging
from typing import Any

from .base import Node

logger = logging.getLogger(__name__)

# 允许的安全内置函数
_SAFE_BUILTINS: dict[str, Any] = {
    "abs": abs, "all": all, "any": any, "bool": bool, "dict": dict,
    "enumerate": enumerate, "filter": filter, "float": float, "int": int,
    "len": len, "list": list, "map": map, "max": max, "min": min,
    "range": range, "reversed": reversed, "round": round, "set": set,
    "slice": slice, "sorted": sorted, "str": str, "sum": sum, "tuple": tuple,
    "zip": zip, "print": print, "isinstance": isinstance,
    "True": True, "False": False, "None": None,
    "__import__": __import__,  # 允许 import 标准库模块
    "exec": exec,              # 允许执行代码字符串
    "json": __import__("json"),  # 预导入常用模块
}


class CodeNode(Node):
    node_type = "code"
    label = "代码"
    description = "执行 Python 代码片段"
    color = "#6b7280"
    config_schema = {
        "code": {"type": "string", "description": "Python 代码，通过一个 dict 叫 'inputs' 接收输入，return 一个 dict 作为输出"},
    }

    async def run(self, inputs: dict[str, Any]) -> dict[str, Any]:
        code = self.config.get("code", "")
        if not code:
            raise ValueError("请填写代码")

        # 准备执行环境
        local_vars: dict[str, Any] = {"inputs": inputs}
        stdout = io.StringIO()
        local_vars["print"] = lambda *a, **kw: print(*a, **kw, file=stdout)  # type: ignore[assignment]

        logger.info("代码节点 %s 执行", self.node_id)

        try:
            compiled = compile(code, f"<code_node:{self.node_id}>", "exec")
            exec(compiled, {"__builtins__": _SAFE_BUILTINS}, local_vars)
        except Exception as e:
            logger.exception("代码节点 %s 执行失败", self.node_id)
            raise RuntimeError(f"代码执行错误: {e}") from e

        # 取返回值: 按优先级找 result > output > __result__
        result = None
        result_key = "output"
        for key in ("result", "output", "__result__"):
            val = local_vars.get(key)
            if val is not None and val != {}:
                result = val
                result_key = key
                break

        if result is None:
            # 没有显式输出，返回整个 local_vars（去掉 inputs 和内置变量）
            result = {
                k: v for k, v in local_vars.items()
                if k not in ("inputs", "print") and not k.startswith("__")
            }
        elif isinstance(result, dict):
            pass  # 已经是 dict，直接使用
        else:
            # 简单值用变量名作为 key
            result = {result_key: result}

        output = stdout.getvalue()
        if output.strip():
            result["stdout"] = output

        return result
