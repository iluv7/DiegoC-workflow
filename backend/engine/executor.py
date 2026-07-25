"""执行器 — asyncio 并行执行工作流，SSE 流式输出。

核心流程:
  1. 从 graph_config (JSON) 构建 WorkflowGraph + 实例化 Node
  2. BFS 分层
  3. 逐层执行: 同层 asyncio.gather 并行，层间屏障
  4. 每完成一个节点就 yield SSE 事件
"""

import asyncio
import json
import logging
import time
from typing import Any, AsyncGenerator, Callable

from .graph import WorkflowGraph
from .node_registry import NodeRegistry
from .variable_pool import VariablePool

logger = logging.getLogger(__name__)


class WorkflowExecutor:
    """工作流执行器。只依赖 NodeRegistry，不依赖 FastAPI。"""

    def __init__(self) -> None:
        self.pool = VariablePool()

    async def run(
        self,
        graph_config: dict[str, Any],
        user_inputs: dict[str, Any],
        *,
        on_node_start: Callable[[str, str], Any] | None = None,
        on_node_finish: Callable[[str, dict[str, Any]], Any] | None = None,
        on_workflow_finish: Callable[[dict[str, Any]], Any] | None = None,
        on_error: Callable[[str], Any] | None = None,
    ) -> dict[str, Any]:
        """执行工作流，支持回调。

        Args:
            graph_config: {"nodes": [...], "edges": [...]}
            user_inputs: 用户初始输入
            on_node_start: 节点开始回调(node_id, node_type)
            on_node_finish: 节点完成回调(node_id, outputs)
            on_workflow_finish: 工作流完成回调(final_output)
            on_error: 错误回调(error_message)

        Returns:
            最终输出 dict
        """
        nodes_config = graph_config.get("nodes", [])
        edges = graph_config.get("edges", [])

        # 1. 实例化所有节点
        node_instances: dict[str, Any] = {}
        for nc in nodes_config:
            node_id = nc["id"]
            data = nc.get("data", {})
            node_type = data.get("type", "")
            config = data.get("config", {})
            input_mapping = data.get("input_mapping", {})
            node_instances[node_id] = NodeRegistry.create(
                node_type=node_type,
                node_id=node_id,
                config=config,
                input_mapping=input_mapping,
            )

        # 2. 构建 DAG
        graph = WorkflowGraph(node_instances, edges)
        graph.validate()
        levels = graph.bfs_levels()

        logger.info("工作流开始，共 %d 层: %s", len(levels), [[n for n in lv] for lv in levels])

        # 3. 分层执行
        for level_idx, level in enumerate(levels):
            logger.info("── 第 %d 层: %s ──", level_idx, level)

            if level_idx == 0:
                # 第一层: 起始节点直接接收 user_inputs
                tasks = []
                for nid in level:
                    node = node_instances[nid]
                    if on_node_start:
                        await on_node_start(nid, node.node_type)
                    tasks.append(self._run_node(nid, node, user_inputs))
            else:
                # 后续层: 从 pool resolve 输入
                tasks = []
                for nid in level:
                    node = node_instances[nid]
                    if on_node_start:
                        await on_node_start(nid, node.node_type)
                    resolved_inputs = {} if node.input_mapping else user_inputs if level_idx == 0 else self.pool.resolve(node.input_mapping)
                    # 注意: 非第一层的 resolve 已经用了前面的逻辑
                    tasks.append(self._run_node_with_resolve(nid, node))

            results = await asyncio.gather(*tasks)

            for nid, outputs in results:
                if on_node_finish:
                    await on_node_finish(nid, outputs)

            logger.info("── 第 %d 层完成 ──", level_idx)

        # 4. 收集最终输出（最后一层的最后一个节点）
        final_nid = levels[-1][-1]
        final_output = self.pool._data.get(final_nid, {})
        if on_workflow_finish:
            await on_workflow_finish(dict(final_output))

        return dict(final_output)

    async def run_sse(self, graph_config: dict[str, Any], user_inputs: dict[str, Any]) -> AsyncGenerator[str, None]:
        """SSE 流式执行，yield SSE 格式的事件字符串。"""
        events: list[str] = []

        async def on_start(nid: str, ntype: str) -> None:
            events.append(self._sse("node_start", {"node_id": nid, "node_type": ntype}))

        async def on_finish(nid: str, outputs: dict[str, Any]) -> None:
            events.append(self._sse("node_finish", {"node_id": nid, "outputs": outputs}))

        async def on_wf_finish(output: dict[str, Any]) -> None:
            events.append(self._sse("workflow_finish", {"result": output}))

        async def on_err(msg: str) -> None:
            events.append(self._sse("workflow_error", {"error": msg}))

        try:
            await self.run(
                graph_config,
                user_inputs,
                on_node_start=on_start,
                on_node_finish=on_finish,
                on_workflow_finish=on_wf_finish,
                on_error=on_err,
            )
        except Exception as e:
            logger.exception("工作流执行失败")
            events.append(self._sse("workflow_error", {"error": str(e)}))

        for event in events:
            yield event

    async def _run_node(self, nid: str, node: Any, inputs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        """执行单个节点并写入 pool"""
        logger.debug("  执行 %s:%s", node.node_type, nid)
        outputs = await node.run(inputs)
        self.pool.set(nid, outputs)
        return nid, outputs

    async def _run_node_with_resolve(self, nid: str, node: Any) -> tuple[str, dict[str, Any]]:
        """解析上游输入后执行节点"""
        inputs = self.pool.resolve(node.input_mapping)
        logger.debug("  执行 %s:%s 输入: %s", node.node_type, nid, inputs)
        outputs = await node.run(inputs)
        self.pool.set(nid, outputs)
        return nid, outputs

    @staticmethod
    def _sse(event: str, data: dict[str, Any]) -> str:
        """生成 SSE 格式的事件"""
        return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
