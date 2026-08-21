"""执行器 — asyncio 并行执行工作流，SSE 流式输出。

核心流程:
  1. 请求输入创建 GraphRuntimeState
  2. Graph.init + NodeFactory 将 JSON 构建为静态图
  3. 节点执行时从运行态变量池解析输入
  4. 逐层 asyncio.gather 并行执行并发送事件
"""

import asyncio
import json
import logging
from typing import Any, AsyncGenerator, Callable

from .graph import WorkflowGraph
from .node_factory import NodeFactory
from .runtime import GraphRuntimeState
from .variable_pool import VariablePool

logger = logging.getLogger(__name__)


class WorkflowExecutor:
    """工作流执行器。只依赖 NodeRegistry，不依赖 FastAPI。"""

    def __init__(self) -> None:
        self.pool = VariablePool()
        self.runtime_state: GraphRuntimeState | None = None

    async def run(
        self,
        graph_config: dict[str, Any],
        user_inputs: dict[str, Any],
        *,
        on_node_start: Callable[[str, str], Any] | None = None,
        on_node_finish: Callable[[str, dict[str, Any]], Any] | None = None,
        on_workflow_finish: Callable[[dict[str, Any]], Any] | None = None,
        on_error: Callable[[str], Any] | None = None,
        system_variables: dict[str, Any] | None = None,
        environment_variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """执行工作流，支持回调。

        Args:
            graph_config: {"nodes": [...], "edges": [...]}
            user_inputs: 用户初始输入
            on_node_start: 节点开始回调(node_id, node_type)
            on_node_finish: 节点完成回调(node_id, outputs)
            on_workflow_finish: 工作流完成回调(final_output)
            on_error: 错误回调(error_message)
            system_variables: 本次运行的系统变量，存入 sys 命名空间
            environment_variables: 本次运行的环境变量，存入 env 命名空间

        Returns:
            最终输出 dict
        """
        # RuntimeState comes from this request, not from workflow JSON. Only the
        # start node id is inspected before node construction so inputs can be
        # placed in its variable namespace.
        root_node_id = self._find_root_node_id(graph_config)
        runtime_state = GraphRuntimeState.bootstrap(
            root_node_id,
            user_inputs,
            system_variables=system_variables,
            environment_variables=environment_variables,
        )
        self.runtime_state = runtime_state
        self.pool = runtime_state.variable_pool

        # Static Graph comes from JSON. Nodes receive a runtime reference, but
        # user input is never copied into their config or constructor arguments.
        graph = WorkflowGraph.init(graph_config, NodeFactory(runtime_state))
        node_instances = graph.nodes
        levels = graph.bfs_levels()

        logger.info("工作流开始，共 %d 层: %s", len(levels), [[n for n in lv] for lv in levels])

        # 3. 分层执行
        for level_idx, level in enumerate(levels):
            logger.info("── 第 %d 层: %s ──", level_idx, level)

            tasks = []
            for nid in level:
                node = node_instances[nid]
                if on_node_start:
                    await on_node_start(nid, node.node_type)
                tasks.append(self._run_node(nid, node))

            results = await asyncio.gather(*tasks)

            for nid, outputs in results:
                if on_node_finish:
                    await on_node_finish(nid, outputs)

            logger.info("── 第 %d 层完成 ──", level_idx)

        # 4. 收集最终输出（最后一层的最后一个节点）
        final_nid = levels[-1][-1]
        final_output = runtime_state.outputs.get(final_nid, {})
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

    async def _run_node(self, nid: str, node: Any) -> tuple[str, dict[str, Any]]:
        """执行时从 RuntimeState 解析输入，再把输出写回同一运行态。"""
        logger.debug("  执行 %s:%s", node.node_type, nid)
        inputs = node.resolve_inputs()
        outputs = await node.run(inputs)
        if not isinstance(outputs, dict):
            raise TypeError(f"节点 {nid} 必须返回 dict")
        if self.runtime_state is None:
            raise RuntimeError("工作流运行态未初始化")
        self.runtime_state.record_output(nid, outputs)
        return nid, outputs

    @staticmethod
    def _find_root_node_id(graph_config: dict[str, Any]) -> str:
        if not isinstance(graph_config, dict):
            raise ValueError("graph_config 必须是对象")
        nodes = graph_config.get("nodes", [])
        if not isinstance(nodes, list):
            raise ValueError("graph_config.nodes 必须是数组")
        starts = [
            node.get("id")
            for node in nodes
            if isinstance(node, dict)
            and isinstance(node.get("data"), dict)
            and node["data"].get("type") == "start"
        ]
        if len(starts) != 1 or not isinstance(starts[0], str) or not starts[0]:
            raise ValueError("工作流必须且只能有一个 start 节点")
        return starts[0]

    @staticmethod
    def _sse(event: str, data: dict[str, Any]) -> str:
        """生成 SSE 格式的事件"""
        return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
