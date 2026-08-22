"""Ready-queue executor backed by a request-scoped GraphRuntimeState."""

import asyncio
import inspect
import json
import logging
import time
import uuid
from typing import Any, AsyncGenerator, Callable

from .execution_store import ExecutionStore
from .graph import WorkflowGraph
from .node_factory import NodeFactory
from .runtime import GraphRuntimeState
from .variable_pool import VariablePool

logger = logging.getLogger(__name__)
Callback = Callable[..., Any] | None


class WorkflowExecutor:
    def __init__(self, store: ExecutionStore | None = None, max_concurrency: int = 10) -> None:
        self.store = store or ExecutionStore()
        self.max_concurrency = max(1, max_concurrency)
        self.pool = VariablePool()
        self.runtime_state: GraphRuntimeState | None = None
        self.run_id: str | None = None

    async def run(
        self,
        graph_config: dict[str, Any],
        user_inputs: dict[str, Any],
        *,
        on_node_start: Callback = None,
        on_node_finish: Callback = None,
        on_workflow_finish: Callback = None,
        on_error: Callback = None,
        on_event: Callback = None,
        run_id: str | None = None,
        system_variables: dict[str, Any] | None = None,
        environment_variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Parse a graph and execute it with fresh state for this request."""
        self.run_id = run_id or str(uuid.uuid4())
        root_node_id = self._find_root_node_id(graph_config)
        runtime_state = GraphRuntimeState.bootstrap(
            root_node_id,
            user_inputs,
            system_variables=system_variables,
            environment_variables=environment_variables,
        )
        self.runtime_state = runtime_state
        self.pool = runtime_state.variable_pool

        # JSON creates the static node/edge objects. Request inputs stay in the
        # runtime pool and are read only when each node is scheduled.
        graph = WorkflowGraph.init(graph_config, NodeFactory(runtime_state))
        instances = graph.nodes
        runtime_config = {
            node_config["id"]: node_config.get("data") or {}
            for node_config in graph_config.get("nodes", [])
        }

        self.store.create_run(self.run_id, user_inputs, time.time())
        await self._call(on_event, "workflow_start", {"run_id": self.run_id})

        pending: dict[asyncio.Task[tuple[dict[str, Any], str | None, int, str]], str] = {}
        queued: set[str] = set()
        completed: set[str] = set()
        skipped: set[str] = set()
        ready: asyncio.Queue[str] = asyncio.Queue()
        for node_id in graph.roots():
            ready.put_nowait(node_id)
            queued.add(node_id)

        async def propagate(targets: set[str]) -> None:
            stack = list(targets)
            while stack:
                target = stack.pop()
                if target in queued or target in completed or target in skipped:
                    continue
                state = graph.readiness(target)
                if state == "ready":
                    ready.put_nowait(target)
                    queued.add(target)
                elif state == "skip":
                    skipped.add(target)
                    self.store.update_node(self.run_id, target, "skipped", 0, time.time())
                    await self._call(on_event, "node_skipped", {"node_id": target})
                    stack.extend(graph.resolve_outgoing(target, "__never__"))

        try:
            while not ready.empty() or pending:
                while not ready.empty() and len(pending) < self.max_concurrency:
                    node_id = ready.get_nowait()
                    node = instances[node_id]
                    node.execution_context = {
                        "workflow_run_id": self.run_id,
                        "node_id": node_id,
                        "logical_execution_id": f"{self.run_id}:{node_id}",
                    }
                    inputs = node.resolve_inputs(skipped)
                    await self._call(on_node_start, node_id, node.node_type)
                    await self._call(on_event, "node_start", {"node_id": node_id, "node_type": node.node_type})
                    task = asyncio.create_task(
                        self._execute_node(node_id, node, inputs, runtime_config[node_id], on_event)
                    )
                    pending[task] = node_id

                if not pending:
                    break
                done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    node_id = pending.pop(task)
                    outputs, selected_handle, retry_count, status = task.result()
                    runtime_state.record_output(node_id, outputs)
                    completed.add(node_id)
                    self.store.update_node(
                        self.run_id,
                        node_id,
                        status,
                        retry_count,
                        time.time(),
                        output=outputs,
                    )
                    await self._call(on_node_finish, node_id, outputs)
                    await self._call(
                        on_event,
                        "node_finish",
                        {
                            "node_id": node_id,
                            "outputs": outputs,
                            "status": status,
                            "retry_count": retry_count,
                        },
                    )
                    await propagate(graph.resolve_outgoing(node_id, selected_handle))

            unresolved = set(instances) - completed - skipped
            if unresolved:
                raise RuntimeError(f"工作流停止但仍有未决节点: {sorted(unresolved)}")
            end_ids = [
                node_id
                for node_id, node in instances.items()
                if node.node_type == "end" and node_id in completed
            ]
            result = {node_id: runtime_state.outputs[node_id] for node_id in end_ids}
            final_output = next(iter(result.values())) if len(result) == 1 else result
            self.store.update_run(self.run_id, "succeeded", time.time(), output=final_output)
            await self._call(on_workflow_finish, final_output)
            await self._call(
                on_event,
                "workflow_finish",
                {"run_id": self.run_id, "result": final_output},
            )
            return final_output
        except asyncio.CancelledError:
            for task in pending:
                task.cancel()
            self.store.update_run(self.run_id, "cancelled", time.time(), error="client cancelled")
            raise
        except Exception as exc:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            self.store.update_run(self.run_id, "failed", time.time(), error=str(exc))
            await self._call(on_error, str(exc))
            await self._call(
                on_event,
                "workflow_error",
                {"run_id": self.run_id, "error": str(exc)},
            )
            raise

    async def _execute_node(
        self,
        node_id: str,
        node: Any,
        inputs: dict[str, Any],
        data: dict[str, Any],
        on_event: Callback,
    ) -> tuple[dict[str, Any], str | None, int, str]:
        retry = data.get("retry_config") or node.config.get("retry_config") or {}
        enabled = bool(retry.get("retry_enabled", False))
        max_retries = max(0, int(retry.get("max_retries", 0))) if enabled else 0
        interval = max(0, int(retry.get("retry_interval", 0))) / 1000
        timeout = data.get("timeout") or node.config.get("timeout")
        strategy = data.get("error_strategy") or node.config.get("error_strategy")
        retries = 0
        while True:
            self.store.update_node(self.run_id, node_id, "running", retries, time.time())
            try:
                call = node.run(inputs)
                outputs = await asyncio.wait_for(call, float(timeout)) if timeout else await call
                if not isinstance(outputs, dict):
                    raise TypeError(f"节点 {node_id} 必须返回 dict")
                selected = outputs.pop("_selected_handle", None)
                return outputs, selected, retries, "succeeded"
            except Exception as exc:
                if retries < max_retries:
                    retries += 1
                    self.store.update_node(
                        self.run_id,
                        node_id,
                        "retry",
                        retries,
                        time.time(),
                        error=str(exc),
                    )
                    await self._call(
                        on_event,
                        "node_retry",
                        {"node_id": node_id, "retry_count": retries, "error": str(exc)},
                    )
                    if interval:
                        await asyncio.sleep(interval)
                    continue
                if strategy == "fail_branch":
                    return {"error": str(exc)}, "fail-branch", retries, "exception"
                if strategy == "default_value":
                    default = data.get("default_value", node.config.get("default_value", {}))
                    if not isinstance(default, dict):
                        raise ValueError(f"节点 {node_id} 的 default_value 必须是对象") from exc
                    return default, None, retries, "exception"
                self.store.update_node(
                    self.run_id,
                    node_id,
                    "failed",
                    retries,
                    time.time(),
                    error=str(exc),
                )
                raise

    async def run_sse(
        self,
        graph_config: dict[str, Any],
        user_inputs: dict[str, Any],
    ) -> AsyncGenerator[str, None]:
        queue: asyncio.Queue[tuple[str, dict[str, Any]] | None] = asyncio.Queue()
        error_emitted = False

        async def emit(event: str, data: dict[str, Any]) -> None:
            nonlocal error_emitted
            if event == "workflow_error":
                error_emitted = True
            await queue.put((event, data))

        async def execute() -> None:
            try:
                await self.run(graph_config, user_inputs, on_event=emit)
            except Exception as exc:
                logger.exception("工作流执行失败")
                if not error_emitted:
                    await queue.put(("workflow_error", {"run_id": self.run_id, "error": str(exc)}))
            finally:
                await queue.put(None)

        task = asyncio.create_task(execute())
        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield self._sse(*item)
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

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
    async def _call(callback: Callback, *args: Any) -> None:
        if callback is None:
            return
        result = callback(*args)
        if inspect.isawaitable(result):
            await result

    @staticmethod
    def _sse(event: str, data: dict[str, Any]) -> str:
        return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
