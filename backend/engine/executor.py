"""Concurrent ready-queue workflow executor with retries and streaming events."""

import asyncio
import inspect
import json
import logging
import time
import uuid
from typing import Any, AsyncGenerator, Callable

from .execution_store import ExecutionStore
from .graph import WorkflowGraph
from .node_registry import NodeRegistry
from .variable_pool import VariablePool

logger = logging.getLogger(__name__)
Callback = Callable[..., Any] | None


class WorkflowExecutor:
    def __init__(self, store: ExecutionStore | None = None, max_concurrency: int = 10) -> None:
        self.store = store or ExecutionStore()
        self.max_concurrency = max(1, max_concurrency)
        self.pool = VariablePool()
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
    ) -> dict[str, Any]:
        self.pool = VariablePool()  # Never leak values between runs.
        self.run_id = run_id or str(uuid.uuid4())
        nodes_config = graph_config.get("nodes", [])
        raw_ids = [n.get("id") for n in nodes_config]
        if len(raw_ids) != len(set(raw_ids)):
            raise ValueError("节点 ID 不能重复")

        instances: dict[str, Any] = {}
        runtime_config: dict[str, dict[str, Any]] = {}
        for nc in nodes_config:
            node_id = nc.get("id")
            if not isinstance(node_id, str) or not node_id:
                raise ValueError("每个节点都必须有非空字符串 ID")
            data = nc.get("data") or {}
            instances[node_id] = NodeRegistry.create(
                node_type=data.get("type", ""), node_id=node_id,
                config=data.get("config") or {}, input_mapping=data.get("input_mapping") or {},
            )
            runtime_config[node_id] = data

        graph = WorkflowGraph(instances, graph_config.get("edges", []))
        graph.validate()
        now = time.time()
        self.store.create_run(self.run_id, user_inputs, now)
        await self._call(on_event, "workflow_start", {"run_id": self.run_id})

        pending: dict[asyncio.Task[tuple[dict[str, Any], str | None, int, str]], str] = {}
        queued: set[str] = set()
        completed: set[str] = set()
        skipped: set[str] = set()
        ready: asyncio.Queue[str] = asyncio.Queue()
        for nid in graph.roots():
            ready.put_nowait(nid)
            queued.add(nid)

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
                    nid = ready.get_nowait()
                    node = instances[nid]
                    node.execution_context = {
                        "workflow_run_id": self.run_id,
                        "node_id": nid,
                        "logical_execution_id": f"{self.run_id}:{nid}",
                    }
                    inputs = user_inputs if node.node_type == "start" else self.pool.resolve(node.input_mapping, skipped)
                    await self._call(on_node_start, nid, node.node_type)
                    await self._call(on_event, "node_start", {"node_id": nid, "node_type": node.node_type})
                    task = asyncio.create_task(self._execute_node(nid, node, inputs, runtime_config[nid], on_event))
                    pending[task] = nid
                if not pending:
                    break
                done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    nid = pending.pop(task)
                    outputs, selected_handle, retry_count, status = task.result()
                    self.pool.set(nid, outputs)
                    completed.add(nid)
                    self.store.update_node(self.run_id, nid, status, retry_count, time.time(), output=outputs)
                    await self._call(on_node_finish, nid, outputs)
                    await self._call(on_event, "node_finish", {
                        "node_id": nid, "outputs": outputs, "status": status,
                        "retry_count": retry_count,
                    })
                    await propagate(graph.resolve_outgoing(nid, selected_handle))

            unresolved = set(instances) - completed - skipped
            if unresolved:
                raise RuntimeError(f"工作流停止但仍有未决节点: {sorted(unresolved)}")
            end_ids = [nid for nid, node in instances.items() if node.node_type == "end" and nid in completed]
            result = {nid: self.pool.snapshot()[nid] for nid in end_ids}
            final_output = next(iter(result.values())) if len(result) == 1 else result
            self.store.update_run(self.run_id, "succeeded", time.time(), output=final_output)
            await self._call(on_workflow_finish, final_output)
            await self._call(on_event, "workflow_finish", {"run_id": self.run_id, "result": final_output})
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
            await self._call(on_event, "workflow_error", {"run_id": self.run_id, "error": str(exc)})
            raise

    async def _execute_node(
        self, nid: str, node: Any, inputs: dict[str, Any], data: dict[str, Any], on_event: Callback
    ) -> tuple[dict[str, Any], str | None, int, str]:
        retry = data.get("retry_config") or node.config.get("retry_config") or {}
        enabled = bool(retry.get("retry_enabled", False))
        max_retries = max(0, int(retry.get("max_retries", 0))) if enabled else 0
        interval = max(0, int(retry.get("retry_interval", 0))) / 1000
        timeout = data.get("timeout") or node.config.get("timeout")
        strategy = data.get("error_strategy") or node.config.get("error_strategy")
        retries = 0
        while True:
            self.store.update_node(self.run_id, nid, "running", retries, time.time())
            try:
                call = node.run(inputs)
                outputs = await asyncio.wait_for(call, float(timeout)) if timeout else await call
                if not isinstance(outputs, dict):
                    raise TypeError(f"节点 {nid} 必须返回 dict")
                selected = outputs.pop("_selected_handle", None)
                return outputs, selected, retries, "succeeded"
            except Exception as exc:
                if retries < max_retries:
                    retries += 1
                    self.store.update_node(self.run_id, nid, "retry", retries, time.time(), error=str(exc))
                    await self._call(on_event, "node_retry", {
                        "node_id": nid, "retry_count": retries, "error": str(exc),
                    })
                    if interval:
                        await asyncio.sleep(interval)
                    continue
                if strategy == "fail_branch":
                    return {"error": str(exc)}, "fail-branch", retries, "exception"
                if strategy == "default_value":
                    default = data.get("default_value", node.config.get("default_value", {}))
                    if not isinstance(default, dict):
                        raise ValueError(f"节点 {nid} 的 default_value 必须是对象") from exc
                    return default, None, retries, "exception"
                self.store.update_node(self.run_id, nid, "failed", retries, time.time(), error=str(exc))
                raise

    async def run_sse(self, graph_config: dict[str, Any], user_inputs: dict[str, Any]) -> AsyncGenerator[str, None]:
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
    async def _call(callback: Callback, *args: Any) -> None:
        if callback is None:
            return
        result = callback(*args)
        if inspect.isawaitable(result):
            await result

    @staticmethod
    def _sse(event: str, data: dict[str, Any]) -> str:
        return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
