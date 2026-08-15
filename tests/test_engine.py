import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from engine import ExecutionStore, NodeRegistry, WorkflowExecutor
from nodes import CodeNode, EndNode, HTTPNode, IfElseNode, StartNode
from nodes.base import Node


class EchoNode(Node):
    node_type = "echo"

    async def run(self, inputs):
        return dict(inputs)


class FlakyNode(Node):
    node_type = "flaky"
    calls = 0

    async def run(self, inputs):
        type(self).calls += 1
        if type(self).calls < 2:
            raise RuntimeError("temporary")
        return {"ok": True}


class FailNode(Node):
    node_type = "fail"

    async def run(self, inputs):
        raise RuntimeError("boom")


for kind, cls in (("start", StartNode), ("end", EndNode), ("if_else", IfElseNode), ("echo", EchoNode), ("flaky", FlakyNode), ("fail", FailNode)):
    NodeRegistry.register(kind, cls)


def executor(tmp_path):
    return WorkflowExecutor(ExecutionStore(str(tmp_path / "runs.db")))


@pytest.mark.asyncio
async def test_branch_skip_and_merge(tmp_path):
    cfg = {
        "nodes": [
            {"id": "s", "data": {"type": "start"}},
            {"id": "if", "data": {"type": "if_else", "config": {"variable": "flag", "operator": "truthy"}, "input_mapping": {"flag": "s.flag"}}},
            {"id": "yes", "data": {"type": "echo", "input_mapping": {"value": "s.flag"}}},
            {"id": "no", "data": {"type": "echo", "input_mapping": {"value": "s.flag"}}},
            {"id": "end", "data": {"type": "end", "input_mapping": {"value": "yes.value"}}},
        ],
        "edges": [
            {"source": "s", "target": "if"},
            {"source": "if", "target": "yes", "sourceHandle": "true"},
            {"source": "if", "target": "no", "sourceHandle": "false"},
            {"source": "yes", "target": "end"},
            {"source": "no", "target": "end"},
        ],
    }
    result = await executor(tmp_path).run(cfg, {"flag": True})
    assert result == {"value": True}


@pytest.mark.asyncio
async def test_retry_and_persistence(tmp_path):
    FlakyNode.calls = 0
    cfg = {
        "nodes": [
            {"id": "s", "data": {"type": "start"}},
            {"id": "f", "data": {"type": "flaky", "retry_config": {"retry_enabled": True, "max_retries": 1}}},
            {"id": "e", "data": {"type": "end", "input_mapping": {"ok": "f.ok"}}},
        ],
        "edges": [{"source": "s", "target": "f"}, {"source": "f", "target": "e"}],
    }
    ex = executor(tmp_path)
    assert await ex.run(cfg, {}) == {"ok": True}
    saved = ex.store.get_run(ex.run_id)
    assert saved["status"] == "succeeded"
    assert next(n for n in saved["nodes"] if n["node_id"] == "f")["retry_count"] == 1


@pytest.mark.asyncio
async def test_fail_branch_and_skipped_input(tmp_path):
    cfg = {
        "nodes": [
            {"id": "s", "data": {"type": "start"}},
            {"id": "f", "data": {"type": "fail", "error_strategy": "fail_branch"}},
            {"id": "normal", "data": {"type": "echo", "input_mapping": {}}},
            {"id": "fallback", "data": {"type": "echo", "input_mapping": {"message": "f.error"}}},
            {"id": "e", "data": {"type": "end", "input_mapping": {"normal": "normal.value", "error": "fallback.message"}}},
        ],
        "edges": [
            {"source": "s", "target": "f"},
            {"source": "f", "target": "normal"},
            {"source": "f", "target": "fallback", "sourceHandle": "fail-branch"},
            {"source": "normal", "target": "e"},
            {"source": "fallback", "target": "e"},
        ],
    }
    result = await executor(tmp_path).run(cfg, {})
    assert result == {"normal": None, "error": "boom"}


@pytest.mark.asyncio
async def test_executor_does_not_reuse_variable_pool(tmp_path):
    ex = executor(tmp_path)
    first = {"nodes": [{"id": "s", "data": {"type": "start"}}, {"id": "e", "data": {"type": "end", "input_mapping": {"x": "s.x"}}}], "edges": [{"source": "s", "target": "e"}]}
    await ex.run(first, {"x": "old"})
    with pytest.raises(KeyError):
        await ex.run(first, {})


@pytest.mark.asyncio
async def test_sse_emits_start_before_completion(tmp_path):
    ex = executor(tmp_path)
    cfg = {"nodes": [{"id": "s", "data": {"type": "start"}}, {"id": "e", "data": {"type": "end", "input_mapping": {"x": "s.x"}}}], "edges": [{"source": "s", "target": "e"}]}
    stream = ex.run_sse(cfg, {"x": 1})
    first = await anext(stream)
    assert "event: workflow_start" in first
    await stream.aclose()


@pytest.mark.asyncio
async def test_invalid_graph_is_reported_as_sse_error(tmp_path):
    stream = executor(tmp_path).run_sse({"nodes": [], "edges": []}, {})
    event = await anext(stream)
    assert "event: workflow_error" in event


@pytest.mark.asyncio
async def test_unsafe_nodes_are_blocked_by_default(monkeypatch):
    monkeypatch.delenv("ALLOW_UNSAFE_CODE_EXECUTION", raising=False)
    code = CodeNode("code", {"code": "result = 1"}, {})
    with pytest.raises(PermissionError):
        await code.run({})
    monkeypatch.delenv("WORKFLOW_ALLOW_PRIVATE_HTTP", raising=False)
    with pytest.raises(ValueError, match="禁止访问"):
        await HTTPNode._validate_destination("http://127.0.0.1/admin")


def test_duplicate_ids_rejected(tmp_path):
    cfg = {"nodes": [{"id": "s", "data": {"type": "start"}}, {"id": "s", "data": {"type": "end"}}], "edges": []}
    with pytest.raises(ValueError, match="重复"):
        asyncio.run(executor(tmp_path).run(cfg, {}))
