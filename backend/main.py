"""FastAPI 入口 — Workflow API 服务。

启动: uvicorn main:app --reload --port 8100

API:
  POST /api/workflow/run   — SSE 流式执行工作流
  GET  /api/workflow/nodes — 获取可用节点类型列表
"""

import json
import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from engine import ExecutionStore, NodeRegistry, WorkflowExecutor
from nodes import CodeNode, EndNode, HTTPNode, IfElseNode, LLMNode, StartNode

# --- Bootstrap: 注册节点类型 ---
NodeRegistry.register("start", StartNode)
NodeRegistry.register("llm", LLMNode)
NodeRegistry.register("code", CodeNode)
NodeRegistry.register("http", HTTPNode)
NodeRegistry.register("end", EndNode)
NodeRegistry.register("if_else", IfElseNode)
store = ExecutionStore()

# --- App ---
app = FastAPI(title="DiegoC-workflow", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:5174"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)


# --- Models ---
class WorkflowRunRequest(BaseModel):
    graph_config: dict[str, Any]
    inputs: dict[str, Any] = Field(default_factory=dict)


# --- Routes ---
@app.get("/api/workflow/nodes")
async def list_nodes():
    """返回所有可用节点类型"""
    return {"nodes": NodeRegistry.list_types()}


@app.post("/api/workflow/run")
async def run_workflow(req: WorkflowRunRequest):
    """SSE 流式执行工作流"""
    executor = WorkflowExecutor(store=store)

    async def event_stream():
        async for event in executor.run_sse(req.graph_config, req.inputs):
            yield event

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/workflow/runs/{run_id}")
async def get_workflow_run(run_id: str):
    """Query persisted workflow and node status."""
    from fastapi import HTTPException

    result = store.get_run(run_id)
    if result is None:
        raise HTTPException(status_code=404, detail="workflow run not found")
    return result


@app.get("/health")
async def health():
    return {"status": "ok"}
