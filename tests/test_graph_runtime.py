import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from engine import Edge, GraphRuntimeState, NodeFactory, NodeRegistry, WorkflowExecutor, WorkflowGraph
from nodes import EndNode, StartNode
from nodes.base import Node


class EchoNode(Node):
    node_type = "echo"

    async def run(self, inputs):
        return dict(inputs)


for node_type, node_class in (("start", StartNode), ("echo", EchoNode), ("end", EndNode)):
    NodeRegistry.register(node_type, node_class)


def workflow_config():
    return {
        "nodes": [
            {"id": "start", "data": {"type": "start"}},
            {
                "id": "echo",
                "data": {
                    "type": "echo",
                    "config": {"label": "static config only"},
                    "input_mapping": {"value": "start.question"},
                },
            },
            {
                "id": "end",
                "data": {"type": "end", "input_mapping": {"answer": "echo.value"}},
            },
        ],
        "edges": [
            {"id": "start-echo", "source": "start", "target": "echo"},
            {"id": "echo-end", "source": "echo", "target": "end"},
        ],
    }


def test_graph_init_parses_node_and_edge_objects():
    runtime = GraphRuntimeState.bootstrap("start", {"question": "hello"})
    graph = WorkflowGraph.init(workflow_config(), NodeFactory(runtime))

    assert graph.root_node_id == "start"
    assert isinstance(graph.edges[0], Edge)
    assert graph.edges[0].source == "start"
    assert graph.edges[0].target == "echo"
    assert all(node.graph_runtime_state is runtime for node in graph.nodes.values())
    assert graph.nodes["echo"].config == {"label": "static config only"}
    assert "question" not in graph.nodes["echo"].config


def test_nodes_resolve_request_inputs_from_runtime_pool():
    executor = WorkflowExecutor()
    result = asyncio.run(executor.run(workflow_config(), {"question": "from runtime"}))

    assert result == {"answer": "from runtime"}
    assert executor.runtime_state is not None
    assert executor.runtime_state.node_run_steps == 3
    assert executor.runtime_state.variable_pool.get("start", "question") == "from runtime"


def test_each_run_gets_a_fresh_runtime_state():
    executor = WorkflowExecutor()
    asyncio.run(executor.run(workflow_config(), {"question": "first"}))
    first_runtime = executor.runtime_state
    asyncio.run(executor.run(workflow_config(), {"question": "second"}))

    assert executor.runtime_state is not first_runtime
    assert executor.runtime_state is not None
    assert executor.runtime_state.variable_pool.get("start", "question") == "second"


def test_system_and_environment_namespaces_are_seeded():
    runtime = GraphRuntimeState.bootstrap(
        "start",
        {},
        system_variables={"run_id": "run-1"},
        environment_variables={"region": "cn"},
    )

    assert runtime.variable_pool.get("sys", "run_id") == "run-1"
    assert runtime.variable_pool.get("env", "region") == "cn"
