"""Per-run state shared by all nodes in one workflow execution."""

from dataclasses import dataclass, field
import time
from typing import Any

from .variable_pool import VariablePool


@dataclass(slots=True)
class GraphRuntimeState:
    """Mutable state that belongs to one run, never to the static graph config."""

    variable_pool: VariablePool = field(default_factory=VariablePool)
    start_at: float = field(default_factory=time.perf_counter)
    outputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    node_run_steps: int = 0

    @classmethod
    def bootstrap(
        cls,
        root_node_id: str,
        user_inputs: dict[str, Any],
        *,
        system_variables: dict[str, Any] | None = None,
        environment_variables: dict[str, Any] | None = None,
    ) -> "GraphRuntimeState":
        """Create a fresh runtime and seed request-scoped namespaces."""
        state = cls()
        state.variable_pool.set("sys", dict(system_variables or {}))
        state.variable_pool.set("env", dict(environment_variables or {}))
        state.variable_pool.set(root_node_id, dict(user_inputs))
        return state

    def record_output(self, node_id: str, outputs: dict[str, Any]) -> None:
        self.variable_pool.set(node_id, outputs)
        self.outputs[node_id] = outputs
        self.node_run_steps += 1
