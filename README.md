# ⚡ DiegoC Workflow

**个人学习项目** — 从零理解 Workflow 引擎的核心原理。
- DAG 有向无环图的拓扑排序
- BFS 分层实现并行执行
- 变量池在节点间传递数据
- SSE 流式推送执行状态

---

## 架构

```mermaid
flowchart TD
    A["POST /api/workflow/run<br/>JSON: nodes + edges + inputs"] --> B["GraphRuntimeState<br/>初始化 sys / env / 用户输入"]
    A --> C["WorkflowGraph.init<br/>NodeFactory + Edge 解析静态图"]
    B --> D["WorkflowExecutor<br/>逐层 asyncio.gather 并行执行"]
    C --> D
    D --> E["VariablePool<br/>执行时解析输入，完成后写回输出"]
    E --> F{"还有下一层?"}
    F -->|是| D
    F -->|否| G["SSE Stream<br/>实时推送执行状态"]
    G --> H["workflow_finish<br/>返回最终结果"]
```

## 后端怎么设计 Workflow 引擎

### 1. 数据结构

Workflow 就是一个 DAG，用 JSON 描述：

```json
{
  "nodes": [
    {"id": "start", "data": {"type": "start", "config": {}, "input_mapping": {}}},
    {"id": "llm1", "data": {"type": "llm", "config": {"model": "deepseek-v4-pro", "api_key": "sk-xxx"}, "input_mapping": {"prompt": "start.question"}}},
    {"id": "end", "data": {"type": "end", "config": {}, "input_mapping": {"answer": "llm1.text"}}}
  ],
  "edges": [
    {"source": "start", "target": "llm1"},
    {"source": "llm1", "target": "end"}
  ]
}
```

- `nodes` — 每个节点有 id、type、config（节点特定参数）、input_mapping（从上游取哪个变量）
- `edges` — 有向边，source → target 决定执行顺序

JSON 只描述静态结构。`WorkflowGraph.init()` 遍历 `nodes`，由
`NodeFactory` 根据 `data.type` 创建具体节点；遍历 `edges` 创建 `Edge`
对象并建立邻接关系。节点构造时只接收静态 `config`、`input_mapping`
和同一次执行共享的运行态引用，不接收用户输入。

每次调用 `run()` 都会新建 `GraphRuntimeState`，把系统变量写入 `sys`、
环境变量写入 `env`、用户输入写入 start 节点命名空间。节点真正执行时
才通过 `resolve_inputs()` 从 `VariablePool` 解析输入，执行结果再写回池中。
因此同一份 Graph JSON 可以用于不同请求，运行数据不会进入节点配置。

### 2. 拓扑排序 — 决定谁先谁后

从 edges 构建邻接表 + 入度表，用 Kahn 算法 (BFS) 生成执行顺序：

```python
def topological_sort(nodes, edges):
    in_degree = {n.id: 0 for n in nodes}
    adj = {n.id: [] for n in nodes}
    for e in edges:
        adj[e.source].append(e.target)
        in_degree[e.target] += 1

    queue = deque([nid for nid, d in in_degree.items() if d == 0])
    order = []
    while queue:
        nid = queue.popleft()
        order.append(nid)
        for neighbor in adj[nid]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    return order
```

如果 `len(order) != len(nodes)` 则存在循环依赖。

### 3. BFS 分层 — 找出哪些可以并行

Kahn 算法天然支持分层：每次 queue 里同时出现的节点没有依赖关系，可以并行：

```python
def bfs_levels(nodes, edges):
    # 构建邻接表和入度
    current_level = deque([入度为0的节点])
    levels = []

    while current_level:
        levels.append(list(current_level))   # 同一层可以并行
        next_level = deque()
        for nid in current_level:
            for neighbor in adj[nid]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    next_level.append(neighbor)
        current_level = next_level

    return levels  # [[A], [B, C], [D]]
```

同层节点用 `asyncio.gather` 并行执行，层与层之间是屏障。

### 4. 变量池 — 节点间传数据

每个节点执行完后把输出写入全局 pool，下游通过 input_mapping 按需读取：

```python
class VariablePool:
    _data = {}  # {"node_id": {"field": value}}

    def set(node_id, outputs):   # 写入
    def get(node_id, field):     # 读取单个值
    def resolve(input_mapping):  # 批量解析 → 组装成当前节点的输入
```

数据流：`上游输出 → pool.set() → pool.get() → 下游输入`，解耦节点间依赖。

### 5. 节点注册 + 工厂模式

每种节点类型（LLM、Code、HTTP 等）实现 `Node` 基类，通过 `NodeRegistry` 注册：

```python
class Node(ABC):
    node_type: str                        # "llm", "http", "code", ...
    @abstractmethod
    async def run(inputs) -> dict: ...    # 异步执行，返回输出

class NodeRegistry:
    _registry = {}                        # {"llm": LLMNode, "http": HTTPNode, ...}
    @classmethod
    def create(node_type, node_id, config) -> Node: ...
```

新增节点类型只需继承 `Node` + 调用 `NodeRegistry.register()`。

### 6. 执行引擎 — asyncio 并行

```python
class WorkflowExecutor:
    async def run(graph_config, user_inputs):
        levels = graph.bfs_levels()  # 分层

        for level in levels:
            # 同层节点并行
            tasks = [node.run(inputs) for node in level]
            results = await asyncio.gather(*tasks)

            # 写入变量池
            for nid, outputs in results:
                pool.set(nid, outputs)
```

单线程 + asyncio 没有 GIL 问题 — 节点做 I/O（调 LLM API、发 HTTP 请求）时 `await` 自动让出，其他协程继续跑。

## 节点类型

| 节点 | 功能 | 典型配置 |
|------|------|----------|
| **start** | 入口，透传用户输入 | `variables: [{name, type}]` |
| **llm** | 调 OpenAI 兼容 API | `model, api_key, base_url, system_prompt, user_prompt` |
| **code** | 执行 Python 片段 | `code` |
| **http** | 发 HTTP 请求 | `method, url, headers, body` |
| **end** | 出口，组装输出 | `output_fields` |

## 项目结构

```
DiegoC-workflow/
├── README.md
├── test_workflow.json        # 示例: 搜 LeetCode + LLM 写代码
└── backend/
    ├── main.py               # FastAPI 入口
    ├── requirements.txt
    ├── engine/
    │   ├── variable_pool.py  # 变量池
    │   ├── graph.py          # DAG + 拓扑排序 + BFS 分层
    │   ├── node_registry.py  # 节点注册表
    │   └── executor.py       # asyncio 执行器 + SSE
    └── nodes/
        ├── base.py           # Node 抽象基类
        ├── start_node.py
        ├── llm_node.py
        ├── code_node.py
        ├── http_node.py
        └── end_node.py
```

## License

MIT
