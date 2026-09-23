import asyncio
import json
import random
import threading
import time
from collections import defaultdict, deque
from typing import Dict, List, Optional, Set, Tuple

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

app = FastAPI(title="DAG Workflow Engine")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

VALID_STRATEGIES = {"fifo", "priority", "max_concurrent"}
MAX_RETRIES = 3
FAILURE_RATE = 0.12
CB_THRESHOLD = 3
CB_COOLDOWN = 5.0
TICK = 0.2

# 任务节点状态
ST_PENDING = "PENDING"
ST_RUNNING = "RUNNING"
ST_SUCCESS = "SUCCESS"
ST_FAILED = "FAILED"
ST_BLOCKED = "BLOCKED"  # 处于环上 / 依赖环上节点，无法排定先后

# 工作流状态：排队待执行 -> 执行中 -> 结束
WF_PENDING = "PENDING"
WF_RUNNING = "RUNNING"
WF_FINISHED = "FINISHED"

# ---------------------------------------------------------------------------
# 请求模型
# ---------------------------------------------------------------------------


class NodeIn(BaseModel):
    id: str
    name: str
    deps: List[str] = []
    duration: float = 1.5
    priority: int = 5
    x: float = 0.0
    y: float = 0.0


class WorkflowCreate(BaseModel):
    name: str = "data-pipeline"
    workers: int = Field(default=3, ge=1, le=10)
    strategy: str = "fifo"
    # 内置编排模板：dag=标准流水线，cyclic=含互相等待环节的编排
    template: str = "dag"
    nodes: Optional[List[NodeIn]] = None


class RunRequest(BaseModel):
    workflowId: int
    # 并发上限/优先级策略在创建时绑定到工作流；执行时若传入且不一致则拒绝，
    # 保证同一份编排的不同执行之间配置一致。
    workers: Optional[int] = None
    strategy: Optional[str] = None


# ---------------------------------------------------------------------------
# 内存存储（带锁）
# ---------------------------------------------------------------------------

WORKFLOWS: Dict[int, dict] = {}
WORKFLOW_LOCK = threading.RLock()
NEXT_ID = 1
STOP_FLAGS: Dict[int, threading.Event] = {}
LOOP: Optional[asyncio.AbstractEventLoop] = None


# WebSocket 客户端按工作流 id 订阅（None 表示尚未订阅）
class WSClient:
    def __init__(self, ws: WebSocket):
        self.ws = ws
        self.wf_id: Optional[int] = None


ACTIVE_CLIENTS: List[WSClient] = []


@app.on_event("startup")
def _save_loop() -> None:
    global LOOP
    LOOP = asyncio.get_event_loop()


# ---------------------------------------------------------------------------
# 编排模板
# ---------------------------------------------------------------------------


def template_dag(name: str, workers: int, strategy: str) -> dict:
    """标准 DAG 流水线"""
    raw = [
        ("extract", "数据提取", [], 2.0, 1, 0, 0),
        ("validate", "数据校验", ["extract"], 1.5, 2, 0, 1),
        ("clean_a", "清洗分支A", ["validate"], 1.8, 3, -1, 2),
        ("clean_b", "清洗分支B", ["validate"], 1.2, 3, 1, 2),
        ("transform", "数据转换", ["clean_a"], 3.0, 4, -1, 3),
        ("enrich", "数据增强", ["clean_a", "clean_b"], 2.0, 4, 0.5, 3),
        ("aggregate", "聚合计算", ["transform", "enrich"], 2.5, 5, -0.3, 4),
        ("quality", "质量检查", ["aggregate"], 1.0, 6, -0.3, 5),
        ("export_db", "入库", ["quality"], 1.8, 7, -1, 6),
        ("export_report", "报表生成", ["quality"], 2.2, 7, 0.5, 6),
        ("notify", "通知", ["export_db", "export_report"], 0.5, 8, -0.3, 7),
    ]
    nodes = [
        {
            "id": nid, "name": nname, "deps": deps, "duration": dur,
            "priority": pri,
            "x": x * 2.5 + 2.5, "y": y * 0.9,
        }
        for nid, nname, deps, dur, pri, x, y in raw
    ]
    return _assemble(name, workers, strategy, nodes)


def template_cyclic(name: str, workers: int, strategy: str) -> dict:
    """带环的编排：clean_a <-> transform 互相等待；独立任务不受影响"""
    raw = [
        ("extract", "数据提取", [], 2.0, 1, 0, 0),
        ("validate", "数据校验", ["extract"], 1.5, 2, 0, 1),
        ("clean_a", "清洗分支A", ["validate", "transform"], 1.8, 3, -1.4, 2),
        ("clean_b", "清洗分支B", ["validate"], 1.2, 3, 1.4, 2),
        ("transform", "数据转换", ["clean_a"], 3.0, 4, -1.4, 3),
        ("enrich", "数据增强", ["clean_b"], 2.0, 4, 1.4, 3),
        ("aggregate", "聚合计算", ["enrich"], 2.5, 5, 1.4, 4),
        ("notify", "通知", ["aggregate"], 0.5, 6, 1.4, 5),
    ]
    nodes = [
        {
            "id": nid, "name": nname, "deps": deps, "duration": dur,
            "priority": pri,
            "x": x * 2.5 + 2.5, "y": y * 0.9,
        }
        for nid, nname, deps, dur, pri, x, y in raw
    ]
    return _assemble(name, workers, strategy, nodes)


def _assemble(name: str, workers: int, strategy: str, nodes: List[dict]) -> dict:
    edges = [[d, n["id"]] for n in nodes for d in n["deps"]]
    return {"name": name, "workers": workers, "strategy": strategy,
            "nodes": nodes, "edges": edges}


# ---------------------------------------------------------------------------
# 校验 / 环检测
# ---------------------------------------------------------------------------


def find_cycles(nodes: List[dict], edges: List[List[str]]) -> List[List[str]]:
    """Tarjan 强连通分量，返回大小 >1（含自环）的分量，即互相等待的环节点。"""
    adj: Dict[str, List[str]] = defaultdict(list)
    ids = {n["id"] for n in nodes}
    for u, v in edges:
        if u in ids and v in ids:
            adj[u].append(v)

    index = 0
    stack: List[str] = []
    on_stack: Set[str] = set()
    indices: Dict[str, int] = {}
    low: Dict[str, int] = {}
    cycles: List[List[str]] = []

    def strongconnect(v: str) -> None:
        nonlocal index
        indices[v] = index
        low[v] = index
        index += 1
        stack.append(v)
        on_stack.add(v)

        for w in adj[v]:
            if w not in indices:
                strongconnect(w)
                low[v] = min(low[v], low[w])
            elif w in on_stack:
                low[v] = min(low[v], indices[w])

        if low[v] == indices[v]:
            comp: List[str] = []
            while True:
                w = stack.pop()
                on_stack.remove(w)
                comp.append(w)
                if w == v:
                    break
            is_self_loop = v in adj[v]
            if len(comp) > 1 or is_self_loop:
                cycles.append(comp)

    for n in nodes:
        if n["id"] not in indices:
            strongconnect(n["id"])
    return cycles


def schedulable_nodes(nodes: List[dict], edges: List[List[str]],
                      cyclic: Set[str]) -> Tuple[Set[str], Set[str]]:
    """Kahn 剥离：返回 (可排定先后的节点, 被环阻塞的节点)。"""
    ids = {n["id"] for n in nodes}
    in_deg = {nid: 0 for nid in ids}
    adj: Dict[str, List[str]] = defaultdict(list)
    for u, v in edges:
        if u in ids and v in ids:
            adj[u].append(v)
            in_deg[v] += 1

    queue = deque(nid for nid, d in in_deg.items() if d == 0)
    ok: Set[str] = set()
    while queue:
        u = queue.popleft()
        ok.add(u)
        for v in adj[u]:
            in_deg[v] -= 1
            if in_deg[v] == 0:
                queue.append(v)
    blocked = ids - ok
    return ok, blocked


def validate_and_build(req: WorkflowCreate) -> Tuple[Optional[dict], List[dict]]:
    """校验创建请求。返回 (workflow_dict 或 None, 字段级错误列表)。"""
    errors: List[dict] = []
    name = (req.name or "").strip()

    # 名称：为空
    if not name:
        errors.append({"field": "name", "message": "工作流名称不能为空，请填写名称后再创建"})
    else:
        # 名称：与已有工作流重复（大小写不敏感、去空格）
        with WORKFLOW_LOCK:
            for wf in WORKFLOWS.values():
                if wf["name"].strip().lower() == name.lower():
                    errors.append({
                        "field": "name",
                        "message": f"工作流名称“{name}”已存在，名称不允许重复，请更换后再创建",
                    })
                    break

    if req.strategy not in VALID_STRATEGIES:
        errors.append({
            "field": "strategy",
            "message": f"调度策略“{req.strategy}”不合法，仅支持 fifo / priority / max_concurrent",
        })

    nodes = req.nodes
    if nodes is None:
        if req.template == "cyclic":
            wf = template_cyclic(name or "data-pipeline", req.workers, req.strategy)
        elif req.template == "dag":
            wf = template_dag(name or "data-pipeline", req.workers, req.strategy)
        else:
            errors.append({
                "field": "template",
                "message": f"编排模板“{req.template}”不合法，仅支持 dag / cyclic",
            })
            return None, errors
        nodes_dict = wf["nodes"]
        edges = wf["edges"]
    else:
        nodes_dict = [n.model_dump() for n in nodes]
        edges = [[d, n.id] for n in nodes for d in n.deps]

        if not nodes_dict:
            errors.append({"field": "nodes", "message": "编排至少需要包含一个环节"})
            return None, errors

        # 节点 id 重复
        seen: Set[str] = set()
        dup: Set[str] = set()
        for n in nodes_dict:
            n["id"] = n["id"].strip()
            n["name"] = n["name"].strip() or n["id"]
            if n["id"] in seen:
                dup.add(n["id"])
            seen.add(n["id"])
        for nid in sorted(dup):
            errors.append({"field": f"nodes.{nid}",
                           "message": f"环节标识“{nid}”重复，每个环节需要唯一的标识"})

        # 依赖的环节不存在
        for n in nodes_dict:
            for d in n["deps"]:
                if d not in seen:
                    errors.append({
                        "field": f"nodes.{n['id']}.deps",
                        "message": f"环节“{n['name']}”依赖的“{d}”不存在，无法建立关联",
                    })
            # 自环单独给出更直白的提示（find_cycles 也能检出）
            if n["id"] in n["deps"]:
                errors.append({
                    "field": f"nodes.{n['id']}.deps",
                    "message": f"环节“{n['name']}”依赖自身，自己等待自己无法排定先后",
                })

    if errors:
        return None, errors

    # 环检测：不阻止创建，但要提示“互相等待、无法排定先后”
    cycles = find_cycles(nodes_dict, edges)
    cyclic: Set[str] = {nid for c in cycles for nid in c}
    ok, blocked = schedulable_nodes(nodes_dict, edges, cyclic)

    warnings: List[dict] = []
    if cycles:
        warnings.append({
            "type": "CYCLE",
            "cycles": cycles,
            "blockedNodeIds": sorted(blocked),
            "message": (
                "以下环节彼此关联、互相等待，无法排定先后顺序："
                + "；".join(" → ".join(c + [c[0]]) for c in cycles)
                + "。这些环节及其下游将被跳过，未受影响的环节照常可以执行。"
            ),
        })

    return {
        "name": name,
        "workers": req.workers,
        "strategy": req.strategy,
        "nodes": nodes_dict,
        "edges": edges,
        "warnings": warnings,
        "cycleNodeIds": sorted(cyclic),
        "blockedNodeIds": sorted(blocked),
    }, []


# ---------------------------------------------------------------------------
# 工作流存储结构 / 快照
# ---------------------------------------------------------------------------


def _new_workflow(spec: dict) -> dict:
    global NEXT_ID
    wf_id = NEXT_ID
    NEXT_ID += 1

    nodes = []
    for n in spec["nodes"]:
        blocked = n["id"] in set(spec["blockedNodeIds"])
        nodes.append({
            "id": n["id"],
            "name": n["name"],
            "deps": list(n["deps"]),
            "duration": n["duration"],
            "priority": n.get("priority", 5),
            "x": n["x"], "y": n["y"],
            "status": ST_BLOCKED if blocked else ST_PENDING,
            "retries": 0,
            "startTime": None,
            "endTime": None,
        })

    return {
        "id": wf_id,
        "name": spec["name"],
        "workers": spec["workers"],
        "strategy": spec["strategy"],
        "nodes": nodes,
        "edges": [list(e) for e in spec["edges"]],
        "status": WF_PENDING,
        "logs": [],
        "circuitBreakers": {},
        "warnings": spec["warnings"],
        "cycleNodeIds": spec["cycleNodeIds"],
        "blockedNodeIds": spec["blockedNodeIds"],
        "createdAt": time.time(),
    }


def snapshot(wf: dict) -> dict:
    with WORKFLOW_LOCK:
        return {
            "workflow": {
                "id": wf["id"],
                "name": wf["name"],
                "nodes": json.loads(json.dumps(wf["nodes"])),
                "edges": [list(e) for e in wf["edges"]],
                "status": wf["status"],
                "workers": wf["workers"],
                "strategy": wf["strategy"],
                "warnings": list(wf["warnings"]),
                "cycleNodeIds": list(wf["cycleNodeIds"]),
                "blockedNodeIds": list(wf["blockedNodeIds"]),
            },
            "logs": list(wf["logs"][-200:]),
            "circuitBreakers": [
                {"taskId": k, **v} for k, v in wf["circuitBreakers"].items()
            ],
            "completed": wf["status"] == WF_FINISHED,
        }


def broadcast(wf: dict) -> None:
    if LOOP is None:
        return
    payload = json.dumps(snapshot(wf))
    dead: List[WSClient] = []
    for c in ACTIVE_CLIENTS:
        if c.wf_id == wf["id"]:
            try:
                asyncio.run_coroutine_threadsafe(c.ws.send_text(payload), LOOP)
            except Exception:
                dead.append(c)
    for c in dead:
        if c in ACTIVE_CLIENTS:
            ACTIVE_CLIENTS.remove(c)


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------


@app.get("/api/workflows")
def list_workflows():
    with WORKFLOW_LOCK:
        return [snapshot(wf)["workflow"] for wf in WORKFLOWS.values()]


@app.get("/api/workflow/{wf_id}")
def get_workflow(wf_id: int):
    with WORKFLOW_LOCK:
        wf = WORKFLOWS.get(wf_id)
        if not wf:
            raise HTTPException(404, f"工作流 {wf_id} 不存在")
        return snapshot(wf)


@app.post("/api/workflow")
def create_workflow(req: WorkflowCreate):
    spec, errors = validate_and_build(req)
    if errors:
        # 指出具体哪一项不合理，不允许继续创建
        raise HTTPException(422, {"errors": errors})

    with WORKFLOW_LOCK:
        wf = _new_workflow(spec)
        WORKFLOWS[wf["id"]] = wf

    return snapshot(wf)


@app.post("/api/run")
def run_workflow(req: RunRequest):
    with WORKFLOW_LOCK:
        wf = WORKFLOWS.get(req.workflowId)
        if not wf:
            raise HTTPException(404, f"工作流 {req.workflowId} 不存在")

        # 并发上限 / 优先级策略：不同执行之间必须一致
        if req.workers is not None and req.workers != wf["workers"]:
            raise HTTPException(
                400,
                f"并发上限必须与创建时保持一致（{wf['workers']}），"
                f"本次请求传入 {req.workers}，已拒绝",
            )
        if req.strategy is not None and req.strategy != wf["strategy"]:
            raise HTTPException(
                400,
                f"优先级策略必须与创建时保持一致（{wf['strategy']}），"
                f"本次请求传入 {req.strategy}，已拒绝",
            )

        if wf["status"] == WF_RUNNING:
            raise HTTPException(409, "工作流正在执行中，请勿重复发起")

        # 已结束的工作流若要再次执行，需先重置
        if wf["status"] == WF_FINISHED:
            raise HTTPException(409, "工作流已结束，请先重置后再执行")

        wf["status"] = WF_RUNNING
        stop = threading.Event()
        STOP_FLAGS[wf["id"]] = stop

    t = threading.Thread(target=execute_workflow, args=(wf["id"], stop), daemon=True)
    t.start()
    return snapshot(wf)


@app.post("/api/workflow/{wf_id}/stop")
def stop_workflow(wf_id: int):
    """中途打断：回到排队待执行，已完成的环节保留。"""
    with WORKFLOW_LOCK:
        wf = WORKFLOWS.get(wf_id)
        if not wf:
            raise HTTPException(404, f"工作流 {wf_id} 不存在")
        if wf["status"] != WF_RUNNING:
            raise HTTPException(409, "仅执行中的工作流可以被打断")
        STOP_FLAGS.get(wf_id, threading.Event()).set()
    return {"message": "已请求中断，工作流将回到排队待执行，已完成的环节会被保留"}


@app.put("/api/workflow/{wf_id}/reset")
def reset_workflow(wf_id: int):
    with WORKFLOW_LOCK:
        wf = WORKFLOWS.get(wf_id)
        if not wf:
            raise HTTPException(404, f"工作流 {wf_id} 不存在")
        if wf["status"] == WF_RUNNING:
            raise HTTPException(409, "执行中无法重置，请先打断")

        blocked = set(wf["blockedNodeIds"])
        for n in wf["nodes"]:
            n["status"] = ST_BLOCKED if n["id"] in blocked else ST_PENDING
            n["retries"] = 0
            n["startTime"] = None
            n["endTime"] = None
        wf["logs"] = []
        wf["circuitBreakers"] = {}
        wf["status"] = WF_PENDING
    return snapshot(wf)


# ---------------------------------------------------------------------------
# 执行引擎
# ---------------------------------------------------------------------------


def _add_log(wf: dict, task_id: str, status: str, message: str) -> None:
    wf["logs"].append({
        "taskId": task_id,
        "status": status,
        "timestamp": time.time(),
        "message": message,
    })


def execute_workflow(wf_id: int, stop: threading.Event) -> None:
    with WORKFLOW_LOCK:
        wf = WORKFLOWS[wf_id]
        nodes = wf["nodes"]
        edges = wf["edges"]
        workers = wf["workers"]
        strategy = wf["strategy"]
        node_map = {n["id"]: n for n in nodes}
        schedulable = {n["id"] for n in nodes if n["status"] != ST_BLOCKED}

        in_degree: Dict[str, int] = defaultdict(int)
        adj: Dict[str, List[str]] = defaultdict(list)
        for u, v in edges:
            if u in schedulable and v in schedulable:
                adj[u].append(v)
                in_degree[v] += 1

        # 恢复执行：已成功的环节保留，其下游相应减少入度；
        # 被打断时正在执行的环节已回到 PENDING。
        ready: List[str] = []
        for nid in schedulable:
            if node_map[nid]["status"] == ST_SUCCESS:
                for v in adj[nid]:
                    in_degree[v] -= 1
        for nid in schedulable:
            node = node_map[nid]
            if node["status"] in (ST_PENDING, ST_FAILED) and in_degree[nid] == 0:
                ready.append(nid)

        cb_state = wf["circuitBreakers"]
        running: Dict[str, dict] = {}
        done_count = sum(1 for n in nodes if n["status"] == ST_SUCCESS)
        total = len(schedulable)

        def pick_ready() -> Optional[str]:
            nonlocal ready
            if not ready:
                return None
            if strategy == "priority":
                # priority 值越小优先级越高
                ready.sort(key=lambda t: (node_map[t]["priority"], t))
            elif strategy == "max_concurrent":
                # 优先释放下游扇出大的环节，尽快扩大可并发面
                ready.sort(key=lambda t: (-len(adj[t]), node_map[t]["priority"], t))
            return ready.pop(0)

    _add_log(wf, "-", "RUNNING",
             f"工作流进入执行中（并发上限 {workers}，策略 {strategy}）")
    if wf["blockedNodeIds"]:
        _add_log(wf, "-", "BLOCKED",
                 f"环节 {', '.join(wf['blockedNodeIds'])} 因循环等待被跳过，其余环节照常执行")
    broadcast(wf)

    def finish_run(interrupted: bool) -> None:
        with WORKFLOW_LOCK:
            if interrupted:
                # 回到排队待执行；正在跑的环节回退，已完成部分保留
                for tid in list(running.keys()):
                    node = node_map[tid]
                    node["status"] = ST_PENDING
                    node["startTime"] = None
                running.clear()
                wf["status"] = WF_PENDING
                _add_log(wf, "-", "PENDING", "执行被打断，已回到排队待执行；已完成的环节予以保留，可继续执行")
            else:
                wf["status"] = WF_FINISHED
                _add_log(wf, "-", "SUCCESS", "工作流结束")
            broadcast(wf)

    while True:
        if stop.is_set():
            finish_run(True)
            return

        # 派发可执行环节（受并发上限约束）
        with WORKFLOW_LOCK:
            while len(running) < workers:
                tid = pick_ready()
                if tid is None:
                    break
                node = node_map[tid]
                cb = cb_state.setdefault(tid, {
                    "failureCount": 0, "state": "CLOSED", "cooldownUntil": 0
                })
                now = time.time()
                if cb["state"] == "OPEN" and now < cb["cooldownUntil"]:
                    ready.append(tid)  # 熔断冷却中，稍后再试
                    continue
                if cb["state"] == "OPEN":
                    cb["state"] = "HALF_OPEN"

                node["status"] = ST_RUNNING
                node["startTime"] = now
                runtime = node["duration"] * random.uniform(0.7, 1.3)
                running[tid] = {
                    "end_time": now + runtime,
                    "will_fail": random.random() < FAILURE_RATE,
                }
                _add_log(wf, tid, "RUNNING", f"开始执行 {node['name']}")

            # 检查完成的环节
            now = time.time()
            finished: List[str] = []
            for tid, info in running.items():
                if now < info["end_time"]:
                    continue
                node = node_map[tid]
                cb = cb_state.setdefault(tid, {
                    "failureCount": 0, "state": "CLOSED", "cooldownUntil": 0
                })
                if info["will_fail"] and node["retries"] < MAX_RETRIES:
                    node["retries"] += 1
                    node["status"] = ST_PENDING
                    node["startTime"] = None
                    ready.append(tid)
                    cb["failureCount"] += 1
                    _add_log(wf, tid, "FAILED",
                             f"{node['name']} 执行失败，重试 {node['retries']}/{MAX_RETRIES}")
                    if cb["failureCount"] >= CB_THRESHOLD:
                        cb["state"] = "OPEN"
                        cb["cooldownUntil"] = now + CB_COOLDOWN
                        _add_log(wf, tid, "CIRCUIT_OPEN",
                                 f"{node['name']} 连续失败 {CB_THRESHOLD} 次，熔断 {CB_COOLDOWN:.0f}s")
                else:
                    if info["will_fail"]:
                        node["status"] = ST_FAILED
                        _add_log(wf, tid, "FAILED",
                                 f"{node['name']} 重试 {MAX_RETRIES} 次后仍失败，标记为失败")
                    else:
                        node["status"] = ST_SUCCESS
                        node["endTime"] = now
                        done_count += 1
                        cb["failureCount"] = 0
                        cb["state"] = "CLOSED"
                        _add_log(wf, tid, "SUCCESS", f"完成 {node['name']}")
                    # 无论成功/最终失败都释放下游
                    for v in adj[tid]:
                        in_degree[v] -= 1
                        if in_degree[v] == 0 and node_map[v]["status"] in (ST_PENDING, ST_FAILED):
                            ready.append(v)
                finished.append(tid)

            for tid in finished:
                del running[tid]

            should_stop = not ready and not running

        broadcast(wf)

        if should_stop:
            finish_run(False)
            return
        time.sleep(TICK)


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    client = WSClient(ws)
    ACTIVE_CLIENTS.append(client)
    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except ValueError:
                continue
            if msg.get("type") == "subscribe":
                wf_id = int(msg.get("workflowId"))
                client.wf_id = wf_id
                with WORKFLOW_LOCK:
                    wf = WORKFLOWS.get(wf_id)
                if wf:
                    # 页面重开：立即恢复最后一次进展
                    await ws.send_text(json.dumps(snapshot(wf)))
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        if client in ACTIVE_CLIENTS:
            ACTIVE_CLIENTS.remove(client)
