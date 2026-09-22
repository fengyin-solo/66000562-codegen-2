import asyncio
import json
import random
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .orchestration import STRATEGIES, default_graph, nodes_to_graph, select_ready

app = FastAPI(title="DAG Workflow Engine")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ACTIVE_CLIENTS: Set[WebSocket] = set()
WORKFLOWS: Dict[int, Dict[str, Any]] = {}
EXECUTIONS: Dict[int, Dict[str, Any]] = {}
NEXT_WORKFLOW_ID = 1
DATA_LOCK = asyncio.Lock()
STOP_EVENTS: Dict[int, asyncio.Event] = {}


class NodeCreate(BaseModel):
    id: str
    name: str
    deps: List[str] = Field(default_factory=list)
    duration: float = 1.5
    priority: int = 3
    x: Optional[float] = None
    y: Optional[float] = None


class WorkflowCreate(BaseModel):
    name: str
    nodes: Optional[List[NodeCreate]] = None
    workers: int = 3
    strategy: str = "fifo"


class RunRequest(BaseModel):
    workflowId: int
    workers: Optional[int] = None
    strategy: Optional[str] = None


def public_node(node: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": node["id"],
        "name": node["name"],
        "deps": list(node["deps"]),
        "duration": node["duration"],
        "priority": node["priority"],
        "x": node["x"],
        "y": node["y"],
        "status": node["status"],
        "startTime": node["startTime"],
        "endTime": node["endTime"],
        "retries": node["retries"],
        "blockedByCycle": node.get("blockedByCycle", False),
        "blockedReason": node.get("blockedReason"),
    }


def public_workflow(workflow: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": workflow["id"],
        "name": workflow["name"],
        "status": workflow["status"],
        "nodes": [public_node(node) for node in workflow["nodes"]],
        "edges": [list(edge) for edge in workflow["edges"]],
        "warnings": list(workflow.get("warnings", [])),
        "executionConfig": dict(workflow["executionConfig"]),
        "createdAt": workflow["createdAt"],
        "updatedAt": workflow["updatedAt"],
    }


def public_execution(execution: Dict[str, Any]) -> Dict[str, Any]:
    workflow = WORKFLOWS[execution["workflowId"]]
    return {
        "id": execution["id"],
        "workflowId": execution["workflowId"],
        "workflow": public_workflow(workflow),
        "logs": list(execution["logs"]),
        "circuitBreakers": list(execution["circuitBreakers"].values()),
        "completed": execution["status"] == "FINISHED",
        "interrupted": execution["status"] == "QUEUED",
        "status": execution["status"],
        "warnings": list(workflow.get("warnings", [])),
        "executionConfig": dict(workflow["executionConfig"]),
    }


def append_log(execution: Dict[str, Any], task_id: str, status: str, message: str) -> None:
    execution["logs"].append({
        "taskId": task_id,
        "status": status,
        "timestamp": time.time(),
        "message": message,
    })


async def broadcast(execution: Dict[str, Any]) -> None:
    payload = json.dumps(public_execution(execution), ensure_ascii=False)
    stale: List[WebSocket] = []
    for client in list(ACTIVE_CLIENTS):
        try:
            await client.send_text(payload)
        except Exception:
            stale.append(client)
    for client in stale:
        ACTIVE_CLIENTS.discard(client)


def validate_runtime_config(workers: int, strategy: str) -> None:
    if not 1 <= workers <= 10:
        raise HTTPException(status_code=400, detail={"field": "workers", "message": "并发上限必须在 1 到 10 之间"})
    if strategy not in STRATEGIES:
        raise HTTPException(
            status_code=400,
            detail={"field": "strategy", "message": "调度策略只能是 fifo、priority 或 max_concurrent"},
        )


@app.post("/api/workflow")
async def create_workflow(req: WorkflowCreate):
    global NEXT_WORKFLOW_ID

    name = req.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail={"field": "name", "message": "工作流名称不能为空"})
    if any(item["name"] == name for item in WORKFLOWS.values()):
        raise HTTPException(status_code=400, detail={"field": "name", "message": "工作流名称「{}」已存在".format(name)})
    validate_runtime_config(req.workers, req.strategy)

    raw_nodes = None
    if req.nodes is not None:
        raw_nodes = [node.model_dump() for node in req.nodes]
    else:
        raw_nodes = default_graph()["nodes"]

    try:
        graph = nodes_to_graph(raw_nodes)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"field": "nodes", "message": str(exc)})

    async with DATA_LOCK:
        workflow_id = NEXT_WORKFLOW_ID
        NEXT_WORKFLOW_ID += 1
        now = time.time()
        workflow = {
            "id": workflow_id,
            "name": name,
            "status": "QUEUED",
            "nodes": graph["nodes"],
            "edges": graph["edges"],
            "warnings": graph["warnings"],
            "executionConfig": {"workers": req.workers, "strategy": req.strategy},
            "createdAt": now,
            "updatedAt": now,
        }
        WORKFLOWS[workflow_id] = workflow
        return public_workflow(workflow)


@app.get("/api/workflow/{workflow_id}")
async def get_workflow(workflow_id: int):
    workflow = WORKFLOWS.get(workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail={"message": "工作流不存在"})
    return public_workflow(workflow)


@app.get("/api/execution/{execution_id}")
async def get_execution(execution_id: int):
    execution = EXECUTIONS.get(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail={"message": "执行不存在"})
    return public_execution(execution)


@app.get("/api/workflow/{workflow_id}/execution")
async def get_workflow_execution(workflow_id: int):
    if workflow_id not in WORKFLOWS:
        raise HTTPException(status_code=404, detail={"message": "工作流不存在"})
    execution = next((item for item in EXECUTIONS.values() if item["workflowId"] == workflow_id), None)
    if not execution:
        raise HTTPException(status_code=404, detail={"message": "该工作流尚无执行记录"})
    return public_execution(execution)


@app.post("/api/run")
async def run_workflow(req: RunRequest):
    workflow = WORKFLOWS.get(req.workflowId)
    if not workflow:
        raise HTTPException(status_code=404, detail={"message": "工作流不存在"})

    fixed_config = workflow["executionConfig"]
    if req.workers is not None and req.workers != fixed_config["workers"]:
        raise HTTPException(
            status_code=409,
            detail={"field": "workers", "message": "同一工作流的并发上限在多次执行之间必须保持一致"},
        )
    if req.strategy is not None and req.strategy != fixed_config["strategy"]:
        raise HTTPException(
            status_code=409,
            detail={"field": "strategy", "message": "同一工作流的优先级策略在多次执行之间必须保持一致"},
        )

    execution = next((item for item in EXECUTIONS.values() if item["workflowId"] == workflow["id"]), None)
    if execution and execution["status"] == "RUNNING":
        raise HTTPException(status_code=409, detail={"message": "工作流正在执行中"})
    if execution and execution["status"] == "FINISHED":
        raise HTTPException(status_code=409, detail={"message": "工作流已结束，不能重复执行"})

    async with DATA_LOCK:
        if not execution:
            execution_id = len(EXECUTIONS) + 1
            execution = {
                "id": execution_id,
                "workflowId": workflow["id"],
                "status": "QUEUED",
                "logs": [],
                "circuitBreakers": {},
                "task": None,
            }
            EXECUTIONS[execution_id] = execution
            append_log(execution, workflow["id"], "QUEUED", "工作流已排队待执行")
            for warning in workflow.get("warnings", []):
                append_log(execution, warning["nodeIds"][0], "WARNING", warning["message"])
        else:
            append_log(execution, str(workflow["id"]), "QUEUED", "从待执行状态恢复，已完成的环节继续保留")

        workflow["status"] = "RUNNING"
        workflow["updatedAt"] = time.time()
        execution["status"] = "RUNNING"
        stop_event = asyncio.Event()
        STOP_EVENTS[execution["id"]] = stop_event
        snapshot = public_execution(execution)

    task = asyncio.create_task(execute_workflow(execution["id"], stop_event))
    execution["task"] = task
    return snapshot


@app.post("/api/interrupt/{execution_id}")
async def interrupt_workflow(execution_id: int):
    execution = EXECUTIONS.get(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail={"message": "执行不存在"})
    if execution["status"] != "RUNNING":
        raise HTTPException(status_code=409, detail={"message": "只有执行中的工作流可以打断"})
    STOP_EVENTS[execution_id].set()
    task = execution.get("task")
    if task:
        await asyncio.wait([task], timeout=1.0)
    return public_execution(execution)


async def execute_workflow(execution_id: int, stop_event: asyncio.Event) -> None:
    execution = EXECUTIONS[execution_id]
    workflow = WORKFLOWS[execution["workflowId"]]
    nodes = workflow["nodes"]
    edges = workflow["edges"]
    workers = int(workflow["executionConfig"]["workers"])
    strategy = workflow["executionConfig"]["strategy"]
    node_map = {node["id"]: node for node in nodes}

    async with DATA_LOCK:
        in_degree = defaultdict(int)
        adjacency: Dict[str, List[str]] = defaultdict(list)
        for source, target in edges:
            target_node = node_map[target]
            # Only edges from work not already completed need to gate a resumed run.
            if target_node["status"] != "SUCCESS" and node_map[source]["status"] != "SUCCESS":
                in_degree[target] += 1
                adjacency[source].append(target)

        ready_ids = [
            node["id"] for node in nodes
            if node["status"] == "PENDING" and in_degree[node["id"]] == 0
        ]
        running: Dict[str, Dict[str, Any]] = {}
        execution["status"] = "RUNNING"
        workflow["status"] = "RUNNING"
        await broadcast(execution)

    while ready_ids or running:
        if stop_event.is_set():
            break

        ordered_ready = select_ready(ready_ids, nodes, strategy)
        ready_ids = []
        for task_id in ordered_ready:
            if len(running) >= workers:
                ready_ids.append(task_id)
                continue
            circuit = execution["circuitBreakers"].get(task_id)
            now = time.time()
            if circuit and circuit["state"] == "OPEN" and now < circuit["cooldownUntil"]:
                ready_ids.append(task_id)
                continue

            node = node_map[task_id]
            if circuit and circuit["state"] == "OPEN":
                circuit["state"] = "HALF_OPEN"
            node["status"] = "RUNNING"
            node["startTime"] = now
            will_fail = random.random() < 0.12
            runtime = float(node["duration"]) * random.uniform(0.7, 1.3)
            running[task_id] = {"endTime": now + runtime, "willFail": will_fail}
            append_log(execution, task_id, "RUNNING", "开始执行 {}".format(node["name"]))

        async with DATA_LOCK:
            await broadcast(execution)

        if not running:
            # Every ready task is waiting for a circuit-breaker cooldown.
            await asyncio.sleep(0.2)
            continue

        next_finish_at = min(item["endTime"] for item in running.values())
        wait_seconds = max(0.05, min(0.25, next_finish_at - time.time()))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=wait_seconds)
            interrupted = True
        except asyncio.TimeoutError:
            interrupted = False
        if interrupted:
            break

        now = time.time()
        finished = [task_id for task_id, item in running.items() if now >= item["endTime"]]
        if not finished:
            continue

        async with DATA_LOCK:
            for task_id in finished:
                item = running.pop(task_id)
                node = node_map[task_id]
                circuit = execution["circuitBreakers"].setdefault(
                    task_id,
                    {"taskId": task_id, "failureCount": 0, "state": "CLOSED", "cooldownUntil": 0},
                )

                if item["willFail"] and node["retries"] < 3:
                    node["retries"] += 1
                    node["status"] = "PENDING"
                    node["startTime"] = None
                    circuit["failureCount"] += 1
                    append_log(execution, task_id, "FAILED", "{} 执行失败，重试 {}/3".format(node["name"], node["retries"]))
                    if circuit["failureCount"] >= 3:
                        circuit["state"] = "OPEN"
                        circuit["cooldownUntil"] = now + 5
                        append_log(execution, task_id, "CIRCUIT_OPEN", "{} 已熔断，冷却后重试".format(node["name"]))
                    ready_ids.append(task_id)
                else:
                    node["status"] = "SUCCESS"
                    node["endTime"] = now
                    completed_count += 1
                    circuit["failureCount"] = 0
                    circuit["state"] = "CLOSED"
                    circuit["cooldownUntil"] = 0
                    append_log(execution, task_id, "SUCCESS", "完成 {}".format(node["name"]))
                    for child in adjacency.get(task_id, []):
                        in_degree[child] -= 1
                        if in_degree[child] == 0:
                            ready_ids.append(child)

    async with DATA_LOCK:
        now = time.time()
        interrupted = stop_event.is_set()
        for task_id, item in list(running.items()):
            node = node_map[task_id]
            node["status"] = "PENDING"
            node["startTime"] = None
            append_log(execution, task_id, "QUEUED", "{} 被打断，回到排队待执行".format(node["name"]))
            running.pop(task_id, None)

        if interrupted:
            execution["status"] = "QUEUED"
            workflow["status"] = "QUEUED"
            append_log(execution, str(workflow["id"]), "QUEUED", "工作流已回到待执行，已完成的环节已保留")
        else:
            execution["status"] = "FINISHED"
            workflow["status"] = "FINISHED"
            blocked_count = sum(1 for node in nodes if node["status"] == "BLOCKED")
            if blocked_count:
                append_log(
                    execution,
                    str(workflow["id"]),
                    "WARNING",
                    "可执行环节已全部结束，{} 个环节因循环依赖无法排定".format(blocked_count),
                )
            else:
                append_log(execution, str(workflow["id"]), "SUCCESS", "工作流已结束")
        workflow["updatedAt"] = now
        STOP_EVENTS.pop(execution_id, None)
        await broadcast(execution)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    ACTIVE_CLIENTS.add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        ACTIVE_CLIENTS.discard(ws)
