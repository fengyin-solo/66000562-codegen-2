"""Pure workflow graph validation and scheduling helpers."""
from collections import defaultdict, deque
from typing import Any, Dict, Iterable, List, Sequence, Set, Tuple

STRATEGIES = ("fifo", "priority", "max_concurrent")

DEFAULT_NODES: List[Dict[str, Any]] = [
    {"id": "extract", "name": "数据提取", "deps": [], "duration": 2.0, "priority": 5, "x": 2.5, "y": 0.0},
    {"id": "validate", "name": "数据校验", "deps": ["extract"], "duration": 1.5, "priority": 5, "x": 2.5, "y": 0.9},
    {"id": "clean_a", "name": "清洗分支A", "deps": ["validate"], "duration": 1.8, "priority": 4, "x": 0.0, "y": 1.8},
    {"id": "clean_b", "name": "清洗分支B", "deps": ["validate"], "duration": 1.2, "priority": 3, "x": 5.0, "y": 1.8},
    {"id": "transform", "name": "数据转换", "deps": ["clean_a"], "duration": 3.0, "priority": 4, "x": 0.0, "y": 2.7},
    {"id": "enrich", "name": "数据增强", "deps": ["clean_a", "clean_b"], "duration": 2.0, "priority": 3, "x": 3.75, "y": 2.7},
    {"id": "aggregate", "name": "聚合计算", "deps": ["transform", "enrich"], "duration": 2.5, "priority": 4, "x": 1.75, "y": 3.6},
    {"id": "quality", "name": "质量检查", "deps": ["aggregate"], "duration": 1.0, "priority": 5, "x": 1.75, "y": 4.5},
    {"id": "export_db", "name": "入库", "deps": ["quality"], "duration": 1.8, "priority": 3, "x": 0.0, "y": 5.4},
    {"id": "export_report", "name": "报表生成", "deps": ["quality"], "duration": 2.2, "priority": 2, "x": 3.75, "y": 5.4},
    {"id": "notify", "name": "通知", "deps": ["export_db", "export_report"], "duration": 0.5, "priority": 1, "x": 1.75, "y": 6.3},
]


def default_graph() -> Dict[str, Any]:
    """Return a fresh copy of the built-in DAG."""
    nodes = []
    for index, item in enumerate(DEFAULT_NODES):
        node = dict(item)
        node["deps"] = list(item["deps"])
        nodes.append(node)
    return nodes_to_graph(nodes)


def nodes_to_graph(raw_nodes: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Validate user supplied nodes and initialize their runtime fields."""
    if not raw_nodes:
        raise ValueError("编排至少需要包含一个环节")

    normalized: List[Dict[str, Any]] = []
    seen_ids: Set[str] = set()

    for index, raw in enumerate(raw_nodes):
        node_id = str(raw.get("id", "")).strip()
        name = str(raw.get("name", "")).strip()
        if not node_id:
            raise ValueError("第 {} 个环节缺少 id".format(index + 1))
        if node_id in seen_ids:
            raise ValueError("环节 id「{}」重复".format(node_id))
        if not name:
            raise ValueError("环节「{}」的名称不能为空".format(node_id))
        seen_ids.add(node_id)

        deps = raw.get("deps", [])
        if not isinstance(deps, list):
            raise ValueError("环节「{}」的 deps 必须是数组".format(node_id))
        # Preserve dependency order while removing duplicate edges.
        unique_deps: List[str] = []
        dep_seen: Set[str] = set()
        for dep in deps:
            dep_id = str(dep).strip()
            if dep_id and dep_id not in dep_seen:
                unique_deps.append(dep_id)
                dep_seen.add(dep_id)

        try:
            duration = float(raw.get("duration", 1.5))
        except (TypeError, ValueError):
            raise ValueError("环节「{}」的执行时长必须是数字".format(node_id))
        if duration <= 0:
            raise ValueError("环节「{}」的执行时长必须大于 0".format(node_id))

        try:
            priority = int(raw.get("priority", 3))
        except (TypeError, ValueError):
            raise ValueError("环节「{}」的优先级必须是整数".format(node_id))
        if not 1 <= priority <= 10:
            raise ValueError("环节「{}」的优先级必须在 1 到 10 之间".format(node_id))

        x_value = raw.get("x")
        y_value = raw.get("y")
        x = float(x_value) if x_value is not None else (index % 4) * 2.2
        y = float(y_value) if y_value is not None else (index // 4) * 1.4

        normalized.append({
            "id": node_id,
            "name": name,
            "deps": unique_deps,
            "duration": duration,
            "priority": priority,
            "x": x,
            "y": y,
            "status": "PENDING",
            "startTime": None,
            "endTime": None,
            "retries": 0,
            "blockedByCycle": False,
            "blockedReason": None,
        })

    node_map = {node["id"]: node for node in normalized}
    for node in normalized:
        for dep in node["deps"]:
            if dep not in node_map:
                raise ValueError("环节「{}」依赖的环节「{}」不存在".format(node["id"], dep))

    edges = []
    adjacency: Dict[str, List[str]] = defaultdict(list)
    for node in normalized:
        for dep in node["deps"]:
            edges.append([dep, node["id"]])
            adjacency[dep].append(node["id"])

    warnings = detect_cycles(normalized, adjacency)
    blocked_ids = blocked_nodes(normalized, warnings, adjacency)
    cycle_ids = {node_id for warning in warnings for node_id in warning["nodeIds"]}
    for node in normalized:
        if node["id"] in blocked_ids:
            node["status"] = "BLOCKED"
            node["blockedByCycle"] = True
            if node["id"] in cycle_ids:
                node["blockedReason"] = "环节彼此等待，无法排定先后"
            else:
                node["blockedReason"] = "依赖的上游位于循环中，暂时无法排定"

    durations = {node["id"]: node["duration"] for node in normalized}
    return {"nodes": normalized, "edges": edges, "durations": durations, "warnings": warnings}


def detect_cycles(nodes: Sequence[Dict[str, Any]],
                  adjacency: Dict[str, List[str]] = None) -> List[Dict[str, Any]]:
    """Find strongly connected components representing cyclic waits."""
    if adjacency is None:
        adjacency = defaultdict(list)
        for node in nodes:
            for dep in node["deps"]:
                adjacency[dep].append(node["id"])

    name_by_id = {node["id"]: node["name"] for node in nodes}
    index_by_id: Dict[str, int] = {}
    low_link: Dict[str, int] = {}
    stack: List[str] = []
    on_stack: Set[str] = set()
    counter = 0
    component_sets: List[List[str]] = []

    def strong_connect(node_id: str) -> None:
        nonlocal counter
        index_by_id[node_id] = counter
        low_link[node_id] = counter
        counter += 1
        stack.append(node_id)
        on_stack.add(node_id)

        for child in adjacency.get(node_id, []):
            if child not in index_by_id:
                strong_connect(child)
                low_link[node_id] = min(low_link[node_id], low_link[child])
            elif child in on_stack:
                low_link[node_id] = min(low_link[node_id], index_by_id[child])

        if low_link[node_id] == index_by_id[node_id]:
            component = []
            while True:
                child = stack.pop()
                on_stack.remove(child)
                component.append(child)
                if child == node_id:
                    break
            component_sets.append(component)

    for node in nodes:
        if node["id"] not in index_by_id:
            strong_connect(node["id"])

    order = {node["id"]: index for index, node in enumerate(nodes)}
    cycles: List[Dict[str, Any]] = []
    for component in component_sets:
        is_self_wait = len(component) == 1 and component[0] in adjacency.get(component[0], [])
        if len(component) == 1 and not is_self_wait:
            continue
        ordered = sorted(component, key=lambda item: order[item])
        display_names = "、".join(name_by_id[item] for item in ordered)
        if is_self_wait:
            message = "环节「{}」依赖自身，无法排定先后".format(display_names)
        else:
            message = "环节「{}」彼此关联互相等待，形成循环依赖，无法排定先后".format(display_names)
        cycles.append({"type": "CYCLE", "nodeIds": ordered, "message": message})

    cycles.sort(key=lambda warning: min(order[item] for item in warning["nodeIds"]))
    return cycles


def blocked_nodes(nodes: Sequence[Dict[str, Any]], warnings: Sequence[Dict[str, Any]],
                  adjacency: Dict[str, List[str]]) -> Set[str]:
    """Cycle nodes and every downstream node cannot be scheduled."""
    blocked: Set[str] = set()
    queue = deque()
    for warning in warnings:
        for node_id in warning["nodeIds"]:
            if node_id not in blocked:
                blocked.add(node_id)
                queue.append(node_id)

    while queue:
        node_id = queue.popleft()
        for child in adjacency.get(node_id, []):
            if child not in blocked:
                blocked.add(child)
                queue.append(child)
    return blocked


def downstream_counts(nodes: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    """Count unique transitively downstream tasks for max-concurrency strategy."""
    adjacency = defaultdict(list)
    for node in nodes:
        for dep in node["deps"]:
            adjacency[dep].append(node["id"])

    counts: Dict[str, int] = {}
    for node in nodes:
        seen: Set[str] = set()
        queue = deque(adjacency.get(node["id"], []))
        while queue:
            child = queue.popleft()
            if child in seen:
                continue
            seen.add(child)
            queue.extend(adjacency.get(child, []))
        counts[node["id"]] = len(seen)
    return counts


def select_ready(ready: Iterable[str], nodes: Sequence[Dict[str, Any]], strategy: str) -> List[str]:
    """Order runnable tasks according to the workflow's fixed scheduling strategy."""
    if strategy not in STRATEGIES:
        raise ValueError("不支持的调度策略：{}".format(strategy))

    order = {node["id"]: index for index, node in enumerate(nodes)}
    node_map = {node["id"]: node for node in nodes}
    downstream = downstream_counts(nodes) if strategy == "max_concurrent" else {}

    def key(node_id: str) -> Tuple[int, int, int]:
        node = node_map[node_id]
        if strategy == "priority":
            return (-int(node["priority"]), order[node_id], 0)
        if strategy == "max_concurrent":
            return (-downstream.get(node_id, 0), -int(node["priority"]), order[node_id])
        return (order[node_id], 0, 0)

    return sorted(ready, key=key)
