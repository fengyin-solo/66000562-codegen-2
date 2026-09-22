import unittest

from app.orchestration import blocked_nodes, default_graph, detect_cycles, nodes_to_graph, select_ready


class OrchestrationTests(unittest.TestCase):
    def test_default_graph_is_acyclic(self):
        graph = default_graph()
        self.assertEqual(graph["warnings"], [])
        self.assertTrue(all(node["status"] == "PENDING" for node in graph["nodes"]))

    def test_cycle_is_reported_and_downstream_is_blocked(self):
        graph = nodes_to_graph([
            {"id": "a", "name": "准备", "deps": []},
            {"id": "b", "name": "训练", "deps": ["c"]},
            {"id": "c", "name": "评估", "deps": ["b"]},
            {"id": "d", "name": "发布", "deps": ["c"]},
            {"id": "e", "name": "独立", "deps": ["a"]},
        ])
        self.assertEqual(len(graph["warnings"]), 1)
        self.assertEqual(graph["warnings"][0]["nodeIds"], ["b", "c"])
        status = {node["id"]: node["status"] for node in graph["nodes"]}
        self.assertEqual(status["b"], "BLOCKED")
        self.assertEqual(status["c"], "BLOCKED")
        self.assertEqual(status["d"], "BLOCKED")
        self.assertEqual(status["e"], "PENDING")
        self.assertIn("无法排定先后", graph["warnings"][0]["message"])

    def test_empty_node_id_and_name_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "id"):
            nodes_to_graph([{"id": "", "name": "空 ID", "deps": []}])
        with self.assertRaisesRegex(ValueError, "名称不能为空"):
            nodes_to_graph([{"id": "a", "name": " ", "deps": []}])

    def test_priority_strategy_prefers_higher_priority(self):
        graph = nodes_to_graph([
            {"id": "a", "name": "低优先级", "deps": [], "priority": 1},
            {"id": "b", "name": "高优先级", "deps": [], "priority": 5},
        ])
        self.assertEqual(select_ready(["a", "b"], graph["nodes"], "priority"), ["b", "a"])

    def test_max_concurrent_strategy_prefers_more_downstream(self):
        graph = nodes_to_graph([
            {"id": "root", "name": "根", "deps": [], "priority": 1},
            {"id": "leaf", "name": "叶子", "deps": [], "priority": 5},
            {"id": "a", "name": "A", "deps": ["root"]},
            {"id": "b", "name": "B", "deps": ["root"]},
        ])
        self.assertEqual(select_ready(["leaf", "root"], graph["nodes"], "max_concurrent"), ["root", "leaf"])

    def test_self_wait_is_a_cycle(self):
        graph = nodes_to_graph([
            {"id": "a", "name": "等待自己", "deps": ["a"]},
            {"id": "b", "name": "独立", "deps": []},
        ])
        self.assertEqual(graph["warnings"][0]["nodeIds"], ["a"])
        self.assertEqual({node["id"] for node in graph["nodes"] if node["status"] == "BLOCKED"}, {"a"})


if __name__ == "__main__":
    unittest.main()
