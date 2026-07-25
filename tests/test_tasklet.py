"""Heavy tests for Tasklet Micro Agent Engine."""
from __future__ import annotations

import unittest

from tasklet_micro_agent_engine import TaskletMicroAgentEngine


class TestTaskletMicroAgentEngine(unittest.TestCase):
    def test_spawn_and_priority_order(self):
        eng = TaskletMicroAgentEngine(max_concurrent_tasklets=8)
        eng.spawn_tasklet("low", {"job": "a"}, priority=50)
        eng.spawn_tasklet("high", {"job": "b"}, priority=1)
        eng.spawn_tasklet("mid", {"job": "c"}, priority=10)
        first = eng.next_tasklet()
        self.assertIsNotNone(first)
        assert first is not None
        self.assertEqual(first["tasklet_id"], "high")
        second = eng.next_tasklet()
        assert second is not None
        self.assertEqual(second["tasklet_id"], "mid")

    def test_concurrency_cap(self):
        eng = TaskletMicroAgentEngine(max_concurrent_tasklets=2)
        self.assertTrue(eng.spawn_tasklet("a", {})["ok"])
        self.assertTrue(eng.spawn_tasklet("b", {})["ok"])
        r = eng.spawn_tasklet("c", {})
        self.assertFalse(r["ok"])
        self.assertEqual(r["error"], "concurrency_cap")
        eff = eng.efficiency()
        self.assertEqual(eff["rejected"], 1)
        self.assertLess(eff["accept_rate"], 1.0)

    def test_suspend_resume(self):
        eng = TaskletMicroAgentEngine(max_concurrent_tasklets=4)
        eng.spawn_tasklet("t1", {"x": 1}, priority=5)
        self.assertTrue(eng.suspend_tasklet("t1")["ok"])
        self.assertEqual(eng.tasklet_registry["t1"]["status"], "SUSPENDED")
        r = eng.resume_tasklet("t1")
        self.assertTrue(r["ok"])
        self.assertEqual(r["payload"]["x"], 1)
        self.assertEqual(eng.efficiency()["resumed"], 1)

    def test_complete(self):
        eng = TaskletMicroAgentEngine()
        eng.spawn_tasklet("done", {"n": 2})
        self.assertTrue(eng.complete_tasklet("done", result={"sum": 2})["ok"])
        self.assertEqual(eng.tasklet_registry["done"]["status"], "COMPLETED")
        self.assertIsNone(eng.next_tasklet())

    def test_no_magic_answer(self):
        eng = TaskletMicroAgentEngine()
        r = eng.spawn_tasklet(payload={"k": "v"})
        self.assertNotIn("answer", r)
        self.assertTrue(r["ok"])


if __name__ == "__main__":
    unittest.main()
