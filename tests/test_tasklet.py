"""Test suite for Tasklet Micro Agent Engine solution."""
import unittest
from tasklet_micro_agent_engine import TaskletMicroAgentEngine

class TestTaskletMicroAgentEngine(unittest.TestCase):

    def test_tasklet_spawn(self):
        engine = TaskletMicroAgentEngine()
        res = engine.spawn_tasklet("task_001", {"action": "parse_dom"})
        
        self.assertEqual(res["status"], "TASKLET_ACTIVE")
        self.assertEqual(res["active_tasklets"], 1)

if __name__ == "__main__":
    unittest.main()
