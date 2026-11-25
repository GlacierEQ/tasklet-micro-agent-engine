import asyncio
import unittest

from tasklet_micro_agent_engine import TaskletMicroAgentEngine


class MixedAdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def test_registry_blocks_runtime_until_release(self):
        engine = TaskletMicroAgentEngine(1)
        self.assertTrue(engine.spawn_tasklet("registry")["ok"])
        entered = asyncio.Event()

        async def handler(payload):
            entered.set()
            return 42

        pending = asyncio.create_task(engine.run_tasklet(handler, tasklet_id="runtime"))
        await asyncio.sleep(0)
        self.assertFalse(entered.is_set())
        self.assertEqual(engine.efficiency()["active"], 1)
        engine.complete_tasklet("registry")
        receipt = await asyncio.wait_for(pending, 1)
        self.assertEqual(receipt["result"], 42)
        self.assertEqual(engine.efficiency()["active"], 0)

    async def test_cancelled_waiter_does_not_release_registry_capacity(self):
        engine = TaskletMicroAgentEngine(1)
        engine.spawn_tasklet("registry")
        pending = asyncio.create_task(engine.run_tasklet(lambda _: 1, tasklet_id="waiting"))
        await asyncio.sleep(0)
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending
        self.assertFalse(engine.spawn_tasklet("extra")["ok"])
        engine.suspend_tasklet("registry")
        self.assertTrue((await engine.run_tasklet(lambda _: 2, timeout_s=1))["ok"])
        self.assertTrue(engine.resume_tasklet("registry")["ok"])
