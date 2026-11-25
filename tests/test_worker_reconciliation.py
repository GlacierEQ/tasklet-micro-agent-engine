import asyncio
import threading
import unittest

from tasklet_micro_agent_engine import TaskletMicroAgentEngine


class WorkerOwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def wait_until(self, predicate):
        async with asyncio.timeout(2):
            while not predicate():
                await asyncio.sleep(0.001)

    async def test_cancel_running_thread_retains_slot_and_key(self):
        engine = TaskletMicroAgentEngine(1)
        entered = threading.Event()
        release = threading.Event()

        def worker(_):
            entered.set()
            if not release.wait(2):
                raise RuntimeError("test release missing")
            return "finished"

        pending = asyncio.create_task(
            engine.run_tasklet(worker, tasklet_id="worker", idempotency_key="one")
        )
        try:
            await self.wait_until(entered.is_set)
            pending.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await pending
            self.assertEqual(engine.efficiency()["active"], 1)
            self.assertFalse(engine.spawn_tasklet("registry")["ok"])
            duplicate_key = await engine.run_tasklet(lambda _: None, idempotency_key="one")
            duplicate_id = await engine.run_tasklet(lambda _: None, tasklet_id="worker")
            self.assertEqual(duplicate_key["error"], "idempotency_in_flight")
            self.assertEqual(duplicate_id["error"], "already_active")
        finally:
            release.set()
        await self.wait_until(lambda: engine.efficiency()["active"] == 0)
        self.assertTrue(engine.spawn_tasklet("registry")["ok"])

    async def test_timeout_keeps_capacity_until_thread_exits(self):
        engine = TaskletMicroAgentEngine(1)
        entered = threading.Event()
        release = threading.Event()

        def worker(_):
            entered.set()
            if not release.wait(2):
                raise RuntimeError("test release missing")
            return 42

        pending = asyncio.create_task(
            engine.run_tasklet(worker, tasklet_id="worker", timeout_s=0.05)
        )
        try:
            await self.wait_until(entered.is_set)
            receipt = await pending
            self.assertEqual(receipt["status"], "TIMED_OUT")
            self.assertTrue(receipt["execution_continues"])
            self.assertEqual(engine.efficiency()["active"], 1)
            self.assertFalse(engine.spawn_tasklet("registry")["ok"])
        finally:
            release.set()
        await self.wait_until(lambda: engine.efficiency()["active"] == 0)
        self.assertTrue((await engine.run_tasklet(lambda _: 1, timeout_s=1))["ok"])

    async def test_mixed_stress_cap(self):
        engine = TaskletMicroAgentEngine(3)
        self.assertTrue(engine.spawn_tasklet("registry")["ok"])
        active = 0
        peak = 0

        async def handler(_):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            self.assertLessEqual(engine.efficiency()["active"], 3)
            await asyncio.sleep(0.001)
            active -= 1
            return True
        receipts = await asyncio.gather(
            *(engine.run_tasklet(handler, timeout_s=2) for _ in range(100))
        )
        self.assertTrue(all(r["ok"] for r in receipts))
        self.assertEqual(peak, 2)
        self.assertEqual(engine.efficiency()["active"], 1)

    async def test_late_success_does_not_repeat_side_effect(self):
        engine = TaskletMicroAgentEngine(1)
        entered = threading.Event()
        release = threading.Event()
        effects = []

        def worker(_):
            entered.set()
            if not release.wait(2):
                raise RuntimeError("test release missing")
            effects.append("effect")
            return "result"

        pending = asyncio.create_task(
            engine.run_tasklet(worker, tasklet_id="worker", idempotency_key="one", timeout_s=0.05)
        )
        try:
            await self.wait_until(entered.is_set)
            receipt = await pending
            self.assertEqual(receipt["status"], "TIMED_OUT")
        finally:
            release.set()
        await self.wait_until(lambda: engine.efficiency()["active"] == 0)
        retry = await engine.run_tasklet(worker, idempotency_key="one", timeout_s=1)
        self.assertEqual(len(effects), 1, retry)
        self.assertTrue(retry["replayed"])
        self.assertEqual(retry["result"], "result")
        self.assertEqual(retry["original_status"], "TIMED_OUT")
        self.assertEqual(engine.tasklet_registry["worker"]["status"], "TIMED_OUT")

    async def test_late_failure_requires_reconciliation(self):
        engine = TaskletMicroAgentEngine(1)
        entered = threading.Event()
        release = threading.Event()
        effects = []

        def worker(_):
            entered.set()
            if not release.wait(2):
                raise RuntimeError("test release missing")
            effects.append("effect")
            raise RuntimeError("failed after effect")

        pending = asyncio.create_task(
            engine.run_tasklet(worker, tasklet_id="worker", idempotency_key="one", timeout_s=0.05)
        )
        try:
            await self.wait_until(entered.is_set)
            self.assertEqual((await pending)["status"], "TIMED_OUT")
        finally:
            release.set()
        await self.wait_until(lambda: engine.efficiency()["active"] == 0)
        retry = await engine.run_tasklet(worker, idempotency_key="one", timeout_s=1)
        self.assertEqual(retry["error"], "reconciliation_required")
        self.assertEqual(len(effects), 1)
        self.assertFalse(engine.spawn_tasklet("worker")["ok"])

    async def test_cancelled_worker_late_result_preserved(self):
        engine = TaskletMicroAgentEngine(1)
        entered = threading.Event()
        release = threading.Event()
        effects = []

        def worker(_):
            entered.set()
            if not release.wait(2):
                raise RuntimeError("test release missing")
            effects.append("effect")
            return None

        pending = asyncio.create_task(
            engine.run_tasklet(worker, tasklet_id="worker", idempotency_key="one")
        )
        try:
            await self.wait_until(entered.is_set)
            pending.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await pending
        finally:
            release.set()
        await self.wait_until(lambda: engine.efficiency()["active"] == 0)
        retry = await engine.run_tasklet(worker, idempotency_key="one", timeout_s=1)
        self.assertTrue(retry["replayed"])
        self.assertIsNone(retry["result"])
        self.assertEqual(retry["original_status"], "CANCELLED")
        self.assertEqual(len(effects), 1)
        self.assertFalse(engine.spawn_tasklet("worker")["ok"])
        self.assertFalse((await engine.run_tasklet(worker, tasklet_id="worker"))["ok"])
