"""Unit tests for downtime-budget accounting with a fully injected clock.

These tests drive ``common.service`` directly against a temporary SQLite
database and pass explicit ``now`` timestamps, so 24-hour windows, silence
extensions/early releases and cross-window faults can be checked
deterministically. End-to-end HTTP behaviour lives in tests/e2e.py.

Run:  python3 -I -m tests.test_budget
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from common import service  # noqa: E402
from common.db import close, connect, init_db  # noqa: E402

DAY = 86400.0


def fire(conn, fp, seq, now, text="f"):
    service.ingest_event(
        conn, {"fingerprint": fp, "seq": seq, "kind": "firing", "summary": text}, now=now
    )


def recover(conn, fp, seq, now, text="r"):
    service.ingest_event(
        conn, {"fingerprint": fp, "seq": seq, "kind": "recovery", "summary": text}, now=now
    )


class BudgetTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="budget-")
        self.db = os.path.join(self.tmp, "t.db")
        init_db(self.db)
        self.conn = connect(self.db)

    def tearDown(self):
        close(self.conn)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _b(self, fp, now):
        return service.compute_budget(self.conn, fp, now=now)

    def assertAlmost(self, got, want, places=3):
        self.assertAlmostEqual(got, want, places=places)

    def test_no_budget_returns_none_and_404_upstream(self):
        fire(self.conn, "x", 1, 1000.0)
        self.assertIsNone(self._b("x", 1010.0))
        self.assertEqual(service.list_budgets(self.conn, now=1010.0), [])

    def test_basic_closed_fault_counted_by_receive_time(self):
        t = 1_000_000.0
        fire(self.conn, "a", 1, t)
        budget = service.set_budget(self.conn, "a", 300.0, now=t + 10)
        recover(self.conn, "a", 2, t + 70)
        b = self._b("a", t + 100)
        # fault 70s long, budget set 10s after firing -> counted from set time: 60s
        self.assertAlmost(b["downtime_seconds"], 60.0)
        self.assertAlmost(b["exempt_seconds"], 0.0)
        self.assertAlmost(b["remaining_seconds"], 240.0)
        self.assertFalse(b["exhausted"])
        self.assertEqual(budget["fingerprint"], "a")
        self.assertEqual(len(b["rounds"]), 1)
        self.assertFalse(b["rounds"][0]["open"])

    def test_fault_already_firing_at_set_time_starts_at_set_at(self):
        t = 5_000.0
        fire(self.conn, "a", 1, t - 100)  # fired long before budgeting
        service.set_budget(self.conn, "a", 1000.0, now=t)
        b = self._b("a", t + 50)  # still open
        self.assertAlmost(b["downtime_seconds"], 50.0)
        self.assertTrue(b["rounds"][0]["open"])

    def test_open_fault_counted_up_to_query_time(self):
        t = 7_000.0
        service.set_budget(self.conn, "a", 100.0, now=t)
        fire(self.conn, "a", 1, t + 10)
        b = self._b("a", t + 35)
        self.assertAlmost(b["downtime_seconds"], 25.0)
        b2 = self._b("a", t + 60)  # later query grows the count
        self.assertAlmost(b2["downtime_seconds"], 50.0)
        self.assertAlmost(b2["remaining_seconds"], 50.0)

    def test_full_silence_exempts_whole_overlap(self):
        t = 2_000.0
        service.set_budget(self.conn, "a", 500.0, now=t)
        fire(self.conn, "a", 1, t)
        service.set_silence(self.conn, t + 20, "maint", now=t + 10)
        service.clear_silence(self.conn, now=t + 40)
        recover(self.conn, "a", 2, t + 50)
        b = self._b("a", t + 60)
        self.assertAlmost(b["downtime_seconds"], 40.0)  # 50 gross - 10 exempt
        self.assertAlmost(b["exempt_seconds"], 10.0)

    def test_partial_silence_overlap_at_edges(self):
        t = 3_000.0
        service.set_budget(self.conn, "a", 500.0, now=t)
        # silence starts before the fault and ends inside it
        service.set_silence(self.conn, t + 30, "m", now=t - 10)
        fire(self.conn, "a", 1, t)
        recover(self.conn, "a", 2, t + 80)
        b = self._b("a", t + 90)
        self.assertAlmost(b["exempt_seconds"], 30.0)
        self.assertAlmost(b["downtime_seconds"], 50.0)

    def test_silence_extension_is_one_continuous_exempt_window(self):
        t = 4_000.0
        service.set_budget(self.conn, "a", 500.0, now=t)
        fire(self.conn, "a", 1, t)
        service.set_silence(self.conn, t + 20, "m1", now=t + 5)
        # extend while still active: must not double-count or leave a gap
        service.set_silence(self.conn, t + 40, "m2", now=t + 15)
        recover(self.conn, "a", 2, t + 60)
        b = self._b("a", t + 70)
        self.assertAlmost(b["exempt_seconds"], 35.0)  # t+5..t+40 silence in effect
        self.assertAlmost(b["downtime_seconds"], 25.0)  # t+40..t+60
        # only one effective interval row exists for the extended silence
        rows = self.conn.execute("SELECT COUNT(*) AS c FROM silence_intervals").fetchone()
        self.assertEqual(rows["c"], 1)

    def test_early_release_ends_exemption_at_release_time(self):
        t = 6_000.0
        service.set_budget(self.conn, "a", 500.0, now=t)
        fire(self.conn, "a", 1, t)
        service.set_silence(self.conn, t + 100, "m", now=t + 10)
        service.clear_silence(self.conn, now=t + 30)  # released 70s early
        recover(self.conn, "a", 2, t + 50)
        b = self._b("a", t + 60)
        self.assertAlmost(b["exempt_seconds"], 20.0)
        self.assertAlmost(b["downtime_seconds"], 30.0)

    def test_natural_expiry_without_sweep_uses_until_ts(self):
        t = 8_000.0
        service.set_budget(self.conn, "a", 500.0, now=t)
        fire(self.conn, "a", 1, t)
        service.set_silence(self.conn, t + 25, "m", now=t + 5)
        # no clear, no sweep: query after natural expiry
        recover(self.conn, "a", 2, t + 60)
        b = self._b("a", t + 90)
        self.assertAlmost(b["exempt_seconds"], 20.0)  # t+5..t+25
        self.assertAlmost(b["downtime_seconds"], 40.0)

    def test_events_older_than_24h_window_are_clipped(self):
        t = 10_000.0
        # budget was set 30 hours ago; only the last 24h can count
        service.set_budget(self.conn, "a", 10_000.0, now=t - 30 * 3600)
        fire(self.conn, "a", 1, t - 25 * 3600)
        recover(self.conn, "a", 2, t - 23 * 3600)  # 1h inside the 24h window
        b = self._b("a", t)
        self.assertAlmost(b["downtime_seconds"], 3600.0)
        self.assertAlmost(b["window_start"], t - DAY)

    def test_fault_straddling_window_boundary(self):
        t = 11_000.0
        service.set_budget(self.conn, "a", 10_000.0, now=t - 2 * DAY)
        fire(self.conn, "a", 1, t - DAY - 100)  # 100s before the window opens
        recover(self.conn, "a", 2, t - DAY + 50)  # 50s inside
        b = self._b("a", t)
        self.assertAlmost(b["downtime_seconds"], 50.0)

    def test_multiple_rounds_and_retrigger_after_recovery(self):
        t = 12_000.0
        service.set_budget(self.conn, "a", 1000.0, now=t)
        fire(self.conn, "a", 1, t)
        recover(self.conn, "a", 2, t + 30)
        fire(self.conn, "a", 3, t + 40)
        recover(self.conn, "a", 4, t + 90)
        b = self._b("a", t + 100)
        self.assertAlmost(b["downtime_seconds"], 80.0)  # 30 + 50
        self.assertEqual([r["round"] for r in b["rounds"]], [1, 2])

    def test_budget_exhaustion_and_negative_remaining(self):
        t = 13_000.0
        service.set_budget(self.conn, "a", 10.0, now=t)
        fire(self.conn, "a", 1, t)
        b = self._b("a", t + 25)
        self.assertAlmost(b["remaining_seconds"], -15.0)
        self.assertTrue(b["exhausted"])

    def test_replacing_budget_restarts_accounting_at_new_set_at(self):
        t = 14_000.0
        fire(self.conn, "a", 1, t)
        service.set_budget(self.conn, "a", 100.0, now=t + 10)
        b1 = self._b("a", t + 40)
        self.assertAlmost(b1["downtime_seconds"], 30.0)
        # replace with a fresh allowance: history before the new set_at is ignored
        service.set_budget(self.conn, "a", 100.0, now=t + 50)
        b2 = self._b("a", t + 80)
        self.assertAlmost(b2["downtime_seconds"], 30.0)
        self.assertGreater(b2["set_at"], b1["set_at"])

    def test_rejected_write_keeps_old_budget_queryable(self):
        t = 15_000.0
        service.set_budget(self.conn, "a", 123.0, now=t)
        with self.assertRaises(service.Reject):
            service.set_budget(self.conn, "a", -5.0, now=t + 5)
        with self.assertRaises(service.Reject):
            service.set_budget(self.conn, "   ", 10.0, now=t + 5)
        b = self._b("a", t + 5)
        self.assertAlmost(b["allowed_seconds"], 123.0)
        self.assertAlmost(b["set_at"], t)

    def test_failed_write_rolls_back_and_old_budget_survives(self):
        t = 16_000.0
        service.set_budget(self.conn, "a", 123.0, now=t)
        # Break the connection so the upsert commit fails; the previous
        # committed budget must remain readable afterwards.
        self.conn.create_function("boom", 0, lambda: (_ for _ in ()).throw(RuntimeError("disk")))
        with self.assertRaises(Exception):  # sqlite wraps the function error
            with self.conn:  # plain autocommit txn to force the failure
                self.conn.execute(
                    "INSERT INTO budgets(fingerprint, allowed_seconds, set_at, updated_at) "
                    "VALUES ('b', 9, ?, ?)", (t, t)
                )
                self.conn.execute("SELECT boom()")
        b = self._b("a", t + 1)
        self.assertIsNotNone(b)
        self.assertAlmost(b["allowed_seconds"], 123.0)
        self.assertIsNone(self._b("b", t + 1))

    def test_persistence_reopen_keeps_accounting(self):
        t = 17_000.0
        service.set_budget(self.conn, "c", 200.0, now=t)
        fire(self.conn, "c", 1, t + 1)
        service.set_silence(self.conn, t + 30, "m", now=t + 10)
        service.clear_silence(self.conn, now=t + 20)
        close(self.conn)
        conn2 = connect(self.db)
        try:
            b = service.compute_budget(conn2, "c", now=t + 60)
            self.assertAlmost(b["exempt_seconds"], 10.0)
            self.assertAlmost(b["downtime_seconds"], 49.0)  # 59 open - 10 exempt
        finally:
            close(conn2)

    def test_silence_after_fault_recovery_does_not_exempt(self):
        t = 18_000.0
        service.set_budget(self.conn, "a", 500.0, now=t)
        fire(self.conn, "a", 1, t)
        recover(self.conn, "a", 2, t + 20)
        service.set_silence(self.conn, t + 100, "late", now=t + 50)
        b = self._b("a", t + 80)
        self.assertAlmost(b["exempt_seconds"], 0.0)
        self.assertAlmost(b["downtime_seconds"], 20.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
