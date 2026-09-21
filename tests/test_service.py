"""调拨系统核心场景测试。

运行方式：
    python3 -m unittest discover -s tests -v
    或 pytest tests/
"""
import unittest
from datetime import datetime, timedelta, timezone

from src.errors import (
    ConflictError,
    IdempotencyConflictError,
    NotFoundError,
    ValidationError,
)
from src.service import RequisitionService
from src.store import Store


class FakeClock:
    def __init__(self):
        self.t = datetime(2026, 9, 21, 8, 0, tzinfo=timezone.utc)

    def __call__(self):
        return self.t

    def advance(self, hours):
        self.t += timedelta(hours=hours)


class Base(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.svc = RequisitionService(Store(":memory:"), clock=self.clock)
        self.svc.register_warehouse("WH-BJ", "北京库", "北京")
        self.svc.register_warehouse("WH-SH", "上海库", "上海")
        self.svc.register_warehouse("WH-CD", "成都库", "成都")
        self.svc.register_part("TB-100", "涡轮叶片", "Z-20")
        self.svc.add_route("WH-BJ", "前沿-7", 10)
        self.svc.add_route("WH-SH", "前沿-7", 20)
        self.svc.add_route("WH-CD", "前沿-7", 30)

    def submit(self, qty=5, level="URGENT", key=None, part="TB-100", hours=48,
               model="Z-20"):
        return self.svc.submit_request(
            idempotency_key=key, unit_id="UNIT-1", destination="前沿-7",
            equipment_model=model, part_number=part, quantity=qty,
            fault_level=level, required_by_hours=hours)

    def approve_all(self, rid, levels=(1, 2)):
        result = None
        for lv in levels:
            result = self.svc.approve(rid, level=lv, approver=f"officer-L{lv}")
        return result


class TestIdempotency(Base):
    def test_duplicate_submission_returns_original_decision(self):
        """网络重试：同一幂等键返回原决定，只建一单。"""
        r1 = self.submit(qty=3, key="K-1")
        r2 = self.submit(qty=3, key="K-1")
        self.assertEqual(r1["request_id"], r2["request_id"])
        self.assertTrue(r2["idempotent_replay"])
        count = self.svc.store.conn.execute("SELECT COUNT(*) c FROM requests").fetchone()["c"]
        self.assertEqual(count, 1)

    def test_same_key_different_payload_rejected(self):
        self.submit(qty=3, key="K-2")
        with self.assertRaises(IdempotencyConflictError):
            self.submit(qty=4, key="K-2")

    def test_idempotent_approve_retry(self):
        """审批重试：返回首次决定，不产生重复审批记录。"""
        self.svc.receive_stock("WH-BJ", "TB-100", 5)
        r = self.submit(qty=2)
        rid = r["request_id"]
        a1 = self.svc.approve(rid, level=1, approver="x", idempotency_key="AP-1")
        a2 = self.svc.approve(rid, level=1, approver="x", idempotency_key="AP-1")
        self.assertTrue(a2["idempotent_replay"])
        self.assertEqual(a1["version"], a2["version"])
        cnt = self.svc.store.conn.execute(
            "SELECT COUNT(*) c FROM approvals WHERE request_id=?", (rid,)).fetchone()["c"]
        self.assertEqual(cnt, 1)

    def test_double_approve_without_key_rejected(self):
        self.submit(qty=2)
        rid = self.svc.list_requests()[0]["request_id"]
        self.svc.approve(rid, level=1, approver="x")
        with self.assertRaises(ConflictError):
            self.svc.approve(rid, level=1, approver="x")


class TestApproval(Base):
    def test_approval_hierarchy_enforced_and_recorded(self):
        """CRITICAL 需 L1+L2+L3 逐级审批，跳级被拒绝，全程留痕。"""
        self.svc.receive_stock("WH-BJ", "TB-100", 5)
        r = self.submit(qty=2, level="CRITICAL")
        rid = r["request_id"]
        self.assertEqual(r["required_approval_levels"], [1, 2, 3])
        with self.assertRaises(ConflictError):
            self.svc.approve(rid, level=2, approver="b")  # 跳级
        self.svc.approve(rid, level=1, approver="a")
        with self.assertRaises(ConflictError):
            self.svc.approve(rid, level=3, approver="c")  # 跳级
        self.svc.approve(rid, level=2, approver="b")
        final = self.svc.approve(rid, level=3, approver="c")
        self.assertTrue(final["final"])
        self.assertEqual(final["status"], "APPROVED")
        view = self.svc.get_request_view(rid)
        self.assertEqual([a["level"] for a in view["approvals"]], [1, 2, 3])

    def test_version_conflict_detected(self):
        r = self.submit(qty=2)
        rid = r["request_id"]
        self.svc.approve(rid, level=1, approver="a")
        with self.assertRaises(ConflictError):
            self.svc.approve(rid, level=2, approver="b", expected_version=1)

    def test_part_model_mismatch_rejected(self):
        with self.assertRaises(ValidationError):
            self.submit(qty=1, model="J-20")

    def test_unknown_part_rejected(self):
        with self.assertRaises(NotFoundError):
            self.submit(qty=1, part="XX-9")


class TestStockLocking(Base):
    def test_reservation_locks_stock_second_request_gets_remainder(self):
        """批准后库存被预留锁定，第二个申请只能分到剩余量。"""
        self.svc.receive_stock("WH-BJ", "TB-100", 10)
        a = self.submit(qty=8, key="A")
        self.approve_all(a["request_id"])
        self.assertEqual(self.svc.inventory_position("TB-100")["available"], 2)

        b = self.submit(qty=5, key="B")
        res = self.approve_all(b["request_id"])
        self.assertEqual(res["shortfall"], 3)
        self.assertEqual(sum(x["quantity"] for x in res["allocations"]), 2)
        view = self.svc.get_request_view(b["request_id"])
        self.assertTrue(any("库存不足" in r for r in view["blocking_reasons"]))

    def test_shipped_quantity_locked_and_never_reusable(self):
        """已出库数量从池中彻底移除，第二个申请不可占用。"""
        self.svc.receive_stock("WH-BJ", "TB-100", 10)
        a = self.submit(qty=8, key="A")
        res = self.approve_all(a["request_id"])
        self.svc.ship(res["allocations"][0]["allocation_id"])  # 8 件全部出库
        pos = self.svc.inventory_position("TB-100")
        self.assertEqual((pos["on_hand"], pos["available"]), (2, 2))

        b = self.submit(qty=5, key="B")
        res_b = self.approve_all(b["request_id"])
        self.assertEqual(res_b["shortfall"], 3)  # 只能拿到剩余 2 件

    def test_cancel_after_approval_releases_unshipped_keeps_shipped_locked(self):
        """批准后撤销：未出库部分释放回池，已出库部分保持锁定。"""
        self.svc.receive_stock("WH-BJ", "TB-100", 10)
        r = self.submit(qty=6)
        rid = r["request_id"]
        res = self.approve_all(rid)
        self.svc.ship(res["allocations"][0]["allocation_id"], quantity=2)

        out = self.svc.cancel(rid, actor="manager", reason="任务取消")
        self.assertEqual(out["released_quantity"], 4)
        self.assertEqual(out["shipped_locked_quantity"], 2)

        pos = self.svc.inventory_position("TB-100")
        self.assertEqual((pos["on_hand"], pos["reserved"], pos["available"]), (8, 0, 8))
        view = self.svc.get_request_view(rid)
        self.assertEqual(view["status"], "CANCELLED")
        self.assertTrue(any("锁定" in b for b in view["blocking_reasons"]))
        cancelled = [e for e in view["audit_trail"] if e["event"] == "CANCELLED"]
        self.assertEqual(cancelled[0]["detail"]["shipped_locked_quantity"], 2)


class TestSplitAndPlan(Base):
    def test_split_allocation_across_warehouses(self):
        """单库不足时拆分到多个仓库，各自给出承诺到货时间。"""
        self.svc.receive_stock("WH-BJ", "TB-100", 5)
        self.svc.receive_stock("WH-SH", "TB-100", 5)
        r = self.submit(qty=8)
        res = self.approve_all(r["request_id"])
        allocs = res["allocations"]
        self.assertEqual(len(allocs), 2)
        self.assertEqual(allocs[0]["warehouse_id"], "WH-BJ")  # 运输时间最短优先
        self.assertEqual(allocs[0]["quantity"], 5)
        self.assertEqual(allocs[1]["warehouse_id"], "WH-SH")
        self.assertEqual(allocs[1]["quantity"], 3)
        # URGENT 提速系数 0.75：BJ 7.5h，SH 15h
        self.assertEqual(allocs[0]["transport_hours"], 7.5)
        self.assertEqual(allocs[1]["transport_hours"], 15.0)
        view = self.svc.get_request_view(r["request_id"])
        self.assertEqual(view["promised_arrival"],
                         max(a["promised_arrival"] for a in allocs))

    def test_critical_fault_speeds_transport(self):
        self.svc.receive_stock("WH-BJ", "TB-100", 3)
        r = self.submit(qty=1, level="CRITICAL")
        res = self.approve_all(r["request_id"], levels=(1, 2, 3))
        self.assertEqual(res["allocations"][0]["transport_hours"], 5.0)  # 10 * 0.5


class TestReturnAndAmend(Base):
    def test_return_for_supplement_and_amend(self):
        """退回补充 -> 修改 -> 重新提交 -> 重新审批，版本递增、全程留痕。"""
        self.svc.receive_stock("WH-BJ", "TB-100", 10)
        r = self.submit(qty=5)
        rid = r["request_id"]
        ret = self.svc.return_for_supplement(rid, level=1, approver="rev",
                                             reason="缺少装备编号")
        self.assertEqual(ret["status"], "RETURNED")
        view = self.svc.get_request_view(rid)
        self.assertTrue(any("退回" in b for b in view["blocking_reasons"]))

        am = self.svc.amend_request(rid, quantity=7, actor="requester")
        self.assertEqual(am["status"], "SUBMITTED")
        self.assertEqual(am["version"], 3)  # 提交 v1 -> 退回 v2 -> 修改 v3

        res = self.approve_all(rid)
        self.assertEqual(sum(a["quantity"] for a in res["allocations"]), 7)
        events = [e["event"] for e in self.svc.get_request_view(rid)["audit_trail"]]
        for ev in ("SUBMITTED", "RETURNED", "AMENDED", "APPROVAL", "APPROVED"):
            self.assertIn(ev, events)

    def test_amend_only_allowed_when_returned(self):
        r = self.submit(qty=5)
        with self.assertRaises(ConflictError):
            self.svc.amend_request(r["request_id"], quantity=3)


class TestTransferFailureRecovery(Base):
    def test_shipment_failure_recovers_unfinished_state(self):
        """跨仓库调拨失败：货物退回并重新预留，申请恢复未完成状态，可重新出库。"""
        self.svc.receive_stock("WH-BJ", "TB-100", 5)
        r = self.submit(qty=5)
        rid = r["request_id"]
        res = self.approve_all(rid)
        aid = res["allocations"][0]["allocation_id"]
        ship = self.svc.ship(aid)
        pos = self.svc.inventory_position("TB-100")
        self.assertEqual((pos["on_hand"], pos["reserved"]), (0, 0))

        fail = self.svc.fail_shipment(ship["shipment_id"], reason="运输途中车辆故障")
        self.assertTrue(fail["recovered"])
        pos = self.svc.inventory_position("TB-100")
        self.assertEqual((pos["on_hand"], pos["reserved"]), (5, 5))  # 退回并重新预留
        view = self.svc.get_request_view(rid)
        self.assertEqual(view["status"], "APPROVED")  # 恢复未完成状态
        self.assertTrue(any("调拨失败" in b for b in view["blocking_reasons"]))

        # 恢复后可重新出库并完成
        ship2 = self.svc.ship(aid)
        done = self.svc.deliver(ship2["shipment_id"])
        self.assertEqual(done["request_status"], "COMPLETED")

    def test_replan_after_failure_uses_other_warehouse(self):
        """失败仓库被排除后，重新规划改从其他仓库调拨。"""
        self.svc.receive_stock("WH-BJ", "TB-100", 5)
        self.svc.receive_stock("WH-SH", "TB-100", 5)
        r = self.submit(qty=5)
        rid = r["request_id"]
        res = self.approve_all(rid)
        self.assertEqual(res["allocations"][0]["warehouse_id"], "WH-BJ")
        ship = self.svc.ship(res["allocations"][0]["allocation_id"])
        self.svc.fail_shipment(ship["shipment_id"], reason="道路中断")

        out = self.svc.replan(rid, exclude_warehouses={"WH-BJ"})
        self.assertEqual(out["released_quantity"], 5)
        self.assertEqual(len(out["new_allocations"]), 1)
        self.assertEqual(out["new_allocations"][0]["warehouse_id"], "WH-SH")
        pos = {w["warehouse_id"]: w for w in
               self.svc.inventory_position("TB-100")["warehouses"]}
        self.assertEqual(pos["WH-BJ"]["available"], 5)   # 退回后可分配
        self.assertEqual(pos["WH-SH"]["reserved"], 5)    # 新预留

    def test_failed_shipment_cannot_be_failed_twice(self):
        self.svc.receive_stock("WH-BJ", "TB-100", 5)
        r = self.submit(qty=5)
        res = self.approve_all(r["request_id"])
        ship = self.svc.ship(res["allocations"][0]["allocation_id"])
        self.svc.fail_shipment(ship["shipment_id"], reason="故障")
        with self.assertRaises(ConflictError):
            self.svc.fail_shipment(ship["shipment_id"], reason="重复上报")


class TestManagerView(Base):
    def test_manager_view_fields(self):
        """管理视图包含可用量、承诺到货时间、阻塞原因与完整审计记录。"""
        self.svc.receive_stock("WH-BJ", "TB-100", 4)
        r = self.submit(qty=6)
        rid = r["request_id"]
        self.approve_all(rid)
        view = self.svc.get_request_view(rid)
        for key in ("available_now", "promised_arrival", "blocking_reasons",
                    "audit_trail", "approvals", "allocations", "shipments",
                    "reserved_quantity", "shipped_quantity", "version"):
            self.assertIn(key, view)
        self.assertEqual(view["available_now"], 0)
        self.assertEqual(view["reserved_quantity"], 4)
        self.assertEqual(view["open_quantity"], 2)
        self.assertTrue(any("库存不足" in b for b in view["blocking_reasons"]))
        self.assertIsNotNone(view["promised_arrival"])
        # 审计记录包含审批层级与库存流水
        kinds = {e["kind"] for e in view["audit_trail"]}
        self.assertEqual(kinds, {"audit", "inventory"})
        levels = [e["level"] for e in view["audit_trail"]
                  if e["event"] == "APPROVAL"]
        self.assertEqual(levels, [1, 2])

    def test_completed_flow(self):
        self.svc.receive_stock("WH-BJ", "TB-100", 3)
        r = self.submit(qty=3)
        rid = r["request_id"]
        res = self.approve_all(rid)
        ship = self.svc.ship(res["allocations"][0]["allocation_id"])
        done = self.svc.deliver(ship["shipment_id"])
        self.assertEqual(done["request_status"], "COMPLETED")
        with self.assertRaises(ConflictError):
            self.svc.ship(res["allocations"][0]["allocation_id"])
        with self.assertRaises(ConflictError):
            self.svc.cancel(rid, actor="manager")


class TestLedgerConsistency(Base):
    def test_ledger_rebuilds_inventory_state(self):
        """任意操作序列后，库存表状态可由流水账重放得到（对账）。"""
        self.svc.receive_stock("WH-BJ", "TB-100", 5)
        self.svc.receive_stock("WH-SH", "TB-100", 6)
        r1 = self.submit(qty=8, key="R1")
        res1 = self.approve_all(r1["request_id"])           # BJ 5 + SH 3
        self.assertEqual([a["quantity"] for a in res1["allocations"]], [5, 3])
        bj_alloc = [a for a in res1["allocations"] if a["warehouse_id"] == "WH-BJ"][0]
        ship = self.svc.ship(bj_alloc["allocation_id"])     # BJ 出库 5
        self.svc.deliver(ship["shipment_id"])
        r2 = self.submit(qty=4, key="R2")
        res2 = self.approve_all(r2["request_id"])           # SH 余 3，缺口 1
        self.assertEqual(res2["shortfall"], 1)
        self.svc.cancel(r2["request_id"], actor="mgr")      # 释放 3

        conn = self.svc.store.conn
        for row in conn.execute("SELECT * FROM inventory").fetchall():
            sums = conn.execute(
                """SELECT COALESCE(SUM(delta_on_hand),0) oh, COALESCE(SUM(delta_reserved),0) rv
                   FROM inventory_ledger WHERE warehouse_id=? AND part_number=?""",
                (row["warehouse_id"], row["part_number"])).fetchone()
            self.assertEqual(row["on_hand"], sums["oh"])
            self.assertEqual(row["reserved"], sums["rv"])
            self.assertGreaterEqual(row["on_hand"] - row["reserved"], 0)


if __name__ == "__main__":
    unittest.main()
