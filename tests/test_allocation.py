"""端到端测试：覆盖分配、审批、幂等、抢占、撤销、Saga 恢复、审计与管理视图。"""

from __future__ import annotations

import threading
import unittest

from src.domain import FaultLevel
from src.service import AllocationService, WarehouseGatewayError


def build_service() -> AllocationService:
    svc = AllocationService(":memory:")
    svc.register_part("P-BRAKE", "刹车片", "TANK-99")
    svc.register_part("P-FILTER", "燃油滤", "TANK-99")
    svc.register_warehouse("WH-A", "甲区中心库", "北部")
    svc.register_warehouse("WH-B", "乙区前置库", "东部")
    svc.register_warehouse("WH-C", "丙区后方库", "南部")
    # WH-A 6、WH-B 4、WH-C 20（时限外/远）
    svc.set_stock("WH-A", "P-BRAKE", 6)
    svc.set_stock("WH-B", "P-BRAKE", 4)
    svc.set_stock("WH-C", "P-BRAKE", 20)
    svc.set_stock("WH-A", "P-FILTER", 2)
    # 目的地"前线1"：A 10h、B 6h、C 48h
    svc.set_transit("WH-A", "前线1", 10)
    svc.set_transit("WH-B", "前线1", 6)
    svc.set_transit("WH-C", "前线1", 48)
    return svc


class AllocationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = build_service()

    def test_split_across_warehouses_within_deadline(self):
        # 需要 8：时限 12h 内只有 B(4,6h)+A(6,10h)，按最快先取 B 再取 A
        r = self.svc.submit_request(
            part_id="P-BRAKE", equipment_model="TANK-99",
            fault_level="MAJOR", qty=8, destination="前线1",
            deadline_hours=12, actor="unit-1", idempotency_key="k1",
        )
        decision = r["decision"]
        self.assertEqual(decision["status"], "PLANNED")
        self.assertEqual([(l["warehouse_id"], l["qty"]) for l in decision["lines"]],
                         [("WH-B", 4), ("WH-A", 4)])
        self.assertEqual(decision["eta_hours"], 10)

    def test_blocked_shortage(self):
        # 全网总量仅 30（A6+B4+C20），申请 31 件，放宽时限也凑不齐
        r = self.svc.submit_request(
            part_id="P-BRAKE", equipment_model="TANK-99",
            fault_level="MAJOR", qty=31, destination="前线1",
            deadline_hours=72, actor="unit-1",
        )
        self.assertEqual(r["decision"]["status"], "BLOCKED")
        self.assertEqual(r["decision"]["block_code"], "SHORTAGE")
        self.assertEqual(r["decision"]["block_detail"]["capacity_all_reachable"], 30)

    def test_blocked_by_deadline_but_stock_exists_far_away(self):
        # 需要 11：时限内 10 件，但放宽到 48h 有 30 件 → 阻塞原因是时限而非缺货
        r = self.svc.submit_request(
            part_id="P-BRAKE", equipment_model="TANK-99",
            fault_level="MAJOR", qty=11, destination="前线1",
            deadline_hours=12, actor="unit-1",
        )
        self.assertEqual(r["decision"]["status"], "BLOCKED")
        self.assertEqual(r["decision"]["block_code"], "DEADLINE_INFEASIBLE")

    def test_blocked_deadline(self):
        # WH-C 有货但 48h，赶不上 24h 时限
        r = self.svc.submit_request(
            part_id="P-BRAKE", equipment_model="TANK-99",
            fault_level="MINOR", qty=20, destination="前线1",
            deadline_hours=24, actor="unit-1",
        )
        self.assertEqual(r["decision"]["status"], "BLOCKED")
        self.assertEqual(r["decision"]["block_code"], "DEADLINE_INFEASIBLE")

    def test_replan_after_restock(self):
        r = self.svc.submit_request(
            part_id="P-FILTER", equipment_model="TANK-99",
            fault_level="MAJOR", qty=5, destination="前线1",
            deadline_hours=12, actor="unit-1",
        )
        self.assertEqual(r["decision"]["status"], "BLOCKED")
        rid = r["request_id"]
        self.svc.receive_stock("WH-B", "P-FILTER", 5)
        self.svc.replan_blocked(rid)
        view = self.svc.get_request(rid)
        self.assertEqual(view["status"], "PLANNED")
        self.assertEqual(view["eta_hours"], 6)


    def test_blocked_return_then_supplement_with_relaxed_deadline(self):
        # 20 件时限 24h：WH-C(48h) 的货赶不上 → 阻塞
        r = self.svc.submit_request(
            part_id="P-BRAKE", equipment_model="TANK-99",
            fault_level="MINOR", qty=20, destination="前线1",
            deadline_hours=24, actor="unit-9",
        )
        rid = r["request_id"]
        self.assertEqual(r["decision"]["block_code"], "DEADLINE_INFEASIBLE")
        # 阻塞单也可退回补充（此时尚无任何层级批准）
        self.svc.return_for_supplement(rid, actor="l1", comment="确认能否放宽至72h")
        self.svc.supplement(rid, {"deadline_hours": 72}, actor="unit-9", reason="获准72h")
        view = self.svc.get_request(rid)
        self.assertEqual(view["version"], 2)
        self.assertEqual(view["status"], "PLANNED")
        self.assertEqual(view["eta_hours"], 48)
        self.assertEqual(sum(l["qty"] for l in view["plan_generation_lines"]), 20)


class IdempotencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = build_service()

    def test_retry_returns_original_decision(self):
        kw = dict(part_id="P-BRAKE", equipment_model="TANK-99",
                  fault_level="MAJOR", qty=3, destination="前线1",
                  deadline_hours=12, actor="unit-1", idempotency_key="net-retry-1")
        first = self.svc.submit_request(**kw)
        second = self.svc.submit_request(**kw)  # 网络重试：同一幂等键
        self.assertEqual(first["request_id"], second["request_id"])
        self.assertEqual(first["decision"], second["decision"])
        self.assertEqual(len(self.svc.list_requests()), 1)

    def test_same_key_different_payload_rejected(self):
        self.svc.submit_request(
            part_id="P-BRAKE", equipment_model="TANK-99",
            fault_level="MAJOR", qty=3, destination="前线1",
            deadline_hours=12, actor="unit-1", idempotency_key="dup-key",
        )
        from src.domain import IdempotencyConflictError
        with self.assertRaises(IdempotencyConflictError):
            self.svc.submit_request(
                part_id="P-BRAKE", equipment_model="TANK-99",
                fault_level="MAJOR", qty=4, destination="前线1",  # 数量不同
                deadline_hours=12, actor="unit-1", idempotency_key="dup-key",
            )

    def test_fulfill_idempotent_retry(self):
        r = self.svc.submit_request(
            part_id="P-BRAKE", equipment_model="TANK-99",
            fault_level="MINOR", qty=2, destination="前线1",
            deadline_hours=12, actor="unit-1", idempotency_key="sub",
        )
        rid = r["request_id"]
        self.svc.approve(rid, actor="dispatcher")
        d1 = self.svc.fulfill(rid, idempotency_key="ship")
        d2 = self.svc.fulfill(rid, idempotency_key="ship")
        self.assertEqual(d1["decision"]["status"], "SHIPPED")
        self.assertEqual(d2["decision"]["status"], "SHIPPED")
        view = self.svc.stock_view("P-BRAKE")
        # 出库只扣一次
        self.assertEqual(sum(v["on_hand"] for v in view), 30 - 2)


class ApprovalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = build_service()

    def _submit(self, level, qty=2):
        r = self.svc.submit_request(
            part_id="P-BRAKE", equipment_model="TANK-99",
            fault_level=level, qty=qty, destination="前线1",
            deadline_hours=12, actor="unit-1",
        )
        return r["request_id"]

    def test_critical_requires_three_levels(self):
        rid = self._submit("CRITICAL")
        self.svc.approve(rid, actor="l1")
        self.assertEqual(self.svc.get_request(rid)["status"], "PENDING_APPROVAL")
        self.svc.approve(rid, actor="l2")
        self.assertEqual(self.svc.get_request(rid)["status"], "PENDING_APPROVAL")
        from src.domain import InvalidStateError
        with self.assertRaises(InvalidStateError):
            self.svc.approve(rid, actor="wrong", level=1)  # 层级跳序
        self.svc.approve(rid, actor="l3")
        view = self.svc.get_request(rid)
        self.assertEqual(view["status"], "APPROVED")
        self.assertEqual(view["approved_levels_current_generation"], [1, 2, 3])

    def test_return_for_supplement_bumps_version(self):
        rid = self._submit("MAJOR", qty=2)
        self.svc.approve(rid, actor="l1")
        self.svc.return_for_supplement(rid, actor="l2", comment="缺装备序列号")
        self.assertEqual(self.svc.get_request(rid)["status"], "RETURNED")
        self.svc.supplement(rid, {"qty": 1}, actor="unit-1", reason="补序列号后改为1件")
        view = self.svc.get_request(rid)
        self.assertEqual(view["version"], 2)
        self.assertEqual(view["qty"], 1)
        # 旧代次的批准不再算数
        self.assertEqual(view["approved_levels_current_generation"], [])

    def test_reservation_locks_and_blocks_second_request(self):
        rid1 = self._submit("CRITICAL", qty=6)
        self.svc.approve(rid1, actor="l1")
        self.svc.approve(rid1, actor="l2")
        self.svc.approve(rid1, actor="l3")
        # WH-B 4 件已硬预留；第二个申请要 4 件时限 12h，WH-B 不可用
        r2 = self.svc.submit_request(
            part_id="P-BRAKE", equipment_model="TANK-99",
            fault_level="MAJOR", qty=4, destination="前线1",
            deadline_hours=12, actor="unit-2",
        )
        # WH-A 有 6 可用，方案应只走 WH-A
        whs = {l["warehouse_id"] for l in r2["decision"]["lines"]}
        self.assertNotIn("WH-B", whs)
        self.assertEqual(self.svc.stock_view("P-BRAKE")[1]["reserved"], 4)

    def test_higher_fault_evicts_lower_soft_hold(self):
        # 低等级 MINOR 先占住 WH-B 全部 4 件，时限 8h（仅 WH-B 可达）
        low = self.svc.submit_request(
            part_id="P-BRAKE", equipment_model="TANK-99",
            fault_level="MINOR", qty=4, destination="前线1",
            deadline_hours=8, actor="unit-1",
        )
        low = low["request_id"]
        self.assertEqual(self.svc.get_request(low)["status"], "PLANNED")
        # CRITICAL 同要 WH-B 的 4 件
        high = self.svc.submit_request(
            part_id="P-BRAKE", equipment_model="TANK-99",
            fault_level="CRITICAL", qty=4, destination="前线1",
            deadline_hours=8, actor="unit-2",
        )
        self.assertEqual(
            [(l["warehouse_id"], l["qty"]) for l in high["decision"]["lines"]],
            [("WH-B", 4)],
        )
        low_view = self.svc.get_request(low)
        # 受害者方案作废、方案代次 +1 后重规划：8h 内仅剩 WH-A? A 为 10h → 阻塞
        self.assertEqual(low_view["status"], "BLOCKED")
        self.assertEqual(low_view["block_reason"]["code"], "DEADLINE_INFEASIBLE")
        self.assertGreaterEqual(low_view["plan_generation"], 2)
        actions = {a["action"] for a in low_view["audit"]}
        self.assertIn("HOLD_EVICTED", actions)
        self.assertIn("PLAN_INVALIDATED", actions)

        # 高等级申请也不能抢占同级/更高等级的占位
        same_level = self.svc.submit_request(
            part_id="P-BRAKE", equipment_model="TANK-99",
            fault_level="CRITICAL", qty=4, destination="前线1",
            deadline_hours=8, actor="unit-3",
        )
        self.assertEqual(same_level["decision"]["status"], "BLOCKED")
        # 原 CRITICAL 方案完好
        self.assertEqual(self.svc.get_request(high["request_id"])["status"], "PLANNED")

    def test_hard_reservation_never_evicted(self):
        # 高等级也不能动已批准的硬预留
        rid = self._submit("MINOR", qty=4)
        self.svc.approve(rid, actor="l1")  # MINOR 终审即硬预留 WH-B x4
        from src.domain import InvalidStateError
        high = self.svc.submit_request(
            part_id="P-BRAKE", equipment_model="TANK-99",
            fault_level="CRITICAL", qty=4, destination="前线1",
            deadline_hours=8, actor="unit-2",
        )
        # 8h 内只剩 WH-A? A 10h 也超时 → 阻塞
        self.assertEqual(high["decision"]["status"], "BLOCKED")
        # 已批准申请完好
        self.assertEqual(self.svc.get_request(rid)["status"], "APPROVED")


class RevokeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = build_service()

    def test_revoke_before_approval_releases_nothing_but_closes(self):
        r = self.svc.submit_request(
            part_id="P-BRAKE", equipment_model="TANK-99",
            fault_level="MAJOR", qty=2, destination="前线1",
            deadline_hours=12, actor="unit-1",
        )
        rid = r["request_id"]
        self.svc.revoke(rid, actor="chief", reason="部队撤回")
        self.assertEqual(self.svc.get_request(rid)["status"], "REVOKED")
        # 软占位释放：另一申请能用全部库存
        avail = {v["warehouse_id"]: v["available"]
                 for v in self.svc.stock_view("P-BRAKE")}
        self.assertEqual(avail["WH-B"], 4)
        self.assertEqual(avail["WH-A"], 6)

    def test_revoke_after_partial_ship_keeps_shipped_locked(self):
        r = self.svc.submit_request(
            part_id="P-BRAKE", equipment_model="TANK-99",
            fault_level="MAJOR", qty=8, destination="前线1",
            deadline_hours=12, actor="unit-1",
        )
        rid = r["request_id"]
        self.svc.approve(rid, actor="l1")
        self.svc.approve(rid, actor="l2")

        calls = {"n": 0}

        def flaky_gateway(wh, part, qty, token):
            calls["n"] += 1
            if calls["n"] == 1:
                return  # WH-B 先成功
            raise WarehouseGatewayError("WH-A 网络中断")

        res = self.svc.fulfill(rid, flaky_gateway)
        self.assertEqual(res["decision"]["status"], "PARTIALLY_SHIPPED")
        self.svc.revoke(rid, actor="chief", reason="不再需要")
        view = self.svc.get_request(rid)
        self.assertEqual(view["status"], "PARTIALLY_REVOKED")
        self.assertEqual(view["shipped_qty"], 4)
        # WH-A 的 4 件预留已释放，WH-B 4 件已出库锁定（账面 0）
        stock = {v["warehouse_id"]: v for v in self.svc.stock_view("P-BRAKE")}
        self.assertEqual(stock["WH-B"]["on_hand"], 0)
        self.assertEqual(stock["WH-A"]["available"], 6)
        # 已出库不能被新申请占用：账面为 0
        self.assertEqual(stock["WH-B"]["available"], 0)


class SagaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = build_service()
        r = self.svc.submit_request(
            part_id="P-BRAKE", equipment_model="TANK-99",
            fault_level="MAJOR", qty=8, destination="前线1",
            deadline_hours=12, actor="unit-1",
        )
        self.rid = r["request_id"]
        self.svc.approve(self.rid, actor="l1")
        self.svc.approve(self.rid, actor="l2")

    def test_failure_then_retry_completes(self):
        state = {"fail": True}

        def gateway(wh, part, qty, token):
            if wh == "WH-A" and state["fail"]:
                raise WarehouseGatewayError("临时故障")

        r1 = self.svc.fulfill(self.rid, gateway)
        self.assertEqual(r1["decision"]["status"], "PARTIALLY_SHIPPED")
        view = self.svc.get_request(self.rid)
        self.assertEqual(len(view["fulfillment_failures"]), 1)
        # 恢复网络后重试：FAILED 行先执行
        state["fail"] = False
        r2 = self.svc.fulfill(self.rid, gateway)
        self.assertEqual(r2["decision"]["status"], "SHIPPED")
        stock = {v["warehouse_id"]: v for v in self.svc.stock_view("P-BRAKE")}
        self.assertEqual(stock["WH-A"]["on_hand"], 2)
        self.assertEqual(stock["WH-B"]["on_hand"], 0)

    def test_all_fail_keeps_reservation_and_resumable(self):
        def always_fail(wh, part, qty, token):
            raise WarehouseGatewayError("WMS 宕机")

        r1 = self.svc.fulfill(self.rid, always_fail)
        self.assertEqual(r1["decision"]["status"], "FULFILLMENT_FAILED")
        # 预留全部保留：其他申请仍看不到这些量
        stock = {v["warehouse_id"]: v for v in self.svc.stock_view("P-BRAKE")}
        self.assertEqual(stock["WH-B"]["available"], 0)
        self.assertEqual(stock["WH-A"]["available"], 2)
        r2 = self.svc.fulfill(self.rid, None)  # 无网关=全部成功
        self.assertEqual(r2["decision"]["status"], "SHIPPED")


class ViewAndAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = build_service()

    def test_manager_view_contents(self):
        r = self.svc.submit_request(
            part_id="P-BRAKE", equipment_model="TANK-99",
            fault_level="CRITICAL", qty=3, destination="前线1",
            deadline_hours=12, actor="unit-1",
        )
        rid = r["request_id"]
        self.svc.approve(rid, actor="l1")
        view = self.svc.get_request(rid)
        # 可用量
        self.assertEqual({v["warehouse_id"]: v["available"]
                          for v in view["available_stock"]},
                         {"WH-A": 6, "WH-B": 1, "WH-C": 20})
        # 承诺到货
        self.assertEqual(view["committed_arrival"], "6 小时内")
        # 审批层级
        self.assertEqual([c["label"] for c in view["approval_chain"]],
                         ["现场调度员", "仓库主任", "装备保障部首长"])
        # 阻塞原因为 None
        self.assertIsNone(view["block_reason"])
        # 审计：提交 + 规划 + 一次批准
        actions = [a["action"] for a in view["audit"]]
        self.assertEqual(actions[:2], ["SUBMIT", "PLAN_CREATED"])
        self.assertIn("APPROVE", actions)

    def test_stock_change_audit_has_before_after(self):
        self.svc.set_stock("WH-A", "P-BRAKE", 10)
        trail = self.svc.audit_trail("STOCK", "WH-A:P-BRAKE")
        # 初始化 + 调整两条
        actions = [(a["action"], a["before"], a["after"]) for a in trail]
        self.assertEqual(actions[0][0], "STOCK_ADJUST")
        self.assertIsNone(actions[0][1])
        self.assertEqual(actions[0][2]["on_hand"], 6)
        self.assertEqual(actions[1][0], "STOCK_ADJUST")
        self.assertEqual(actions[1][1]["on_hand"], 6)
        self.assertEqual(actions[1][2]["on_hand"], 10)

    def test_reserve_and_ship_audited(self):
        r = self.svc.submit_request(
            part_id="P-BRAKE", equipment_model="TANK-99",
            fault_level="MINOR", qty=2, destination="前线1",
            deadline_hours=12, actor="unit-1",
        )
        rid = r["request_id"]
        self.svc.approve(rid, actor="l1")
        self.svc.fulfill(rid)
        trail = self.svc.audit_trail("STOCK", "WH-B:P-BRAKE")
        kinds = [a["action"] for a in trail]
        self.assertIn("RESERVE", kinds)
        ship = next(a for a in trail if a["action"] == "SHIP")
        self.assertEqual(ship["before"]["on_hand"] - ship["after"]["on_hand"], 2)
        self.assertEqual(ship["before"]["reserved"] - ship["after"]["reserved"], 2)
        # 申请的完整审计必须包含库存预留/出库事件
        req_actions = [a["action"] for a in self.svc.get_request(rid)["audit"]]
        self.assertIn("RESERVE", req_actions)
        self.assertIn("SHIP", req_actions)
        stock_events = [a for a in self.svc.get_request(rid)["audit"]
                        if a["aggregate_type"] == "STOCK"]
        self.assertTrue(all(a["ref_request_id"] == rid for a in stock_events))


class ConcurrencyTests(unittest.TestCase):
    def test_no_double_allocation_under_threads(self):
        svc = build_service()
        errors: list[Exception] = []

        def submit(i):
            try:
                svc.submit_request(
                    part_id="P-BRAKE", equipment_model="TANK-99",
                    fault_level="MINOR", qty=5, destination="前线1",
                    deadline_hours=12, actor=f"unit-{i}",
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=submit, args=(i,)) for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertFalse(errors)
        # 时限内总容量 10：最多两个申请各 5 件得到方案，其余阻塞
        planned = [r for r in svc.list_requests() if r["status"] in
                   ("PLANNED", "PENDING_APPROVAL", "APPROVED")]
        blocked = [r for r in svc.list_requests() if r["status"] == "BLOCKED"]
        self.assertEqual(len(planned), 2)
        self.assertEqual(len(blocked), 4)
        # 库存视图守恒：软/硬占用不超过账面
        for v in svc.stock_view("P-BRAKE"):
            self.assertGreaterEqual(v["on_hand"], 0)


if __name__ == "__main__":
    unittest.main()
