"""调拨领域服务：申请流转、逐级审批、库存预留/出库、幂等与 Saga 恢复。

并发模型：进程内一把 RLock 串行化所有写操作；SQLite 以 BEGIN IMMEDIATE
开启写事务，保证崩溃时事务原子性。多实例部署时把 Repository 换成同构的
中央数据库实现即可，SQL 与锁序无需调整。
"""

from __future__ import annotations

import hashlib
import json
import threading
import uuid
from collections.abc import Callable
from typing import Any

from .allocation import Hold, PlanOutcome, StockCell, build_plan
from .domain import (
    ApprovalLevel,
    BlockerCode,
    FaultLevel,
    IdempotencyConflictError,
    InvalidStateError,
    RequestStatus,
    ReservationLostError,
)
from .storage import Repository, dumps, loads, now_iso

# 故障等级 -> 审批链
_APPROVAL_CHAIN: dict[FaultLevel, tuple[ApprovalLevel, ...]] = {
    FaultLevel.MINOR: (ApprovalLevel.LEVEL1,),
    FaultLevel.MAJOR: (ApprovalLevel.LEVEL1, ApprovalLevel.LEVEL2),
    FaultLevel.CRITICAL: (
        ApprovalLevel.LEVEL1,
        ApprovalLevel.LEVEL2,
        ApprovalLevel.LEVEL3,
    ),
}



class WarehouseGatewayError(Exception):
    """仓库 WMS 出库网关失败（可恢复：预留仍在，稍后重试）。"""


class AllocationService:
    """装备维修备件调拨服务。"""

    def __init__(self, repo: Repository | str = ":memory:"):
        self.repo = repo if isinstance(repo, Repository) else Repository(repo)
        self._lock = threading.RLock()
        self._migrate()

    # ------------------------------------------------------------------ #
    # 基础数据维护
    # ------------------------------------------------------------------ #

    def register_part(self, part_id: str, name: str, equipment_model: str, spec: str = "") -> None:
        with self._lock, self._tx():
            exists = self.repo.query_one("SELECT 1 FROM parts WHERE part_id=?", (part_id,))
            if exists:
                self.repo.execute(
                    "UPDATE parts SET name=?, equipment_model=?, spec=? WHERE part_id=?",
                    (name, equipment_model, spec, part_id),
                )
            else:
                self.repo.execute(
                    "INSERT INTO parts(part_id,name,equipment_model,spec) VALUES(?,?,?,?)",
                    (part_id, name, equipment_model, spec),
                )

    def register_warehouse(self, warehouse_id: str, name: str, location: str) -> None:
        with self._lock, self._tx():
            exists = self.repo.query_one(
                "SELECT 1 FROM warehouses WHERE warehouse_id=?", (warehouse_id,)
            )
            if exists:
                self.repo.execute(
                    "UPDATE warehouses SET name=?, location=? WHERE warehouse_id=?",
                    (name, location, warehouse_id),
                )
            else:
                self.repo.execute(
                    "INSERT INTO warehouses(warehouse_id,name,location) VALUES(?,?,?)",
                    (warehouse_id, name, location),
                )

    def set_stock(self, warehouse_id: str, part_id: str, on_hand: int, *, actor: str = "system") -> None:
        """设置/初始化账面库存（部署期盘点录入），记录库存变化审计。"""
        if on_hand < 0:
            raise ValueError("on_hand 不能为负")
        with self._lock, self._tx():
            self._require_warehouse(warehouse_id)
            self._require_part(part_id)
            row = self.repo.query_one(
                "SELECT on_hand, reserved, issued_total FROM stock "
                "WHERE warehouse_id=? AND part_id=?",
                (warehouse_id, part_id),
            )
            before = dict(row) if row else None
            if row:
                # 盘点调整不得低于已预留量
                if on_hand < row["reserved"]:
                    raise InvalidStateError(
                        f"调整后账面 {on_hand} 低于已预留 {row['reserved']}，拒绝调整"
                    )
                self.repo.execute(
                    "UPDATE stock SET on_hand=? WHERE warehouse_id=? AND part_id=?",
                    (on_hand, warehouse_id, part_id),
                )
            else:
                self.repo.execute(
                    "INSERT INTO stock(warehouse_id,part_id,on_hand,reserved,issued_total)"
                    " VALUES(?,?,?,0,0)",
                    (warehouse_id, part_id, on_hand),
                )
            after = self._stock_row(warehouse_id, part_id)
            self._audit(
                "STOCK", f"{warehouse_id}:{part_id}", "STOCK_ADJUST", actor,
                before=before, after=dict(after),
            )

    def receive_stock(self, warehouse_id: str, part_id: str, qty: int, *, actor: str = "system") -> None:
        """入库补货：增加账面量。补货后可对阻塞申请重新规划。"""
        if qty <= 0:
            raise ValueError("入库数量必须为正")
        with self._lock, self._tx():
            self._require_warehouse(warehouse_id)
            self._require_part(part_id)
            row = self._stock_row(warehouse_id, part_id)
            before = dict(row) if row else None
            if row:
                self.repo.execute(
                    "UPDATE stock SET on_hand=on_hand+? WHERE warehouse_id=? AND part_id=?",
                    (qty, warehouse_id, part_id),
                )
            else:
                self.repo.execute(
                    "INSERT INTO stock(warehouse_id,part_id,on_hand,reserved,issued_total)"
                    " VALUES(?,?,?,0,0)",
                    (warehouse_id, part_id, qty),
                )
            after = self._stock_row(warehouse_id, part_id)
            self._audit(
                "STOCK", f"{warehouse_id}:{part_id}", "STOCK_RECEIVE", actor,
                before=before, after=dict(after), detail={"qty": qty},
            )

    def set_transit(self, warehouse_id: str, destination: str, hours: int) -> None:
        if hours <= 0:
            raise ValueError("运输时限必须为正")
        with self._lock, self._tx():
            self._require_warehouse(warehouse_id)
            self.repo.execute(
                "INSERT INTO transit(warehouse_id,destination,hours) VALUES(?,?,?) "
                "ON CONFLICT(warehouse_id,destination) DO UPDATE SET hours=excluded.hours",
                (warehouse_id, destination, hours),
            )

    # ------------------------------------------------------------------ #
    # 申请提交（幂等）
    # ------------------------------------------------------------------ #

    def submit_request(
        self,
        *,
        part_id: str,
        equipment_model: str,
        fault_level: FaultLevel | str,
        qty: int,
        destination: str,
        deadline_hours: int,
        actor: str,
        idempotency_key: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """提交维修申请并立即生成分配方案。网络重试携带同一幂等键返回原决定。"""
        fl = FaultLevel(fault_level) if isinstance(fault_level, str) else fault_level
        if qty <= 0 or deadline_hours <= 0:
            raise ValueError("数量与运输时限必须为正")
        fp = self._fingerprint(
            part_id, equipment_model, fl.value, qty, destination, deadline_hours
        )

        def work() -> dict[str, Any]:
            part = self._require_part(part_id)
            if part["equipment_model"] != equipment_model:
                raise InvalidStateError(
                    f"备件 {part_id} 属于装备型号 {part['equipment_model']}，"
                    f"与申请型号 {equipment_model} 不符"
                )
            rid = request_id or f"REQ-{uuid.uuid4().hex[:12].upper()}"
            if self.repo.query_one("SELECT 1 FROM requests WHERE request_id=?", (rid,)):
                raise InvalidStateError(f"申请号 {rid} 已存在")
            ts = now_iso()
            self.repo.execute(
                "INSERT INTO requests(request_id,version,part_id,equipment_model,"
                "fault_level,qty,destination,deadline_hours,status,plan_generation,"
                "idempotency_key,created_at,updated_at) "
                "VALUES(?,1,?,?,?,?,?,?,'SUBMITTED',1,?,?,?)",
                (rid, part_id, equipment_model, fl.value, qty, destination,
                 deadline_hours, idempotency_key, ts, ts),
            )
            payload = {
                "part_id": part_id, "equipment_model": equipment_model,
                "fault_level": fl.value, "qty": qty,
                "destination": destination, "deadline_hours": deadline_hours,
            }
            self.repo.execute(
                "INSERT INTO revisions(request_id,version,actor,reason,payload,created_at)"
                " VALUES(?,1,?,'initial',?,?)",
                (rid, actor, dumps(payload), ts),
            )
            self._audit("REQUEST", rid, "SUBMIT", actor, version=1, after=payload)
            # 立即规划
            req = self._request_row(rid)
            self._apply_plan(req, actor)
            return {"request_id": rid, "decision": self._snapshot_decision(rid)}

        return self._idem("SUBMIT", idempotency_key, fp, actor, work)

    # ------------------------------------------------------------------ #
    # 退回补充 / 补充后再规划（均版本化）
    # ------------------------------------------------------------------ #

    def return_for_supplement(
        self, request_id: str, *, actor: str, comment: str = "",
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """审批人在当前待审层级退回，申请方补充后重新提交即升版本。"""
        fp = self._fingerprint("return", request_id, comment)

        def work() -> dict[str, Any]:
            req = self._request_row(request_id)
            self._assert_status(req, ("PLANNED", "PENDING_APPROVAL", "BLOCKED"))
            # 阻塞申请可能尚无任何层级批准，退回层级记为审批链第一级
            try:
                level = self._pending_level(req)
            except InvalidStateError:
                level = _APPROVAL_CHAIN[FaultLevel(req["fault_level"])][0]
            ts = now_iso()
            self.repo.execute(
                "INSERT INTO approvals(request_id,version,generation,level,decision,"
                "actor,comment,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (request_id, req["version"], req["plan_generation"], level.value,
                 "RETURNED", actor, comment, ts),
            )
            self._supersede_planned(request_id)
            self.repo.execute(
                "UPDATE requests SET status='RETURNED',block_code=?,block_detail=?,"
                "updated_at=? WHERE request_id=?",
                (BlockerCode.SUPPLEMENT_REQUIRED.value,
                 dumps({"comment": comment, "at_level": level.value}), ts, request_id),
            )
            self._audit(
                "REQUEST", request_id, "RETURN_FOR_SUPPLEMENT", actor,
                version=req["version"], detail={"level": level.value, "comment": comment},
            )
            return {"request_id": request_id, "decision": self._snapshot_decision(request_id)}

        return self._idem("RETURN", idempotency_key, fp, actor, work)

    def supplement(
        self, request_id: str, payload: dict[str, Any], *, actor: str,
        reason: str = "", idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """补充材料/修订参数，产生新版本并重新规划。"""
        fp = self._fingerprint("supplement", request_id, dumps(payload))

        def work() -> dict[str, Any]:
            req = self._request_row(request_id)
            self._assert_status(req, ("RETURNED",))
            new_version = req["version"] + 1
            new_gen = req["plan_generation"] + 1
            ts = now_iso()
            # 允许补充时修订数量/时限/等级/目的地；未提供字段沿用原值
            merged = {
                "qty": req["qty"],
                "deadline_hours": req["deadline_hours"],
                "fault_level": req["fault_level"],
                "destination": req["destination"],
                "part_id": req["part_id"],
                "equipment_model": req["equipment_model"],
            }
            for key in ("qty", "deadline_hours", "fault_level", "destination"):
                if key in payload:
                    merged[key] = payload[key]
            if merged["qty"] <= 0 or merged["deadline_hours"] <= 0:
                raise ValueError("数量与运输时限必须为正")
            FaultLevel(merged["fault_level"])
            self.repo.execute(
                "INSERT INTO revisions(request_id,version,actor,reason,payload,created_at)"
                " VALUES(?,?,?,?,?,?)",
                (request_id, new_version, actor, reason, dumps(merged), ts),
            )
            self.repo.execute(
                "UPDATE requests SET version=?,qty=?,deadline_hours=?,fault_level=?,"
                "destination=?,status='SUBMITTED',block_code=NULL,block_detail=NULL,"
                "plan_generation=?,updated_at=? WHERE request_id=?",
                (new_version, merged["qty"], merged["deadline_hours"],
                 merged["fault_level"], merged["destination"], new_gen, ts, request_id),
            )
            self._audit(
                "REQUEST", request_id, "SUPPLEMENT", actor,
                version=new_version, after=merged, detail={"reason": reason},
            )
            req = self._request_row(request_id)
            self._apply_plan(req, actor)
            return {"request_id": request_id, "decision": self._snapshot_decision(request_id)}

        return self._idem("SUPPLEMENT", idempotency_key, fp, actor, work)

    def replan_blocked(
        self, request_id: str, *, actor: str = "system", idempotency_key: str | None = None
    ) -> dict[str, Any]:
        """库存/运输条件变化后（如补货到位），对阻塞申请重新规划。"""
        fp = self._fingerprint("replan", request_id)

        def work() -> dict[str, Any]:
            req = self._request_row(request_id)
            self._assert_status(req, ("BLOCKED",))
            self.repo.execute(
                "UPDATE requests SET plan_generation=plan_generation+1,updated_at=? "
                "WHERE request_id=?",
                (now_iso(), request_id),
            )
            self._supersede_planned(request_id)
            req = self._request_row(request_id)
            self._apply_plan(req, actor)
            self._audit(
                "REQUEST", request_id, "REPLAN", actor,
                version=req["version"], detail={"reason": "manual/条件变化"},
            )
            return {"request_id": request_id, "decision": self._snapshot_decision(request_id)}

        return self._idem("REPLAN", idempotency_key, fp, actor, work)

    # ------------------------------------------------------------------ #
    # 逐级审批（终审做硬预留，全成或全不成）
    # ------------------------------------------------------------------ #

    def approve(
        self, request_id: str, *, actor: str,
        level: ApprovalLevel | int | None = None, comment: str = "",
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """在指定层级批准。level 省略时默认为当前待审层级。终审通过即硬预留库存。

        幂等语义：同一键的网络重试原样回放首次决定，绝不产生第二次审批；
        继续下一层级审批应使用新的幂等键。
        """
        fp = self._fingerprint(
            "approve", request_id, "default" if level is None else level, comment,
        )

        def work() -> dict[str, Any]:
            req = self._request_row(request_id)
            self._assert_status(req, ("PLANNED", "PENDING_APPROVAL"))
            chain = _APPROVAL_CHAIN[FaultLevel(req["fault_level"])]
            pending = self._pending_level(req)
            if level is None:
                lvl = pending
            else:
                lvl = ApprovalLevel(level) if isinstance(level, int) else level
                if lvl != pending:
                    raise InvalidStateError(
                        f"当前待审层级为 {pending.value} 级，不能由 {lvl.value} 级审批"
                    )
            ts = now_iso()
            self.repo.execute(
                "INSERT INTO approvals(request_id,version,generation,level,decision,"
                "actor,comment,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (request_id, req["version"], req["plan_generation"], lvl.value,
                 "APPROVED", actor, comment, ts),
            )
            self._audit(
                "REQUEST", request_id, "APPROVE", actor, version=req["version"],
                detail={"level": lvl.value, "comment": comment},
            )
            if lvl == chain[-1]:
                self._finalize_approval(req, actor)
            else:
                self.repo.execute(
                    "UPDATE requests SET status='PENDING_APPROVAL',updated_at=? "
                    "WHERE request_id=?",
                    (ts, request_id),
                )
            return {"request_id": request_id, "decision": self._snapshot_decision(request_id)}

        return self._idem("APPROVE", idempotency_key, fp, actor, work)

    def _finalize_approval(self, req: Any, actor: str) -> None:
        """终审：对当前方案代次的全部行做条件硬预留；任一仓库不足则整单退回阻塞。"""
        rid = req["request_id"]
        gen = req["plan_generation"]
        lines = self.repo.query(
            "SELECT * FROM plan_lines WHERE request_id=? AND generation=? AND status='PLANNED' "
            "ORDER BY seq",
            (rid, gen),
        )
        if not lines:
            raise InvalidStateError("当前方案没有可预留的方案行")
        ts = now_iso()
        self.repo.execute("SAVEPOINT reserve")
        befores: dict[str, dict[str, Any]] = {}
        try:
            for line in lines:
                key = f"{line['warehouse_id']}:{line['part_id']}"
                befores.setdefault(key, dict(self._stock_row(
                    line["warehouse_id"], line["part_id"])))
                cur = self.repo.execute(
                    "UPDATE stock SET reserved=reserved+? "
                    "WHERE warehouse_id=? AND part_id=? AND on_hand-reserved>=?",
                    (line["qty"], line["warehouse_id"], line["part_id"], line["qty"]),
                )
                if cur.rowcount != 1:
                    raise ReservationLostError(
                        f"仓库 {line['warehouse_id']} 可用量不足，无法预留 {line['qty']}"
                    )
        except ReservationLostError:
            self.repo.execute("ROLLBACK TO SAVEPOINT reserve")
            self.repo.execute("RELEASE SAVEPOINT reserve")
            # 预留失败：方案作废、转阻塞，先前层级的批准保留在审计中但对新一代次失效
            self._supersede_planned(rid)
            self.repo.execute(
                "UPDATE requests SET status='BLOCKED',block_code=?,"
                "block_detail=?,plan_generation=plan_generation+1,updated_at=? "
                "WHERE request_id=?",
                (BlockerCode.RESERVATION_LOST.value,
                 dumps({"at_generation": gen}), ts, rid),
            )
            self._audit(
                "REQUEST", rid, "RESERVATION_LOST", actor,
                version=req["version"], detail={"generation": gen},
            )
            return
        self.repo.execute("RELEASE SAVEPOINT reserve")
        for line in lines:
            self.repo.execute(
                "UPDATE plan_lines SET status='RESERVED',updated_at=? WHERE line_id=?",
                (ts, line["line_id"]),
            )
            self._audit(
                "STOCK", f"{line['warehouse_id']}:{line['part_id']}", "RESERVE", actor,
                version=req["version"], ref_request_id=rid,
                before=befores[f"{line['warehouse_id']}:{line['part_id']}"],
                after=dict(self._stock_row(line["warehouse_id"], line["part_id"])),
                detail={"request_id": rid, "qty": line["qty"], "line_id": line["line_id"]},
            )
        self.repo.execute(
            "UPDATE requests SET status='APPROVED',approved_at=?,updated_at=? "
            "WHERE request_id=?",
            (ts, ts, rid),
        )
        self._audit(
            "REQUEST", rid, "APPROVED_FINAL", actor, version=req["version"],
            detail={"generation": gen, "lines": [ln["line_id"] for ln in lines]},
        )

    # ------------------------------------------------------------------ #
    # 批准后撤销（已出库数量锁定不动，仅释放未出库预留）
    # ------------------------------------------------------------------ #

    def revoke(
        self, request_id: str, *, actor: str, reason: str = "",
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        fp = self._fingerprint("revoke", request_id, reason)

        def work() -> dict[str, Any]:
            req = self._request_row(request_id)
            status = req["status"]
            self._assert_status(
                req,
                ("PLANNED", "PENDING_APPROVAL", "BLOCKED", "APPROVED",
                 "FULFILLMENT_FAILED", "PARTIALLY_SHIPPED"),
            )
            ts = now_iso()
            shipped = self.repo.query(
                "SELECT COALESCE(SUM(qty),0) n FROM plan_lines "
                "WHERE request_id=? AND status='SHIPPED'",
                (request_id,),
            )[0]["n"]

            # 显式撤销：释放所有尚未出库的预留（含出库失败待恢复的 FAILED 行）；
            # 已 SHIPPED 的行锁定不动，账面数量已经离开仓库，无法撤销
            for line in self.repo.query(
                "SELECT * FROM plan_lines WHERE request_id=? AND status IN ('RESERVED','FAILED')",
                (request_id,),
            ):
                self._release_line(line, actor, req["version"], reason="REVOKE")
            self._supersede_planned(request_id)

            new_status = "PARTIALLY_REVOKED" if shipped > 0 else "REVOKED"
            self.repo.execute(
                "UPDATE requests SET status=?,updated_at=? WHERE request_id=?",
                (new_status, ts, request_id),
            )
            self._audit(
                "REQUEST", request_id, "REVOKE", actor, version=req["version"],
                after={"status": new_status, "shipped_locked_qty": shipped},
                detail={"reason": reason},
            )
            return {"request_id": request_id, "decision": self._snapshot_decision(request_id)}

        return self._idem("REVOKE", idempotency_key, fp, actor, work)

    def _release_line(self, line: Any, actor: str, version: int, *, reason: str) -> None:
        before = dict(self._stock_row(line["warehouse_id"], line["part_id"]))
        self.repo.execute(
            "UPDATE stock SET reserved=reserved-? WHERE warehouse_id=? AND part_id=?",
            (line["qty"], line["warehouse_id"], line["part_id"]),
        )
        after = dict(self._stock_row(line["warehouse_id"], line["part_id"]))
        self.repo.execute(
            "UPDATE plan_lines SET status='RELEASED',updated_at=? WHERE line_id=?",
            (now_iso(), line["line_id"]),
        )
        self._audit(
            "STOCK", f"{line['warehouse_id']}:{line['part_id']}", "RELEASE", actor,
            version=version, ref_request_id=line["request_id"],
            before=before, after=after,
            detail={"request_id": line["request_id"], "qty": line["qty"], "reason": reason},
        )

    # ------------------------------------------------------------------ #
    # 出库 Saga：逐仓库执行，失败保留预留，可重试恢复
    # ------------------------------------------------------------------ #

    def fulfill(
        self,
        request_id: str,
        gateway: Callable[[str, str, int, str], None] | None = None,
        *,
        actor: str = "warehouse-wms",
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """依次执行各仓库出库（saga）。

        gateway(warehouse_id, part_id, qty, idem_token) 执行真正的 WMS 出库调用；
        idem_token 为“行号:代次”的稳定标识，外部系统应据此对网络重试去重。
        gateway 抛 WarehouseGatewayError 表示该仓库暂时失败：已成功的行保持 SHIPPED
        （账面与预留同步扣减、数量锁定），失败行置 FAILED 且预留保留；
        再次调用 fulfill() 会先重试 FAILED 行，实现跨仓库调拨失败后的恢复。
        gateway 为 None 时全部成功。
        """
        fp = self._fingerprint("fulfill", request_id)

        def work() -> dict[str, Any]:
            req = self._request_row(request_id)
            self._assert_status(
                req,
                ("APPROVED", "FULFILLMENT_FAILED", "PARTIALLY_SHIPPED"),
            )
            # 先重试曾失败的行（按 seq），再执行未出库的行
            lines = self.repo.query(
                "SELECT * FROM plan_lines WHERE request_id=? "
                "AND status IN ('FAILED','RESERVED') ORDER BY CASE status "
                "WHEN 'FAILED' THEN 0 ELSE 1 END, seq",
                (request_id,),
            )
            failure: dict[str, Any] | None = None
            shipped_any = bool(self.repo.query_one(
                "SELECT 1 FROM plan_lines WHERE request_id=? AND status='SHIPPED' LIMIT 1",
                (request_id,),
            ))
            for line in lines:
                retry = line["status"] == "FAILED"
                try:
                    if gateway is not None:
                        token = f"{request_id}:{line['line_id']}:gen{line['generation']}"
                        gateway(line["warehouse_id"], line["part_id"], line["qty"], token)
                except WarehouseGatewayError as exc:
                    self.repo.execute(
                        "UPDATE plan_lines SET status='FAILED',attempts=attempts+1,"
                        "fail_reason=?,updated_at=? WHERE line_id=?",
                        (str(exc), now_iso(), line["line_id"]),
                    )
                    self._audit(
                        "REQUEST", request_id,
                        "FULFILL_RETRY_FAILED" if retry else "FULFILL_FAILED",
                        actor, version=req["version"],
                        detail={"line_id": line["line_id"],
                                "warehouse_id": line["warehouse_id"],
                                "attempts": line["attempts"] + 1, "error": str(exc)},
                    )
                    failure = {"warehouse_id": line["warehouse_id"], "error": str(exc)}
                    break
                self._ship_line(line, actor, req["version"], retry=retry)
                shipped_any = True

            ts = now_iso()
            if failure is not None:
                new_status = "PARTIALLY_SHIPPED" if shipped_any else "FULFILLMENT_FAILED"
                self.repo.execute(
                    "UPDATE requests SET status=?,updated_at=? WHERE request_id=?",
                    (new_status, ts, request_id),
                )
            else:
                remaining = self.repo.query_one(
                    "SELECT COUNT(*) n FROM plan_lines WHERE request_id=? "
                    "AND status IN ('RESERVED','FAILED')",
                    (request_id,),
                )["n"]
                if remaining == 0:
                    self.repo.execute(
                        "UPDATE requests SET status='SHIPPED',shipped_at=?,updated_at=? "
                        "WHERE request_id=?",
                        (ts, ts, request_id),
                    )
                    self._audit(
                        "REQUEST", request_id, "FULFILL_COMPLETE", actor,
                        version=req["version"],
                    )
                else:
                    # gateway 未覆盖全部行（无自定义 gateway 时不会发生）
                    self.repo.execute(
                        "UPDATE requests SET status='PARTIALLY_SHIPPED',updated_at=? "
                        "WHERE request_id=?",
                        (ts, request_id),
                    )
            return {"request_id": request_id, "decision": self._snapshot_decision(request_id)}

        return self._idem("FULFILL", idempotency_key, fp, actor, work)

    def _ship_line(self, line: Any, actor: str, version: int, *, retry: bool) -> None:
        before = dict(self._stock_row(line["warehouse_id"], line["part_id"]))
        # 出库：账面与预留同时扣减，累计出库量增加；CHECK 约束保证不会扣成负数
        cur = self.repo.execute(
            "UPDATE stock SET on_hand=on_hand-?, reserved=reserved-?, "
            "issued_total=issued_total+? WHERE warehouse_id=? AND part_id=? "
            "AND on_hand>=? AND reserved>=?",
            (line["qty"], line["qty"], line["qty"],
             line["warehouse_id"], line["part_id"], line["qty"], line["qty"]),
        )
        if cur.rowcount != 1:
            # 预留已在批准时锁定，正常不会走到；走到说明数据被外部破坏
            raise InvalidStateError(
                f"行 {line['line_id']} 出库时库存/预留不足，数据异常"
            )
        ts = now_iso()
        self.repo.execute(
            "UPDATE plan_lines SET status='SHIPPED',fail_reason=NULL,updated_at=? "
            "WHERE line_id=?",
            (ts, line["line_id"]),
        )
        after = dict(self._stock_row(line["warehouse_id"], line["part_id"]))
        self._audit(
            "STOCK", f"{line['warehouse_id']}:{line['part_id']}", "SHIP", actor,
            version=version, ref_request_id=line["request_id"],
            before=before, after=after,
            detail={"request_id": line["request_id"], "qty": line["qty"],
                    "line_id": line["line_id"]},
        )
        self._audit(
            "REQUEST", line["request_id"],
            "FULFILL_RETRY_SUCCEEDED" if retry else "FULFILL_LINE_SHIPPED",
            actor, version=version,
            detail={"line_id": line["line_id"],
                    "warehouse_id": line["warehouse_id"], "qty": line["qty"]},
        )

    # ------------------------------------------------------------------ #
    # 规划持久化（含高等级软占位抢占的级联重规划）
    # ------------------------------------------------------------------ #

    def _apply_plan(self, req: Any, actor: str) -> PlanOutcome:
        rid = req["request_id"]
        part_id = req["part_id"]
        gen = req["plan_generation"]
        qty = req["qty"]
        deadline = req["deadline_hours"]
        rank = FaultLevel(req["fault_level"]).rank

        stock = [
            StockCell(r["warehouse_id"], r["on_hand"], r["reserved"])
            for r in self.repo.query(
                "SELECT warehouse_id,on_hand,reserved FROM stock WHERE part_id=?",
                (part_id,),
            )
        ]
        holds = [
            Hold(r["line_id"], r["request_id"], r["warehouse_id"], r["qty"],
                 FaultLevel(r["fault_level"]).rank, r["created_at"])
            for r in self.repo.query(
                "SELECT l.line_id,l.request_id,l.warehouse_id,l.qty,l.created_at,"
                "r.fault_level FROM plan_lines l "
                "JOIN requests r ON r.request_id=l.request_id "
                "WHERE l.part_id=? AND l.status='PLANNED'",
                (part_id,),
            )
        ]
        transit = {
            r["warehouse_id"]: r["hours"]
            for r in self.repo.query(
                "SELECT warehouse_id,hours FROM transit WHERE destination=?",
                (req["destination"],),
            )
        }
        outcome = build_plan(
            stock=stock, holds=holds, transit=transit, qty=qty,
            deadline_hours=deadline, self_request_id=rid, self_rank=rank,
        )

        ts = now_iso()
        # 同一代次内先作废自己仍有效的旧行
        self._supersede_planned(rid)

        if outcome.blocked:
            self.repo.execute(
                "UPDATE requests SET status='BLOCKED',block_code=?,block_detail=?,"
                "updated_at=? WHERE request_id=?",
                (outcome.block_code, dumps(outcome.block_detail), ts, rid),
            )
            self._audit(
                "REQUEST", rid, "PLAN_BLOCKED", actor, version=req["version"],
                detail={"code": outcome.block_code, "detail": outcome.block_detail},
            )
            return outcome

        for seq, line in enumerate(outcome.lines, start=1):
            self.repo.execute(
                "INSERT INTO plan_lines(line_id,request_id,generation,warehouse_id,part_id,"
                "qty,lead_time_hours,seq,status,attempts,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,'PLANNED',0,?,?)",
                (f"L-{uuid.uuid4().hex[:12].upper()}", rid, gen, line.warehouse_id,
                 part_id, line.qty, line.lead_time_hours, seq, ts, ts),
            )

        approved_levels = self._approved_levels(rid, gen)
        new_status = "PENDING_APPROVAL" if approved_levels else "PLANNED"
        self.repo.execute(
            "UPDATE requests SET status=?,block_code=NULL,block_detail=NULL,updated_at=? "
            "WHERE request_id=?",
            (new_status, ts, rid),
        )
        self._audit(
            "REQUEST", rid, "PLAN_CREATED", actor, version=req["version"],
            after={"generation": gen,
                   "lines": [vars(ln) for ln in outcome.lines],
                   "eta_hours": outcome.eta_hours},
        )

        # 抢占的低等级占位：整行作废并对受害申请做级联重规划（等级严格递减，无环）
        victims: dict[str, None] = {}
        for h in outcome.evicted_holds:
            self.repo.execute(
                "UPDATE plan_lines SET status='SUPERSEDED',updated_at=? WHERE line_id=?",
                (ts, h.line_id),
            )
            victims[h.request_id] = None
            self._audit(
                "REQUEST", h.request_id, "HOLD_EVICTED", actor,
                version=self._request_row(h.request_id)["version"],
                detail={"by_request": rid, "line_id": h.line_id, "qty": h.qty},
            )
        for victim_id in victims:
            victim = self._request_row(victim_id)
            self.repo.execute(
                "UPDATE requests SET plan_generation=plan_generation+1,updated_at=? "
                "WHERE request_id=?",
                (ts, victim_id),
            )
            victim = self._request_row(victim_id)
            self._audit(
                "REQUEST", victim_id, "PLAN_INVALIDATED", actor,
                version=victim["version"],
                detail={"generation": victim["plan_generation"], "cause": rid},
            )
            self._apply_plan(victim, actor)
        return outcome

    # ------------------------------------------------------------------ #
    # 查询：管理视图（可用量 / 承诺到货 / 阻塞原因 / 完整审计）
    # ------------------------------------------------------------------ #

    def get_request(self, request_id: str) -> dict[str, Any]:
        with self._lock:
            req = self._request_row(request_id)
            req = dict(req)
            part = self._require_part(req["part_id"])
            req["part_name"] = part["name"]
            req["fault_label"] = FaultLevel(req["fault_level"]).label
            req["status_label"] = req["status"]
            try:
                req["status_label"] = RequestStatus(req["status"]).label
            except ValueError:
                pass

            lines = [dict(r) for r in self.repo.query(
                "SELECT l.*, w.name AS warehouse_name FROM plan_lines l "
                "JOIN warehouses w ON w.warehouse_id=l.warehouse_id "
                "WHERE l.request_id=? ORDER BY l.generation,l.seq,l.line_id",
                (request_id,),
            )]
            current_lines = [ln for ln in lines if ln["generation"] == req["plan_generation"]]
            req["plan_generation_lines"] = current_lines
            req["all_plan_lines"] = lines

            req["eta_hours"] = None
            active_for_eta = [ln for ln in current_lines
                              if ln["status"] in ("PLANNED", "RESERVED", "FAILED", "SHIPPED")]
            if active_for_eta:
                req["eta_hours"] = max(ln["lead_time_hours"] for ln in active_for_eta)
            req["committed_arrival"] = (
                f"{req['eta_hours']} 小时内" if req["eta_hours"] is not None else None
            )

            req["block_reason"] = None
            if req["block_code"]:
                try:
                    label = BlockerCode(req["block_code"]).label
                except ValueError:
                    label = req["block_code"]
                req["block_reason"] = {
                    "code": req["block_code"],
                    "label": label,
                    "detail": loads(req["block_detail"]),
                }
            failed = [ln for ln in current_lines if ln["status"] == "FAILED"]
            if failed:
                req["fulfillment_failures"] = [
                    {"warehouse_id": ln["warehouse_id"],
                     "warehouse_name": ln["warehouse_name"],
                     "qty": ln["qty"], "attempts": ln["attempts"],
                     "reason": ln["fail_reason"]}
                    for ln in failed
                ]

            req["approvals"] = [dict(r) for r in self.repo.query(
                "SELECT * FROM approvals WHERE request_id=? ORDER BY id",
                (request_id,),
            )]
            req["revisions"] = [
                {**dict(r), "payload": loads(r["payload"])}
                for r in self.repo.query(
                    "SELECT * FROM revisions WHERE request_id=? ORDER BY version",
                    (request_id,),
                )
            ]
            chain = _APPROVAL_CHAIN[FaultLevel(req["fault_level"])]
            req["approval_chain"] = [
                {"level": lvl.value, "label": lvl.label} for lvl in chain
            ]
            req["approved_levels_current_generation"] = [
                a["level"] for a in req["approvals"]
                if a["generation"] == req["plan_generation"] and a["decision"] == "APPROVED"
            ]
            req["available_stock"] = self.stock_view(req["part_id"])
            req["shipped_qty"] = self.repo.query_one(
                "SELECT COALESCE(SUM(qty),0) n FROM plan_lines "
                "WHERE request_id=? AND status='SHIPPED'", (request_id,)
            )["n"]
            # 完整审计：申请事件 + 由该申请引发的库存变化（预留/释放/出库）
            req["audit"] = [
                {**dict(r), "before": loads(r["before"]),
                 "after": loads(r["after"]), "detail": loads(r["detail"])}
                for r in self.repo.query(
                    "SELECT * FROM audit_log "
                    "WHERE (aggregate_type='REQUEST' AND aggregate_id=?) "
                    "OR ref_request_id=? ORDER BY id",
                    (request_id, request_id),
                )
            ]
            return req

    def stock_view(self, part_id: str) -> list[dict[str, Any]]:
        """各仓库可用量视图。

        available（可承诺量）= 账面 - 硬预留(已批准未出库) - 软占位(方案审批中)。
        已出库数量在出库时即从账面扣减，天然不可能被第二个申请再次分配。
        """
        rows = self.repo.query(
            "SELECT s.warehouse_id,w.name warehouse_name,w.location,s.on_hand,"
            "s.reserved,s.issued_total,"
            "COALESCE((SELECT SUM(l.qty) FROM plan_lines l "
            "  WHERE l.warehouse_id=s.warehouse_id AND l.part_id=s.part_id "
            "  AND l.status='PLANNED'),0) AS held "
            "FROM stock s JOIN warehouses w ON w.warehouse_id=s.warehouse_id "
            "WHERE s.part_id=? ORDER BY s.warehouse_id",
            (part_id,),
        )
        result = []
        for r in rows:
            d = dict(r)
            d["available"] = d["on_hand"] - d["reserved"] - d["held"]
            result.append(d)
        return result

    def audit_trail(self, aggregate_type: str, aggregate_id: str) -> list[dict[str, Any]]:
        return [
            {**dict(r), "before": loads(r["before"]),
             "after": loads(r["after"]), "detail": loads(r["detail"])}
            for r in self.repo.query(
                "SELECT * FROM audit_log WHERE aggregate_type=? AND aggregate_id=? "
                "ORDER BY id",
                (aggregate_type, aggregate_id),
            )
        ]

    def list_requests(self) -> list[dict[str, Any]]:
        return [dict(r) for r in self.repo.query(
            "SELECT request_id,version,fault_level,qty,status,block_code,"
            "plan_generation,created_at,updated_at FROM requests ORDER BY created_at"
        )]

    # ------------------------------------------------------------------ #
    # 内部工具
    # ------------------------------------------------------------------ #

    def _snapshot_decision(self, request_id: str) -> dict[str, Any]:
        """幂等重试时原样返回的“决定”快照。"""
        req = self._request_row(request_id)
        lines = [
            {"warehouse_id": r["warehouse_id"], "qty": r["qty"],
             "lead_time_hours": r["lead_time_hours"], "status": r["status"]}
            for r in self.repo.query(
                "SELECT * FROM plan_lines WHERE request_id=? AND generation=? ORDER BY seq",
                (request_id, req["plan_generation"]),
            )
        ]
        eta = max((ln["lead_time_hours"] for ln in lines), default=None)
        return {
            "status": req["status"],
            "plan_generation": req["plan_generation"],
            "version": req["version"],
            "lines": lines,
            "eta_hours": eta,
            "block_code": req["block_code"],
            "block_detail": loads(req["block_detail"]),
            "recorded_at": now_iso(),
        }

    def _pending_level(self, req: Any) -> ApprovalLevel:
        chain = _APPROVAL_CHAIN[FaultLevel(req["fault_level"])]
        approved = set(self._approved_levels(req["request_id"], req["plan_generation"]))
        for lvl in chain:
            if lvl.value not in approved:
                return lvl
        raise InvalidStateError("审批链已全部通过，没有待审层级")

    def _approved_levels(self, rid: str, gen: int) -> list[int]:
        # 审批记录按方案代次匹配；被抢占重规划后旧代次的批准自动失效
        return [r["level"] for r in self.repo.query(
            "SELECT level FROM approvals WHERE request_id=? AND generation=? "
            "AND decision='APPROVED'",
            (rid, gen),
        )]

    def _supersede_planned(self, rid: str) -> None:
        """作废该申请所有尚未硬预留的方案行（退回/重规划/抢占时使用）。

        不变量：一个申请任一时刻只可能在当前代次上存在 PLANNED 行，因此无需按代次过滤；
        这样也能在级联抢占时连同受害申请同代次的兄弟行一起作废。
        """
        self.repo.execute(
            "UPDATE plan_lines SET status='SUPERSEDED',updated_at=? "
            "WHERE request_id=? AND status='PLANNED'",
            (now_iso(), rid),
        )

    def _stock_row(self, warehouse_id: str, part_id: str) -> Any:
        return self.repo.query_one(
            "SELECT on_hand,reserved,issued_total FROM stock "
            "WHERE warehouse_id=? AND part_id=?",
            (warehouse_id, part_id),
        )

    def _require_part(self, part_id: str) -> Any:
        row = self.repo.query_one("SELECT * FROM parts WHERE part_id=?", (part_id,))
        if not row:
            raise InvalidStateError(f"备件 {part_id} 未登记")
        return row

    def _require_warehouse(self, warehouse_id: str) -> Any:
        row = self.repo.query_one(
            "SELECT * FROM warehouses WHERE warehouse_id=?", (warehouse_id,)
        )
        if not row:
            raise InvalidStateError(f"仓库 {warehouse_id} 未登记")
        return row

    def _request_row(self, request_id: str) -> Any:
        row = self.repo.query_one(
            "SELECT * FROM requests WHERE request_id=?", (request_id,)
        )
        if not row:
            raise InvalidStateError(f"申请 {request_id} 不存在")
        return row

    def _assert_status(self, req: Any, allowed: tuple[str, ...]) -> None:
        if req["status"] not in allowed:
            raise InvalidStateError(
                f"申请 {req['request_id']} 当前状态 {req['status']}，"
                f"允许该操作的状态：{', '.join(allowed)}"
            )

    def _audit(
        self, aggregate_type: str, aggregate_id: str, action: str, actor: str,
        *, version: int | None = None, ref_request_id: str | None = None,
        before: Any = None, after: Any = None, detail: Any = None,
    ) -> None:
        self.repo.execute(
            "INSERT INTO audit_log(ts,aggregate_type,aggregate_id,action,actor,"
            "version,ref_request_id,before,after,detail) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (now_iso(), aggregate_type, aggregate_id, action, actor,
             version, ref_request_id, dumps(before), dumps(after), dumps(detail)),
        )

    def _fingerprint(self, *parts: Any) -> str:
        blob = json.dumps(parts, ensure_ascii=False, sort_keys=True, default=str)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def _idem(
        self,
        scope: str,
        key: str | None,
        fingerprint: str | Callable[[], str],
        actor: str,
        work: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        with self._lock:
            def _fp() -> str:
                return fingerprint() if callable(fingerprint) else fingerprint

            if key is not None:
                row = self.repo.query_one(
                    "SELECT * FROM idempotency WHERE idem_key=?", (key,)
                )
                if row:
                    if row["fingerprint"] != _fp() or row["scope"] != scope:
                        raise IdempotencyConflictError(
                            f"幂等键 {key} 曾用于不同操作/内容，拒绝执行"
                        )
                    return loads(row["response"])
            fp_value = _fp()
            with self._tx():
                result = work()
                if key is not None:
                    rid = result.get("request_id")
                    self.repo.execute(
                        "INSERT INTO idempotency(idem_key,scope,request_id,fingerprint,"
                        "response,created_at) VALUES(?,?,?,?,?,?)",
                        (key, scope, rid, fp_value, dumps(result), now_iso()),
                    )
                return result

    class _Transaction:
        def __init__(self, svc: "AllocationService") -> None:
            self.svc = svc

        def __enter__(self) -> None:
            self.svc.repo.begin()

        def __exit__(self, exc_type, exc, tb) -> None:
            if exc_type is None:
                self.svc.repo.commit()
            else:
                self.svc.repo.rollback()

    def _tx(self) -> "AllocationService._Transaction":
        return AllocationService._Transaction(self)

    def _migrate(self) -> None:
        """对早于代次字段的旧库做无损加列（新库由 SCHEMA 直接建好）。"""
        with self._lock:
            req_cols = {r["name"] for r in self.repo.query("PRAGMA table_info(requests)")}
            if req_cols and "plan_generation" not in req_cols:
                self.repo.execute(
                    "ALTER TABLE requests ADD COLUMN plan_generation INTEGER NOT NULL DEFAULT 1"
                )
            line_cols = {r["name"] for r in self.repo.query("PRAGMA table_info(plan_lines)")}
            if line_cols and "generation" not in line_cols:
                self.repo.execute(
                    "ALTER TABLE plan_lines ADD COLUMN generation INTEGER NOT NULL DEFAULT 1"
                )
            appr_cols = {r["name"] for r in self.repo.query("PRAGMA table_info(approvals)")}
            if appr_cols and "generation" not in appr_cols:
                self.repo.execute(
                    "ALTER TABLE approvals ADD COLUMN generation INTEGER NOT NULL DEFAULT 1"
                )
            audit_cols = {r["name"] for r in self.repo.query("PRAGMA table_info(audit_log)")}
            if audit_cols and "ref_request_id" not in audit_cols:
                self.repo.execute(
                    "ALTER TABLE audit_log ADD COLUMN ref_request_id TEXT"
                )
                self.repo.execute(
                    "CREATE INDEX IF NOT EXISTS idx_audit_req ON audit_log(ref_request_id)"
                )

    def close(self) -> None:
        self.repo.close()


# 兼容原入口名
Service = AllocationService
