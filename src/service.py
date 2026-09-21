"""装备维修备件调拨领域服务。

核心不变量：
1. 幂等：所有写操作接受幂等键，网络重试返回首次决定，不产生重复单据。
2. 锁定：available = on_hand - reserved；批准即预留，出库即从池中移除，
   已出库数量任何申请都不可再占用。
3. 留痕：审批层级、版本号、每次库存变化全部落库（audit_log + inventory_ledger）。
4. 可恢复：跨仓库调拨失败时补偿回滚，申请恢复到未完成状态，可重新规划。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

from .errors import (
    ConflictError,
    IdempotencyConflictError,
    NotFoundError,
    ValidationError,
)
from .models import (
    APPROVAL_CHAIN,
    AllocationStatus,
    FaultLevel,
    LedgerEvent,
    RequestStatus,
    ShipmentStatus,
)
from .planner import plan_allocation
from .store import Store

_ACTIVE_ALLOC = (AllocationStatus.RESERVED.value, AllocationStatus.PARTIALLY_SHIPPED.value)


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _json(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


class RequisitionService:
    """调拨业务服务：申请、审批、预留、出库、撤销、失败恢复与审计查询。"""

    def __init__(self, store: Store | None = None, clock=None):
        self.store = store or Store()
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def _now_iso(self) -> str:
        return self.clock().isoformat()

    def _get_request(self, conn, request_id: str):
        row = conn.execute("SELECT * FROM requests WHERE request_id=?", (request_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"申请 {request_id} 不存在")
        return row

    @staticmethod
    def _check_version(req, expected_version):
        if expected_version is not None and expected_version != req["version"]:
            raise ConflictError(
                f"版本冲突：期望 v{expected_version}，当前 v{req['version']}"
                "（可能已被他人修改，或为重试请求）"
            )

    def _audit(self, conn, request_id, event, *, actor, version=None, level=None,
               from_status=None, to_status=None, detail=None):
        conn.execute(
            """INSERT INTO audit_log(request_id, event, actor, level, from_status, to_status,
                                     version, detail_json, created_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (request_id, event, actor, level, from_status, to_status,
             version, _json(detail or {}), self._now_iso()),
        )

    def _ledger(self, conn, warehouse_id, part_number, event: LedgerEvent, *,
                delta_on_hand, delta_reserved, request_id=None, ref_id=None, note=""):
        row = conn.execute(
            "SELECT on_hand, reserved FROM inventory WHERE warehouse_id=? AND part_number=?",
            (warehouse_id, part_number),
        ).fetchone()
        conn.execute(
            """INSERT INTO inventory_ledger(warehouse_id, part_number, event, delta_on_hand,
                                            delta_reserved, on_hand_after, reserved_after,
                                            request_id, ref_id, note, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (warehouse_id, part_number, event.value, delta_on_hand, delta_reserved,
             row["on_hand"], row["reserved"], request_id, ref_id, note, self._now_iso()),
        )

    def _idempotent(self, conn, key, operation, payload, fn):
        """幂等执行：键已存在且载荷一致 -> 返回首次决定；载荷不一致 -> 拒绝。"""
        if key is None:
            return fn()
        row = conn.execute(
            "SELECT * FROM idempotency_keys WHERE key=?", (key,)).fetchone()
        if row is not None:
            if row["operation"] != operation or row["payload_json"] != _json(payload):
                raise IdempotencyConflictError(
                    f"幂等键 {key} 已用于不同的请求，拒绝执行")
            response = json.loads(row["response_json"])
            response["idempotent_replay"] = True
            return response
        response = fn()
        conn.execute(
            """INSERT INTO idempotency_keys(key, operation, payload_json, request_id,
                                            response_json, created_at)
               VALUES (?,?,?,?,?,?)""",
            (key, operation, _json(payload), response.get("request_id"),
             _json(response), self._now_iso()),
        )
        return response

    # ------------------------------------------------------------------
    # 基础资料与入库
    # ------------------------------------------------------------------
    def register_warehouse(self, warehouse_id, name, location):
        with self.store.transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO warehouses(warehouse_id, name, location) VALUES (?,?,?)",
                (warehouse_id, name, location))
        return {"warehouse_id": warehouse_id}

    def register_part(self, part_number, name, equipment_model):
        with self.store.transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO parts(part_number, name, equipment_model) VALUES (?,?,?)",
                (part_number, name, equipment_model))
        return {"part_number": part_number}

    def add_route(self, warehouse_id, destination, base_hours):
        if base_hours <= 0:
            raise ValidationError("运输时间必须为正数（小时）")
        with self.store.transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO routes(warehouse_id, destination, base_hours) VALUES (?,?,?)",
                (warehouse_id, destination, base_hours))
        return {"warehouse_id": warehouse_id, "destination": destination}

    def receive_stock(self, warehouse_id, part_number, quantity, actor="system", note="入库"):
        if not isinstance(quantity, int) or quantity <= 0:
            raise ValidationError("入库数量必须为正整数")
        with self.store.transaction() as conn:
            conn.execute(
                """INSERT INTO inventory(warehouse_id, part_number, on_hand, reserved)
                   VALUES (?,?,?,0)
                   ON CONFLICT(warehouse_id, part_number)
                   DO UPDATE SET on_hand = on_hand + excluded.on_hand""",
                (warehouse_id, part_number, quantity))
            self._ledger(conn, warehouse_id, part_number, LedgerEvent.STOCK_RECEIVED,
                         delta_on_hand=quantity, delta_reserved=0, note=note)
            self._audit(conn, None, "STOCK_RECEIVED", actor=actor,
                        detail={"warehouse_id": warehouse_id, "part_number": part_number,
                                "quantity": quantity, "note": note})
        return self.inventory_position(part_number)

    # ------------------------------------------------------------------
    # 申请
    # ------------------------------------------------------------------
    def submit_request(self, *, idempotency_key=None, unit_id, destination, equipment_model,
                       part_number, quantity, fault_level, required_by_hours, actor="requester"):
        """提交维修申请。网络重试携带同一幂等键时返回首次决定，不会重复建单。"""
        try:
            fault = FaultLevel(fault_level)
        except ValueError:
            raise ValidationError(f"未知故障等级：{fault_level}") from None
        if not isinstance(quantity, int) or quantity <= 0:
            raise ValidationError("申请数量必须为正整数")
        if required_by_hours <= 0:
            raise ValidationError("运输时限必须为正数（小时）")

        payload = {
            "unit_id": unit_id, "destination": destination,
            "equipment_model": equipment_model, "part_number": part_number,
            "quantity": quantity, "fault_level": fault.value,
            "required_by_hours": required_by_hours,
        }
        with self.store.transaction() as conn:
            return self._idempotent(
                conn, idempotency_key, "submit_request", payload,
                lambda: self._submit(conn, actor=actor, **payload))

    def _submit(self, conn, *, actor, unit_id, destination, equipment_model,
                part_number, quantity, fault_level, required_by_hours):
        part = conn.execute("SELECT * FROM parts WHERE part_number=?", (part_number,)).fetchone()
        if part is None:
            raise NotFoundError(f"备件 {part_number} 未登记")
        if part["equipment_model"] != equipment_model:
            raise ValidationError(
                f"备件 {part_number} 适配机型 {part['equipment_model']}，"
                f"与申请机型 {equipment_model} 不符")
        now = self._now_iso()
        request_id = _new_id("REQ")
        levels = list(APPROVAL_CHAIN[FaultLevel(fault_level)])
        conn.execute(
            """INSERT INTO requests(request_id, unit_id, destination, equipment_model, part_number,
                                    quantity, fault_level, required_by_hours, status, version,
                                    current_approval_level, required_approval_levels,
                                    created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,1,0,?,?,?)""",
            (request_id, unit_id, destination, equipment_model, part_number, quantity,
             fault_level, required_by_hours, RequestStatus.SUBMITTED.value,
             _json(levels), now, now))
        self._audit(conn, request_id, "SUBMITTED", actor=actor, version=1,
                    to_status=RequestStatus.SUBMITTED.value,
                    detail={"unit_id": unit_id, "part_number": part_number,
                            "quantity": quantity, "fault_level": fault_level,
                            "required_by_hours": required_by_hours})
        return {"request_id": request_id, "status": RequestStatus.SUBMITTED.value,
                "version": 1, "required_approval_levels": levels}

    # ------------------------------------------------------------------
    # 审批（逐级；最终一级触发预留与拆分方案）
    # ------------------------------------------------------------------
    def approve(self, request_id, *, level, approver, note="",
                expected_version=None, idempotency_key=None):
        payload = {"request_id": request_id, "level": level,
                   "approver": approver, "note": note}
        with self.store.transaction() as conn:
            return self._idempotent(
                conn, idempotency_key, "approve", payload,
                lambda: self._approve(conn, request_id, level=level, approver=approver,
                                      note=note, expected_version=expected_version))

    def _approve(self, conn, request_id, *, level, approver, note, expected_version):
        req = self._get_request(conn, request_id)
        self._check_version(req, expected_version)
        status = req["status"]
        if status not in (RequestStatus.SUBMITTED.value, RequestStatus.UNDER_REVIEW.value):
            raise ConflictError(f"当前状态 {status} 不可审批")
        levels = json.loads(req["required_approval_levels"])
        expected_level = req["current_approval_level"] + 1
        if level != expected_level or level > len(levels):
            raise ConflictError(f"需逐级审批：下一级为 L{expected_level}，收到 L{level}")

        now = self._now_iso()
        new_version = req["version"] + 1
        conn.execute(
            """INSERT INTO approvals(request_id, level, approver, decision, note, decided_at)
               VALUES (?,?,?,?,?,?)""",
            (request_id, level, approver, "APPROVED", note, now))
        self._audit(conn, request_id, "APPROVAL", actor=approver, level=level,
                    from_status=status, version=new_version, detail={"note": note})

        final = level == len(levels)
        new_status = RequestStatus.APPROVED.value if final else RequestStatus.UNDER_REVIEW.value
        allocations, shortfall, reasons = [], 0, []
        if final:
            plan = self._plan_for_request(conn, req, quantity=req["quantity"],
                                          exclude_warehouses=())
            for line in plan.lines:
                allocation_id = _new_id("ALC")
                self._reserve(conn, req, line, allocation_id)
                allocations.append({
                    "allocation_id": allocation_id, "warehouse_id": line.warehouse_id,
                    "quantity": line.quantity, "transport_hours": line.transport_hours,
                    "promised_arrival": line.promised_arrival,
                    "within_deadline": line.within_deadline,
                })
            shortfall, reasons = plan.shortfall, plan.reasons
            self._audit(conn, request_id, "APPROVED", actor=approver, version=new_version,
                        from_status=status, to_status=new_status,
                        detail={"allocations": allocations, "shortfall": shortfall})

        conn.execute(
            """UPDATE requests SET status=?, current_approval_level=?, version=?, updated_at=?
               WHERE request_id=?""",
            (new_status, level, new_version, now, request_id))
        return {"request_id": request_id, "status": new_status, "version": new_version,
                "level": level, "final": final, "allocations": allocations,
                "shortfall": shortfall, "blocking_reasons": reasons}

    def return_for_supplement(self, request_id, *, level, approver, reason,
                              idempotency_key=None):
        """退回补充：申请资料不全时退回，补充后重新提交（版本 +1，审批链重置）。"""
        if not reason:
            raise ValidationError("退回必须填写原因")
        payload = {"request_id": request_id, "level": level,
                   "approver": approver, "reason": reason}
        with self.store.transaction() as conn:
            return self._idempotent(
                conn, idempotency_key, "return_for_supplement", payload,
                lambda: self._return(conn, request_id, level=level,
                                     approver=approver, reason=reason))

    def _return(self, conn, request_id, *, level, approver, reason):
        req = self._get_request(conn, request_id)
        status = req["status"]
        if status not in (RequestStatus.SUBMITTED.value, RequestStatus.UNDER_REVIEW.value):
            raise ConflictError(f"当前状态 {status} 不可退回")
        now = self._now_iso()
        new_version = req["version"] + 1
        conn.execute(
            """INSERT INTO approvals(request_id, level, approver, decision, note, decided_at)
               VALUES (?,?,?,?,?,?)""",
            (request_id, level, approver, "RETURNED", reason, now))
        conn.execute(
            """UPDATE requests SET status=?, return_reason=?, version=?, updated_at=?
               WHERE request_id=?""",
            (RequestStatus.RETURNED.value, reason, new_version, now, request_id))
        self._audit(conn, request_id, "RETURNED", actor=approver, level=level,
                    from_status=status, to_status=RequestStatus.RETURNED.value,
                    version=new_version, detail={"reason": reason})
        return {"request_id": request_id, "status": RequestStatus.RETURNED.value,
                "version": new_version, "return_reason": reason}

    def reject(self, request_id, *, level, approver, reason, idempotency_key=None):
        if not reason:
            raise ValidationError("驳回必须填写原因")
        payload = {"request_id": request_id, "level": level,
                   "approver": approver, "reason": reason}
        with self.store.transaction() as conn:
            return self._idempotent(
                conn, idempotency_key, "reject", payload,
                lambda: self._reject(conn, request_id, level=level,
                                     approver=approver, reason=reason))

    def _reject(self, conn, request_id, *, level, approver, reason):
        req = self._get_request(conn, request_id)
        status = req["status"]
        if status not in (RequestStatus.SUBMITTED.value, RequestStatus.UNDER_REVIEW.value):
            raise ConflictError(f"当前状态 {status} 不可驳回")
        now = self._now_iso()
        new_version = req["version"] + 1
        conn.execute(
            """INSERT INTO approvals(request_id, level, approver, decision, note, decided_at)
               VALUES (?,?,?,?,?,?)""",
            (request_id, level, approver, "REJECTED", reason, now))
        conn.execute(
            "UPDATE requests SET status=?, version=?, updated_at=? WHERE request_id=?",
            (RequestStatus.REJECTED.value, new_version, now, request_id))
        self._audit(conn, request_id, "REJECTED", actor=approver, level=level,
                    from_status=status, to_status=RequestStatus.REJECTED.value,
                    version=new_version, detail={"reason": reason})
        return {"request_id": request_id, "status": RequestStatus.REJECTED.value,
                "version": new_version}

    def amend_request(self, request_id, *, quantity=None, required_by_hours=None,
                      destination=None, actor="requester", note=""):
        """退回补充后修改并重新提交：版本 +1，审批链重置为待审。"""
        with self.store.transaction() as conn:
            req = self._get_request(conn, request_id)
            if req["status"] != RequestStatus.RETURNED.value:
                raise ConflictError("仅退回补充状态的申请可修改")
            changes = {}
            if quantity is not None:
                if not isinstance(quantity, int) or quantity <= 0:
                    raise ValidationError("申请数量必须为正整数")
                changes["quantity"] = {"old": req["quantity"], "new": quantity}
            if required_by_hours is not None:
                if required_by_hours <= 0:
                    raise ValidationError("运输时限必须为正数（小时）")
                changes["required_by_hours"] = {"old": req["required_by_hours"],
                                                "new": required_by_hours}
            if destination is not None:
                changes["destination"] = {"old": req["destination"], "new": destination}
            if not changes:
                raise ValidationError("没有需要修改的内容")

            new_version = req["version"] + 1
            now = self._now_iso()
            conn.execute(
                """UPDATE requests SET quantity=?, required_by_hours=?, destination=?,
                       status=?, current_approval_level=0, return_reason=NULL,
                       version=?, updated_at=?
                   WHERE request_id=?""",
                (quantity if quantity is not None else req["quantity"],
                 required_by_hours if required_by_hours is not None else req["required_by_hours"],
                 destination if destination is not None else req["destination"],
                 RequestStatus.SUBMITTED.value, new_version, now, request_id))
            self._audit(conn, request_id, "AMENDED", actor=actor,
                        from_status=RequestStatus.RETURNED.value,
                        to_status=RequestStatus.SUBMITTED.value,
                        version=new_version, detail={"changes": changes, "note": note})
            return {"request_id": request_id, "status": RequestStatus.SUBMITTED.value,
                    "version": new_version, "changes": changes}

    # ------------------------------------------------------------------
    # 撤销（批准后也可撤销；已出库数量保持锁定）
    # ------------------------------------------------------------------
    def cancel(self, request_id, *, actor, reason="",
               expected_version=None, idempotency_key=None):
        payload = {"request_id": request_id, "actor": actor, "reason": reason}
        with self.store.transaction() as conn:
            return self._idempotent(
                conn, idempotency_key, "cancel", payload,
                lambda: self._cancel(conn, request_id, actor=actor, reason=reason,
                                     expected_version=expected_version))

    def _cancel(self, conn, request_id, *, actor, reason, expected_version):
        req = self._get_request(conn, request_id)
        self._check_version(req, expected_version)
        status = req["status"]
        if status in (RequestStatus.COMPLETED.value, RequestStatus.CANCELLED.value,
                      RequestStatus.REJECTED.value):
            raise ConflictError(f"状态 {status} 不可撤销")

        released = 0
        allocs = conn.execute(
            f"""SELECT * FROM allocations
                WHERE request_id=? AND status IN ({','.join('?' * len(_ACTIVE_ALLOC))})""",
            (request_id, *_ACTIVE_ALLOC)).fetchall()
        for a in allocs:
            unshipped = a["quantity"] - a["shipped_quantity"]
            if unshipped > 0:
                cur = conn.execute(
                    """UPDATE inventory SET reserved = reserved - ?
                       WHERE warehouse_id=? AND part_number=? AND reserved >= ?""",
                    (unshipped, a["warehouse_id"], req["part_number"], unshipped))
                if cur.rowcount != 1:
                    raise ConflictError("预留数量异常，释放失败")
                self._ledger(conn, a["warehouse_id"], req["part_number"],
                             LedgerEvent.RESERVATION_RELEASED,
                             delta_on_hand=0, delta_reserved=-unshipped,
                             request_id=request_id, ref_id=a["allocation_id"],
                             note="撤销释放")
                released += unshipped
            conn.execute("UPDATE allocations SET status=? WHERE allocation_id=?",
                         (AllocationStatus.RELEASED.value, a["allocation_id"]))

        shipped_locked = conn.execute(
            "SELECT COALESCE(SUM(shipped_quantity),0) AS s FROM allocations WHERE request_id=?",
            (request_id,)).fetchone()["s"]
        now = self._now_iso()
        new_version = req["version"] + 1
        conn.execute("UPDATE requests SET status=?, version=?, updated_at=? WHERE request_id=?",
                     (RequestStatus.CANCELLED.value, new_version, now, request_id))
        self._audit(conn, request_id, "CANCELLED", actor=actor, from_status=status,
                    to_status=RequestStatus.CANCELLED.value, version=new_version,
                    detail={"reason": reason, "released_quantity": released,
                            "shipped_locked_quantity": shipped_locked})
        return {"request_id": request_id, "status": RequestStatus.CANCELLED.value,
                "version": new_version, "released_quantity": released,
                "shipped_locked_quantity": shipped_locked}

    # ------------------------------------------------------------------
    # 出库 / 送达 / 调拨失败恢复
    # ------------------------------------------------------------------
    def ship(self, allocation_id, *, quantity=None, actor="warehouse",
             idempotency_key=None):
        """出库：数量从库存池彻底移除并锁定，任何其他申请不可再占用。"""
        payload = {"allocation_id": allocation_id, "quantity": quantity, "actor": actor}
        with self.store.transaction() as conn:
            return self._idempotent(
                conn, idempotency_key, "ship", payload,
                lambda: self._ship(conn, allocation_id, quantity=quantity, actor=actor))

    def _ship(self, conn, allocation_id, *, quantity, actor):
        alloc = conn.execute("SELECT * FROM allocations WHERE allocation_id=?",
                             (allocation_id,)).fetchone()
        if alloc is None:
            raise NotFoundError(f"分配单 {allocation_id} 不存在")
        req = self._get_request(conn, alloc["request_id"])
        if req["status"] not in (RequestStatus.APPROVED.value, RequestStatus.FULFILLING.value):
            raise ConflictError(f"申请状态 {req['status']} 不可出库")
        if alloc["status"] not in _ACTIVE_ALLOC:
            raise ConflictError(f"分配单状态 {alloc['status']} 不可出库")
        remaining = alloc["quantity"] - alloc["shipped_quantity"]
        q = remaining if quantity is None else quantity
        if not isinstance(q, int) or q <= 0 or q > remaining:
            raise ValidationError(f"出库数量须为 1..{remaining}")

        cur = conn.execute(
            """UPDATE inventory SET on_hand = on_hand - ?, reserved = reserved - ?
               WHERE warehouse_id=? AND part_number=? AND on_hand >= ? AND reserved >= ?""",
            (q, q, alloc["warehouse_id"], req["part_number"], q, q))
        if cur.rowcount != 1:
            raise ConflictError("库存状态异常，出库失败")

        now = self._now_iso()
        shipment_id = _new_id("SHP")
        conn.execute(
            """INSERT INTO shipments(shipment_id, request_id, allocation_id, from_warehouse,
                                     quantity, status, shipped_at)
               VALUES (?,?,?,?,?,?,?)""",
            (shipment_id, req["request_id"], allocation_id, alloc["warehouse_id"],
             q, ShipmentStatus.IN_TRANSIT.value, now))
        new_shipped = alloc["shipped_quantity"] + q
        new_a_status = (AllocationStatus.SHIPPED.value if new_shipped == alloc["quantity"]
                        else AllocationStatus.PARTIALLY_SHIPPED.value)
        conn.execute("UPDATE allocations SET shipped_quantity=?, status=? WHERE allocation_id=?",
                     (new_shipped, new_a_status, allocation_id))
        self._ledger(conn, alloc["warehouse_id"], req["part_number"], LedgerEvent.SHIPPED_OUT,
                     delta_on_hand=-q, delta_reserved=-q,
                     request_id=req["request_id"], ref_id=shipment_id, note="出库锁定")
        new_version = req["version"] + 1
        conn.execute("UPDATE requests SET status=?, version=?, updated_at=? WHERE request_id=?",
                     (RequestStatus.FULFILLING.value, new_version, now, req["request_id"]))
        self._audit(conn, req["request_id"], "SHIPPED", actor=actor,
                    from_status=req["status"], to_status=RequestStatus.FULFILLING.value,
                    version=new_version,
                    detail={"shipment_id": shipment_id, "allocation_id": allocation_id,
                            "quantity": q})
        return {"shipment_id": shipment_id, "request_id": req["request_id"],
                "allocation_id": allocation_id, "quantity": q,
                "status": ShipmentStatus.IN_TRANSIT.value,
                "request_status": RequestStatus.FULFILLING.value, "version": new_version}

    def deliver(self, shipment_id, *, actor="carrier"):
        with self.store.transaction() as conn:
            sh = conn.execute("SELECT * FROM shipments WHERE shipment_id=?",
                              (shipment_id,)).fetchone()
            if sh is None:
                raise NotFoundError(f"运单 {shipment_id} 不存在")
            if sh["status"] != ShipmentStatus.IN_TRANSIT.value:
                raise ConflictError(f"运单状态 {sh['status']} 不可确认送达")
            now = self._now_iso()
            conn.execute("UPDATE shipments SET status=?, delivered_at=? WHERE shipment_id=?",
                         (ShipmentStatus.DELIVERED.value, now, shipment_id))
            req = self._get_request(conn, sh["request_id"])
            self._audit(conn, req["request_id"], "DELIVERED", actor=actor,
                        version=req["version"],
                        detail={"shipment_id": shipment_id, "quantity": sh["quantity"]})

            shipped_total = conn.execute(
                "SELECT COALESCE(SUM(shipped_quantity),0) AS s FROM allocations WHERE request_id=?",
                (req["request_id"],)).fetchone()["s"]
            in_transit = conn.execute(
                "SELECT COUNT(*) AS c FROM shipments WHERE request_id=? AND status=?",
                (req["request_id"], ShipmentStatus.IN_TRANSIT.value)).fetchone()["c"]
            new_status = req["status"]
            if shipped_total == req["quantity"] and in_transit == 0:
                new_status = RequestStatus.COMPLETED.value
                conn.execute(
                    "UPDATE requests SET status=?, version=version+1, updated_at=? "
                    "WHERE request_id=?",
                    (new_status, now, req["request_id"]))
                self._audit(conn, req["request_id"], "COMPLETED", actor=actor,
                            from_status=req["status"], to_status=new_status,
                            version=req["version"] + 1)
            return {"shipment_id": shipment_id, "status": ShipmentStatus.DELIVERED.value,
                    "request_id": req["request_id"], "request_status": new_status}

    def fail_shipment(self, shipment_id, *, reason, actor="carrier", idempotency_key=None):
        """跨仓库调拨失败：补偿回滚——货物退回原仓库并重新预留，申请恢复未完成状态。"""
        if not reason:
            raise ValidationError("调拨失败必须填写原因")
        payload = {"shipment_id": shipment_id, "reason": reason, "actor": actor}
        with self.store.transaction() as conn:
            return self._idempotent(
                conn, idempotency_key, "fail_shipment", payload,
                lambda: self._fail_shipment(conn, shipment_id, reason=reason, actor=actor))

    def _fail_shipment(self, conn, shipment_id, *, reason, actor):
        sh = conn.execute("SELECT * FROM shipments WHERE shipment_id=?",
                          (shipment_id,)).fetchone()
        if sh is None:
            raise NotFoundError(f"运单 {shipment_id} 不存在")
        if sh["status"] != ShipmentStatus.IN_TRANSIT.value:
            raise ConflictError(f"运单状态 {sh['status']} 不可标记失败")
        req = self._get_request(conn, sh["request_id"])
        alloc = conn.execute("SELECT * FROM allocations WHERE allocation_id=?",
                             (sh["allocation_id"],)).fetchone()
        q = sh["quantity"]

        # 补偿：货物退回原仓库并重新预留（恢复未完成状态）
        conn.execute(
            """UPDATE inventory SET on_hand = on_hand + ?, reserved = reserved + ?
               WHERE warehouse_id=? AND part_number=?""",
            (q, q, sh["from_warehouse"], req["part_number"]))
        self._ledger(conn, sh["from_warehouse"], req["part_number"],
                     LedgerEvent.TRANSFER_RETURNED,
                     delta_on_hand=q, delta_reserved=q,
                     request_id=req["request_id"], ref_id=shipment_id,
                     note=f"调拨失败退回：{reason}")
        new_shipped = alloc["shipped_quantity"] - q
        new_a_status = (AllocationStatus.RESERVED.value if new_shipped == 0
                        else AllocationStatus.PARTIALLY_SHIPPED.value)
        conn.execute("UPDATE allocations SET shipped_quantity=?, status=? WHERE allocation_id=?",
                     (new_shipped, new_a_status, alloc["allocation_id"]))
        conn.execute("UPDATE shipments SET status=?, failure_reason=? WHERE shipment_id=?",
                     (ShipmentStatus.FAILED_RETURNED.value, reason, shipment_id))

        now = self._now_iso()
        new_version = req["version"] + 1
        conn.execute("UPDATE requests SET status=?, version=?, updated_at=? WHERE request_id=?",
                     (RequestStatus.APPROVED.value, new_version, now, req["request_id"]))
        self._audit(conn, req["request_id"], "TRANSFER_FAILED", actor=actor,
                    from_status=req["status"], to_status=RequestStatus.APPROVED.value,
                    version=new_version,
                    detail={"shipment_id": shipment_id, "reason": reason,
                            "quantity": q, "recovered": True})
        return {"shipment_id": shipment_id, "status": ShipmentStatus.FAILED_RETURNED.value,
                "request_id": req["request_id"],
                "request_status": RequestStatus.APPROVED.value,
                "recovered": True, "returned_quantity": q, "version": new_version}

    def replan(self, request_id, *, exclude_warehouses=(), actor="planner",
               idempotency_key=None):
        """重新规划：释放被排除仓库的未出库预留，就剩余缺口重新生成拆分方案。"""
        payload = {"request_id": request_id,
                   "exclude_warehouses": sorted(exclude_warehouses), "actor": actor}
        with self.store.transaction() as conn:
            return self._idempotent(
                conn, idempotency_key, "replan", payload,
                lambda: self._replan(conn, request_id,
                                     exclude_warehouses=set(exclude_warehouses),
                                     actor=actor))

    def _replan(self, conn, request_id, *, exclude_warehouses, actor):
        req = self._get_request(conn, request_id)
        if req["status"] not in (RequestStatus.APPROVED.value, RequestStatus.FULFILLING.value):
            raise ConflictError(f"状态 {req['status']} 不可重新规划")

        released = 0
        placeholders = ",".join("?" * len(_ACTIVE_ALLOC))
        allocs = conn.execute(
            f"""SELECT * FROM allocations
                WHERE request_id=? AND status IN ({placeholders})""",
            (request_id, *_ACTIVE_ALLOC)).fetchall()
        for a in allocs:
            if a["warehouse_id"] not in exclude_warehouses:
                continue
            unshipped = a["quantity"] - a["shipped_quantity"]
            if unshipped > 0:
                cur = conn.execute(
                    """UPDATE inventory SET reserved = reserved - ?
                       WHERE warehouse_id=? AND part_number=? AND reserved >= ?""",
                    (unshipped, a["warehouse_id"], req["part_number"], unshipped))
                if cur.rowcount != 1:
                    raise ConflictError("预留数量异常，释放失败")
                self._ledger(conn, a["warehouse_id"], req["part_number"],
                             LedgerEvent.RESERVATION_RELEASED,
                             delta_on_hand=0, delta_reserved=-unshipped,
                             request_id=request_id, ref_id=a["allocation_id"],
                             note="重新规划释放")
                released += unshipped
            conn.execute("UPDATE allocations SET status=? WHERE allocation_id=?",
                         (AllocationStatus.RELEASED.value, a["allocation_id"]))

        shipped_total = conn.execute(
            "SELECT COALESCE(SUM(shipped_quantity),0) AS s FROM allocations WHERE request_id=?",
            (request_id,)).fetchone()["s"]
        active_reserved = sum(
            a["quantity"] - a["shipped_quantity"] for a in conn.execute(
                f"""SELECT * FROM allocations
                    WHERE request_id=? AND status IN ({placeholders})""",
                (request_id, *_ACTIVE_ALLOC)).fetchall())
        remaining = req["quantity"] - shipped_total - active_reserved

        new_allocations, shortfall, reasons = [], 0, []
        if remaining > 0:
            plan = self._plan_for_request(conn, req, quantity=remaining,
                                          exclude_warehouses=exclude_warehouses)
            for line in plan.lines:
                allocation_id = _new_id("ALC")
                self._reserve(conn, req, line, allocation_id)
                new_allocations.append({
                    "allocation_id": allocation_id, "warehouse_id": line.warehouse_id,
                    "quantity": line.quantity, "transport_hours": line.transport_hours,
                    "promised_arrival": line.promised_arrival,
                    "within_deadline": line.within_deadline,
                })
            shortfall, reasons = plan.shortfall, plan.reasons

        new_version = req["version"] + 1
        conn.execute("UPDATE requests SET version=?, updated_at=? WHERE request_id=?",
                     (new_version, self._now_iso(), request_id))
        self._audit(conn, request_id, "REPLANNED", actor=actor, version=new_version,
                    detail={"excluded_warehouses": sorted(exclude_warehouses),
                            "released_quantity": released,
                            "new_allocations": new_allocations, "shortfall": shortfall})
        return {"request_id": request_id, "released_quantity": released,
                "new_allocations": new_allocations, "shortfall": shortfall,
                "blocking_reasons": reasons, "version": new_version}

    # ------------------------------------------------------------------
    # 规划与预留（内部）
    # ------------------------------------------------------------------
    def _plan_for_request(self, conn, req, *, quantity, exclude_warehouses):
        rows = conn.execute(
            """SELECT i.warehouse_id, (i.on_hand - i.reserved) AS available, r.base_hours
               FROM inventory i
               LEFT JOIN routes r ON r.warehouse_id = i.warehouse_id AND r.destination = ?
               WHERE i.part_number = ?""",
            (req["destination"], req["part_number"])).fetchall()
        return plan_allocation(
            candidates=[dict(r) for r in rows],
            quantity=quantity,
            required_by_hours=req["required_by_hours"],
            fault_level=FaultLevel(req["fault_level"]),
            now=self.clock(),
            exclude_warehouses=exclude_warehouses,
        )

    def _reserve(self, conn, req, line, allocation_id):
        cur = conn.execute(
            """UPDATE inventory SET reserved = reserved + ?
               WHERE warehouse_id=? AND part_number=? AND on_hand - reserved >= ?""",
            (line.quantity, line.warehouse_id, req["part_number"], line.quantity))
        if cur.rowcount != 1:
            raise ConflictError(f"仓库 {line.warehouse_id} 可用库存不足，预留失败")
        conn.execute(
            """INSERT INTO allocations(allocation_id, request_id, warehouse_id, quantity,
                                       shipped_quantity, status, transport_hours,
                                       promised_arrival, created_at)
               VALUES (?,?,?,?,0,?,?,?,?)""",
            (allocation_id, req["request_id"], line.warehouse_id, line.quantity,
             AllocationStatus.RESERVED.value, line.transport_hours,
             line.promised_arrival, self._now_iso()))
        self._ledger(conn, line.warehouse_id, req["part_number"], LedgerEvent.RESERVED,
                     delta_on_hand=0, delta_reserved=line.quantity,
                     request_id=req["request_id"], ref_id=allocation_id, note="批准预留")

    # ------------------------------------------------------------------
    # 查询（管理人员视图）
    # ------------------------------------------------------------------
    def get_request_view(self, request_id):
        """管理人员视图：可用量、承诺到货时间、阻塞原因、完整审计记录。"""
        conn = self.store.conn
        req = self._get_request(conn, request_id)
        allocations = [dict(a) for a in conn.execute(
            "SELECT * FROM allocations WHERE request_id=? ORDER BY created_at, allocation_id",
            (request_id,))]
        shipments = [dict(s) for s in conn.execute(
            "SELECT * FROM shipments WHERE request_id=? ORDER BY shipped_at, shipment_id",
            (request_id,))]
        approvals = [dict(a) for a in conn.execute(
            "SELECT * FROM approvals WHERE request_id=? ORDER BY id", (request_id,))]
        inv_rows = conn.execute(
            """SELECT warehouse_id, on_hand, reserved, (on_hand - reserved) AS available
               FROM inventory WHERE part_number=? ORDER BY warehouse_id""",
            (req["part_number"],)).fetchall()

        shipped_total = sum(a["shipped_quantity"] for a in allocations)
        reserved_active = sum(a["quantity"] - a["shipped_quantity"] for a in allocations
                              if a["status"] in _ACTIVE_ALLOC)
        promised = max(
            (a["promised_arrival"] for a in allocations
             if a["status"] in _ACTIVE_ALLOC and a["promised_arrival"]),
            default=None)
        blocking = self._blocking_reasons(req, allocations, shipments,
                                          reserved_active, shipped_total)
        return {
            "request_id": req["request_id"],
            "status": req["status"],
            "version": req["version"],
            "unit_id": req["unit_id"],
            "destination": req["destination"],
            "equipment_model": req["equipment_model"],
            "part_number": req["part_number"],
            "quantity": req["quantity"],
            "fault_level": req["fault_level"],
            "required_by_hours": req["required_by_hours"],
            "current_approval_level": req["current_approval_level"],
            "required_approval_levels": json.loads(req["required_approval_levels"]),
            "reserved_quantity": reserved_active,
            "shipped_quantity": shipped_total,
            "delivered_quantity": sum(s["quantity"] for s in shipments
                                      if s["status"] == ShipmentStatus.DELIVERED.value),
            "open_quantity": req["quantity"] - shipped_total - reserved_active,
            "available_now": sum(r["available"] for r in inv_rows),
            "inventory_by_warehouse": [dict(r) for r in inv_rows],
            "promised_arrival": promised,
            "blocking_reasons": blocking,
            "allocations": allocations,
            "shipments": shipments,
            "approvals": approvals,
            "audit_trail": self._audit_trail(conn, request_id),
        }

    def _blocking_reasons(self, req, allocations, shipments, reserved_active, shipped_total):
        reasons = []
        status = req["status"]
        if status == RequestStatus.RETURNED.value:
            reasons.append(f"已退回补充：{req['return_reason'] or '未说明'}")
        if status in (RequestStatus.SUBMITTED.value, RequestStatus.UNDER_REVIEW.value):
            total = len(json.loads(req["required_approval_levels"]))
            reasons.append(f"等待第 {req['current_approval_level'] + 1} 级审批（共 {total} 级）")
        if status in (RequestStatus.APPROVED.value, RequestStatus.FULFILLING.value):
            open_qty = req["quantity"] - shipped_total - reserved_active
            if open_qty > 0:
                reasons.append(f"库存不足，{open_qty} 件尚未分配")
            for s in shipments:
                if s["status"] == ShipmentStatus.FAILED_RETURNED.value:
                    reasons.append(
                        f"运单 {s['shipment_id']} 调拨失败：{s['failure_reason']}；"
                        "货物已退回原仓库，待重新调拨")
        deadline = (datetime.fromisoformat(req["created_at"])
                    + timedelta(hours=req["required_by_hours"])).isoformat()
        for a in allocations:
            if (a["status"] in _ACTIVE_ALLOC and a["promised_arrival"]
                    and a["promised_arrival"] > deadline):
                reasons.append(
                    f"分配 {a['allocation_id']} 承诺到货 {a['promised_arrival']} 超出运输时限")
                break
        if status == RequestStatus.CANCELLED.value and shipped_total:
            reasons.append(f"已撤销；{shipped_total} 件已出库锁定，不可回收")
        return reasons

    def _audit_trail(self, conn, request_id):
        """合并审批/状态审计与库存流水，按时间排序，形成完整审计记录。"""
        trail = []
        for a in conn.execute(
                "SELECT * FROM audit_log WHERE request_id=? ORDER BY id", (request_id,)):
            trail.append({
                "kind": "audit", "seq": a["id"], "time": a["created_at"],
                "event": a["event"], "actor": a["actor"], "level": a["level"],
                "from_status": a["from_status"], "to_status": a["to_status"],
                "version": a["version"], "detail": json.loads(a["detail_json"] or "{}"),
            })
        for l in conn.execute(
                "SELECT * FROM inventory_ledger WHERE request_id=? ORDER BY id", (request_id,)):
            trail.append({
                "kind": "inventory", "seq": l["id"], "time": l["created_at"],
                "event": l["event"], "warehouse_id": l["warehouse_id"],
                "delta_on_hand": l["delta_on_hand"], "delta_reserved": l["delta_reserved"],
                "on_hand_after": l["on_hand_after"], "reserved_after": l["reserved_after"],
                "ref_id": l["ref_id"], "note": l["note"],
            })
        trail.sort(key=lambda e: (e["time"], 0 if e["kind"] == "audit" else 1, e["seq"]))
        return trail

    def inventory_position(self, part_number):
        rows = self.store.conn.execute(
            """SELECT warehouse_id, on_hand, reserved, (on_hand - reserved) AS available
               FROM inventory WHERE part_number=? ORDER BY warehouse_id""",
            (part_number,)).fetchall()
        return {
            "part_number": part_number,
            "warehouses": [dict(r) for r in rows],
            "on_hand": sum(r["on_hand"] for r in rows),
            "reserved": sum(r["reserved"] for r in rows),
            "available": sum(r["available"] for r in rows),
        }

    def list_requests(self, status=None):
        if status is None:
            rows = self.store.conn.execute(
                "SELECT request_id, unit_id, part_number, quantity, fault_level, status, "
                "version, created_at FROM requests ORDER BY created_at, request_id").fetchall()
        else:
            rows = self.store.conn.execute(
                "SELECT request_id, unit_id, part_number, quantity, fault_level, status, "
                "version, created_at FROM requests WHERE status=? "
                "ORDER BY created_at, request_id", (status,)).fetchall()
        return [dict(r) for r in rows]
