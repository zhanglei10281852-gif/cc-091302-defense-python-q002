"""领域模型：枚举、数据类与异常定义。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime


class FaultLevel(enum.Enum):
    """故障等级：等级越高，调拨优先级越高，审批链越长。"""

    MINOR = "MINOR"          # 一般故障
    MAJOR = "MAJOR"          # 严重故障
    CRITICAL = "CRITICAL"    # 紧急/战备故障

    @property
    def label(self) -> str:
        return _FAULT_LABELS[self]

    @property
    def rank(self) -> int:
        """优先级排序值，越大越紧急。"""
        return {FaultLevel.MINOR: 1, FaultLevel.MAJOR: 2, FaultLevel.CRITICAL: 3}[self]


_FAULT_LABELS = {
    FaultLevel.MINOR: "一般",
    FaultLevel.MAJOR: "严重",
    FaultLevel.CRITICAL: "紧急",
}


class ApprovalLevel(enum.Enum):
    """审批层级，按 rank 逐级审批。"""

    LEVEL1 = 1   # 现场调度员
    LEVEL2 = 2   # 仓库主任
    LEVEL3 = 3   # 装备保障部首长

    @property
    def label(self) -> str:
        return {
            ApprovalLevel.LEVEL1: "现场调度员",
            ApprovalLevel.LEVEL2: "仓库主任",
            ApprovalLevel.LEVEL3: "装备保障部首长",
        }[self]


class RequestStatus(enum.Enum):
    SUBMITTED = "SUBMITTED"                 # 已提交
    RETURNED = "RETURNED"                   # 退回补充
    PLANNED = "PLANNED"                     # 方案已生成，待逐级审批
    BLOCKED = "BLOCKED"                     # 库存/时限不可行，阻塞
    PENDING_APPROVAL = "PENDING_APPROVAL"   # 审批中（已通过部分层级）
    APPROVED = "APPROVED"                   # 全部层级批准，库存已预留
    FULFILLMENT_FAILED = "FULFILLMENT_FAILED"  # 跨仓库出库中断，待恢复
    PARTIALLY_SHIPPED = "PARTIALLY_SHIPPED"    # 部分仓库已出库
    SHIPPED = "SHIPPED"                     # 全部出库完成
    REVOKED = "REVOKED"                     # 撤销（无出库）
    PARTIALLY_REVOKED = "PARTIALLY_REVOKED"    # 撤销但已有出库（出库量保留锁定）

    @property
    def label(self) -> str:
        return _STATUS_LABELS[self]


_STATUS_LABELS = {
    RequestStatus.SUBMITTED: "已提交",
    RequestStatus.RETURNED: "退回补充",
    RequestStatus.PLANNED: "方案待审批",
    RequestStatus.BLOCKED: "阻塞",
    RequestStatus.PENDING_APPROVAL: "审批中",
    RequestStatus.APPROVED: "已批准",
    RequestStatus.FULFILLMENT_FAILED: "出库中断待恢复",
    RequestStatus.PARTIALLY_SHIPPED: "部分已出库",
    RequestStatus.SHIPPED: "全部出库",
    RequestStatus.REVOKED: "已撤销",
    RequestStatus.PARTIALLY_REVOKED: "撤销（含已出库）",
}


class LineStatus(enum.Enum):
    PLANNED = "PLANNED"        # 方案行，尚未预留库存
    SUPERSEDED = "SUPERSEDED"  # 被重新规划/新版本取代
    RESERVED = "RESERVED"      # 已批准，库存已预留
    FAILED = "FAILED"          # 出库失败，预留仍保留，可恢复
    SHIPPED = "SHIPPED"        # 已出库：数量锁定，不可再分配
    RELEASED = "RELEASED"      # 预留释放（撤销等）


class ApprovalDecision(enum.Enum):
    APPROVED = "APPROVED"
    RETURNED = "RETURNED"


class BlockerCode(enum.Enum):
    SHORTAGE = "SHORTAGE"                       # 可用库存不足
    DEADLINE_INFEASIBLE = "DEADLINE_INFEASIBLE"  # 无仓库能在时限内运达
    RESERVATION_LOST = "RESERVATION_LOST"        # 终审时库存被更高优先级申请占用
    SUPPLEMENT_REQUIRED = "SUPPLEMENT_REQUIRED"  # 等待申请方补充材料

    @property
    def label(self) -> str:
        return {
            BlockerCode.SHORTAGE: "库存不足",
            BlockerCode.DEADLINE_INFEASIBLE: "时限内无法运达",
            BlockerCode.RESERVATION_LOST: "批准时库存被占用",
            BlockerCode.SUPPLEMENT_REQUIRED: "待补充材料",
        }[self]


class AllocationError(Exception):
    """业务规则错误基类。"""


class InvalidStateError(AllocationError):
    """申请当前状态不允许该操作。"""


class ReservationLostError(AllocationError):
    """终审预留库存时发现可用量不足（并发占用）。"""


class IdempotencyConflictError(AllocationError):
    """同一幂等键携带了不同的请求内容。"""


@dataclass(frozen=True)
class Part:
    part_id: str
    name: str
    equipment_model: str
    spec: str = ""


@dataclass(frozen=True)
class Warehouse:
    warehouse_id: str
    name: str
    location: str


@dataclass(frozen=True)
class StockView:
    warehouse: Warehouse
    part: Part
    on_hand: int          # 账面库存
    reserved: int         # 已预留（批准未出库）
    issued: int           # 累计已出库（仅统计用，出库时已从 on_hand 扣减）

    @property
    def available(self) -> int:
        """可承诺量 = 账面 - 已预留。已出库部分早已离开库存，不可能被二次分配。"""
        return self.on_hand - self.reserved


@dataclass(frozen=True)
class AuditEntry:
    id: int
    ts: datetime
    aggregate_type: str
    aggregate_id: str
    action: str
    actor: str
    version: int | None
    before: dict | None
    after: dict | None
    detail: dict | None = field(default=None)
