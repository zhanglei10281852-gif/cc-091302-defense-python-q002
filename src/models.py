"""领域枚举与策略常量。"""
from __future__ import annotations

import enum


class FaultLevel(str, enum.Enum):
    """故障等级：决定审批层级与运输提速系数。"""

    ROUTINE = "ROUTINE"      # 一般
    URGENT = "URGENT"        # 紧急
    CRITICAL = "CRITICAL"    # 特急


class RequestStatus(str, enum.Enum):
    SUBMITTED = "SUBMITTED"          # 已提交
    UNDER_REVIEW = "UNDER_REVIEW"    # 审批中
    RETURNED = "RETURNED"            # 已退回补充
    APPROVED = "APPROVED"            # 已批准（库存已预留）
    REJECTED = "REJECTED"            # 已驳回
    FULFILLING = "FULFILLING"        # 调拨执行中
    COMPLETED = "COMPLETED"          # 已完成
    CANCELLED = "CANCELLED"          # 已撤销


class AllocationStatus(str, enum.Enum):
    RESERVED = "RESERVED"                    # 已预留
    PARTIALLY_SHIPPED = "PARTIALLY_SHIPPED"  # 部分出库
    SHIPPED = "SHIPPED"                      # 全部出库
    RELEASED = "RELEASED"                    # 已释放（未出库部分退回可分配池）


class ShipmentStatus(str, enum.Enum):
    IN_TRANSIT = "IN_TRANSIT"            # 在途（数量已锁定，不可被其他申请占用）
    DELIVERED = "DELIVERED"              # 已送达
    FAILED_RETURNED = "FAILED_RETURNED"  # 调拨失败，货物已退回原仓库并恢复待调拨


class LedgerEvent(str, enum.Enum):
    STOCK_RECEIVED = "STOCK_RECEIVED"                # 入库
    RESERVED = "RESERVED"                            # 预留
    RESERVATION_RELEASED = "RESERVATION_RELEASED"    # 释放预留
    SHIPPED_OUT = "SHIPPED_OUT"                      # 出库
    TRANSFER_RETURNED = "TRANSFER_RETURNED"          # 调拨失败退回


# 审批层级：故障等级 -> 需要依次通过的审批级别
APPROVAL_CHAIN = {
    FaultLevel.ROUTINE: (1,),
    FaultLevel.URGENT: (1, 2),
    FaultLevel.CRITICAL: (1, 2, 3),
}

# 运输提速系数：故障越急，采用越快的运输方式，承诺到货时间越短
TRANSPORT_FACTOR = {
    FaultLevel.ROUTINE: 1.0,
    FaultLevel.URGENT: 0.75,
    FaultLevel.CRITICAL: 0.5,
}
