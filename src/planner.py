"""分配方案规划：按故障等级、库存地点与运输时限生成拆分方案。

纯函数，不触碰数据库，便于独立测试。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .models import FaultLevel, TRANSPORT_FACTOR


@dataclass
class PlanLine:
    warehouse_id: str
    quantity: int
    transport_hours: float
    promised_arrival: str   # ISO 时间
    within_deadline: bool


@dataclass
class Plan:
    lines: list[PlanLine] = field(default_factory=list)
    shortfall: int = 0
    reasons: list[str] = field(default_factory=list)


def plan_allocation(
    *,
    candidates: list[dict],
    quantity: int,
    required_by_hours: float,
    fault_level: FaultLevel,
    now: datetime,
    exclude_warehouses=(),
) -> Plan:
    """贪心拆分：先满足运输时限，再按运输时间升序、可用量降序取货。

    candidates: [{"warehouse_id", "available", "base_hours"}]
    返回 Plan：lines 为各仓库拆分数量与承诺到货时间，shortfall 为未满足缺口，
    reasons 说明阻塞原因（库存不足 / 无线路 / 超时 / 仓库被排除）。
    """
    factor = TRANSPORT_FACTOR[fault_level]
    reasons: list[str] = []
    excluded = set(exclude_warehouses)
    enriched = []
    for c in candidates:
        wh = c["warehouse_id"]
        avail = c["available"]
        if wh in excluded:
            if avail > 0:
                reasons.append(f"仓库 {wh} 已被排除（{avail} 件可用但未采用）")
            continue
        if avail <= 0:
            continue
        if c["base_hours"] is None:
            reasons.append(f"仓库 {wh} 到目的地无运输线路")
            continue
        eta = round(c["base_hours"] * factor, 2)
        feasible = eta <= required_by_hours
        # 排序键：可行优先、运输时间最短优先、可用量最大优先
        enriched.append((not feasible, eta, -avail, wh, avail))

    enriched.sort()

    lines: list[PlanLine] = []
    remaining = quantity
    for infeasible, eta, _neg_avail, wh, avail in enriched:
        if remaining <= 0:
            break
        take = min(avail, remaining)
        promised = (now + timedelta(hours=eta)).isoformat()
        lines.append(PlanLine(
            warehouse_id=wh,
            quantity=take,
            transport_hours=eta,
            promised_arrival=promised,
            within_deadline=not infeasible,
        ))
        remaining -= take

    if remaining > 0:
        reasons.append(f"库存不足，缺口 {remaining} 件")
    if any(not line.within_deadline for line in lines):
        reasons.append("部分分配的承诺到货时间超出运输时限")

    return Plan(lines=lines, shortfall=remaining, reasons=reasons)
