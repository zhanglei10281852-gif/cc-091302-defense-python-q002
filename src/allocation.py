"""分配规划：纯函数，按型号/故障等级/库存地点/运输时限生成多仓库拆分方案。

库存承诺分两层：
  * 硬预留 reserved —— 已批准未出库，任何方案都不能动用；
  * 软占位 held —— 方案已生成、尚在逐级审批中（plan_lines.status=PLANNED）。
高等级故障在规划时可以抢占*低等级*申请的软占位（被抢占者转阻塞并升版本重审），
但永远不能动用硬预留，更不可能碰到已出库数量。
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class StockCell:
    warehouse_id: str
    on_hand: int
    reserved: int


@dataclass(frozen=True)
class Hold:
    """一条待审批方案行对 (仓库, 备件) 的软占位。"""

    line_id: str
    request_id: str
    warehouse_id: str
    qty: int
    fault_rank: int
    created_at: str


@dataclass(frozen=True)
class CandidateLine:
    warehouse_id: str
    qty: int
    lead_time_hours: int


@dataclass
class PlanOutcome:
    lines: list[CandidateLine] = field(default_factory=list)
    eta_hours: int | None = None
    blocked: bool = False
    block_code: str | None = None
    block_detail: dict | None = None
    evicted_holds: list[Hold] = field(default_factory=list)


def build_plan(
    *,
    stock: list[StockCell],
    holds: list[Hold],
    transit: dict[str, int],
    qty: int,
    deadline_hours: int,
    self_request_id: str,
    self_rank: int,
) -> PlanOutcome:
    """生成单个备件申请的拆分方案。

    transit 只含能服务该目的地的仓库（键为 warehouse_id，值为运输小时数）。
    """
    stock_by_wh = {c.warehouse_id: c for c in stock}

    # 所有能向该目的地发货、且有该备件库存记录的仓库（含超出时限的）
    reachable = [
        (wh, hours)
        for wh, hours in transit.items()
        if wh in stock_by_wh
    ]
    if not reachable:
        return PlanOutcome(
            blocked=True,
            block_code="DEADLINE_INFEASIBLE" if transit else "SHORTAGE",
            block_detail={
                "deadline_hours": deadline_hours,
                "fastest_hours": min(transit.values()) if transit else None,
                "reason": "无仓库可送达该目的地" if not transit else "时限内仓库均无该备件",
            },
        )

    # 按仓库汇总其他申请的软占位，并保留可驱逐明细（等级低、生成早的先被驱逐）
    held_total: dict[str, int] = {}
    reclaimable: dict[str, list[Hold]] = {}
    for h in holds:
        if h.request_id == self_request_id:
            continue
        held_total[h.warehouse_id] = held_total.get(h.warehouse_id, 0) + h.qty
        if h.fault_rank < self_rank:
            reclaimable.setdefault(h.warehouse_id, []).append(h)
    for lst in reclaimable.values():
        lst.sort(key=lambda h: (h.fault_rank, h.created_at, h.line_id))

    def _make_pool(wh: str, hours: int) -> dict | None:
        cell = stock_by_wh[wh]
        held = held_total.get(wh, 0)
        free = max(0, cell.on_hand - cell.reserved - held)
        # 占位行整行驱逐（重规划按行作废），取最小前缀使总量满足需求
        reclaim: list[Hold] = []
        reclaim_qty = 0
        for h in reclaimable.get(wh, ()):
            if free + reclaim_qty >= qty:
                break
            reclaim.append(h)
            reclaim_qty += h.qty
        if free <= 0 and not reclaim:
            return None
        return {
            "warehouse_id": wh,
            "hours": hours,
            "free": free,
            "reclaim": reclaim,
            "capacity": free + reclaim_qty,
        }

    all_pools = [p for wh, hrs in reachable if (p := _make_pool(wh, hrs))]
    feasible_pools = [p for p in all_pools if p["hours"] <= deadline_hours]

    capacity_all = sum(p["capacity"] for p in all_pools)
    capacity_in_time = sum(p["capacity"] for p in feasible_pools)
    fastest = min((hrs for _, hrs in reachable), default=None)

    if capacity_all < qty:
        return PlanOutcome(
            blocked=True,
            block_code="SHORTAGE",
            block_detail={
                "need": qty,
                "capacity_all_reachable": capacity_all,
                "capacity_within_deadline": capacity_in_time,
                "deadline_hours": deadline_hours,
                "fastest_hours": fastest,
            },
        )
    if capacity_in_time < qty:
        return PlanOutcome(
            blocked=True,
            block_code="DEADLINE_INFEASIBLE",
            block_detail={
                "need": qty,
                "capacity_all_reachable": capacity_all,
                "capacity_within_deadline": capacity_in_time,
                "deadline_hours": deadline_hours,
                "fastest_hours": fastest,
            },
        )

    # 运输时间最短优先（承诺到货最快），同时间容量大的优先以减少拆分行数
    pools = sorted(
        feasible_pools, key=lambda p: (p["hours"], -p["capacity"], p["warehouse_id"])
    )

    lines: list[CandidateLine] = []
    evicted: list[Hold] = []
    remaining = qty
    for pool in pools:
        if remaining <= 0:
            break
        take = min(remaining, pool["capacity"])
        # 先吃普通可用量；不足部分整行驱逐低等级软占位（池构建时已选好最小集合）
        covered = min(take, pool["free"])
        for h in pool["reclaim"]:
            if covered >= take:
                break
            evicted.append(h)
            covered += h.qty
        lines.append(
            CandidateLine(
                warehouse_id=pool["warehouse_id"],
                qty=take,
                lead_time_hours=pool["hours"],
            )
        )
        remaining -= take

    return PlanOutcome(
        lines=lines,
        eta_hours=max(line.lead_time_hours for line in lines),
        evicted_holds=evicted,
    )
