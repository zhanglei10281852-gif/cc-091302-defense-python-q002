"""场景演示：不稳定网络下的备件调拨全流程。

运行：python3 -m examples.demo
"""

from __future__ import annotations

from src.domain import FaultLevel
from src.service import AllocationService, WarehouseGatewayError


def main() -> None:
    svc = AllocationService(":memory:")

    # ---- 基础数据 ----
    svc.register_part("P-001", "液压泵", "ZTZ-99")
    svc.register_warehouse("WH-1", "一号综合库", "华北")
    svc.register_warehouse("WH-2", "二号前沿库", "华东")
    svc.register_warehouse("WH-3", "三号战略库", "西北")
    svc.set_stock("WH-1", "P-001", 3)
    svc.set_stock("WH-2", "P-001", 2)
    svc.set_stock("WH-3", "P-001", 50)
    svc.set_transit("WH-1", "前沿阵地", 12)
    svc.set_transit("WH-2", "前沿阵地", 6)
    svc.set_transit("WH-3", "前沿阵地", 72)

    print("=" * 70)
    print("场景 1：网络抖动导致重复提交 —— 幂等键返回同一份决定")
    print("=" * 70)
    kw = dict(
        part_id="P-001", equipment_model="ZTZ-99",
        fault_level=FaultLevel.MAJOR, qty=4,
        destination="前沿阵地", deadline_hours=24, actor="前沿一营",
        idempotency_key="battle-net-req-7752",
    )
    r1 = svc.submit_request(**kw)
    r2 = svc.submit_request(**kw)  # 前线断网后的自动重试
    print(f"首次/重试返回同一申请号: {r1['request_id'] == r2['request_id']}")
    print(f"库中申请总数: {len(svc.list_requests())}（不会出现两份重复调拨）")
    rid = r1["request_id"]
    print("分配方案（按最快到货拆分）:")
    for line in r1["decision"]["lines"]:
        print(f"  {line['warehouse_id']}: {line['qty']} 件, 运输 {line['lead_time_hours']}h")

    print()
    print("=" * 70)
    print("场景 2：严重故障两级审批，终审硬预留库存")
    print("=" * 70)
    svc.approve(rid, actor="值班调度员", comment="同意")
    svc.approve(rid, actor="仓库主任", comment="同意")
    view = svc.get_request(rid)
    print(f"状态: {view['status']}，承诺到货: {view['committed_arrival']}")
    print("各仓库可用量（已预留部分不再可承诺）:")
    for s in view["available_stock"]:
        print(f"  {s['warehouse_id']}: 账面 {s['on_hand']}, 预留 {s['reserved']}, "
              f"审批中占位 {s['held']}, 可用 {s['available']}")

    print()
    print("=" * 70)
    print("场景 3：跨仓库出库，WH-1 网络中断 —— Saga 部分成功 + 恢复")
    print("=" * 70)
    attempts = {"n": 0}

    def wms_gateway(wh: str, part: str, qty: int, token: str) -> None:
        # 外部 WMS 用 token 做自己的幂等去重
        attempts["n"] += 1
        if wh == "WH-1" and attempts["n"] >= 2:
            raise WarehouseGatewayError("WH-1 链路中断")

    res = svc.fulfill(rid, wms_gateway)
    print(f"首次出库后状态: {res['decision']['status']}")
    view = svc.get_request(rid)
    for f in view.get("fulfillment_failures", []):
        print(f"  阻塞: {f['warehouse_name']} {f['qty']} 件，原因: {f['reason']}")
    print("  已出库数量保持锁定；WH-1 的预留未释放，不会被其他申请占用")

    # 链路恢复，恢复未完成部分
    res2 = svc.fulfill(rid, lambda *a: None)
    print(f"恢复后状态: {res2['decision']['status']}")
    print(f"累计出库锁定: {svc.get_request(rid)['shipped_qty']} 件")

    print()
    print("=" * 70)
    print("场景 4：退回补充产生新版本，完整审计可追溯")
    print("=" * 70)
    r3 = svc.submit_request(
        part_id="P-001", equipment_model="ZTZ-99",
        fault_level=FaultLevel.CRITICAL, qty=1,
        destination="前沿阵地", deadline_hours=24, actor="前沿二营",
    )
    rid3 = r3["request_id"]
    svc.return_for_supplement(rid3, actor="值班调度员", comment="请补充装备序列号")
    svc.supplement(rid3, {"qty": 2}, actor="前沿二营", reason="序列号已补，实测需2件")
    v3 = svc.get_request(rid3)
    print(f"当前版本: v{v3['version']}，方案代次: gen{v3['plan_generation']}")
    print(f"审批链: {' → '.join(c['label'] for c in v3['approval_chain'])}")
    print(f"审计记录 {len(v3['audit'])} 条，操作序列:")
    for a in v3["audit"]:
        extra = f" v{a['version']}" if a["version"] else ""
        print(f"  [{a['ts']}] {a['action']}{extra} by {a['actor']}")


if __name__ == "__main__":
    main()
