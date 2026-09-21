"""调拨系统演示：复现"网络不稳定重复提交"问题及系统防护。

运行：python3 demo.py
"""
import json

from src.service import RequisitionService
from src.store import Store


def show(title, obj):
    print(f"\n{'=' * 20} {title} {'=' * 20}")
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def main():
    svc = RequisitionService(Store(":memory:"))

    # 基础数据：三个仓库、一种备件、到前沿阵地的运输线路
    svc.register_warehouse("WH-BJ", "北京库", "北京")
    svc.register_warehouse("WH-SH", "上海库", "上海")
    svc.register_warehouse("WH-CD", "成都库", "成都")
    svc.register_part("TB-100", "涡轮叶片", "Z-20")
    svc.add_route("WH-BJ", "前沿-7", 10)
    svc.add_route("WH-SH", "前沿-7", 20)
    svc.add_route("WH-CD", "前沿-7", 30)
    svc.receive_stock("WH-BJ", "TB-100", 5)
    svc.receive_stock("WH-SH", "TB-100", 5)

    # 1) 网络不稳定，前线重复提交同一申请（同一幂等键）——只建一单
    r1 = svc.submit_request(idempotency_key="UNIT1-TB100-0001", unit_id="UNIT-1",
                            destination="前沿-7", equipment_model="Z-20",
                            part_number="TB-100", quantity=8,
                            fault_level="URGENT", required_by_hours=48)
    r1_retry = svc.submit_request(idempotency_key="UNIT1-TB100-0001", unit_id="UNIT-1",
                                  destination="前沿-7", equipment_model="Z-20",
                                  part_number="TB-100", quantity=8,
                                  fault_level="URGENT", required_by_hours=48)
    show("1. 重复提交（网络重试）——返回原决定", {
        "首次": r1, "重试": r1_retry,
        "系统中申请数": len(svc.list_requests()),
    })

    # 2) 逐级审批（URGENT 需 L1+L2），最终一级自动生成拆分方案并预留库存
    rid = r1["request_id"]
    svc.approve(rid, level=1, approver="仓库调度员")
    final = svc.approve(rid, level=2, approver="保障部长", idempotency_key="APV-0001-L2")
    show("2. 批准后拆分方案（BJ 5 + SH 3，库存已预留）", final)

    # 3) 第二个申请到达：只能拿到剩余 2 件，紧缺备件不会被重复批准
    r2 = svc.submit_request(idempotency_key="UNIT2-TB100-0002", unit_id="UNIT-2",
                            destination="前沿-7", equipment_model="Z-20",
                            part_number="TB-100", quantity=5,
                            fault_level="ROUTINE", required_by_hours=72)
    svc.approve(r2["request_id"], level=1, approver="仓库调度员")
    show("3. 第二个申请（5 件）只分到剩余 2 件，缺口 3 件",
         svc.get_request_view(r2["request_id"]))

    # 4) 北京库出库 5 件——已出库数量锁定
    bj_alloc = final["allocations"][0]["allocation_id"]
    ship = svc.ship(bj_alloc)
    show("4. 北京库出库 5 件（锁定）", svc.inventory_position("TB-100"))

    # 5) 跨仓库调拨失败——补偿恢复：货物退回、重新预留、申请回到未完成状态
    fail = svc.fail_shipment(ship["shipment_id"], reason="运输途中车辆故障")
    show("5. 调拨失败恢复（货物退回并重新预留）", {
        "恢复结果": fail, "库存": svc.inventory_position("TB-100"),
    })

    # 6) 成都库紧急入库后，排除故障仓库重新规划，改从成都库调拨
    svc.receive_stock("WH-CD", "TB-100", 5, note="紧急调拨入库")
    replanned = svc.replan(rid, exclude_warehouses={"WH-BJ"})
    show("6. 重新规划（排除北京库，改从成都库调拨 5 件）", replanned)

    # 6.5) 上海库 3 件 + 成都库 5 件出库并送达，申请完成
    view = svc.get_request_view(rid)
    for alloc in view["allocations"]:
        if alloc["status"] == "RESERVED":
            sh = svc.ship(alloc["allocation_id"])
            svc.deliver(sh["shipment_id"])
    show("6.5 全部送达后申请完成", {
        "状态": svc.get_request_view(rid)["status"],
        "库存": svc.inventory_position("TB-100"),
    })

    # 7) 管理人员查询：可用量、承诺到货时间、阻塞原因、完整审计记录
    view = svc.get_request_view(rid)
    show("7. 管理人员视图", {
        "状态": view["status"], "版本": view["version"],
        "可用量": view["available_now"],
        "已预留": view["reserved_quantity"],
        "承诺到货": view["promised_arrival"],
        "阻塞原因": view["blocking_reasons"],
        "审批记录": view["approvals"],
        "审计记录条数": len(view["audit_trail"]),
    })
    show("7.1 完整审计记录（审批层级 + 版本 + 每次库存变化）", view["audit_trail"])


if __name__ == "__main__":
    main()
