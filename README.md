# 装备维修备件调拨

面向装备保障业务的维修备件调拨系统。针对"网络不稳定导致重复提交、相同调拨被重复批准、
紧缺备件被延误"的问题，提供幂等提交、库存锁定、分级审批、失败恢复与全程留痕能力。

运行环境：Python 3.11，仅依赖标准库（SQLite 持久化）。代码位于 `src` 目录。

## 快速开始

```bash
python3 -m unittest tests.test_service -v   # 运行测试（21 个场景）
python3 demo.py                              # 运行端到端演示
```

## 核心设计

**状态机（申请）**
`SUBMITTED → UNDER_REVIEW → APPROVED → FULFILLING → COMPLETED`，旁路
`RETURNED`（退回补充，修改后重新提交）、`REJECTED`、`CANCELLED`（批准后也可撤销）。

**幂等（网络重试安全）**
所有写操作接受 `idempotency_key`。重试时若键已存在且载荷一致，直接返回首次决定
（`idempotent_replay: true`）；键相同但载荷不同则拒绝（`IdempotencyConflictError`）。
幂等记录与业务写入在同一事务中落库，不会出现"批了两次"的情况。

**库存锁定（杜绝重复占用）**
`available = on_hand - reserved`。批准即在事务内预留（带守卫条件的 UPDATE），
出库时 `on_hand` 与 `reserved` 同步扣减——已出库数量从可分配池中彻底移除，
任何后续申请都看不到它。撤销只释放未出库的预留，已出库部分保持锁定并记入审计。

**分配方案**
按装备型号匹配备件，按故障等级确定审批链（ROUTINE→L1，URGENT→L1+L2，
CRITICAL→L1+L2+L3）与运输提速系数（1.0 / 0.75 / 0.5）。规划器贪心拆分：
先满足运输时限，再按运输时间升序、可用量降序从多个仓库取货，
输出行项级承诺到货时间与缺口原因。

**失败恢复（补偿）**
跨仓库调拨失败（`fail_shipment`）在单个事务内完成补偿：货物退回原仓库、
重新预留、运单标记 `FAILED_RETURNED`、申请回到 `APPROVED` 未完成状态。
之后可重新出库，或 `replan(exclude_warehouses=...)` 释放故障仓库预留、
改从其他仓库调拨。

**留痕**
- `audit_log`：提交/审批（含层级）/退回/修改/批准/撤销/出库/送达/失败/重规划，每条带版本号；
- `inventory_ledger`：只追加的库存流水，记录每次变化前后余额，可由流水重放对账；
- `requests.version`：每次变更 +1，写操作可带 `expected_version` 做乐观锁校验。

**管理人员视图**（`get_request_view`）
可用量、各仓库库存分布、承诺到货时间、阻塞原因（待审批层级 / 库存缺口 /
调拨失败 / 超时 / 退回原因）、审批记录与合并排序的完整审计记录。

## API 一览（`src.service.RequisitionService`）

| 方法 | 说明 |
| --- | --- |
| `submit_request(..., idempotency_key=)` | 提交申请（幂等） |
| `approve(request_id, level=, ...)` | 逐级审批；最终一级生成拆分方案并预留 |
| `return_for_supplement(...)` / `amend_request(...)` | 退回补充 / 修改后重新提交 |
| `reject(...)` | 驳回 |
| `cancel(request_id, ...)` | 撤销（含批准后）；释放未出库预留，已出库保持锁定 |
| `ship(allocation_id, quantity=)` | 出库（数量锁定） |
| `deliver(shipment_id)` | 确认送达；全部送达后申请完成 |
| `fail_shipment(shipment_id, reason=)` | 调拨失败补偿恢复 |
| `replan(request_id, exclude_warehouses=)` | 重新规划剩余缺口 |
| `get_request_view(request_id)` | 管理人员视图 |
| `inventory_position(part_number)` | 库存可用量查询 |

## 目录结构

```
src/
  models.py    # 枚举与策略（审批链、运输提速系数）
  errors.py    # 领域异常
  store.py     # SQLite 表结构与事务助手
  planner.py   # 分配方案规划（纯函数）
  service.py   # 业务服务（事务、幂等、锁定、恢复、审计、视图）
tests/test_service.py  # 21 个核心场景测试
demo.py                # 端到端演示（复现重复提交问题及防护）
```
