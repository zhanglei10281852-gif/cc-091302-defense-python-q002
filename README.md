# 装备维修备件调拨系统

面向不稳定网络环境的装备备件调拨服务：依据**装备型号、故障等级、库存地点、运输时限**
自动生成（可跨仓库拆分的）分配方案，支持退回补充、逐级审批、批准后撤销、跨仓库出库
失败恢复，并对审批层级、版本与每一次库存变化完整留痕；网络重试凭幂等键返回原决定，
已出库数量锁定，绝不可能被第二个申请重复占用。

运行环境：Python 3.11，仅用标准库（`sqlite3`），无第三方依赖。

## 快速开始

```bash
python3 -m examples.demo                      # 场景演示
python3 -m unittest discover -s tests -v      # 22 项端到端测试
```

```python
from src.service import AllocationService
from src.domain import FaultLevel

svc = AllocationService("alloc.db")            # 或 ":memory:"
svc.register_part("P-001", "液压泵", "ZTZ-99")
svc.register_warehouse("WH-1", "一号库", "华北")
svc.set_stock("WH-1", "P-001", 10)
svc.set_transit("WH-1", "前沿阵地", 12)        # 运输时限（小时）

r = svc.submit_request(
    part_id="P-001", equipment_model="ZTZ-99",
    fault_level=FaultLevel.MAJOR, qty=4,
    destination="前沿阵地", deadline_hours=24,
    actor="前沿一营",
    idempotency_key="battle-net-req-7752",    # 断网重试原样返回
)
svc.approve(r["request_id"], actor="值班调度员")
svc.approve(r["request_id"], actor="仓库主任")  # MAJOR 两级审批
svc.fulfill(r["request_id"])                    # 出库（可注入 WMS 网关）
```

## 核心能力与设计

### 1. 分配方案（`src/allocation.py`，纯函数可测）
- 仅选择**运输时限内可达**的仓库，按"到货最快优先、减少拆分行数"排序，自动拆分到多个仓库。
- 阻塞原因区分：`SHORTAGE`（放宽时限全网总量也不足）与 `DEADLINE_INFEASIBLE`
  （远处仓库有货但赶不到），附带时限内容量、全网容量、最快运输时间等明细。
- **两级库存承诺**：
  - 硬预留 `reserved`——终审通过即锁定，任何方案都不能动用；
  - 软占位 `held`——方案审批中的计划量。高等级故障规划时可**抢占低等级**申请的
    软占位（被抢占申请方案作废、方案代次递增、自动重规划并留痕），同级不可抢占；
    已硬预留与已出库数量在任何情况下都不可抢占。

### 2. 申请流转与版本
- `SUBMITTED → PLANNED → PENDING_APPROVAL → APPROVED → SHIPPED`，
  另有 `RETURNED / BLOCKED / FULFILLMENT_FAILED / PARTIALLY_SHIPPED /
  REVOKED / PARTIALLY_REVOKED`。
- **退回补充**（任意待审层级）与**补充修订**（可改数量/时限/等级/目的地）均产生
  新版本（`revisions` 表），并触发重新规划；审批链与**方案代次**绑定，
  重规划后旧代次批准自动失效，须逐级重审。

### 3. 审批层级
按故障等级配置审批链（可在 `service._APPROVAL_CHAIN` 调整）：
一般故障 1 级（现场调度员），严重故障 2 级（＋仓库主任），
紧急故障 3 级（＋装备保障部首长）。终审在单个事务内对全部方案行做条件预留
（`UPDATE ... WHERE on_hand-reserved >= qty`），任一仓库不足则**整单不预留**、
转 `BLOCKED`（`RESERVATION_LOST`），杜绝部分预留。

### 4. 幂等（网络重试返回原决定）
所有写操作接受 `idempotency_key`：首次执行的决定快照（状态、方案行、ETA、阻塞原因）
连同请求内容指纹存入 `idempotency` 表；同键重试直接回放原决定，不重复建单/审批/出库。
同键不同内容抛 `IdempotencyConflictError`。

### 5. 出库 Saga 与失败恢复（`fulfill`）
逐仓库执行，可注入真实 WMS 网关 `gateway(warehouse_id, part_id, qty, idem_token)`；
网关收到稳定的 `申请:行:代次` 令牌用于自身重试去重。某仓库失败时：
- 已成功行 `SHIPPED`：账面与预留同步扣减、累计出库量增加，**数量锁定**；
- 失败行 `FAILED`：**预留保留**，申请转 `PARTIALLY_SHIPPED`/`FULFILLMENT_FAILED`；
- 再次 `fulfill()` 先重试 FAILED 行，恢复未完成状态直至 `SHIPPED`。

### 6. 撤销
批准前后均可撤销；仅释放未出库的预留（含 FAILED 待恢复行），已出库行锁定不动，
状态据是否有出库落为 `REVOKED` 或 `PARTIALLY_REVOKED`。

### 7. 管理视图与审计（`get_request`）
- **可用量**：各仓库账面 / 硬预留 / 审批中占位 / 可承诺量 / 累计出库；
- **承诺到货时间**：当前方案各仓库运输时长的最大值（`eta_hours`）；
- **阻塞原因**：编码 + 中文含义 + 明细，出库失败另附失败仓库/次数/原因；
- **完整审计**：提交、每一级审批/退回、版本修订、方案创建/作废/被抢占、
  库存预留/释放/出库/盘点/补货，全部带操作人、时间、版本、变化前后快照，
  审计表只追加（`audit_log`）。

## 代码结构

| 文件 | 职责 |
| --- | --- |
| `src/domain.py` | 枚举（故障等级/审批层级/状态/阻塞码）、数据类、业务异常 |
| `src/storage.py` | SQLite schema、事务与查询封装、JSON 序列化 |
| `src/allocation.py` | 纯函数分配规划器：可达性、容量、拆分、软占位抢占 |
| `src/service.py` | `AllocationService`：申请/审批/预留/撤销/Saga/幂等/审计/查询 |
| `examples/demo.py` | 重复提交、两级审批、中断恢复、退回补充四个场景演示 |
| `tests/test_allocation.py` | 22 项端到端测试（含 6 线程并发不超卖） |

## 并发与部署

进程内用一把可重入锁串行化写操作，SQLite 以 `BEGIN IMMEDIATE` 开启写事务，
CHECK 约束（`reserved <= on_hand`、非负）与条件 UPDATE 构成最后防线。
多实例部署时把 `Repository` 换成同一 schema 的中央数据库实现即可，锁序与 SQL 不变。
