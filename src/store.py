"""SQLite 持久化层：表结构定义与事务助手。

所有写操作都在 ``BEGIN IMMEDIATE`` 事务中完成：进入事务即取得写锁，
保证"读-改-写"序列不会被其他写入者插入，从根上杜绝超卖/重复占用。
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS warehouses (
    warehouse_id TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    location     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS parts (
    part_number     TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    equipment_model TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS routes (
    warehouse_id TEXT NOT NULL REFERENCES warehouses(warehouse_id),
    destination  TEXT NOT NULL,
    base_hours   REAL NOT NULL CHECK (base_hours > 0),
    PRIMARY KEY (warehouse_id, destination)
);

-- 库存：available = on_hand - reserved，reserved 永不超过 on_hand
CREATE TABLE IF NOT EXISTS inventory (
    warehouse_id TEXT NOT NULL REFERENCES warehouses(warehouse_id),
    part_number  TEXT NOT NULL REFERENCES parts(part_number),
    on_hand      INTEGER NOT NULL CHECK (on_hand >= 0),
    reserved     INTEGER NOT NULL CHECK (reserved >= 0),
    PRIMARY KEY (warehouse_id, part_number),
    CHECK (reserved <= on_hand)
);

CREATE TABLE IF NOT EXISTS requests (
    request_id               TEXT PRIMARY KEY,
    unit_id                  TEXT NOT NULL,
    destination              TEXT NOT NULL,
    equipment_model          TEXT NOT NULL,
    part_number              TEXT NOT NULL,
    quantity                 INTEGER NOT NULL CHECK (quantity > 0),
    fault_level              TEXT NOT NULL,
    required_by_hours        REAL NOT NULL CHECK (required_by_hours > 0),
    status                   TEXT NOT NULL,
    version                  INTEGER NOT NULL,
    current_approval_level   INTEGER NOT NULL DEFAULT 0,
    required_approval_levels TEXT NOT NULL,   -- JSON 数组，如 [1,2,3]
    return_reason            TEXT,
    created_at               TEXT NOT NULL,
    updated_at               TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS approvals (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL REFERENCES requests(request_id),
    level      INTEGER NOT NULL,
    approver   TEXT NOT NULL,
    decision   TEXT NOT NULL,   -- APPROVED / RETURNED / REJECTED
    note       TEXT,
    decided_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS allocations (
    allocation_id    TEXT PRIMARY KEY,
    request_id       TEXT NOT NULL REFERENCES requests(request_id),
    warehouse_id     TEXT NOT NULL REFERENCES warehouses(warehouse_id),
    quantity         INTEGER NOT NULL CHECK (quantity > 0),
    shipped_quantity INTEGER NOT NULL DEFAULT 0 CHECK (shipped_quantity >= 0),
    status           TEXT NOT NULL,
    transport_hours  REAL,
    promised_arrival TEXT,
    created_at       TEXT NOT NULL,
    CHECK (shipped_quantity <= quantity)
);

CREATE TABLE IF NOT EXISTS shipments (
    shipment_id    TEXT PRIMARY KEY,
    request_id     TEXT NOT NULL REFERENCES requests(request_id),
    allocation_id  TEXT NOT NULL REFERENCES allocations(allocation_id),
    from_warehouse TEXT NOT NULL,
    quantity       INTEGER NOT NULL CHECK (quantity > 0),
    status         TEXT NOT NULL,
    failure_reason TEXT,
    shipped_at     TEXT NOT NULL,
    delivered_at   TEXT
);

-- 库存流水账：只追加，不更新。每次库存变化一行，含变化后余额。
CREATE TABLE IF NOT EXISTS inventory_ledger (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    warehouse_id   TEXT NOT NULL,
    part_number    TEXT NOT NULL,
    event          TEXT NOT NULL,
    delta_on_hand  INTEGER NOT NULL,
    delta_reserved INTEGER NOT NULL,
    on_hand_after  INTEGER NOT NULL,
    reserved_after INTEGER NOT NULL,
    request_id     TEXT,
    ref_id         TEXT,
    note           TEXT,
    created_at     TEXT NOT NULL
);

-- 幂等键：网络重试时按键返回首次决定
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key           TEXT PRIMARY KEY,
    operation     TEXT NOT NULL,
    payload_json  TEXT NOT NULL,
    request_id    TEXT,
    response_json TEXT NOT NULL,
    created_at    TEXT NOT NULL
);

-- 审计日志：申请生命周期内的每次状态/版本变化
CREATE TABLE IF NOT EXISTS audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id  TEXT,
    event       TEXT NOT NULL,
    actor       TEXT,
    level       INTEGER,
    from_status TEXT,
    to_status   TEXT,
    version     INTEGER,
    detail_json TEXT,
    created_at  TEXT NOT NULL
);
"""


class Store:
    """单连接 SQLite 存储。"""

    def __init__(self, path: str = ":memory:"):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.isolation_level = None  # 手动控制事务
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript(SCHEMA)

    @contextmanager
    def transaction(self):
        """写事务：BEGIN IMMEDIATE 保证读写序列化，异常自动回滚。"""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
        except BaseException:
            self.conn.rollback()
            raise
        self.conn.commit()

    def close(self):
        self.conn.close()
