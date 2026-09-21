"""SQLite 持久层：建表脚本与轻量仓储封装。"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS parts (
    part_id         TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    equipment_model TEXT NOT NULL,
    spec            TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS warehouses (
    warehouse_id TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    location     TEXT NOT NULL
);

-- 库存：on_hand 出库即扣；reserved 为已批准未出库的承诺量；issued_total 仅统计
CREATE TABLE IF NOT EXISTS stock (
    warehouse_id TEXT NOT NULL,
    part_id      TEXT NOT NULL,
    on_hand      INTEGER NOT NULL DEFAULT 0 CHECK (on_hand >= 0),
    reserved     INTEGER NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    issued_total INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (warehouse_id, part_id),
    CHECK (reserved <= on_hand),
    FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id),
    FOREIGN KEY (part_id) REFERENCES parts(part_id)
);

-- 仓库 -> 申请目的地 的运输时限（小时）
CREATE TABLE IF NOT EXISTS transit (
    warehouse_id TEXT NOT NULL,
    destination  TEXT NOT NULL,
    hours        INTEGER NOT NULL,
    PRIMARY KEY (warehouse_id, destination),
    FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id)
);

CREATE TABLE IF NOT EXISTS requests (
    request_id       TEXT PRIMARY KEY,
    version          INTEGER NOT NULL DEFAULT 1,
    part_id          TEXT NOT NULL,
    equipment_model  TEXT NOT NULL,
    fault_level      TEXT NOT NULL,
    qty              INTEGER NOT NULL CHECK (qty > 0),
    destination      TEXT NOT NULL,
    deadline_hours   INTEGER NOT NULL CHECK (deadline_hours > 0),
    status           TEXT NOT NULL,
    block_code       TEXT,
    block_detail     TEXT,
    plan_generation  INTEGER NOT NULL DEFAULT 1,
    idempotency_key  TEXT UNIQUE,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    approved_at      TEXT,
    shipped_at       TEXT,
    FOREIGN KEY (part_id) REFERENCES parts(part_id)
);

-- 每次退回补充/修订都产生一个新版本
CREATE TABLE IF NOT EXISTS revisions (
    request_id  TEXT NOT NULL,
    version     INTEGER NOT NULL,
    actor       TEXT NOT NULL,
    reason      TEXT NOT NULL DEFAULT '',
    payload     TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    PRIMARY KEY (request_id, version),
    FOREIGN KEY (request_id) REFERENCES requests(request_id)
);

-- 分配方案行：同一申请每个方案代次一组，旧代次行置为 SUPERSEDED
CREATE TABLE IF NOT EXISTS plan_lines (
    line_id         TEXT PRIMARY KEY,
    request_id      TEXT NOT NULL,
    generation      INTEGER NOT NULL,
    warehouse_id    TEXT NOT NULL,
    part_id         TEXT NOT NULL,
    qty             INTEGER NOT NULL CHECK (qty > 0),
    lead_time_hours INTEGER NOT NULL,
    seq             INTEGER NOT NULL,
    status          TEXT NOT NULL,
    fail_reason     TEXT,
    attempts        INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    FOREIGN KEY (request_id) REFERENCES requests(request_id),
    FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id)
);
CREATE INDEX IF NOT EXISTS idx_plan_lines_req ON plan_lines(request_id, generation);

-- 逐级审批留痕（退回是终局决定，同样记录）
CREATE TABLE IF NOT EXISTS approvals (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id  TEXT NOT NULL,
    version     INTEGER NOT NULL,
    generation  INTEGER NOT NULL DEFAULT 1,
    level       INTEGER NOT NULL,
    decision    TEXT NOT NULL,
    actor       TEXT NOT NULL,
    comment     TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL,
    FOREIGN KEY (request_id) REFERENCES requests(request_id)
);
CREATE INDEX IF NOT EXISTS idx_approvals_req ON approvals(request_id, version);

-- 通用幂等表：网络重试直接返回首次决定
CREATE TABLE IF NOT EXISTS idempotency (
    idem_key    TEXT PRIMARY KEY,
    scope       TEXT NOT NULL,
    request_id  TEXT,
    fingerprint TEXT NOT NULL,
    response    TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

-- 审计日志：审批层级、版本跃迁、每次库存变化全部落表，只追加
CREATE TABLE IF NOT EXISTS audit_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    ts             TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    action         TEXT NOT NULL,
    actor          TEXT NOT NULL,
    version        INTEGER,
    ref_request_id TEXT,
    before         TEXT,
    after          TEXT,
    detail         TEXT
);
CREATE INDEX IF NOT EXISTS idx_audit_agg ON audit_log(aggregate_type, aggregate_id);
CREATE INDEX IF NOT EXISTS idx_audit_req ON audit_log(ref_request_id);
"""


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


class Repository:
    """持有单一 SQLite 连接。写事务由服务层用 RLock 串行化后显式开启。"""

    def __init__(self, path: str | Path = ":memory:"):
        self.path = str(path)
        self.conn = sqlite3.connect(
            self.path,
            check_same_thread=False,
            isolation_level=None,  # 手工事务
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.init_schema()

    def init_schema(self) -> None:
        self.conn.executescript(SCHEMA)

    def begin(self) -> None:
        self.conn.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        self.conn.execute("COMMIT")

    def rollback(self) -> None:
        self.conn.execute("ROLLBACK")

    def query(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        return self.conn.execute(sql, tuple(params)).fetchall()

    def query_one(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Row | None:
        return self.conn.execute(sql, tuple(params)).fetchone()

    def execute(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, tuple(params))

    def close(self) -> None:
        self.conn.close()


def dumps(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=str, sort_keys=True)


def loads(value: str | None) -> Any:
    if value is None:
        return None
    return json.loads(value)
