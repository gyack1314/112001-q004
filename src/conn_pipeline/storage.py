"""SQLite schema 与连接初始化。

设计原则：
1. landing 层（ingest_batches / raw_records / source_files）只追加，触发器拒绝 UPDATE/DELETE；
2. 观测层与证据层同样只追加，重复摄入靠内容哈希幂等；
3. 快照层（snapshots/clusters/connection_facts/cluster_observations/fact_evidence）
   按 snapshot_id 分区，快照一经提交即为历史，迟到证据只能生成新快照，不能改写旧快照。
"""
from __future__ import annotations

import sqlite3

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

-- ---------------------------------------------------------------- landing 层
CREATE TABLE IF NOT EXISTS ingest_batches (
    batch_id        TEXT PRIMARY KEY,              -- 期号 + 文件哈希前缀派生
    period          TEXT NOT NULL,
    source_name     TEXT NOT NULL,
    file_name       TEXT NOT NULL,
    file_sha256     TEXT NOT NULL,
    byte_size       INTEGER NOT NULL,
    row_count       INTEGER NOT NULL,
    ingested_at     TEXT NOT NULL,
    UNIQUE (file_sha256, file_name)                -- 同一文件重复摄入幂等
);

CREATE TABLE IF NOT EXISTS raw_records (
    raw_record_id   TEXT PRIMARY KEY,              -- 文件哈希 + 行号 + 行哈希
    batch_id        TEXT NOT NULL REFERENCES ingest_batches(batch_id),
    source_name     TEXT NOT NULL,
    period          TEXT NOT NULL,
    row_index       INTEGER NOT NULL,
    row_sha256      TEXT NOT NULL,
    raw_payload     TEXT NOT NULL,                 -- 原样 JSON，不做任何改写
    UNIQUE (batch_id, row_index)
);

CREATE TABLE IF NOT EXISTS source_files (
    source_name     TEXT PRIMARY KEY,
    registered_at   TEXT NOT NULL,
    description     TEXT NOT NULL DEFAULT ''
);

-- ---------------------------------------------------------------- 观测层
CREATE TABLE IF NOT EXISTS observations (
    observation_uid TEXT PRIMARY KEY,              -- 内容哈希派生，跨快照稳定
    period          TEXT NOT NULL,
    source_name     TEXT NOT NULL,
    batch_id        TEXT NOT NULL REFERENCES ingest_batches(batch_id),
    raw_record_id   TEXT NOT NULL REFERENCES raw_records(raw_record_id),
    device_id       TEXT,
    imsi            TEXT,
    iccid           TEXT,
    network         TEXT,
    other_attrs     TEXT NOT NULL DEFAULT '{}',
    UNIQUE (raw_record_id)
);
CREATE INDEX IF NOT EXISTS idx_obs_period ON observations(period);
CREATE INDEX IF NOT EXISTS idx_obs_imsi ON observations(imsi);
CREATE INDEX IF NOT EXISTS idx_obs_iccid ON observations(iccid);
CREATE INDEX IF NOT EXISTS idx_obs_device ON observations(device_id);

CREATE TABLE IF NOT EXISTS aggregate_observations (
    aggregate_uid   TEXT PRIMARY KEY,
    period          TEXT NOT NULL,
    source_name     TEXT NOT NULL,
    batch_id        TEXT NOT NULL REFERENCES ingest_batches(batch_id),
    raw_record_id   TEXT NOT NULL REFERENCES raw_records(raw_record_id),
    reported_count  INTEGER NOT NULL,
    account_ref     TEXT,
    UNIQUE (raw_record_id)
);
CREATE INDEX IF NOT EXISTS idx_agg_period ON aggregate_observations(period);

-- ---------------------------------------------------------------- 身份证据层
CREATE TABLE IF NOT EXISTS identity_evidence (
    evidence_uid    TEXT PRIMARY KEY,              -- 证据内容哈希
    period          TEXT NOT NULL,                 -- 该证据适用/声明的报告期
    source_name     TEXT NOT NULL,
    batch_id        TEXT NOT NULL REFERENCES ingest_batches(batch_id),
    raw_record_id   TEXT NOT NULL REFERENCES raw_records(raw_record_id),
    evidence_type   TEXT NOT NULL,                 -- co_occurrence | identifier_mapping
    mapping_group   TEXT NOT NULL,                 -- 同一映射关系的分组键
    id_type         TEXT NOT NULL,                 -- imsi | iccid | device_id
    id_value        TEXT NOT NULL,
    applies_from    TEXT,                          -- 含端点，None 表示不限
    applies_to      TEXT,                          -- 含端点，None 表示不限
    UNIQUE (raw_record_id, evidence_type, mapping_group, id_type, id_value)
);
CREATE INDEX IF NOT EXISTS idx_ev_period ON identity_evidence(period);

-- ---------------------------------------------------------------- 口径层
CREATE TABLE IF NOT EXISTS caliber_versions (
    caliber_id      TEXT PRIMARY KEY,
    version_no      INTEGER NOT NULL UNIQUE,
    created_at      TEXT NOT NULL,
    description     TEXT NOT NULL,
    rules_json      TEXT NOT NULL
);

-- ---------------------------------------------------------------- 快照层（不可变历史）
CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id     TEXT PRIMARY KEY,
    period          TEXT NOT NULL,
    revision_no     INTEGER NOT NULL,
    caliber_id      TEXT NOT NULL REFERENCES caliber_versions(caliber_id),
    created_at      TEXT NOT NULL,
    committed       INTEGER NOT NULL DEFAULT 1,    -- 建快照即提交
    input_fingerprint TEXT NOT NULL,               -- 摄入证据/观测的内容哈希
    note            TEXT NOT NULL DEFAULT '',
    UNIQUE (period, revision_no)
);
CREATE INDEX IF NOT EXISTS idx_snap_period ON snapshots(period);

CREATE TABLE IF NOT EXISTS clusters (
    cluster_uid     TEXT PRIMARY KEY,                  -- 内含 snapshot 哈希，全表唯一
    snapshot_id     TEXT NOT NULL REFERENCES snapshots(snapshot_id),
    terminal_uid    TEXT NOT NULL,                 -- 簇内最小观测键派生，跨修订稳定
    status          TEXT NOT NULL,                 -- confirmed | undetermined
    status_reason   TEXT NOT NULL,
    network         TEXT
);
CREATE INDEX IF NOT EXISTS idx_cluster_snap ON clusters(snapshot_id);

CREATE TABLE IF NOT EXISTS cluster_observations (
    snapshot_id     TEXT NOT NULL REFERENCES snapshots(snapshot_id),
    cluster_uid     TEXT NOT NULL,
    observation_uid TEXT NOT NULL REFERENCES observations(observation_uid),
    PRIMARY KEY (snapshot_id, observation_uid)
);

CREATE TABLE IF NOT EXISTS connection_facts (
    fact_uid        TEXT NOT NULL,
    snapshot_id     TEXT NOT NULL REFERENCES snapshots(snapshot_id),
    cluster_uid     TEXT REFERENCES clusters(cluster_uid),  -- 汇总类事实为 NULL
    fact_type       TEXT NOT NULL,                 -- terminal | aggregate_constraint
    status          TEXT NOT NULL,                 -- confirmed | undetermined | constraint
    device_id       TEXT,
    imsi            TEXT,
    iccid           TEXT,
    network         TEXT,
    reported_count  INTEGER,                       -- 仅 aggregate_constraint
    account_ref     TEXT,
    detail_covered  INTEGER,                       -- 仅 aggregate_constraint
    unexplained_gap INTEGER,                       -- 仅 aggregate_constraint
    UNIQUE (snapshot_id, fact_uid)
);
CREATE INDEX IF NOT EXISTS idx_fact_snap ON connection_facts(snapshot_id);

CREATE TABLE IF NOT EXISTS fact_evidence (
    snapshot_id     TEXT NOT NULL,
    fact_uid        TEXT NOT NULL,
    evidence_uid    TEXT REFERENCES identity_evidence(evidence_uid),
    basis           TEXT NOT NULL,                 -- shared_identifier | explicit_mapping | observed_alone
    detail          TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (snapshot_id, fact_uid, evidence_uid, basis)
);

-- 追加写保护：任何 UPDATE/DELETE 直接拒绝，保证历史不可篡改
CREATE TRIGGER IF NOT EXISTS trg_raw_records_no_update
    BEFORE UPDATE ON raw_records BEGIN
        SELECT RAISE(ABORT, 'raw_records 为追加写表，禁止更新'); END;
CREATE TRIGGER IF NOT EXISTS trg_raw_records_no_delete
    BEFORE DELETE ON raw_records BEGIN
        SELECT RAISE(ABORT, 'raw_records 为追加写表，禁止删除'); END;
CREATE TRIGGER IF NOT EXISTS trg_batches_no_update
    BEFORE UPDATE ON ingest_batches BEGIN
        SELECT RAISE(ABORT, 'ingest_batches 为追加写表，禁止更新'); END;
CREATE TRIGGER IF NOT EXISTS trg_batches_no_delete
    BEFORE DELETE ON ingest_batches BEGIN
        SELECT RAISE(ABORT, 'ingest_batches 为追加写表，禁止删除'); END;
CREATE TRIGGER IF NOT EXISTS trg_obs_no_update
    BEFORE UPDATE ON observations BEGIN
        SELECT RAISE(ABORT, 'observations 为追加写表，禁止更新'); END;
CREATE TRIGGER IF NOT EXISTS trg_obs_no_delete
    BEFORE DELETE ON observations BEGIN
        SELECT RAISE(ABORT, 'observations 为追加写表，禁止删除'); END;
CREATE TRIGGER IF NOT EXISTS trg_agg_no_update
    BEFORE UPDATE ON aggregate_observations BEGIN
        SELECT RAISE(ABORT, 'aggregate_observations 为追加写表，禁止更新'); END;
CREATE TRIGGER IF NOT EXISTS trg_agg_no_delete
    BEFORE DELETE ON aggregate_observations BEGIN
        SELECT RAISE(ABORT, 'aggregate_observations 为追加写表，禁止删除'); END;
CREATE TRIGGER IF NOT EXISTS trg_evidence_no_update
    BEFORE UPDATE ON identity_evidence BEGIN
        SELECT RAISE(ABORT, 'identity_evidence 为追加写表，禁止更新'); END;
CREATE TRIGGER IF NOT EXISTS trg_evidence_no_delete
    BEFORE DELETE ON identity_evidence BEGIN
        SELECT RAISE(ABORT, 'identity_evidence 为追加写表，禁止删除'); END;
CREATE TRIGGER IF NOT EXISTS trg_snapshots_no_update
    BEFORE UPDATE ON snapshots BEGIN
        SELECT RAISE(ABORT, '快照一经提交不可修改'); END;
CREATE TRIGGER IF NOT EXISTS trg_snapshots_no_delete
    BEFORE DELETE ON snapshots BEGIN
        SELECT RAISE(ABORT, '快照一经提交不可删除'); END;
CREATE TRIGGER IF NOT EXISTS trg_facts_no_update
    BEFORE UPDATE ON connection_facts BEGIN
        SELECT RAISE(ABORT, '连接事实属于不可变快照，禁止更新'); END;
CREATE TRIGGER IF NOT EXISTS trg_facts_no_delete
    BEFORE DELETE ON connection_facts BEGIN
        SELECT RAISE(ABORT, '连接事实属于不可变快照，禁止删除'); END;
"""


def connect(db_path: str) -> sqlite3.Connection:
    """打开（必要时创建）管道数据库并初始化 schema。"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA_SQL)
    return conn
