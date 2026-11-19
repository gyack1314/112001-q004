"""不可变快照：注册口径版本并按期生成连接事实。

同一期可生成多个 revision：
- 原始事实变化（迟到证据/补录） => 输入指纹变化；
- 口径变化（换 caliber）       => 输入指纹不变、caliber_id 变化；
两类原因在 compare 中分别标注，旧快照永远保留。
"""
from __future__ import annotations

import json
import sqlite3

from .config import caliber_fingerprint
from .identity import Cluster, _resolve_clusters
from .utils import canonical_json, now_iso, sha256_text, short_id


def register_caliber(conn: sqlite3.Connection, rules: dict,
                     description: str | None = None) -> str:
    """登记口径版本；规则内容相同则复用同一 caliber_id。"""
    fingerprint = caliber_fingerprint(rules)
    caliber_id = short_id("cal", fingerprint, length=24)
    existing = conn.execute(
        "SELECT caliber_id FROM caliber_versions WHERE caliber_id = ?",
        (caliber_id,),
    ).fetchone()
    if existing:
        return caliber_id
    next_no = conn.execute(
        "SELECT COALESCE(MAX(version_no), 0) + 1 AS n FROM caliber_versions"
    ).fetchone()["n"]
    conn.execute(
        "INSERT INTO caliber_versions("
        "caliber_id, version_no, created_at, description, rules_json) "
        "VALUES (?, ?, ?, ?, ?)",
        (caliber_id, next_no, now_iso(),
         description or rules.get("description", ""),
         canonical_json(rules)),
    )
    return caliber_id


def _input_fingerprint(conn: sqlite3.Connection, period: str) -> str:
    """对该期全部观测、汇总观测与证据的内容做哈希，作为事实输入指纹。"""
    parts = []
    for table in ("observations", "aggregate_observations", "identity_evidence"):
        rows = conn.execute(
            f"SELECT * FROM {table} WHERE period = ? ORDER BY 1", (period,)
        ).fetchall()
        parts.append(table + ":" + canonical_json([dict(r) for r in rows]))
    return sha256_text("\n".join(parts))


def _next_revision(conn: sqlite3.Connection, period: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(revision_no), 0) + 1 AS n FROM snapshots "
        "WHERE period = ?",
        (period,),
    ).fetchone()
    return row["n"]


def create_snapshot(conn: sqlite3.Connection, period: str,
                    caliber_rules: dict, note: str = "") -> dict:
    """为报告期构建并提交一个不可变快照，返回快照摘要。"""
    caliber_id = register_caliber(conn, caliber_rules)
    fingerprint = _input_fingerprint(conn, period)
    revision_no = _next_revision(conn, period)
    snapshot_id = short_id("snap", period, revision_no, fingerprint, caliber_id,
                           length=24)
    created_at = now_iso()
    try:
        conn.execute(
            "INSERT INTO snapshots("
            "snapshot_id, period, revision_no, caliber_id, created_at, committed, "
            "input_fingerprint, note) VALUES (?, ?, ?, ?, ?, 1, ?, ?)",
            (snapshot_id, period, revision_no, caliber_id, created_at,
             fingerprint, note),
        )

        clusters = _resolve_clusters(conn, period, caliber_rules)
        for cluster in clusters:
            cluster_uid = short_id("clu", snapshot_id, cluster.terminal_uid)
            conn.execute(
                "INSERT INTO clusters("
                "cluster_uid, snapshot_id, terminal_uid, status, status_reason, "
                "network) VALUES (?, ?, ?, ?, ?, ?)",
                (cluster_uid, snapshot_id, cluster.terminal_uid, cluster.status,
                 cluster.status_reason, cluster.network),
            )
            for obs_uid in cluster.members:
                conn.execute(
                    "INSERT INTO cluster_observations("
                    "snapshot_id, cluster_uid, observation_uid) VALUES (?, ?, ?)",
                    (snapshot_id, cluster_uid, obs_uid),
                )
            fact_uid = short_id("fact", snapshot_id, "term", cluster.terminal_uid)
            conn.execute(
                "INSERT INTO connection_facts("
                "fact_uid, snapshot_id, cluster_uid, fact_type, status, "
                "device_id, imsi, iccid, network) "
                "VALUES (?, ?, ?, 'terminal', ?, ?, ?, ?, ?)",
                (fact_uid, snapshot_id, cluster_uid, cluster.status,
                 cluster.device_id, cluster.imsi, cluster.iccid, cluster.network),
            )
            if cluster.evidence_uids:
                for ev_uid in sorted(cluster.evidence_uids):
                    basis = _basis_of(ev_uid, conn)
                    conn.execute(
                        "INSERT OR IGNORE INTO fact_evidence("
                        "snapshot_id, fact_uid, evidence_uid, basis, detail) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (snapshot_id, fact_uid, ev_uid, basis,
                         "该终端簇的关联证据链"),
                    )
            else:
                conn.execute(
                    "INSERT INTO fact_evidence("
                    "snapshot_id, fact_uid, evidence_uid, basis, detail) "
                    "VALUES (?, ?, NULL, 'observed_alone', ?)",
                    (snapshot_id, fact_uid,
                     f"{cluster.status_reason}：无来源证据可与其他观测关联"),
                )

        _write_aggregate_facts(conn, period, snapshot_id, clusters)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {
        "snapshot_id": snapshot_id,
        "period": period,
        "revision_no": revision_no,
        "caliber_id": caliber_id,
        "created_at": created_at,
        "input_fingerprint": fingerprint,
        "clusters": len(clusters),
        "confirmed": sum(1 for c in clusters if c.status == "confirmed"),
        "undetermined": sum(1 for c in clusters if c.status == "undetermined"),
    }


def _basis_of(evidence_uid: str, conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "SELECT evidence_type FROM identity_evidence WHERE evidence_uid = ?",
        (evidence_uid,),
    ).fetchone()
    return "explicit_mapping" if row and row["evidence_type"] == "identifier_mapping" \
        else "shared_identifier"


def _write_aggregate_facts(conn, period, snapshot_id, clusters: list[Cluster]) -> None:
    """汇总数来源只产生 constraint 事实，并与可相加明细做覆盖对照。"""
    aggs = conn.execute(
        "SELECT * FROM aggregate_observations WHERE period = ? ORDER BY aggregate_uid",
        (period,),
    ).fetchall()
    # 每个汇总来源：可相加明细中属于该来源的终端簇覆盖数
    covered_by_source: dict[str, int] = {}
    for agg in aggs:
        source = agg["source_name"]
        if source not in covered_by_source:
            covered_by_source[source] = conn.execute(
                "SELECT COUNT(DISTINCT c.cluster_uid) AS n "
                "FROM clusters c "
                "JOIN cluster_observations co ON co.cluster_uid = c.cluster_uid "
                "JOIN observations o ON o.observation_uid = co.observation_uid "
                "WHERE c.snapshot_id = ? AND o.source_name = ?",
                (snapshot_id, source),
            ).fetchone()["n"]
        covered = covered_by_source[source]
        gap = max(agg["reported_count"] - covered, 0)
        fact_uid = short_id("fact", snapshot_id, "agg", agg["aggregate_uid"])
        conn.execute(
            "INSERT INTO connection_facts("
            "fact_uid, snapshot_id, cluster_uid, fact_type, status, "
            "reported_count, account_ref, detail_covered, unexplained_gap) "
            "VALUES (?, ?, NULL, 'aggregate_constraint', 'constraint', ?, ?, ?, ?)",
            (fact_uid, snapshot_id, agg["reported_count"], agg["account_ref"],
             covered, gap),
        )
        conn.execute(
            "INSERT INTO fact_evidence("
            "snapshot_id, fact_uid, evidence_uid, basis, detail) "
            "VALUES (?, ?, NULL, 'observed_alone', ?)",
            (snapshot_id, fact_uid,
             "来源仅有汇总数、无逐终端明细，按口径作为覆盖约束，"
             "不参与终端去重相加；差额保留为无法解释缺口"),
        )


def _caliber_aggregate_sources(conn, snapshot_id) -> set[str]:
    row = conn.execute(
        "SELECT cv.rules_json FROM snapshots s "
        "JOIN caliber_versions cv ON cv.caliber_id = s.caliber_id "
        "WHERE s.snapshot_id = ?",
        (snapshot_id,),
    ).fetchone()
    rules = json.loads(row["rules_json"])
    return set(rules.get("aggregate_roles", {}).keys())
