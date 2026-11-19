"""报告期报表、来源覆盖、下钻与修订前后对比。"""
from __future__ import annotations

import json
import re
import sqlite3
from difflib import SequenceMatcher

def list_snapshots(conn: sqlite3.Connection, period: str | None = None) -> list[dict]:
    sql = (
        "SELECT s.snapshot_id, s.period, s.revision_no, s.caliber_id, "
        "s.created_at, s.input_fingerprint, s.note, cv.version_no AS caliber_version "
        "FROM snapshots s JOIN caliber_versions cv ON cv.caliber_id = s.caliber_id"
    )
    params: tuple = ()
    if period:
        sql += " WHERE s.period = ?"
        params = (period,)
    sql += " ORDER BY s.period, s.revision_no"
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def latest_snapshot_id(conn: sqlite3.Connection, period: str) -> str | None:
    row = conn.execute(
        "SELECT snapshot_id FROM snapshots WHERE period = ? "
        "ORDER BY revision_no DESC LIMIT 1",
        (period,),
    ).fetchone()
    return row["snapshot_id"] if row else None


def _caliber_rules(conn: sqlite3.Connection, snapshot_id: str) -> dict:
    row = conn.execute(
        "SELECT cv.rules_json FROM snapshots s "
        "JOIN caliber_versions cv ON cv.caliber_id = s.caliber_id "
        "WHERE s.snapshot_id = ?",
        (snapshot_id,),
    ).fetchone()
    return json.loads(row["rules_json"])


def source_coverage(conn: sqlite3.Connection, period: str) -> list[dict]:
    """来源覆盖：每个来源在该期的摄入批次、明细行数、汇总行数、证据条数。"""
    out = []
    for row in conn.execute(
        "SELECT sf.source_name, sf.description FROM source_files sf "
        "ORDER BY sf.source_name"
    ).fetchall():
        source = row["source_name"]
        detail = conn.execute(
            "SELECT COUNT(*) n, COUNT(DISTINCT batch_id) b "
            "FROM observations WHERE period = ? AND source_name = ?",
            (period, source),
        ).fetchone()
        agg = conn.execute(
            "SELECT COUNT(*) n, COALESCE(SUM(reported_count), 0) total "
            "FROM aggregate_observations WHERE period = ? AND source_name = ?",
            (period, source),
        ).fetchone()
        ev = conn.execute(
            "SELECT COUNT(*) n, "
            "SUM(CASE WHEN evidence_type = 'co_occurrence' THEN 1 ELSE 0 END) co, "
            "SUM(CASE WHEN evidence_type = 'identifier_mapping' THEN 1 ELSE 0 END) mp "
            "FROM identity_evidence WHERE period = ? AND source_name = ?",
            (period, source),
        ).fetchone()
        out.append({
            "source": source,
            "detail_rows": detail["n"],
            "batches": detail["b"],
            "aggregate_rows": agg["n"],
            "aggregate_reported_total": agg["total"],
            "evidence_rows": ev["n"] or 0,
            "co_occurrence_evidence": ev["co"] or 0,
            "mapping_evidence": ev["mp"] or 0,
        })
    return out


def build_report(conn: sqlite3.Connection, snapshot_id: str) -> dict:
    snap = conn.execute(
        "SELECT * FROM snapshots WHERE snapshot_id = ?", (snapshot_id,)
    ).fetchone()
    if snap is None:
        raise KeyError(f"快照不存在：{snapshot_id}")
    period = snap["period"]
    rules = _caliber_rules(conn, snapshot_id)
    facts = conn.execute(
        "SELECT * FROM connection_facts WHERE snapshot_id = ? "
        "ORDER BY fact_type DESC, fact_uid",
        (snapshot_id,),
    ).fetchall()
    terminal_facts = [f for f in facts if f["fact_type"] == "terminal"]
    agg_facts = [f for f in facts if f["fact_type"] == "aggregate_constraint"]
    confirmed = [f for f in terminal_facts if f["status"] == "confirmed"]
    undetermined = [f for f in terminal_facts if f["status"] == "undetermined"]

    additive_sources = sorted(
        s for s, cfg in rules["source_roles"].items()
        if cfg["role"] == "additive_population"
    )
    cross_sources = sorted(
        s for s, cfg in rules["source_roles"].items()
        if cfg["role"] == "cross_observation"
    )
    constraint_sources = sorted(rules.get("aggregate_roles", {}).keys())

    total_gap = sum(f["unexplained_gap"] or 0 for f in agg_facts)
    return {
        "snapshot": dict(snap),
        "caliber": {
            "version": json.loads(
                conn.execute(
                    "SELECT rules_json FROM caliber_versions WHERE caliber_id = ?",
                    (snap["caliber_id"],),
                ).fetchone()["rules_json"]
            )["version"],
            "additive_population_sources": additive_sources,
            "cross_observation_sources": cross_sources,
            "constraint_sources": constraint_sources,
        },
        "coverage": source_coverage(conn, period),
        "counts": {
            "confirmed_terminals": len(confirmed),
            "undetermined_terminals": len(undetermined),
            "distinct_observed_entities": len(terminal_facts),
            "aggregate_constraints": len(agg_facts),
            "unexplained_aggregate_gap": total_gap,
        },
        "terminal_facts": [dict(f) for f in terminal_facts],
        "aggregate_facts": [dict(f) for f in agg_facts],
        "name_similarity_diagnostics": _name_similarity(conn, snapshot_id),
    }


def _name_key(value: str | None) -> str:
    return re.sub(r"[^a-z0-9一-鿿]", "", (value or "").lower())


def _common_prefix_len(a: str, b: str) -> int:
    n = 0
    for x, y in zip(a, b):
        if x != y:
            break
        n += 1
    return n


def _name_similarity(conn: sqlite3.Connection, snapshot_id: str) -> list[dict]:
    """名称相似但未合并的终端：仅作诊断提示，绝不据此合并。

    判定刻意保守：整体相似度 >= 0.9、名称长度差 <= 2 且共同前缀覆盖
    较短名称的 80%，避免把 IMEI-1001/IMEI-1002 这类同前缀序列号误报。
    """
    rows = conn.execute(
        "SELECT f.fact_uid, f.device_id, f.status, c.status_reason "
        "FROM connection_facts f "
        "JOIN clusters c ON c.cluster_uid = f.cluster_uid "
        "WHERE f.snapshot_id = ? AND f.fact_type = 'terminal' "
        "AND f.device_id IS NOT NULL",
        (snapshot_id,),
    ).fetchall()
    out = []
    for i, a in enumerate(rows):
        for b in rows[i + 1:]:
            ka, kb = _name_key(a["device_id"]), _name_key(b["device_id"])
            if not ka or not kb or ka == kb:
                continue
            score = SequenceMatcher(None, ka, kb).ratio()
            prefix = _common_prefix_len(ka, kb)
            length_close = abs(len(ka) - len(kb)) <= 2
            prefix_cover = prefix >= 0.8 * min(len(ka), len(kb))
            if score >= 0.9 and length_close and prefix_cover:
                out.append({
                    "left_fact": a["fact_uid"], "left_device_id": a["device_id"],
                    "right_fact": b["fact_uid"], "right_device_id": b["device_id"],
                    "similarity": round(score, 3),
                    "action": "kept_separate",
                    "reason": "无来源提供的显式关联证据，名称相似不得合并",
                })
    return out


def drill_fact(conn: sqlite3.Connection, snapshot_id: str,
               fact_uid: str) -> dict:
    """终端事实下钻：事实 -> 簇 -> 观测 -> 原始行 -> 证据。"""
    fact = conn.execute(
        "SELECT * FROM connection_facts WHERE snapshot_id = ? AND fact_uid = ?",
        (snapshot_id, fact_uid),
    ).fetchone()
    if fact is None:
        raise KeyError(f"事实不存在：{fact_uid}")
    fact = dict(fact)
    if fact["fact_type"] == "aggregate_constraint":
        return {"fact": fact, "drill_note": "汇总约束无逐终端明细，仅可追溯到汇总原始行"}

    cluster = conn.execute(
        "SELECT * FROM clusters WHERE snapshot_id = ? AND cluster_uid = ?",
        (snapshot_id, fact["cluster_uid"]),
    ).fetchone()
    observations = [
        dict(r) for r in conn.execute(
            "SELECT o.*, co.cluster_uid FROM observations o "
            "JOIN cluster_observations co "
            "ON co.observation_uid = o.observation_uid AND co.snapshot_id = ? "
            "WHERE co.cluster_uid = ? ORDER BY o.observation_uid",
            (snapshot_id, fact["cluster_uid"]),
        ).fetchall()
    ]
    for obs in observations:
        obs["raw_record"] = dict(conn.execute(
            "SELECT * FROM raw_records WHERE raw_record_id = ?",
            (obs["raw_record_id"],),
        ).fetchone())
    evidence = [
        dict(r) for r in conn.execute(
            "SELECT ie.*, fe.basis, fe.detail FROM fact_evidence fe "
            "LEFT JOIN identity_evidence ie ON ie.evidence_uid = fe.evidence_uid "
            "WHERE fe.snapshot_id = ? AND fe.fact_uid = ?",
            (snapshot_id, fact_uid),
        ).fetchall()
    ]
    return {"fact": fact, "cluster": dict(cluster),
            "observations": observations, "evidence": evidence}


def drill_aggregate(conn: sqlite3.Connection, snapshot_id: str,
                    fact_uid: str) -> dict:
    """汇总事实下钻：汇总原始行 + 可相加明细覆盖到的终端簇清单。"""
    fact = conn.execute(
        "SELECT * FROM connection_facts WHERE snapshot_id = ? AND fact_uid = ?",
        (snapshot_id, fact_uid),
    ).fetchone()
    if fact is None or fact["fact_type"] != "aggregate_constraint":
        raise KeyError(f"汇总事实不存在：{fact_uid}")
    period = conn.execute(
        "SELECT period FROM snapshots WHERE snapshot_id = ?", (snapshot_id,)
    ).fetchone()["period"]
    # 同一快照内 reported_count/account_ref 唯一对应一条汇总观测
    agg_rows = [
        dict(r) for r in conn.execute(
            "SELECT ao.*, rr.raw_payload FROM aggregate_observations ao "
            "JOIN raw_records rr ON rr.raw_record_id = ao.raw_record_id "
            "WHERE ao.period = ? AND ao.reported_count = ? "
            "AND COALESCE(ao.account_ref, '') = COALESCE(?, '')",
            (period, fact["reported_count"], fact["account_ref"]),
        ).fetchall()
    ]
    constraint_sources = list(
        _caliber_rules(conn, snapshot_id).get("aggregate_roles", {}).keys()
    )
    placeholders = ",".join("?" for _ in constraint_sources)
    # 按终端簇去重：一个簇即使含多条同来源观测也只算一个被覆盖终端
    covered = [
        dict(r) for r in conn.execute(
            f"SELECT c.terminal_uid, c.status, c.status_reason, "
            f"f.fact_uid, c.cluster_uid, "
            f"GROUP_CONCAT(DISTINCT o.source_name) AS source_names, "
            f"MIN(o.device_id) AS device_id, MIN(o.imsi) AS imsi, "
            f"MIN(o.iccid) AS iccid "
            f"FROM clusters c "
            f"JOIN cluster_observations co ON co.cluster_uid = c.cluster_uid "
            f"JOIN observations o ON o.observation_uid = co.observation_uid "
            f"JOIN connection_facts f ON f.cluster_uid = c.cluster_uid "
            f"AND f.snapshot_id = c.snapshot_id "
            f"WHERE c.snapshot_id = ? AND o.source_name IN ({placeholders}) "
            f"GROUP BY c.cluster_uid ORDER BY c.terminal_uid",
            (snapshot_id, *constraint_sources),
        ).fetchall()
    ]
    return {"fact": dict(fact),
            "aggregate_observations": agg_rows,
            "covered_detail_terminals": covered,
            "covered_detail_count": len(covered)}


def compare_snapshots(conn: sqlite3.Connection,
                      left_snapshot_id: str, right_snapshot_id: str) -> dict:
    """比较两个快照：终端级差异 + 差异原因（事实变化 vs 口径变化）。"""
    left = conn.execute(
        "SELECT * FROM snapshots WHERE snapshot_id = ?", (left_snapshot_id,)
    ).fetchone()
    right = conn.execute(
        "SELECT * FROM snapshots WHERE snapshot_id = ?", (right_snapshot_id,)
    ).fetchone()
    if left is None or right is None:
        raise KeyError("快照不存在，无法比较")
    if left["period"] != right["period"]:
        raise ValueError("只能比较同一报告期的两个修订快照")

    fact_change = left["input_fingerprint"] != right["input_fingerprint"]
    caliber_change = left["caliber_id"] != right["caliber_id"]
    l_facts = {r["terminal_uid"]: dict(r) for r in conn.execute(
        "SELECT f.*, c.terminal_uid FROM connection_facts f "
        "JOIN clusters c ON c.cluster_uid = f.cluster_uid "
        "WHERE f.snapshot_id = ? AND f.fact_type = 'terminal'",
        (left_snapshot_id,)).fetchall()}
    r_facts = {r["terminal_uid"]: dict(r) for r in conn.execute(
        "SELECT f.*, c.terminal_uid FROM connection_facts f "
        "JOIN clusters c ON c.cluster_uid = f.cluster_uid "
        "WHERE f.snapshot_id = ? AND f.fact_type = 'terminal'",
        (right_snapshot_id,)).fetchall()}

    added, removed, status_changed = [], [], []
    for uid, fact in r_facts.items():
        if uid not in l_facts:
            added.append({"terminal_uid": uid, **_fact_brief(fact),
                          "why": _why(fact_change, caliber_change)})
        elif l_facts[uid]["status"] != fact["status"]:
            status_changed.append({
                "terminal_uid": uid,
                "from_status": l_facts[uid]["status"],
                "to_status": fact["status"],
                "from_reason": l_facts[uid]["status_reason"] if "status_reason"
                in l_facts[uid] else None,
                "to_reason": _cluster_reason(conn, right_snapshot_id, uid),
                "why": _why(fact_change, caliber_change),
            })
    for uid, fact in l_facts.items():
        if uid not in r_facts:
            removed.append({"terminal_uid": uid, **_fact_brief(fact),
                            "why": _why(fact_change, caliber_change)})

    def counts(sid):
        rows = conn.execute(
            "SELECT status, COUNT(*) n FROM connection_facts "
            "WHERE snapshot_id = ? AND fact_type = 'terminal' GROUP BY status",
            (sid,),
        ).fetchall()
        return {r["status"]: r["n"] for r in rows}

    return {
        "period": left["period"],
        "left": {"snapshot_id": left_snapshot_id,
                 "revision_no": left["revision_no"],
                 "caliber_id": left["caliber_id"]},
        "right": {"snapshot_id": right_snapshot_id,
                  "revision_no": right["revision_no"],
                  "caliber_id": right["caliber_id"]},
        "difference_drivers": {
            "raw_fact_change": fact_change,
            "caliber_change": caliber_change,
            "explanation": _driver_text(fact_change, caliber_change),
        },
        "counts_left": counts(left_snapshot_id),
        "counts_right": counts(right_snapshot_id),
        "terminals_added": added,
        "terminals_removed": removed,
        "status_changed": status_changed,
    }


def _why(fact_change: bool, caliber_change: bool) -> str:
    if fact_change and caliber_change:
        return "原始事实与口径同时变化"
    if fact_change:
        return "原始事实变化（迟到证据/补录，历史快照未改动）"
    if caliber_change:
        return "口径版本变化（原始事实未改动）"
    return "无输入差异"


def _driver_text(fact_change: bool, caliber_change: bool) -> str:
    if fact_change and not caliber_change:
        return ("两快照口径相同，差异来自来源证据变化；旧快照保持原样，"
                "本结果为追加的新修订。")
    if caliber_change and not fact_change:
        return "两快照原始输入完全相同，差异完全来自汇总口径版本变化。"
    if fact_change and caliber_change:
        return "来源证据与口径均发生变化，逐项差异已分别标注。"
    return "两快照输入与口径均相同。"


def _fact_brief(fact: dict) -> dict:
    return {k: fact.get(k) for k in
            ("status", "device_id", "imsi", "iccid", "network")}


def _cluster_reason(conn, snapshot_id, terminal_uid) -> str | None:
    row = conn.execute(
        "SELECT status_reason FROM clusters WHERE snapshot_id = ? "
        "AND terminal_uid = ?",
        (snapshot_id, terminal_uid),
    ).fetchone()
    return row["status_reason"] if row else None
