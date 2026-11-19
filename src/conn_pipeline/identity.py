"""终端身份解析与不可变快照构建。

关联规则（硬约束）：
- 只有来源实际提供的强标识共现（co_occurrence）或显式标识映射
  （identifier_mapping，且适用区间覆盖报告期）才能把两个观测并为同一终端；
- 名称/相似字符串不产生合并；证据不足的观测保持独立，状态 undetermined；
- 每个快照自包含，迟到证据只能生成新 revision，旧快照行原样保留。
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field

from .utils import evidence_applies, short_id


class DSU:
    """并查集：节点是观测 UID。"""

    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def add(self, x: str) -> None:
        self.parent.setdefault(x, x)

    def find(self, x: str) -> str:
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != x:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            # 取字典序较小者为根，保证簇结果对输入顺序不敏感
            if ra > rb:
                ra, rb = rb, ra
            self.parent[rb] = ra


@dataclass
class Cluster:
    members: list[str]
    status: str
    status_reason: str
    terminal_uid: str
    device_id: str | None = None
    imsi: str | None = None
    iccid: str | None = None
    network: str | None = None
    evidence_uids: set[str] = field(default_factory=set)
    bases: list[tuple[str, str]] = field(default_factory=list)


def _load_observations(conn: sqlite3.Connection, period: str) -> list[sqlite3.Row]:
    return list(conn.execute(
        "SELECT * FROM observations WHERE period = ? ORDER BY observation_uid",
        (period,),
    ).fetchall())


def _load_applicable_evidence(conn: sqlite3.Connection,
                              period: str) -> list[sqlite3.Row]:
    rows = conn.execute(
        "SELECT * FROM identity_evidence WHERE period = ? ORDER BY evidence_uid",
        (period,),
    ).fetchall()
    return [r for r in rows
            if evidence_applies(r["applies_from"], r["applies_to"], period)]


def _resolve_clusters(conn: sqlite3.Connection, period: str,
                      caliber: dict) -> list[Cluster]:
    observations = _load_observations(conn, period)
    dsu = DSU()
    for obs in observations:
        dsu.add(obs["observation_uid"])

    evidence = _load_applicable_evidence(conn, period)
    evidence_for_obs: dict[str, set[str]] = defaultdict(set)

    # 观测按强标识建索引：
    # 显式映射引用的标识即使没有共现行也可关联（映射本身就是来源证据）；
    # 共现引用的标识用于跨观测同值匹配；两者之外的标识不参与任何合并。
    co_keys = {(ev["id_type"], ev["id_value"]) for ev in evidence
               if ev["evidence_type"] == "co_occurrence"}
    map_keys = {(ev["id_type"], ev["id_value"]) for ev in evidence
                if ev["evidence_type"] == "identifier_mapping"}
    holders_by_key: dict[tuple[str, str], set[str]] = defaultdict(set)
    for obs in observations:
        for id_type in ("device_id", "imsi", "iccid"):
            value = obs[id_type]
            key = (id_type, value)
            if not value:
                continue
            if key in co_keys or key in map_keys:
                holders_by_key[key].add(obs["observation_uid"])

    # 共现证据：同一标识的全部持有者并簇，证据挂载到簇内每个观测
    for ev in evidence:
        if ev["evidence_type"] != "co_occurrence":
            continue
        key = (ev["id_type"], ev["id_value"])
        holders = sorted(holders_by_key.get(key, set()))
        for uid in holders:
            evidence_for_obs[uid].add(ev["evidence_uid"])
        for other in holders[1:]:
            dsu.union(holders[0], other)

    # 显式映射：mapping_group 声明的标识互为同一终端，持有者并簇
    mapping_members: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for ev in evidence:
        if ev["evidence_type"] == "identifier_mapping":
            mapping_members[ev["mapping_group"]].append(
                (ev["id_type"], ev["id_value"])
            )
    for group, id_keys in mapping_members.items():
        holders: set[str] = set()
        for key in id_keys:
            for uid in holders_by_key.get(key, set()):
                holders.add(uid)
                evidence_for_obs[uid].add(
                    _evidence_uid_for(evidence, group, key)
                )
        holders = sorted(holders)
        for other in holders[1:]:
            dsu.union(holders[0], other)

    grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for obs in observations:
        grouped[dsu.find(obs["observation_uid"])].append(obs)

    policy = caliber["identity_policy"]
    iot_rule = policy.get("source_specific", {}).get("iot_registry", {})
    iccid_only_status = iot_rule.get("iccid_only_status", "undetermined")
    clusters: list[Cluster] = []
    for root in sorted(grouped):
        members_rows = sorted(grouped[root], key=lambda o: o["observation_uid"])
        member_uids = [o["observation_uid"] for o in members_rows]
        sources = {o["source_name"] for o in members_rows}
        has_mapping = any(
            _obs_uses_mapping(evidence_for_obs.get(o["observation_uid"], set()),
                              evidence)
            for o in members_rows
        )
        ev_uids: set[str] = set()
        for o in members_rows:
            ev_uids |= evidence_for_obs.get(o["observation_uid"], set())

        device_ids = {o["device_id"] for o in members_rows if o["device_id"]}
        imsis = {o["imsi"] for o in members_rows if o["imsi"]}
        iccids = {o["iccid"] for o in members_rows if o["iccid"]}
        networks = {o["network"] for o in members_rows if o["network"]}

        # 状态判定：跨来源/多观测经证据关联 => 已确认；
        # 单观测仅在口径明确认可其标识时确认，否则保留未判定
        cross_linked = len(sources) > 1 or len(member_uids) > 1
        only_iccid_iot = (
            len(member_uids) == 1
            and members_rows[0]["source_name"] == "iot_registry"
            and not device_ids and not imsis and bool(iccids)
        )
        if cross_linked:
            status = "confirmed"
            reason = "explicit_identifier_evidence" if has_mapping \
                else "shared_strong_identifier"
        elif only_iccid_iot and iccid_only_status == "confirmed_distinct":
            status = "confirmed"
            reason = "caliber_recognized_iccid"
        else:
            status = "undetermined"
            if only_iccid_iot:
                reason = "iccid_only_no_terminal_link"
            elif not (device_ids or imsis or iccids):
                reason = "no_strong_identifier"
            else:
                reason = "single_observation_no_corroborating_evidence"

        bases: list[tuple[str, str]] = []
        if ev_uids:
            if has_mapping:
                bases.append(("explicit_mapping", "显式标识映射"))
            if any(_is_co(uid, evidence) for uid in ev_uids):
                bases.append(("shared_identifier", "强标识共现/共享"))
        else:
            bases.append(("observed_alone", "仅单来源单观测，无关联证据"))

        clusters.append(Cluster(
            members=member_uids,
            status=status,
            status_reason=reason,
            terminal_uid=short_id("term", period, *member_uids),
            device_id=sorted(device_ids)[0] if device_ids else None,
            imsi=sorted(imsis)[0] if imsis else None,
            iccid=sorted(iccids)[0] if iccids else None,
            network=sorted(networks)[0] if networks else None,
            evidence_uids=ev_uids,
            bases=bases,
        ))
    return clusters


def _evidence_uid_for(evidence, group, key):
    for ev in evidence:
        if (ev["mapping_group"] == group
                and (ev["id_type"], ev["id_value"]) == key):
            return ev["evidence_uid"]
    raise RuntimeError("内部错误：证据查找失败")


def _obs_uses_mapping(ev_uids: set[str], evidence: list[sqlite3.Row]) -> bool:
    types = {ev["evidence_type"] for ev in evidence
             if ev["evidence_uid"] in ev_uids}
    return "identifier_mapping" in types


def _is_co(evidence_uid: str, evidence: list[sqlite3.Row]) -> bool:
    return any(ev["evidence_uid"] == evidence_uid
               and ev["evidence_type"] == "co_occurrence" for ev in evidence)
