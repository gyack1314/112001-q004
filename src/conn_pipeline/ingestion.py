"""摄入与规范化（landing + staging）。

- 来源文件按字节哈希原样登记，逐行 JSON 原样存入 raw_records（追加写，重复摄入幂等）；
- device 行规范化为 observations，account_total 行进入 aggregate_observations；
- 同一行内同时出现的强标识生成 co_occurrence 证据，identity_mapping 文件生成
  explicit identifier_mapping 证据；证据带适用区间，后续只在适用期内使用；
- 名称相似性不产生任何证据，也不产生合并。
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .config import ManifestFile, PeriodManifest, load_manifest, map_field
from .utils import now_iso, sha256_bytes, sha256_text, short_id

STRONG_ID_TYPES = ("device_id", "imsi", "iccid")
DEVICE_KNOWN_FIELDS = {"device_id", "imsi", "iccid", "network"}


def _insert_source_file(conn: sqlite3.Connection, source: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO source_files(source_name, registered_at, description) "
        "VALUES (?, ?, ?)",
        (source, now_iso(), ""),
    )


def _load_file_bytes(manifest_dir: Path, mf: ManifestFile) -> bytes:
    path = manifest_dir / (mf.path or mf.file)
    if not path.is_absolute():
        path = manifest_dir / path
    if not path.is_file():
        raise FileNotFoundError(f"清单声明的来源文件不存在：{path}")
    return path.read_bytes()


def _add_co_occurrence(conn, *, period, source, batch_id, raw_record_id,
                       identifiers: dict[str, str]) -> None:
    """同一来源行内同时观测到的强标识 => 共现证据（该期适用）。"""
    group = short_id("grp", "co", raw_record_id)
    for id_type, id_value in identifiers.items():
        uid = short_id("ev", period, source, raw_record_id, group, id_type, id_value)
        conn.execute(
            "INSERT OR IGNORE INTO identity_evidence("
            "evidence_uid, period, source_name, batch_id, raw_record_id, "
            "evidence_type, mapping_group, id_type, id_value, applies_from, applies_to) "
            "VALUES (?, ?, ?, ?, ?, 'co_occurrence', ?, ?, ?, NULL, NULL)",
            (uid, period, source, batch_id, raw_record_id,
             group, id_type, id_value),
        )


def _add_mapping_evidence(conn, *, period, source, batch_id, raw_record_id,
                          row: dict, mf: ManifestFile) -> None:
    """显式标识映射行 => 成对 identifier_mapping 证据，带适用区间。"""
    group = map_field(row, mf.field_map, "mapping_id") or short_id(
        "grp", "map", raw_record_id
    )
    applies_from = map_field(row, mf.field_map, "applies_from")
    applies_to = map_field(row, mf.field_map, "applies_to")
    pairs = [
        (map_field(row, mf.field_map, "id_type"),
         map_field(row, mf.field_map, "id_value")),
        (map_field(row, mf.field_map, "pair_type"),
         map_field(row, mf.field_map, "pair_value")),
    ]
    for id_type, id_value in pairs:
        if not id_type or not id_value:
            raise ValueError(f"{mf.file} 行 {row!r} 映射证据缺少 id_type/id_value")
        if id_type not in STRONG_ID_TYPES:
            raise ValueError(f"不支持的标识类型：{id_type}")
        uid = short_id("ev", period, source, raw_record_id, group, id_type, id_value)
        conn.execute(
            "INSERT OR IGNORE INTO identity_evidence("
            "evidence_uid, period, source_name, batch_id, raw_record_id, "
            "evidence_type, mapping_group, id_type, id_value, applies_from, applies_to) "
            "VALUES (?, ?, ?, ?, ?, 'identifier_mapping', ?, ?, ?, ?, ?)",
            (uid, period, source, batch_id, raw_record_id,
             group, id_type, id_value, applies_from, applies_to),
        )


def ingest_file(conn: sqlite3.Connection, manifest: PeriodManifest,
                mf: ManifestFile, manifest_dir: Path) -> dict:
    """摄入清单中的单个来源文件，返回 {status, batch_id, rows}。"""
    payload = _load_file_bytes(manifest_dir, mf)
    file_hash = sha256_bytes(payload)
    existing = conn.execute(
        "SELECT batch_id, row_count FROM ingest_batches "
        "WHERE file_sha256 = ? AND file_name = ?",
        (file_hash, mf.file),
    ).fetchone()
    if existing:
        return {"status": "skipped_duplicate", "batch_id": existing["batch_id"],
                "rows": existing["row_count"], "file": mf.file}

    rows = json.loads(payload.decode("utf-8"))
    if not isinstance(rows, list):
        raise ValueError(f"来源文件必须是 JSON 数组：{mf.file}")

    batch_id = short_id("bat", manifest.period, mf.source, mf.file, file_hash)
    conn.execute(
        "INSERT INTO ingest_batches("
        "batch_id, period, source_name, file_name, file_sha256, byte_size, "
        "row_count, ingested_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (batch_id, manifest.period, mf.source, mf.file, file_hash,
         len(payload), len(rows), now_iso()),
    )
    _insert_source_file(conn, mf.source)

    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"{mf.file} 第 {index} 行不是 JSON 对象")
        row_sha = sha256_text(json.dumps(row, ensure_ascii=False, sort_keys=True))
        raw_id = short_id("raw", batch_id, index, row_sha)
        conn.execute(
            "INSERT INTO raw_records("
            "raw_record_id, batch_id, source_name, period, row_index, "
            "row_sha256, raw_payload) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (raw_id, batch_id, mf.source, manifest.period, index,
             row_sha, json.dumps(row, ensure_ascii=False)),
        )

        if mf.grain == "device":
            identifiers = {
                key: map_field(row, mf.field_map, key)
                for key in STRONG_ID_TYPES
            }
            identifiers = {k: v for k, v in identifiers.items() if v}
            network = map_field(row, mf.field_map, "network")
            mapped_fields = set(mf.field_map.values())
            extras = {
                k: v for k, v in row.items()
                if k not in DEVICE_KNOWN_FIELDS and k not in mapped_fields
            }
            obs_uid = short_id("obs", manifest.period, mf.source, row_sha)
            conn.execute(
                "INSERT OR IGNORE INTO observations("
                "observation_uid, period, source_name, batch_id, raw_record_id, "
                "device_id, imsi, iccid, network, other_attrs) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (obs_uid, manifest.period, mf.source, batch_id, raw_id,
                 identifiers.get("device_id"), identifiers.get("imsi"),
                 identifiers.get("iccid"), network,
                 json.dumps(extras, ensure_ascii=False)),
            )
            if len(identifiers) >= 2:
                _add_co_occurrence(
                    conn, period=manifest.period, source=mf.source,
                    batch_id=batch_id, raw_record_id=raw_id,
                    identifiers=identifiers,
                )

        elif mf.grain == "account_total":
            count = row.get(mf.field_map.get("count", "count"))
            if not isinstance(count, int) or count < 0:
                raise ValueError(f"{mf.file} 第 {index} 行 count 必须是非负整数")
            account_ref = map_field(row, mf.field_map, "account_ref")
            agg_uid = short_id("agg", raw_id)
            conn.execute(
                "INSERT OR IGNORE INTO aggregate_observations("
                "aggregate_uid, period, source_name, batch_id, raw_record_id, "
                "reported_count, account_ref) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (agg_uid, manifest.period, mf.source, batch_id, raw_id,
                 count, account_ref),
            )

        elif mf.grain == "identity_mapping":
            _add_mapping_evidence(
                conn, period=manifest.period, source=mf.source,
                batch_id=batch_id, raw_record_id=raw_id, row=row, mf=mf,
            )

    return {"status": "ingested", "batch_id": batch_id,
            "rows": len(rows), "file": mf.file}


def ingest_manifest(conn: sqlite3.Connection, manifest_path: str | Path) -> dict:
    """按期清单摄入全部来源文件，返回逐文件结果与期号。"""
    path = Path(manifest_path).resolve()
    manifest = load_manifest(path)
    manifest_dir = path.parent
    try:
        results = [
            ingest_file(conn, manifest, mf, manifest_dir) for mf in manifest.files
        ]
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {
        "period": manifest.period,
        "manifest": str(path),
        "files": results,
        "new_batches": sum(1 for r in results if r["status"] == "ingested"),
        "skipped": sum(1 for r in results if r["status"] == "skipped_duplicate"),
    }
